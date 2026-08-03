//! The one owner of *mutations* to the function-library registry.
//!
//! `FUNCTION LOAD`/`DELETE`/`FLUSH`/`RESTORE` have two callers, not one:
//!
//! - a client connection (`connection::scripting::function`), which parses the
//!   command, mutates, persists, replies, **and propagates to replicas**;
//! - the replica apply loop, which receives that propagated frame and must
//!   reach exactly the same registry state without a connection, without a
//!   reply, and without re-propagating.
//!
//! Both go through this type, so the two can not drift: a subcommand the
//! connection accepts is a subcommand the replica applies, with the same
//! validation and the same on-disk side effect. Before this existed the
//! mutation bodies lived only on `ConnectionHandler` and the replica had no
//! way in at all, which is why libraries never replicated (issue 48).
//!
//! Everything here is synchronous: the registry is an `RwLock` and persistence
//! is a single `write` of a small file, so there is nothing to await.

use std::path::PathBuf;

use bytes::Bytes;
use frogdb_core::{RwLockExt, SharedFunctionRegistry};
use frogdb_protocol::Response;
use tracing::warn;

use crate::runtime_config::ConfigManager;

use frogdb_core::sync::Arc;

/// Mutating access to the process-wide function registry.
///
/// Cheap to construct (two `Arc` clones) — connections build one per command
/// rather than holding one, so there is no extra field on `ConnectionHandler`.
#[derive(Clone)]
pub struct FunctionStore {
    registry: SharedFunctionRegistry,
    config: Arc<ConfigManager>,
}

/// The `FUNCTION` subcommands that change registry state and therefore
/// replicate. `LIST`/`STATS`/`DUMP`/`HELP` are reads and `KILL` is a per-shard
/// control message, so none of them appear here.
pub const MUTATING_SUBCOMMANDS: [&str; 4] = ["LOAD", "DELETE", "FLUSH", "RESTORE"];

/// Orders a registry mutation against the frame that announces it, and both
/// against the whole-registry snapshot a full resync ships.
///
/// Without it the two can invert: a `FUNCTION LOAD` may mutate the registry,
/// then a concurrent full sync may read the (already updated) registry into its
/// snapshot and broadcast it *before* the `LOAD`'s own frame reaches the
/// stream — leaving the replica applying `LOAD` after a snapshot that already
/// contained the library, or worse, a snapshot taken *before* the mutation
/// landing *after* the mutation's frame and silently dropping it. Registry state
/// is process-wide, so the lock is too; it is held only across an in-memory
/// mutation plus a channel send.
static PROPAGATION_ORDER: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Acquire the propagation-ordering lock (see [`PROPAGATION_ORDER`]).
///
/// Poisoning is ignored deliberately: the guarded data is `()`, so a panic
/// elsewhere leaves nothing inconsistent to protect, and refusing to propagate
/// afterwards would be a worse failure than proceeding.
pub fn propagation_order() -> std::sync::MutexGuard<'static, ()> {
    PROPAGATION_ORDER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Whether a `FUNCTION` argument vector names a mutating subcommand — the test
/// both the propagation site and the replica applier use, so "what replicates"
/// has one definition.
pub fn is_mutating_function_command(args: &[Bytes]) -> bool {
    args.first().is_some_and(|sub| {
        let sub = sub.to_ascii_uppercase();
        MUTATING_SUBCOMMANDS
            .iter()
            .any(|m| m.as_bytes() == sub.as_slice())
    })
}

impl FunctionStore {
    pub fn new(registry: SharedFunctionRegistry, config: Arc<ConfigManager>) -> Self {
        Self { registry, config }
    }

    /// FUNCTION LOAD [REPLACE] code — returns the loaded library's name.
    pub fn load(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|load' command");
        }

        let (replace, code) = if args.len() == 1 {
            (false, &args[0])
        } else if args.len() == 2 && args[0].to_ascii_uppercase() == b"REPLACE".as_slice() {
            (true, &args[1])
        } else {
            return Response::error("ERR Unknown option given");
        };

        let code_str = match std::str::from_utf8(code) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR library code must be valid UTF-8"),
        };

        let library = match frogdb_core::load_library(code_str) {
            Ok(lib) => lib,
            Err(e) => return Response::error(e.to_string()),
        };

        let library_name = library.name.clone();

        {
            let mut registry = match self.registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            match registry.load_library(library, replace) {
                Ok(_) => {}
                Err(frogdb_core::FunctionError::LibraryAlreadyExists { name }) => {
                    return Response::error(format!("ERR Library '{}' already exists", name));
                }
                Err(e) => return Response::error(e.to_string()),
            }
        }

        self.persist();
        Response::bulk(Bytes::from(library_name))
    }

    /// FUNCTION DELETE library-name.
    pub fn delete(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|delete' command");
        }

        let library_name = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR library name must be valid UTF-8"),
        };

        {
            let mut registry = match self.registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            if let Err(e) = registry.delete_library(library_name) {
                return Response::error(e.to_string());
            }
        }

        self.persist();
        Response::ok()
    }

    /// FUNCTION FLUSH [ASYNC|SYNC]. The mode is parsed for validation and then
    /// ignored: the registry is in memory, so there is no asynchronous variant
    /// to take.
    pub fn flush(&self, args: &[Bytes]) -> Response {
        if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            if mode.as_slice() != b"ASYNC" && mode.as_slice() != b"SYNC" {
                return Response::error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option");
            }
            if args.len() > 1 {
                return Response::error(
                    "ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.",
                );
            }
        }

        {
            let mut registry = match self.registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            registry.flush();
        }

        self.persist();
        Response::ok()
    }

    /// FUNCTION RESTORE payload [APPEND|REPLACE|FLUSH].
    pub fn restore(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|restore' command");
        }

        let payload = &args[0];

        if args.len() > 2 {
            return Response::error(
                "ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.",
            );
        }

        let policy = if args.len() > 1 {
            match frogdb_core::RestorePolicy::from_str(&String::from_utf8_lossy(&args[1])) {
                Ok(p) => p,
                Err(e) => return Response::error(e.to_string()),
            }
        } else {
            frogdb_core::RestorePolicy::Append
        };

        let libraries = match frogdb_core::restore_libraries(payload) {
            Ok(libs) => libs,
            Err(e) => return Response::error(e.to_string()),
        };

        {
            let mut registry = match self.registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };

            if policy == frogdb_core::RestorePolicy::Flush {
                registry.flush();
            }

            let replace = policy == frogdb_core::RestorePolicy::Replace;

            for (name, code) in libraries {
                let library = match frogdb_core::load_library(&code) {
                    Ok(lib) => lib,
                    Err(e) => {
                        return Response::error(format!(
                            "ERR Failed to load library '{}': {}",
                            name, e
                        ));
                    }
                };

                if let Err(e) = registry.load_library(library, replace) {
                    return Response::error(format!(
                        "ERR Failed to restore library '{}': {}",
                        name, e
                    ));
                }
            }
        }

        self.persist();
        Response::ok()
    }

    /// The whole registry as one self-contained command:
    /// `FUNCTION RESTORE <dump> FLUSH`.
    ///
    /// This is what a full resync ships. `FLUSH` (not `APPEND`/`REPLACE`) is the
    /// policy because the receiving node's registry is *not* part of the
    /// checkpoint: a replica that booted with its own `functions.fdb`, or a
    /// demoted primary adopting a new master, must end up with the primary's
    /// libraries and *only* those. `restore` performs the flush and the loads
    /// under a single write lock, so no replica ever serves an FCALL against a
    /// half-installed registry.
    ///
    /// Returns `None` only when the registry lock could not be taken, in which
    /// case the caller ships nothing rather than a snapshot it cannot vouch for.
    ///
    /// Deliberately a *free function over the registry alone*, not a method on
    /// [`FunctionStore`]. Its only caller is the full-resync hook, which the
    /// primary replication handler owns for the life of the process; a
    /// `FunctionStore` there would park an `Arc<ConfigManager>` inside that
    /// handler, and the config manager transitively owns the snapshot
    /// coordinator and the shard notifier - i.e. the storage engine. That edge
    /// closes a reference cycle which keeps RocksDB open past `shutdown()`, so
    /// the next open in the same process fails with `LOCK: No locks available`
    /// (it broke every restart test in the suite). Snapshotting needs no
    /// config, so it takes none.
    pub fn snapshot_command_args(registry: &SharedFunctionRegistry) -> Option<Vec<Bytes>> {
        let registry = registry.try_read_err().ok()?;
        let dump = frogdb_core::dump_libraries(&registry);
        Some(vec![
            Bytes::from_static(b"RESTORE"),
            Bytes::from(dump),
            Bytes::from_static(b"FLUSH"),
        ])
    }

    /// Apply a `FUNCTION` command that arrived on the replication stream.
    ///
    /// `args` is the command's arguments including the subcommand, exactly as
    /// the primary propagated them. Returns `Err` with the primary-visible
    /// error text when the mutation did not apply, which the caller surfaces as
    /// a replication divergence — a replica that silently failed to load a
    /// library would answer `FCALL` with "function not found" while claiming to
    /// be in sync.
    ///
    /// A non-mutating subcommand is a no-op rather than an error: only the four
    /// in [`MUTATING_SUBCOMMANDS`] are ever propagated, so anything else is a
    /// newer primary speaking a dialect this build does not have, and the
    /// established rule for that is "step over it" (see the parse-failure arm of
    /// the replica consume loop).
    pub fn apply_replicated(&self, args: &[Bytes]) -> Result<(), String> {
        let Some(sub) = args.first() else {
            return Err("FUNCTION with no subcommand".to_string());
        };
        let sub_upper = sub.to_ascii_uppercase();
        let rest = &args[1..];

        let response = match sub_upper.as_slice() {
            b"LOAD" => self.load(rest),
            b"DELETE" => self.delete(rest),
            b"FLUSH" => self.flush(rest),
            b"RESTORE" => self.restore(rest),
            other => {
                warn!(
                    subcommand = %String::from_utf8_lossy(other),
                    "Ignoring a replicated FUNCTION subcommand this build does not mutate on",
                );
                return Ok(());
            }
        };

        match response {
            Response::Error(e) | Response::BlobError(e) => {
                Err(String::from_utf8_lossy(&e).into_owned())
            }
            _ => Ok(()),
        }
    }

    /// Persist the registry to `<data_dir>/functions.fdb`, if persistence is on.
    ///
    /// With persistence disabled this is a no-op and the libraries live only in
    /// memory — the same contract the keyspace has in that configuration. A
    /// replica keeps its libraries across a *link* drop (they are in RAM) but
    /// not across a restart, and re-acquires them from the primary's next
    /// propagation or full sync.
    fn persist(&self) {
        if !self.config.persistence_enabled() {
            return;
        }

        let path = PathBuf::from(self.config.data_dir()).join("functions.fdb");
        let registry = match self.registry.try_read_err() {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to acquire function registry lock for persistence");
                return;
            }
        };

        if let Err(e) = frogdb_core::save_to_file(&registry, &path) {
            warn!(error = %e, "Failed to persist functions to disk");
        }
    }
}

/// The replica-side entry point: a `FUNCTION` frame the primary tagged
/// `CONTROL_SHARD` arrives here and reaches the same registry the connection
/// layer mutates.
///
/// Only `FUNCTION` is expected; anything else on the control shard is a command
/// this build does not know how to apply process-wide, and the loop's rule for
/// that is to step over it rather than break the link.
impl frogdb_replication::ControlApplier for FunctionStore {
    fn apply(&self, command: &frogdb_protocol::ParsedCommand) -> Result<(), String> {
        let name = command.name_uppercase_string();
        if name != "FUNCTION" {
            warn!(
                command = %name,
                "Ignoring a control-shard command this build does not apply",
            );
            return Ok(());
        }
        self.apply_replicated(&command.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FM-REPLICATION-054
    #[test]
    fn only_the_four_state_changing_subcommands_replicate() {
        for sub in ["LOAD", "load", "DELETE", "FLUSH", "RESTORE"] {
            assert!(
                is_mutating_function_command(&[Bytes::from(sub.to_string())]),
                "{sub} should replicate",
            );
        }
        for sub in ["LIST", "STATS", "DUMP", "HELP", "KILL"] {
            assert!(
                !is_mutating_function_command(&[Bytes::from(sub.to_string())]),
                "{sub} is a read or a per-shard control message; it must not replicate",
            );
        }
        assert!(!is_mutating_function_command(&[]));
    }
}
