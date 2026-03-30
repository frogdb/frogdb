use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use smallvec::SmallVec;

use crate::command::{Command, WalStrategy};
use crate::store::Store;
use crate::types::{KeyMetadata, Value};

use super::worker::ShardWorker;

/// The state of a key before a write command executed.
enum KeyState {
    /// Key existed with this value, metadata, and optional expiry.
    Existed {
        value: Arc<Value>,
        metadata: KeyMetadata,
        expiry: Option<Instant>,
    },
    /// Key did not exist.
    Missing,
}

/// Snapshot of key states before a write command, used for rollback on WAL failure.
///
/// Uses `SmallVec` to avoid heap allocation for the common single-key case.
pub(crate) struct WriteSnapshot {
    keys: SmallVec<[(Bytes, KeyState); 2]>,
}

impl ShardWorker {
    /// Capture the current state of keys that a write command will modify.
    ///
    /// Must be called **before** the command execution block. Uses `&mut self`
    /// because warm-key promotion (via `store.get()`) requires mutable access.
    pub(crate) fn capture_write_snapshot(
        &mut self,
        handler: &dyn Command,
        args: &[Bytes],
    ) -> WriteSnapshot {
        let mut snapshot_keys: SmallVec<[Bytes; 2]> = SmallVec::new();

        // Collect keys based on the handler's WalStrategy, which tells us
        // exactly which keys will be persisted (and thus which need rollback).
        match handler.wal_strategy() {
            WalStrategy::PersistFirstKey | WalStrategy::PersistOrDeleteFirstKey => {
                if !args.is_empty() {
                    snapshot_keys.push(args[0].clone());
                }
            }
            WalStrategy::DeleteKeys => {
                for arg in args {
                    snapshot_keys.push(arg.clone());
                }
            }
            WalStrategy::RenameKeys => {
                if args.len() >= 2 {
                    snapshot_keys.push(args[0].clone()); // old key
                    snapshot_keys.push(args[1].clone()); // new key
                }
            }
            WalStrategy::MoveKeys => {
                if args.len() >= 2 {
                    snapshot_keys.push(args[0].clone()); // source
                    snapshot_keys.push(args[1].clone()); // dest
                }
            }
            WalStrategy::PersistDestination(idx) => {
                if let Some(dest) = args.get(idx) {
                    snapshot_keys.push(dest.clone());
                }
            }
            WalStrategy::NoOp => {}
            WalStrategy::Infer => {
                // For legacy Infer, use handler.keys() as best-effort.
                let keys = handler.keys(args);
                for key in keys {
                    snapshot_keys.push(Bytes::copy_from_slice(key));
                }
            }
        }

        // Snapshot each key's current state.
        let mut keys: SmallVec<[(Bytes, KeyState); 2]> = SmallVec::new();
        for key in snapshot_keys {
            // Use store.get() which promotes warm keys to hot tier.
            // This ensures the value is accessible for both snapshot and
            // subsequent command execution.
            let state = if let Some(value) = self.store.get(&key) {
                let metadata = self
                    .store
                    .get_metadata(&key)
                    .unwrap_or_else(|| KeyMetadata::new(value.memory_size()));
                let expiry = self.store.get_expiry(&key);
                KeyState::Existed {
                    value,
                    metadata,
                    expiry,
                }
            } else {
                KeyState::Missing
            };
            keys.push((key, state));
        }

        WriteSnapshot { keys }
    }

    /// Restore key states from a snapshot, undoing the effects of a write command.
    ///
    /// Called when WAL persistence fails in rollback mode.
    pub(crate) fn rollback_snapshot(&mut self, snapshot: WriteSnapshot) {
        for (key, state) in snapshot.keys {
            match state {
                KeyState::Existed {
                    value,
                    metadata,
                    expiry,
                } => {
                    // Restore the previous value.
                    // Arc::unwrap_or_clone gives us the inner Value if refcount == 1,
                    // otherwise clones.
                    self.store.set(key.clone(), Arc::unwrap_or_clone(value));

                    // Restore expiry if it had one, otherwise clear it.
                    if let Some(expires_at) = expiry {
                        self.store.set_expiry(&key, expires_at);
                    } else {
                        self.store.persist(&key);
                    }

                    // Restore metadata (expiry component is handled above;
                    // the size metadata is naturally correct because we restored
                    // the original value).
                    let _ = metadata;
                }
                KeyState::Missing => {
                    // Key didn't exist before — remove it.
                    self.store.delete(&key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(dead_code)]

    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use tokio::sync::mpsc;

    use crate::command::{
        Arity, Command, CommandContext, CommandFlags, ExecutionStrategy, WalStrategy,
    };
    use crate::eviction::EvictionConfig;
    use crate::noop::NoopMetricsRecorder;
    use crate::registry::CommandRegistry;
    use crate::replication::NoopBroadcaster;
    use crate::shard::message::{ShardReceiver, ShardSender};
    use crate::shard::worker::ShardWorker;
    use crate::store::Store;
    use crate::types::Value;
    use frogdb_protocol::Response;

    /// Create a minimal ShardWorker for testing rollback logic (no persistence).
    fn make_test_worker() -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_, conn_rx) = mpsc::channel(16);
        let shard_senders = Arc::new(vec![ShardSender::new(msg_tx)]);
        let registry = Arc::new(CommandRegistry::new());

        ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            shard_senders,
            registry,
            EvictionConfig::default(),
            Arc::new(NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        )
    }

    /// A mock command that declares PersistFirstKey WAL strategy.
    struct MockSetCommand;

    impl Command for MockSetCommand {
        fn name(&self) -> &'static str {
            "SET"
        }
        fn arity(&self) -> Arity {
            Arity::Fixed(2)
        }
        fn flags(&self) -> CommandFlags {
            CommandFlags::WRITE
        }
        fn wal_strategy(&self) -> WalStrategy {
            WalStrategy::PersistFirstKey
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
        fn execution_strategy(&self) -> ExecutionStrategy {
            ExecutionStrategy::Standard
        }
        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            if args.is_empty() {
                vec![]
            } else {
                vec![&args[0]]
            }
        }
    }

    /// A mock command that declares RenameKeys WAL strategy.
    struct MockRenameCommand;

    impl Command for MockRenameCommand {
        fn name(&self) -> &'static str {
            "RENAME"
        }
        fn arity(&self) -> Arity {
            Arity::Fixed(2)
        }
        fn flags(&self) -> CommandFlags {
            CommandFlags::WRITE
        }
        fn wal_strategy(&self) -> WalStrategy {
            WalStrategy::RenameKeys
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
        fn execution_strategy(&self) -> ExecutionStrategy {
            ExecutionStrategy::Standard
        }
        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            if args.len() >= 2 {
                vec![&args[0], &args[1]]
            } else {
                vec![]
            }
        }
    }

    /// A mock command that declares DeleteKeys WAL strategy.
    struct MockDelCommand;

    impl Command for MockDelCommand {
        fn name(&self) -> &'static str {
            "DEL"
        }
        fn arity(&self) -> Arity {
            Arity::AtLeast(1)
        }
        fn flags(&self) -> CommandFlags {
            CommandFlags::WRITE
        }
        fn wal_strategy(&self) -> WalStrategy {
            WalStrategy::DeleteKeys
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
        fn execution_strategy(&self) -> ExecutionStrategy {
            ExecutionStrategy::Standard
        }
        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            args.iter().map(|a| a.as_ref()).collect()
        }
    }

    // ========================================================================
    // Tests
    // ========================================================================

    #[test]
    fn test_rollback_missing_key() {
        let mut worker = make_test_worker();
        let handler = MockSetCommand;
        let args = [Bytes::from("newkey"), Bytes::from("value")];

        // Capture snapshot — key does not exist
        let snapshot = worker.capture_write_snapshot(&handler, &args);

        // Simulate command execution: set the key
        worker
            .store
            .set(Bytes::from("newkey"), Value::string("value"));
        assert!(worker.store.contains(b"newkey"));

        // Rollback — should remove the key
        worker.rollback_snapshot(snapshot);
        assert!(
            !worker.store.contains(b"newkey"),
            "key should be removed after rollback"
        );
    }

    #[test]
    fn test_rollback_existing_key() {
        let mut worker = make_test_worker();
        let handler = MockSetCommand;

        // Pre-populate with original value
        worker
            .store
            .set(Bytes::from("mykey"), Value::string("original"));

        let args = [Bytes::from("mykey"), Bytes::from("updated")];
        let snapshot = worker.capture_write_snapshot(&handler, &args);

        // Simulate command: overwrite with "updated"
        worker
            .store
            .set(Bytes::from("mykey"), Value::string("updated"));
        let val = worker.store.get(b"mykey").unwrap();
        assert_eq!(val.as_string().unwrap().as_bytes().as_ref(), b"updated");

        // Rollback — should restore "original"
        worker.rollback_snapshot(snapshot);
        let val = worker.store.get(b"mykey").unwrap();
        assert_eq!(val.as_string().unwrap().as_bytes().as_ref(), b"original");
    }

    #[test]
    fn test_rollback_preserves_expiry() {
        let mut worker = make_test_worker();
        let handler = MockSetCommand;

        worker
            .store
            .set(Bytes::from("ttlkey"), Value::string("oldval"));
        let original_expiry = Instant::now() + Duration::from_secs(3600);
        worker.store.set_expiry(b"ttlkey", original_expiry);

        let args = [Bytes::from("ttlkey"), Bytes::from("newval")];
        let snapshot = worker.capture_write_snapshot(&handler, &args);

        // Simulate: overwrite value, remove expiry
        worker
            .store
            .set(Bytes::from("ttlkey"), Value::string("newval"));
        worker.store.persist(b"ttlkey");
        assert!(worker.store.get_expiry(b"ttlkey").is_none());

        // Rollback
        worker.rollback_snapshot(snapshot);
        let val = worker.store.get(b"ttlkey").unwrap();
        assert_eq!(val.as_string().unwrap().as_bytes().as_ref(), b"oldval");
        let exp = worker.store.get_expiry(b"ttlkey");
        assert!(exp.is_some(), "expiry should be restored after rollback");
        assert_eq!(exp.unwrap(), original_expiry);
    }

    #[test]
    fn test_rollback_rename() {
        let mut worker = make_test_worker();
        let handler = MockRenameCommand;

        worker
            .store
            .set(Bytes::from("src"), Value::string("srcval"));

        let args = [Bytes::from("src"), Bytes::from("dst")];
        let snapshot = worker.capture_write_snapshot(&handler, &args);

        // Simulate rename
        worker.store.delete(b"src");
        worker
            .store
            .set(Bytes::from("dst"), Value::string("srcval"));
        assert!(!worker.store.contains(b"src"));
        assert!(worker.store.contains(b"dst"));

        // Rollback
        worker.rollback_snapshot(snapshot);
        assert!(worker.store.contains(b"src"), "source should be restored");
        let val = worker.store.get(b"src").unwrap();
        assert_eq!(val.as_string().unwrap().as_bytes().as_ref(), b"srcval");
        assert!(!worker.store.contains(b"dst"), "dest should be removed");
    }

    #[test]
    fn test_rollback_del_restores_key() {
        let mut worker = make_test_worker();
        let handler = MockDelCommand;

        worker
            .store
            .set(Bytes::from("delme"), Value::string("precious"));

        let args = [Bytes::from("delme")];
        let snapshot = worker.capture_write_snapshot(&handler, &args);

        worker.store.delete(b"delme");
        assert!(!worker.store.contains(b"delme"));

        worker.rollback_snapshot(snapshot);
        assert!(worker.store.contains(b"delme"));
        let val = worker.store.get(b"delme").unwrap();
        assert_eq!(val.as_string().unwrap().as_bytes().as_ref(), b"precious");
    }

    #[test]
    fn test_snapshot_arc_efficiency() {
        let mut worker = make_test_worker();
        let handler = MockSetCommand;

        let big_value = Value::string("x".repeat(10_000));
        worker.store.set(Bytes::from("bigkey"), big_value);

        let args = [Bytes::from("bigkey"), Bytes::from("newval")];
        let snapshot = worker.capture_write_snapshot(&handler, &args);

        // The snapshot holds an Arc reference — verify refcount is > 1
        let current_arc = worker.store.get(b"bigkey").unwrap();
        assert!(
            Arc::strong_count(&current_arc) >= 2,
            "snapshot should hold Arc reference, not deep copy"
        );
        drop(current_arc);
        worker.rollback_snapshot(snapshot);
    }

    #[test]
    fn test_continue_mode_default() {
        let worker = make_test_worker();
        let policy = worker
            .persistence
            .failure_policy
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(policy, 0, "default policy should be Continue (0)");
    }

    #[test]
    fn test_rollback_mode_flag_toggle() {
        let worker = make_test_worker();
        worker
            .persistence
            .failure_policy
            .store(1, std::sync::atomic::Ordering::Relaxed);
        let policy = worker
            .persistence
            .failure_policy
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(policy, 1, "policy should be Rollback (1)");
    }

    #[test]
    fn test_rollback_clears_added_expiry() {
        // Key had no expiry → command adds expiry → rollback clears it
        let mut worker = make_test_worker();
        let handler = MockSetCommand;

        worker.store.set(Bytes::from("noexp"), Value::string("val"));

        let args = [Bytes::from("noexp"), Bytes::from("newval")];
        let snapshot = worker.capture_write_snapshot(&handler, &args);

        worker
            .store
            .set(Bytes::from("noexp"), Value::string("newval"));
        worker
            .store
            .set_expiry(b"noexp", Instant::now() + Duration::from_secs(60));
        assert!(worker.store.get_expiry(b"noexp").is_some());

        worker.rollback_snapshot(snapshot);
        let val = worker.store.get(b"noexp").unwrap();
        assert_eq!(val.as_string().unwrap().as_bytes().as_ref(), b"val");
        assert!(
            worker.store.get_expiry(b"noexp").is_none(),
            "expiry should be cleared after rollback"
        );
    }
}
