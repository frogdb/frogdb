//! The script command gate and the safe re-entrancy seam.
//!
//! Every `redis.call()` / `redis.pcall()` a Lua script issues is one decision:
//! *given this command and its keys, is it allowed, and where does it run?*
//! [`ScriptCommandGate`] is the single owner of that contract. It extracts the
//! command's keys **once**, makes **one** classification (forbidden / RO-write /
//! cross-slot / local / remote shard), and dispatches. Validation and routing
//! read the *same* [`Plan`], so the cross-slot check and the routing decision
//! can never disagree about a command's keys.
//!
//! The gate does not touch the store directly. It dispatches through a
//! [`CommandInvoker`] — the safe re-entrancy seam. The concrete
//! [`ScriptInvoker`] owns a *real* `&mut` store borrow for the exact span of one
//! script execution; the Lua VM holds only a `&dyn CommandInvoker` handle,
//! scoped by `mlua`'s `scope` so it can never be called after the borrow ends.
//! This replaces the old lifetime-erased `*mut dyn Store` that scripts used to
//! smuggle across the boundary — no `transmute`, no aliased pointer, no
//! `unsafe` deref.
//!
//! The `call` and `pcall` closures in [`super::lua_vm`] shrink to "invoke the
//! gate, then surface the `Result` in my dialect" — `call` raises, `pcall`
//! returns a `{err = ...}` table.

use std::cell::RefCell;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};

use super::bindings::{
    extract_keys_from_command, is_forbidden_in_script, is_forbidden_subcommand, is_write_command,
};
use super::lua_vm::ExecutionState;
use crate::command::CommandContext;
use crate::registry::CommandRegistry;
use crate::shard::message::ShardMessage;
use crate::shard::{ShardSender, shard_for_key, slot_for_key};
use crate::store::Store;
use crate::sync::MutexExt;

/// Shared cross-slot accumulator for one script execution.
///
/// Was the inline `Mutex<Option<u16>>` threaded through the `call`/`pcall`
/// closures as `stc`/`stp`. It is seeded with the slot of the script's first
/// declared key (when cross-slot enforcement is active) and then accumulates
/// the slots of every key touched by `redis.call`/`redis.pcall`. If any two
/// keys hash to different slots, the script is rejected with `CROSSSLOT`.
#[derive(Clone)]
pub(crate) struct CrossSlotTracker {
    slot: Arc<Mutex<Option<u16>>>,
}

impl CrossSlotTracker {
    /// Create a tracker seeded with an optional initial slot.
    pub(crate) fn new(initial: Option<u16>) -> Self {
        Self {
            slot: Arc::new(Mutex::new(initial)),
        }
    }

    /// Accumulate every key's slot; error if any key hashes to a different slot
    /// than one already seen.
    fn check_all(&self, keys: &[Bytes]) -> Result<(), String> {
        for key in keys {
            let key_slot = slot_for_key(key);
            let mut seen = self.slot.lock_or_panic("CrossSlotTracker::check_all");
            match *seen {
                None => *seen = Some(key_slot),
                Some(existing) if existing != key_slot => {
                    return Err(
                        "ERR Script attempted to access keys that do not hash to the same slot"
                            .to_string(),
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Where a classified sub-command runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    /// Keys (if any) belong to this shard; run against the local store.
    Local,
    /// Keys belong to shard N; dispatch a [`ShardMessage::ScriptSubCommand`].
    Remote(usize),
}

/// Why a remote dispatch failed, so [`ScriptInvoker::run_remote`] can choose the
/// message it surfaces to the script.
enum RemoteError {
    /// `try_send` to the target shard failed (queue closed or full).
    Dispatch(String),
    /// The target shard dropped the synchronous response channel.
    Response(String),
    /// `block_in_place` is unavailable (current-thread runtime): a cross-shard
    /// call cannot block to await the remote reply. NEVER a silent local write.
    RuntimeUnavailable,
}

impl RemoteError {
    fn into_message(self) -> String {
        match self {
            RemoteError::Dispatch(m) | RemoteError::Response(m) => m,
            RemoteError::RuntimeUnavailable => {
                "ERR cross-shard script call requires a multi-thread runtime".to_string()
            }
        }
    }
}

/// The safe re-entrancy seam a Lua `redis.call` / `redis.pcall` invokes.
///
/// Replaces the lifetime-erased `*mut dyn Store` that scripts used to smuggle
/// across the boundary. The implementor ([`ScriptInvoker`]) owns the live store
/// borrow for the exact span of one script execution; the Lua VM holds only a
/// `&dyn CommandInvoker` handle, scoped by `mlua`'s `scope`, so a callback can
/// never run after the borrow ends. No `unsafe`, no aliased pointer.
pub(crate) trait CommandInvoker {
    /// Total shard count of the running shard (routing dimension).
    fn num_shards(&self) -> usize;
    /// This shard's id (routing dimension).
    fn shard_id(&self) -> usize;
    /// Run a fully-validated command against this shard's local store.
    fn run_local(&self, parts: &[Bytes]) -> Result<Response, String>;
    /// Dispatch a command to the shard that owns its keys and await the reply.
    /// On any failure the message is an EXPLICIT error — never a silent local
    /// write to the wrong shard.
    fn run_remote(&self, target_shard: usize, parts: &[Bytes]) -> Result<Response, String>;
}

/// Single owner of the `redis.call` / `redis.pcall` policy contract.
///
/// Holds the execution state used to record writes, the per-call policy
/// (`read_only`, `enforce_cross_slot`), and the shared [`CrossSlotTracker`].
/// It is entirely store-agnostic: dispatch happens through a
/// [`CommandInvoker`] handed to [`Self::dispatch`]. Cheap to clone (all
/// `Arc`/`Copy`); the `call` and `pcall` closures each hold a clone that shares
/// the same state and cross-slot accumulator.
#[derive(Clone)]
pub(crate) struct ScriptCommandGate {
    state: Arc<Mutex<ExecutionState>>,
    read_only: bool,
    enforce_cross_slot: bool,
    cross_slot: CrossSlotTracker,
}

impl ScriptCommandGate {
    pub(crate) fn new(
        state: Arc<Mutex<ExecutionState>>,
        read_only: bool,
        enforce_cross_slot: bool,
        cross_slot: CrossSlotTracker,
    ) -> Self {
        Self {
            state,
            read_only,
            enforce_cross_slot,
            cross_slot,
        }
    }

    /// Mark the running script as having performed a write.
    fn mark_write(&self) {
        self.state
            .lock_or_panic("ScriptCommandGate::mark_write")
            .has_writes = true;
    }

    /// THE single decision: one key extraction; all validation; one routing
    /// choice; cross-slot accumulation. Returns an error string on rejection.
    ///
    /// `num_shards` / `shard_id` are the routing dimensions of the running
    /// shard, read once by [`Self::dispatch`] from the invoker and passed in so
    /// this decision is unit-testable without a live invoker.
    fn classify(
        &self,
        parts: &[Bytes],
        num_shards: usize,
        shard_id: usize,
    ) -> Result<Plan, String> {
        if parts.is_empty() {
            return Err("ERR wrong number of arguments for redis command".to_string());
        }
        let name = String::from_utf8_lossy(&parts[0]).to_uppercase();
        if let Some(err) = is_forbidden_in_script(&name) {
            return Err(err.to_string());
        }
        if let Some(err) = is_forbidden_subcommand(parts) {
            return Err(err.to_string());
        }
        let is_write = is_write_command(&name);
        if self.read_only && is_write {
            return Err("ERR Write commands are not allowed from read-only scripts".to_string());
        }

        // ONE extraction, shared by the cross-slot check AND the routing choice,
        // so the two can never disagree about this command's keys.
        let keys = extract_keys_from_command(&name, parts);
        if self.enforce_cross_slot {
            self.cross_slot.check_all(&keys)?;
        }
        if is_write {
            self.mark_write();
        }

        Ok(match keys.first() {
            Some(first_key) if num_shards > 1 => {
                let target = shard_for_key(first_key, num_shards);
                if target == shard_id {
                    Plan::Local
                } else {
                    Plan::Remote(target)
                }
            }
            _ => Plan::Local,
        })
    }

    /// Classify then dispatch through the [`CommandInvoker`] seam. On a
    /// cross-shard call, the fallback is an EXPLICIT error — never a silent
    /// local write to the wrong shard.
    pub(crate) fn dispatch(
        &self,
        invoker: &dyn CommandInvoker,
        parts: &[Bytes],
    ) -> Result<Response, String> {
        match self.classify(parts, invoker.num_shards(), invoker.shard_id())? {
            Plan::Local => invoker.run_local(parts),
            Plan::Remote(target) => invoker.run_remote(target, parts),
        }
    }
}

/// The live execution environment a script's `redis.call` runs against.
///
/// Owns a real `&mut` store borrow (behind a [`RefCell`], because the Lua
/// callbacks that reach it are shared `Fn` closures), plus the shard routing
/// context, for the exact span of one script execution. Constructed on the
/// executor's stack and handed to the Lua VM as a `&dyn CommandInvoker`;
/// `mlua`'s `scope` guarantees the callbacks holding that reference are
/// invalidated before this borrow ends.
pub(crate) struct ScriptInvoker<'a> {
    store: RefCell<&'a mut dyn Store>,
    registry: &'a CommandRegistry,
    shard_senders: Arc<Vec<ShardSender>>,
    shard_id: usize,
    num_shards: usize,
    conn_id: u64,
    protocol_version: ProtocolVersion,
    is_replica: bool,
    is_replica_flag: Option<Arc<AtomicBool>>,
    master_host: Option<String>,
    master_port: Option<u16>,
}

impl<'a> ScriptInvoker<'a> {
    /// Borrow the store + shard context out of a live [`CommandContext`] for the
    /// duration of one script execution. The store is a genuine `&mut` reborrow,
    /// not a transmuted pointer — its lifetime is tied to `ctx`.
    pub(crate) fn from_context(
        ctx: &'a mut CommandContext<'_>,
        registry: &'a CommandRegistry,
    ) -> Self {
        Self {
            registry,
            shard_senders: Arc::clone(ctx.shard_senders),
            shard_id: ctx.shard_id,
            num_shards: ctx.num_shards,
            conn_id: ctx.conn_id,
            protocol_version: ctx.protocol_version,
            is_replica: ctx.is_replica,
            is_replica_flag: ctx.is_replica_flag.clone(),
            master_host: ctx.master_host.clone(),
            master_port: ctx.master_port,
            // Reborrow last, after every scalar field is read, so the mutable
            // borrow of the store does not shadow the disjoint field reads.
            store: RefCell::new(&mut *ctx.store),
        }
    }

    /// Dispatch a synchronous [`ShardMessage::ScriptSubCommand`] to the owning
    /// shard and block on the reply.
    ///
    /// The wait uses `block_in_place`, which requires a multi-thread Tokio
    /// runtime; on a current-thread runtime it panics. We catch that panic and
    /// return [`RemoteError::RuntimeUnavailable`] rather than falling through to
    /// local execution — a cross-shard write must never land on the wrong shard.
    fn run_remote_inner(
        &self,
        target_shard: usize,
        parts: &[Bytes],
    ) -> Result<Response, RemoteError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.shard_senders[target_shard]
            .try_send(ShardMessage::ScriptSubCommand {
                command: parts.to_vec(),
                conn_id: self.conn_id,
                protocol_version: self.protocol_version,
                response_tx: tx,
            })
            .map_err(|e| RemoteError::Dispatch(format!("ERR cross-shard dispatch failed: {e}")))?;

        // block_in_place releases the tokio worker thread while we synchronously
        // wait for the target shard's response. It requires a multi-thread
        // runtime; on current_thread it panics, which catch_unwind converts into
        // an explicit RuntimeUnavailable error instead of a wrong-shard write.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| rx.recv())
        }));
        match result {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(RemoteError::Response(format!(
                "ERR cross-shard response failed: {e}"
            ))),
            Err(_) => Err(RemoteError::RuntimeUnavailable),
        }
    }
}

impl CommandInvoker for ScriptInvoker<'_> {
    fn num_shards(&self) -> usize {
        self.num_shards
    }

    fn shard_id(&self) -> usize {
        self.shard_id
    }

    /// Execute the command against this shard's local store.
    fn run_local(&self, parts: &[Bytes]) -> Result<Response, String> {
        let cmd_name = String::from_utf8_lossy(&parts[0]).to_uppercase();

        let handler = self
            .registry
            .get(&cmd_name)
            .ok_or_else(|| format!("ERR unknown command '{}'", cmd_name))?;

        let args = &parts[1..];
        if !handler.arity().check(args.len()) {
            return Err(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name().to_ascii_lowercase()
            ));
        }

        // Real, scoped `&mut dyn Store` — no `unsafe`, no aliased pointer.
        let mut store = self.store.borrow_mut();
        let mut ctx = CommandContext::new(
            &mut **store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            self.conn_id,
            self.protocol_version,
        );
        // Propagate replica identity from the script's execution context so
        // `redis.call('ROLE')` / INFO replication report the correct role on a
        // replica (the context this builds is what the sub-command actually
        // sees, not the outer EVAL context).
        ctx.is_replica = self.is_replica;
        ctx.is_replica_flag = self.is_replica_flag.clone();
        ctx.master_host = self.master_host.clone();
        ctx.master_port = self.master_port;

        match handler.execute(&mut ctx, args) {
            Ok(response) => Ok(response),
            Err(err) => Err(err.to_string()),
        }
    }

    fn run_remote(&self, target_shard: usize, parts: &[Bytes]) -> Result<Response, String> {
        self.run_remote_inner(target_shard, parts)
            .map_err(RemoteError::into_message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::message::Envelope;
    use crate::store::HashMapStore;
    use tokio::sync::mpsc;

    /// Build a gate for pure `classify` tests. The gate is store-agnostic, so no
    /// invoker is needed.
    fn detached_gate(
        read_only: bool,
        enforce_cross_slot: bool,
        seed: Option<u16>,
    ) -> ScriptCommandGate {
        ScriptCommandGate::new(
            Arc::new(Mutex::new(ExecutionState::default())),
            read_only,
            enforce_cross_slot,
            CrossSlotTracker::new(seed),
        )
    }

    /// A default policy gate for live-dispatch tests.
    fn open_gate() -> ScriptCommandGate {
        detached_gate(false, false, None)
    }

    fn part(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    /// Find a key that routes to `target_shard` under `num_shards`.
    fn key_for_shard(target_shard: usize, num_shards: usize) -> Bytes {
        for i in 0u32..1_000_000 {
            let key = Bytes::from(format!("k{i}"));
            if shard_for_key(&key, num_shards) == target_shard {
                return key;
            }
        }
        panic!("no key routed to shard {target_shard}");
    }

    #[test]
    fn classify_rejects_forbidden_command() {
        let gate = detached_gate(false, false, None);
        let err = gate
            .classify(&[part("MULTI")], 1, 0)
            .expect_err("MULTI is forbidden in scripts");
        assert!(
            err.contains("MULTI") || err.to_lowercase().contains("not allowed"),
            "got: {err}"
        );
    }

    #[test]
    fn classify_rejects_write_in_readonly() {
        let gate = detached_gate(true, false, None);
        let err = gate
            .classify(&[part("SET"), part("k"), part("v")], 1, 0)
            .expect_err("write in read-only script must reject");
        assert_eq!(
            err,
            "ERR Write commands are not allowed from read-only scripts"
        );
    }

    #[test]
    fn classify_allows_read_in_readonly() {
        let gate = detached_gate(true, false, None);
        let plan = gate
            .classify(&[part("GET"), part("k")], 1, 0)
            .expect("read in read-only script is allowed");
        assert_eq!(plan, Plan::Local);
    }

    #[test]
    fn classify_marks_write() {
        let gate = detached_gate(false, false, None);
        gate.classify(&[part("SET"), part("k"), part("v")], 1, 0)
            .unwrap();
        assert!(gate.state.lock_or_panic("test").has_writes);
    }

    #[test]
    fn classify_does_not_mark_read() {
        let gate = detached_gate(false, false, None);
        gate.classify(&[part("GET"), part("k")], 1, 0).unwrap();
        assert!(!gate.state.lock_or_panic("test").has_writes);
    }

    #[test]
    fn classify_cross_slot_span_rejected_when_enforced() {
        // MSET over two keys whose slots differ; enforcement on -> CROSSSLOT.
        let gate = detached_gate(false, true, None);
        // Find two keys with distinct slots.
        let a = part("{a}1");
        let mut b = part("{a}2"); // same tag -> same slot; replace below
        for i in 0u32..100_000 {
            let cand = Bytes::from(format!("k{i}"));
            if slot_for_key(&cand) != slot_for_key(&a) {
                b = cand;
                break;
            }
        }
        let err = gate
            .classify(&[part("MSET"), a, part("va"), b, part("vb")], 1, 0)
            .expect_err("cross-slot span must reject when enforced");
        assert!(err.contains("same slot"), "got: {err}");
    }

    #[test]
    fn classify_cross_slot_span_allowed_when_not_enforced() {
        // Same span, enforcement off -> no rejection (routes by first key).
        let gate = detached_gate(false, false, None);
        let a = part("{a}1");
        let mut b = part("kx");
        for i in 0u32..100_000 {
            let cand = Bytes::from(format!("k{i}"));
            if slot_for_key(&cand) != slot_for_key(&a) {
                b = cand;
                break;
            }
        }
        let plan = gate
            .classify(&[part("MSET"), a, part("va"), b, part("vb")], 1, 0)
            .expect("cross-slot span allowed when enforcement off");
        assert_eq!(plan, Plan::Local);
    }

    #[test]
    fn classify_single_shard_is_always_local() {
        let gate = detached_gate(false, false, None);
        let plan = gate
            .classify(&[part("GET"), key_for_shard(3, 4)], 1, 0)
            .unwrap();
        assert_eq!(plan, Plan::Local);
    }

    #[test]
    fn classify_routes_local_when_key_on_this_shard() {
        let gate = detached_gate(false, false, None);
        let key = key_for_shard(2, 4);
        let plan = gate.classify(&[part("GET"), key], 4, 2).unwrap();
        assert_eq!(plan, Plan::Local);
    }

    #[test]
    fn classify_routes_remote_when_key_on_other_shard() {
        let gate = detached_gate(false, false, None);
        let key = key_for_shard(3, 4);
        // Running on shard 0, key owned by shard 3.
        let plan = gate.classify(&[part("GET"), key], 4, 0).unwrap();
        assert_eq!(plan, Plan::Remote(3));
    }

    #[test]
    fn classify_routes_by_first_key_from_single_extraction() {
        // A multi-key command routes by its first key; the same extraction feeds
        // both the (skipped, enforcement off) cross-slot check and routing.
        let gate = detached_gate(false, false, None);
        let first = key_for_shard(1, 4);
        let second = key_for_shard(3, 4);
        let plan = gate
            .classify(&[part("MSET"), first, part("v1"), second, part("v2")], 4, 0)
            .unwrap();
        assert_eq!(plan, Plan::Remote(1));
    }

    #[test]
    fn classify_no_keys_is_local() {
        let gate = detached_gate(false, false, None);
        // PING has no keys -> always local regardless of shard count.
        let plan = gate.classify(&[part("PING")], 4, 0).unwrap();
        assert_eq!(plan, Plan::Local);
    }

    // ---- Live-invoker dispatch tests (the re-entrancy seam) -----------------

    /// Build a plain [`ScriptInvoker`] over a borrowed store for dispatch tests.
    fn live_invoker<'a>(
        num_shards: usize,
        shard_id: usize,
        senders: Vec<ShardSender>,
        store: &'a mut HashMapStore,
        registry: &'a CommandRegistry,
    ) -> ScriptInvoker<'a> {
        ScriptInvoker {
            registry,
            shard_senders: Arc::new(senders),
            shard_id,
            num_shards,
            conn_id: 1,
            protocol_version: ProtocolVersion::Resp2,
            is_replica: false,
            is_replica_flag: None,
            master_host: None,
            master_port: None,
            store: RefCell::<&mut dyn Store>::new(store),
        }
    }

    /// Minimal probe command: replies `:1` when the execution context reports a
    /// replica, `:0` otherwise. Lets us assert that `run_local` propagates
    /// replica identity into the context a `redis.call` sub-command actually
    /// sees — independent of the server crate's ROLE command.
    struct ReplicaProbe;

    impl crate::command::Command for ReplicaProbe {
        fn spec(&self) -> &'static crate::command_spec::CommandSpec {
            use crate::command::{Arity, CommandFlags, ExecutionStrategy, WaiterWake, WalStrategy};
            use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
            static SPEC: CommandSpec = CommandSpec {
                name: "__REPLICAPROBE",
                arity: Arity::Fixed(0),
                flags: CommandFlags::READONLY,
                keys: KeySpec::None,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::NotApplicable,
                requires_same_slot: false,
                lookup: LookupSpec::None,
                strategy: ExecutionStrategy::Standard,
            };
            &SPEC
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, crate::error::CommandError> {
            Ok(Response::Integer(if ctx.is_replica { 1 } else { 0 }))
        }
    }

    /// A test command that writes a fixed key into the live store, so we can
    /// prove a `redis.call` sub-command mutates the real store through the seam
    /// (not an aliased copy).
    struct SeamWrite;

    impl crate::command::Command for SeamWrite {
        fn spec(&self) -> &'static crate::command_spec::CommandSpec {
            use crate::command::{Arity, CommandFlags, WaiterWake, WalStrategy};
            use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
            static SPEC: CommandSpec = CommandSpec {
                name: "__SEAMWRITE",
                arity: Arity::Fixed(0),
                flags: CommandFlags::WRITE,
                keys: KeySpec::None,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                lookup: LookupSpec::None,
            };
            &SPEC
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, crate::error::CommandError> {
            ctx.store.set(
                Bytes::from_static(b"seam"),
                crate::types::Value::string("v1"),
            );
            Ok(Response::Simple(Bytes::from_static(b"OK")))
        }
    }

    /// A re-entrant sub-command dispatched through the seam runs against the
    /// *live* store: the write it performs is visible in the borrowed store
    /// afterward, proving there is no aliased/transmuted pointer copy.
    #[test]
    fn dispatch_reenters_against_live_store() {
        let mut store = HashMapStore::new();
        let mut registry = CommandRegistry::new();
        registry.register(SeamWrite);
        let gate = open_gate();

        {
            let invoker = live_invoker(1, 0, vec![], &mut store, &registry);
            let resp = gate
                .dispatch(&invoker, &[part("__SEAMWRITE")])
                .expect("__SEAMWRITE dispatches locally through the seam");
            assert!(matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"));
        }
        // The write really landed in the borrowed store (no aliased pointer).
        assert!(
            store.contains(b"seam"),
            "re-entrant seam write must persist in the live store"
        );
        assert_eq!(store.len(), 1);
    }

    /// Regression pin: a script sub-command must observe the shard's replica
    /// identity. Before the `command_context` builder, EVAL/EVALSHA/FCALL dropped
    /// `is_replica`/`master_host`/`master_port` at every layer, so a Lua
    /// `redis.call('ROLE')` on a replica wrongly reported `master`.
    #[test]
    fn run_local_propagates_replica_identity() {
        let mut store = HashMapStore::new();
        let mut registry = CommandRegistry::new();
        registry.register(ReplicaProbe);

        let invoker = ScriptInvoker {
            registry: &registry,
            shard_senders: Arc::new(vec![]),
            shard_id: 0,
            num_shards: 1,
            conn_id: 1,
            protocol_version: ProtocolVersion::Resp2,
            is_replica: true,
            is_replica_flag: Some(Arc::new(AtomicBool::new(true))),
            master_host: Some("primary.example".to_string()),
            master_port: Some(6390),
            store: RefCell::<&mut dyn Store>::new(&mut store),
        };
        let gate = open_gate();

        let resp = gate
            .dispatch(&invoker, &[part("__REPLICAPROBE")])
            .expect("probe dispatch should run locally");
        match resp {
            Response::Integer(1) => {}
            other => panic!("sub-command must observe replica identity, got {other:?}"),
        }
    }

    /// The complementary case: on a primary the same seam reports `:0`.
    #[test]
    fn run_local_reports_primary_when_not_replica() {
        let mut store = HashMapStore::new();
        let mut registry = CommandRegistry::new();
        registry.register(ReplicaProbe);
        let invoker = live_invoker(1, 0, vec![], &mut store, &registry);
        let gate = open_gate();
        let resp = gate
            .dispatch(&invoker, &[part("__REPLICAPROBE")])
            .expect("probe dispatch should run locally");
        match resp {
            Response::Integer(0) => {}
            other => panic!("primary sub-command must report non-replica, got {other:?}"),
        }
    }

    /// Regression pin for Flag 1: on a current-thread runtime, a cross-shard
    /// `redis.call` must HARD ERROR — never silently write to the local shard.
    #[tokio::test]
    async fn cross_shard_call_hard_errors_on_current_thread() {
        let mut store = HashMapStore::new();
        let registry = CommandRegistry::new();
        // Two shards; keep both receivers alive so try_send succeeds.
        let mut receivers = Vec::new();
        let mut senders = Vec::new();
        for _ in 0..2 {
            let (tx, rx) = mpsc::channel::<Envelope>(8);
            senders.push(ShardSender::new(tx));
            receivers.push(rx);
        }
        let key = key_for_shard(1, 2);
        let gate = open_gate();
        let parts = vec![part("SET"), key, part("v")];

        {
            let invoker = live_invoker(2, 0, senders, &mut store, &registry);
            let err = gate
                .dispatch(&invoker, &parts)
                .expect_err("cross-shard call on current_thread must error, not fall through");
            assert!(
                err.contains("multi-thread runtime"),
                "expected explicit runtime error, got: {err}"
            );
        }
        // No silent local write: shard 0's store must be untouched.
        assert_eq!(store.len(), 0, "cross-shard write must not land locally");
    }

    /// On a multi-thread runtime the cross-shard path really dispatches a
    /// ScriptSubCommand to the owning shard and returns its reply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_shard_call_dispatches_on_multi_thread() {
        let mut store = HashMapStore::new();
        let registry = CommandRegistry::new();
        let (tx0, _rx0) = mpsc::channel::<Envelope>(8);
        let (tx1, mut rx1) = mpsc::channel::<Envelope>(8);
        let senders = vec![ShardSender::new(tx0), ShardSender::new(tx1)];

        // Stand in for shard 1's worker: reply to the ScriptSubCommand.
        let responder = tokio::spawn(async move {
            if let Some(env) = rx1.recv().await
                && let ShardMessage::ScriptSubCommand { response_tx, .. } = env.message
            {
                let _ = response_tx.send(Response::Simple(Bytes::from_static(b"OK")));
            }
        });

        let key = key_for_shard(1, 2);
        let gate = open_gate();
        let parts = vec![part("SET"), key, part("v")];

        let invoker = live_invoker(2, 0, senders, &mut store, &registry);
        let resp = gate
            .dispatch(&invoker, &parts)
            .expect("remote dispatch should succeed");
        assert!(
            matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"),
            "expected remote OK, got: {resp:?}"
        );
        responder.await.unwrap();
    }
}
