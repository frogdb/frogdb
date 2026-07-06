//! The script command gate.
//!
//! Every `redis.call()` / `redis.pcall()` a Lua script issues is one decision:
//! *given this command and its keys, is it allowed, and where does it run?*
//! [`ScriptCommandGate`] is the single owner of that contract. It extracts the
//! command's keys **once**, makes **one** classification (forbidden / RO-write /
//! cross-slot / local / remote shard), and dispatches. Validation and routing
//! read the *same* [`Plan`], so the cross-slot check and the routing decision
//! can never disagree about a command's keys.
//!
//! The `call` and `pcall` closures in [`super::executor`] shrink to "invoke the
//! gate, then surface the `Result` in my dialect" — `call` raises, `pcall`
//! returns a `{err = ...}` table.

use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use frogdb_protocol::Response;

use super::bindings::{
    extract_keys_from_command, is_forbidden_in_script, is_forbidden_subcommand, is_write_command,
};
use super::lua_vm::{CommandExecutionContext, ExecutionState};
use crate::command::CommandContext;
use crate::shard::message::ShardMessage;
use crate::shard::{shard_for_key, slot_for_key};
use crate::sync::{MutexExt, RwLockExt};

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

/// Why a remote dispatch failed, so [`ScriptCommandGate::dispatch`] can choose
/// the message it surfaces to the script.
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

/// Single owner of the `redis.call` / `redis.pcall` contract.
///
/// Holds the same VM handles as the (removed) `VmContextAccessor` — the command
/// execution context set for the duration of a script, and the execution state
/// used to record writes — plus the per-call policy (`read_only`,
/// `enforce_cross_slot`) and the shared [`CrossSlotTracker`]. Cheap to clone
/// (all `Arc`/`Copy`); the `call` and `pcall` closures each hold a clone that
/// shares the same context, state, and cross-slot accumulator.
#[derive(Clone)]
pub(crate) struct ScriptCommandGate {
    cmd_ctx: Arc<RwLock<Option<CommandExecutionContext>>>,
    state: Arc<Mutex<ExecutionState>>,
    read_only: bool,
    enforce_cross_slot: bool,
    cross_slot: CrossSlotTracker,
}

impl ScriptCommandGate {
    pub(crate) fn new(
        cmd_ctx: Arc<RwLock<Option<CommandExecutionContext>>>,
        state: Arc<Mutex<ExecutionState>>,
        read_only: bool,
        enforce_cross_slot: bool,
        cross_slot: CrossSlotTracker,
    ) -> Self {
        Self {
            cmd_ctx,
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
    /// shard, read once by [`Self::dispatch`] and passed in so this decision is
    /// unit-testable without a live command execution context.
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

    /// Classify then dispatch. On a cross-shard call, the fallback is an
    /// EXPLICIT error — never a silent local write to the wrong shard.
    pub(crate) fn dispatch(&self, parts: &[Bytes]) -> Result<Response, String> {
        let guard = self
            .cmd_ctx
            .try_read_err()
            .map_err(|e| format!("ERR lock error: {e}"))?;
        let exec_ctx = guard
            .as_ref()
            .ok_or_else(|| "ERR command execution context not available".to_string())?;

        match self.classify(parts, exec_ctx.num_shards, exec_ctx.shard_id)? {
            Plan::Local => self.run_local(exec_ctx, parts),
            Plan::Remote(target) => self
                .run_remote(exec_ctx, target, parts)
                .map_err(RemoteError::into_message),
        }
    }

    /// Execute the command against this shard's local store.
    fn run_local(
        &self,
        exec_ctx: &CommandExecutionContext,
        parts: &[Bytes],
    ) -> Result<Response, String> {
        let cmd_name = String::from_utf8_lossy(&parts[0]).to_uppercase();

        // SAFETY: These pointers are valid during script execution, which is
        // synchronous and single-threaded within a shard.
        let store = unsafe { &mut *exec_ctx.store_ptr };
        let registry = unsafe { &*exec_ctx.registry_ptr };

        let handler = registry
            .get(&cmd_name)
            .ok_or_else(|| format!("ERR unknown command '{}'", cmd_name))?;

        let args = &parts[1..];
        if !handler.arity().check(args.len()) {
            return Err(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name().to_ascii_lowercase()
            ));
        }

        let mut ctx = CommandContext::new(
            store,
            &exec_ctx.shard_senders,
            exec_ctx.shard_id,
            exec_ctx.num_shards,
            exec_ctx.conn_id,
            exec_ctx.protocol_version,
        );
        // Propagate replica identity from the script's execution context so
        // `redis.call('ROLE')` / INFO replication report the correct role on a
        // replica (the context this builds is what the sub-command actually
        // sees, not the outer EVAL context).
        ctx.is_replica = exec_ctx.is_replica;
        ctx.is_replica_flag = exec_ctx.is_replica_flag.clone();
        ctx.master_host = exec_ctx.master_host.clone();
        ctx.master_port = exec_ctx.master_port;

        match handler.execute(&mut ctx, args) {
            Ok(response) => Ok(response),
            Err(err) => Err(err.to_string()),
        }
    }

    /// Dispatch a synchronous [`ShardMessage::ScriptSubCommand`] to the owning
    /// shard and block on the reply.
    ///
    /// The wait uses `block_in_place`, which requires a multi-thread Tokio
    /// runtime; on a current-thread runtime it panics. We catch that panic and
    /// return [`RemoteError::RuntimeUnavailable`] rather than falling through to
    /// local execution — a cross-shard write must never land on the wrong shard.
    fn run_remote(
        &self,
        exec_ctx: &CommandExecutionContext,
        target_shard: usize,
        parts: &[Bytes],
    ) -> Result<Response, RemoteError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        exec_ctx.shard_senders[target_shard]
            .try_send(ShardMessage::ScriptSubCommand {
                command: parts.to_vec(),
                conn_id: exec_ctx.conn_id,
                protocol_version: exec_ctx.protocol_version,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::CommandRegistry;
    use crate::shard::ShardSender;
    use crate::shard::message::Envelope;
    use crate::store::{HashMapStore, Store};
    use frogdb_protocol::ProtocolVersion;
    use tokio::sync::mpsc;

    /// Build a gate with no live command context (for pure `classify` tests).
    fn detached_gate(
        read_only: bool,
        enforce_cross_slot: bool,
        seed: Option<u16>,
    ) -> ScriptCommandGate {
        ScriptCommandGate::new(
            Arc::new(RwLock::new(None)),
            Arc::new(Mutex::new(ExecutionState::default())),
            read_only,
            enforce_cross_slot,
            CrossSlotTracker::new(seed),
        )
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

    // ---- Live-context dispatch tests (cross-shard routing) -----------------

    fn live_gate(
        num_shards: usize,
        shard_id: usize,
        senders: Vec<ShardSender>,
        store: &mut HashMapStore,
        registry: &CommandRegistry,
    ) -> ScriptCommandGate {
        let store_dyn: &mut dyn Store = store;
        let cec = CommandExecutionContext {
            store_ptr: store_dyn as *mut dyn Store,
            registry_ptr: registry as *const CommandRegistry,
            shard_senders: Arc::new(senders),
            shard_id,
            num_shards,
            conn_id: 1,
            protocol_version: ProtocolVersion::Resp2,
            is_replica: false,
            is_replica_flag: None,
            master_host: None,
            master_port: None,
        };
        ScriptCommandGate::new(
            Arc::new(RwLock::new(Some(cec))),
            Arc::new(Mutex::new(ExecutionState::default())),
            false,
            false,
            CrossSlotTracker::new(None),
        )
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

    /// Regression pin: a script sub-command must observe the shard's replica
    /// identity. Before the `command_context` builder, EVAL/EVALSHA/FCALL dropped
    /// `is_replica`/`master_host`/`master_port` at every layer, so a Lua
    /// `redis.call('ROLE')` on a replica wrongly reported `master`.
    #[test]
    fn run_local_propagates_replica_identity() {
        let mut store = HashMapStore::new();
        let mut registry = CommandRegistry::new();
        registry.register(ReplicaProbe);

        let store_dyn: &mut dyn Store = &mut store;
        let cec = CommandExecutionContext {
            store_ptr: store_dyn as *mut dyn Store,
            registry_ptr: &registry as *const CommandRegistry,
            shard_senders: Arc::new(vec![]),
            shard_id: 0,
            num_shards: 1,
            conn_id: 1,
            protocol_version: ProtocolVersion::Resp2,
            is_replica: true,
            is_replica_flag: Some(Arc::new(std::sync::atomic::AtomicBool::new(true))),
            master_host: Some("primary.example".to_string()),
            master_port: Some(6390),
        };
        let gate = ScriptCommandGate::new(
            Arc::new(RwLock::new(Some(cec))),
            Arc::new(Mutex::new(ExecutionState::default())),
            false,
            false,
            CrossSlotTracker::new(None),
        );

        let resp = gate
            .dispatch(&[part("__REPLICAPROBE")])
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
        let gate = live_gate(1, 0, vec![], &mut store, &registry);
        let resp = gate
            .dispatch(&[part("__REPLICAPROBE")])
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
        let gate = live_gate(2, 0, senders, &mut store, &registry);
        let parts = vec![part("SET"), key, part("v")];

        let err = gate
            .dispatch(&parts)
            .expect_err("cross-shard call on current_thread must error, not fall through");
        assert!(
            err.contains("multi-thread runtime"),
            "expected explicit runtime error, got: {err}"
        );
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
        let gate = live_gate(2, 0, senders, &mut store, &registry);
        let parts = vec![part("SET"), key, part("v")];

        let resp = gate
            .dispatch(&parts)
            .expect("remote dispatch should succeed");
        assert!(
            matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"),
            "expected remote OK, got: {resp:?}"
        );
        responder.await.unwrap();
    }
}
