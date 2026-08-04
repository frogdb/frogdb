//! Level-3 scenario for issue 60: what `lua-time-limit` does — and no longer
//! does — to a script that overruns it.
//!
//! Policy (issue 60, **option A**, Redis/Valkey parity): the time limit bounds a
//! script only until it writes. A read-only script that overruns is aborted with
//! `-BUSY`; a *write-dirty* script is never aborted — not by the deadline
//! (`frogdb_scripting::sandbox::deadline_aborts`) and not by `SCRIPT KILL`
//! (`LuaVm::request_kill` → `Unkillable`). It runs to completion and its writes
//! land on the primary and propagate as one MULTI/EXEC batch.
//!
//! Why level 3: the subject is the effect pipeline (what is stored vs. what is
//! propagated, and how the propagated batch is framed). Only the worker produces
//! both, and a `RecordingBroadcaster` can see them side by side. A level-4
//! server test would add a socket and a clock without adding signal for *these*
//! assertions.
//!
//! Both probe scripts are **bounded**: under option A an unbounded write loop
//! would never return, so the write case must terminate on its own.

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::noop::NoopMetricsRecorder;
use frogdb_core::scripting::ScriptingConfig;
use frogdb_core::store::{HashMapStore, Store};
use frogdb_core::{
    CommandRegistry, ScriptingMsg, ShardReceiver, ShardSender, ShardWorker, ShardWorkerBuilder,
};
use frogdb_protocol::{ProtocolVersion, Response};
use frogdb_shard_harness::recording_broadcaster::RecordingBroadcaster;
use tokio::sync::{mpsc, oneshot};

/// The Lua time limit every scenario here runs under. Small enough that both
/// probes blow through it by more than an order of magnitude.
const TIME_LIMIT_MS: u64 = 20;

/// A script that never writes and spins well past [`TIME_LIMIT_MS`].
///
/// Bounded twice over — by a wall-clock deadline and by an iteration cap — so a
/// regression that stops aborting read-only overruns fails the test instead of
/// hanging the suite.
const READ_ONLY_OVERRUN: &str = r#"
local deadline = os.clock() + 1.0
local n = 0
for _ = 1, 500000000 do
    n = n + 1
    if n % 5000 == 0 and os.clock() >= deadline then break end
end
return n
"#;

/// A script that writes *first* — so it is write-dirty before the deadline
/// passes — then spins well past [`TIME_LIMIT_MS`], then writes again and
/// returns. Under option A it must run all the way through.
const WRITE_DIRTY_OVERRUN: &str = r#"
redis.call('SET', KEYS[1] .. 'a', '1')
local deadline = os.clock() + 0.2
local n = 0
for _ = 1, 500000000 do
    n = n + 1
    if n % 5000 == 0 and os.clock() >= deadline then break end
end
redis.call('SET', KEYS[1] .. 'b', '2')
return 1
"#;

struct Fixture {
    worker: ShardWorker,
    broadcaster: Arc<RecordingBroadcaster>,
    _conn_tx: mpsc::Sender<frogdb_core::shard::NewConnection>,
}

/// A 1-shard worker with the real command set, a recording broadcaster, and a
/// deliberately tiny Lua time limit.
fn fixture(time_limit_ms: u64) -> Fixture {
    let (msg_tx, msg_rx) = mpsc::channel(16);
    let (conn_tx, conn_rx) = mpsc::channel(16);
    let mut registry = CommandRegistry::new();
    frogdb_commands::register_all(&mut registry);
    let broadcaster = Arc::new(RecordingBroadcaster::new());

    let worker = ShardWorkerBuilder::new(0, 1)
        .with_message_rx(ShardReceiver::new(msg_rx))
        .with_new_conn_rx(conn_rx)
        .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
        .with_registry(Arc::new(registry))
        .with_metrics(Arc::new(NoopMetricsRecorder::new()))
        .with_store(HashMapStore::new())
        .with_replication(broadcaster.clone())
        .with_scripting(ScriptingConfig {
            lua_time_limit_ms: time_limit_ms,
            lua_timeout_grace_ms: 0,
            ..ScriptingConfig::default()
        })
        .build();

    Fixture {
        worker,
        broadcaster,
        _conn_tx: conn_tx,
    }
}

impl Fixture {
    /// EVAL does not go through the command registry — the connection layer
    /// parses it and hands the shard a `ScriptingMsg::EvalScript`.
    async fn eval(&mut self, script: &str, key_prefix: &str) -> Response {
        let (tx, rx) = oneshot::channel();
        let msg = ScriptingMsg::EvalScript {
            script_source: Bytes::copy_from_slice(script.as_bytes()),
            keys: vec![Bytes::copy_from_slice(key_prefix.as_bytes())],
            argv: vec![],
            conn_id: 1,
            protocol_version: ProtocolVersion::Resp2,
            read_only: false,
            response_tx: tx,
        };
        self.worker.drive(msg).await;
        rx.await.expect("eval response")
    }
}

/// A **read-only** script that overruns the limit is still aborted, and the RESP
/// error *code* is `BUSY` — not `ERR` with the word BUSY demoted to prose.
///
/// This is the half of the deadline that option A keeps: nothing has been
/// written, so terminating the script costs nothing and bounds a runaway.
#[tokio::test]
async fn a_read_only_script_that_overruns_the_time_limit_is_aborted_with_busy() {
    let mut f = fixture(TIME_LIMIT_MS);
    let reply = f.eval(READ_ONLY_OVERRUN, "k").await;
    match &reply {
        Response::Error(e) => assert!(
            e.starts_with(b"BUSY"),
            "expected a BUSY abort, got {:?}",
            String::from_utf8_lossy(e),
        ),
        other => panic!("expected the read-only script to be aborted, got {other:?}"),
    }

    assert_eq!(
        f.worker.store.len(),
        0,
        "the read-only probe must not write anything",
    );
    assert!(
        f.broadcaster.command_names().is_empty(),
        "an aborted read-only script propagates nothing: {:?}",
        f.broadcaster.command_names(),
    );
}

/// **Issue 60, option A.** A script that has already written is never aborted by
/// `lua-time-limit`: it runs to completion, every write lands, and the whole set
/// propagates as one MULTI/EXEC batch identical to what the primary applied.
///
/// Against the pre-fix code this failed with a `BUSY` reply and only the first
/// `SET` applied.
#[tokio::test]
async fn a_write_dirty_script_is_never_aborted_by_the_time_limit() {
    let mut f = fixture(TIME_LIMIT_MS);
    let reply = f.eval(WRITE_DIRTY_OVERRUN, "k").await;
    assert_eq!(
        reply,
        Response::Integer(1),
        "a write-dirty script must run to completion despite overrunning \
         lua-time-limit (option A); got {reply:?}",
    );

    // Both writes landed on the primary — including the one *after* the
    // overrun, which the old abort path never reached.
    assert_eq!(f.worker.store.len(), 2, "both SETs must be applied");

    // ... and the replica sees exactly the same set, framed atomically.
    let sets = f.broadcaster.frames_named("SET");
    assert_eq!(
        sets.len(),
        2,
        "primary applied 2 keys but propagated {} SETs — replicas would diverge",
        sets.len(),
    );
    let names = f.broadcaster.command_names();
    assert_eq!(
        names,
        vec!["MULTI", "SET", "SET", "EXEC"],
        "a multi-write script batch must be MULTI/EXEC-framed: {names:?}",
    );
}
