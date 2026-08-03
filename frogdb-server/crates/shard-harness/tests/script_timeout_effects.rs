//! Level-3 scenario for issue 60: what a `lua-time-limit` abort leaves behind.
//!
//! When a script overruns `lua_time_limit_ms`, the sandbox instruction hook
//! raises a `BUSY` runtime error. `run_script` has by then already drained
//! `ctx.effects.script_writes` and pushes them through
//! `run_script_write_effects` unconditionally, so the sub-commands that
//! completed before the abort really land — on the primary *and* on replicas.
//!
//! Why level 3: the subject is the effect pipeline (what is stored vs. what is
//! propagated, and how the propagated batch is framed). Only the worker
//! produces both, and a `RecordingBroadcaster` can see them side by side. A
//! level-4 server test would add a socket and a clock without adding signal for
//! *these* assertions.

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

/// A script that writes forever, one key per iteration.
const RUNAWAY_WRITER: &str = "for i=1,1000000000 do redis.call('SET', KEYS[1]..i, i) end return 1";

/// A script that spins forever without writing anything.
const RUNAWAY_READER: &str = "local n=0 for i=1,1000000000 do n=n+i end return n";

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

/// A script that overruns the limit is aborted, and the RESP error *code* is
/// `BUSY` — not `ERR` with the word BUSY demoted to prose.
///
/// **Fails against the pre-fix code**: `ScriptError::Timeout` rendered
/// `ERR BUSY Lua script running. ...`, so a client switching on the error code
/// (as the architecture glossary's error table says it may) saw `ERR`.
#[tokio::test]
async fn a_script_that_overruns_the_time_limit_is_aborted_with_busy() {
    let mut f = fixture(50);
    let reply = f.eval(RUNAWAY_READER, "k").await;
    match &reply {
        Response::Error(e) => assert!(
            e.starts_with(b"BUSY"),
            "expected a BUSY abort, got {:?}",
            String::from_utf8_lossy(e),
        ),
        other => panic!("expected the script to be aborted, got {other:?}"),
    }
}

/// **Issue 60, the characterization.** Whatever a timed-out *write* script
/// leaves on the primary, replicas must see exactly the same set, framed as one
/// atomic MULTI/EXEC batch. This holds under either candidate policy ("no
/// writes survive" or "writes survive and replicate identically"), so it is
/// safe to pin before the policy question is settled.
#[tokio::test]
async fn a_timed_out_write_script_replicates_exactly_what_it_applied() {
    let mut f = fixture(50);
    let reply = f.eval(RUNAWAY_WRITER, "k").await;
    match &reply {
        Response::Error(e) => assert!(
            e.starts_with(b"BUSY"),
            "expected a BUSY abort, got {:?}",
            String::from_utf8_lossy(e),
        ),
        other => panic!("expected the script to be aborted, got {other:?}"),
    }

    let applied = f.worker.store.len();
    let sets = f.broadcaster.frames_named("SET");
    assert_eq!(
        sets.len(),
        applied,
        "primary applied {applied} keys but propagated {} SETs — replicas would diverge",
        sets.len(),
    );

    let names = f.broadcaster.command_names();
    if applied > 1 {
        assert_eq!(
            names.first().map(String::as_str),
            Some("MULTI"),
            "a multi-write script batch must be MULTI-framed: {names:?}",
        );
        assert_eq!(
            names.last().map(String::as_str),
            Some("EXEC"),
            "a multi-write script batch must be EXEC-framed: {names:?}",
        );
    }

    assert!(
        applied > 0,
        "the probe script never got a write in before the abort; the scenario \
         under test did not happen",
    );
}
