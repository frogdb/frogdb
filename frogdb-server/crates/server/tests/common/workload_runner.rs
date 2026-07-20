//! Turmoil harness v2: execute a generated [`Workload`] as N concurrent sim
//! clients over turmoil TCP, recording a canonical `OperationHistory`, and
//! return the completed `frogdb_testing::History`.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use frogdb_testing::workload::Workload;
use frogdb_testing::{WaiterOrdinal, WaiterRegistrationOrder};
use turmoil::net::TcpStream;

use super::quiescence_probe::{QuiescenceSnapshots, parse_waitqueue};
use super::sim_harness::{OperationHistory, OperationResult, SimConfig, build_sim};
use super::sim_helpers::{
    SERVER_HOST, SERVER_PORT, encode_command, real_frogdb_server,
    real_frogdb_server_fake_persistence,
};

// Matches turmoil's `Result` error type exactly, so `?` needs no conversion
// between the client-future closures and the helper fns.
type BoxError = Box<dyn std::error::Error>;

/// Execute `workload` against a real server inside a seeded turmoil sim and
/// return the recorded, canonicalized [`frogdb_testing::History`].
///
/// `fake_persistence` selects the deterministic WAL fake (its effect log is
/// reachable via `frogdb_core::shard::FakeWalRegistry`); otherwise persistence
/// is disabled (`real_frogdb_server`).
pub fn run_workload(
    workload: &Workload,
    num_shards: usize,
    fake_persistence: bool,
) -> frogdb_testing::History {
    run_workload_capturing(workload, num_shards, fake_persistence).history
}

/// The captured outputs of a workload run: the recorded history, the final
/// list contents for exactly-once conservation, and the tier-4 quiescence
/// snapshots gathered once the server drained.
pub struct CapturedRun {
    pub history: frogdb_testing::History,
    pub final_elements: std::collections::HashMap<Bytes, Vec<Bytes>>,
    pub quiescence: QuiescenceSnapshots,
    /// True registration order gathered by the mid-run `DEBUG WAITQUEUE`
    /// prober, correlated to workload clients via the CLIENT ID map. Feeds the
    /// exact FIFO wake-order checker. Empty when no waiter was ever observed
    /// (e.g. a workload with no blocking pops).
    pub registration_order: WaiterRegistrationOrder,
}

/// Prober cadence: how often (sim-ms) the prober samples `DEBUG WAITQUEUE`.
/// Well under the multi-waiter blocking timeout so every concurrent waiter's
/// registration ordinal is observed while it is parked.
const PROBE_INTERVAL_MS: u64 = 50;

/// Sim-time (ms) the drainer waits before reading final list state — long
/// enough for every client script (short think delays + sub-second blocking
/// timeouts) to finish, well under the 60s sim duration cap.
const DRAIN_SETTLE_MS: u64 = 30_000;

/// Like [`run_workload`], but also captures the final list contents (via
/// LRANGE readback after all clients finish) as `final_elements` for the
/// exactly-once conservation checker. Non-list keys reply WRONGTYPE and are
/// skipped; the readback ops are NOT recorded into the returned history.
pub fn run_workload_capturing(
    workload: &Workload,
    num_shards: usize,
    fake_persistence: bool,
) -> CapturedRun {
    // Isolate the process-global fake-WAL registry from previous runs.
    frogdb_core::shard::FakeWalRegistry::clear();

    let mut sim = build_sim(&SimConfig {
        seed: workload.seed,
        num_shards,
        ..SimConfig::default()
    });

    if fake_persistence {
        sim.host(SERVER_HOST, move || {
            real_frogdb_server_fake_persistence(num_shards)
        });
    } else {
        sim.host(SERVER_HOST, move || real_frogdb_server(num_shards));
    }

    let history = Arc::new(Mutex::new(OperationHistory::new()));

    /// Connect with retry: under `enable_random_order()` a seeded schedule can
    /// run a client's connect before the server host has bound its listener,
    /// yielding ConnectionRefused. Retry over sim-time until the server is up.
    async fn connect_with_retry(
        addr: std::net::IpAddr,
    ) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let mut last_err: Option<std::io::Error> = None;
        for _ in 0..50 {
            match TcpStream::connect((addr, SERVER_PORT)).await {
                Ok(s) => return Ok(s),
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(last_err.expect("retry loop ran at least once").into())
    }

    // client_id → server conn_id, populated by each client's `CLIENT ID` at
    // connect. Inverted after the sim to correlate WAITQUEUE waiter conn_ids
    // back to workload clients. `CLIENT ID` returns `conn_state.id()`, the SAME
    // id the shard records for a blocking waiter (both are `self.state.id`; see
    // client_conn_command.rs::conn_id and connection/handlers/blocking.rs's
    // BlockWait), so this join is over one id space.
    let client_ids: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));

    // Mid-run prober observations: (shard, waiter ordinal) folded on every poll.
    let waiter_obs: Arc<Mutex<Vec<(u16, WaiterOrdinal)>>> = Arc::new(Mutex::new(Vec::new()));

    // Post-workload PING sweep: AND-fold, across every workload client (NOT
    // the prober — it's not a workload client), whether its connection still
    // replied `+PONG` after its scripted ops finished. A leaked/wedged
    // connection or a shard that stopped servicing one is a bug even when the
    // wait queue looks empty, so this feeds `QuiescenceSnapshots::
    // connections_responsive` (see quiescence_probe.rs).
    let connections_responsive: Arc<Mutex<bool>> = Arc::new(Mutex::new(true));

    for script in &workload.clients {
        let h = history.clone();
        let script = script.clone();
        let client_ids = client_ids.clone();
        let connections_responsive = connections_responsive.clone();
        let client_name = format!("client{}", script.client_id);
        sim.client(client_name, async move {
            let addr = turmoil::lookup(SERVER_HOST);
            let mut stream = connect_with_retry(addr).await?;
            let mut buf = vec![0u8; 65536];
            let mut acc: Vec<u8> = Vec::with_capacity(4096);

            // Register this client's server-side conn_id for the WAITQUEUE join.
            if let Some(conn_id) = client_id_query(&mut stream, &mut buf, &mut acc).await? {
                client_ids.lock().unwrap().insert(script.client_id, conn_id);
            }

            for op in &script.ops {
                if op.think_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(op.think_ms)).await;
                }
                run_op(&h, script.client_id, op, &mut stream, &mut buf, &mut acc).await?;
            }

            // Final action: a trailing PING this connection must still answer.
            // A transport error counts as unresponsive too, rather than
            // aborting the whole sim run.
            let responsive = ping_is_pong(&mut stream, &mut buf, &mut acc)
                .await
                .unwrap_or(false);
            if !responsive {
                *connections_responsive.lock().unwrap() = false;
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        });
    }

    // Prober: a dedicated read-only client that polls `DEBUG WAITQUEUE` every
    // ~50 sim-ms until the drain settles, folding every observed waiter's
    // registration ordinal into `waiter_obs`. Long-timeout multi-waiter pops
    // stay parked far longer than the cadence, so each concurrent waiter is
    // captured while blocked.
    let obs_out = waiter_obs.clone();
    sim.client("prober", async move {
        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = connect_with_retry(addr).await?;
        let mut buf = vec![0u8; 65536];
        let mut acc: Vec<u8> = Vec::with_capacity(4096);
        let mut elapsed = 0u64;
        while elapsed < DRAIN_SETTLE_MS {
            tokio::time::sleep(Duration::from_millis(PROBE_INTERVAL_MS)).await;
            elapsed += PROBE_INTERVAL_MS;
            let waitqueue = debug_probe(&mut stream, &mut buf, &mut acc, b"WAITQUEUE").await?;
            let snaps = parse_waitqueue(&waitqueue);
            let mut obs = obs_out.lock().unwrap();
            for snap in snaps {
                let shard = snap.shard_id as u16;
                for w in snap.waiters {
                    obs.push((shard, w));
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    });

    // Drainer: after all clients finish, read final list contents for every
    // key (LRANGE); non-list keys reply WRONGTYPE and are skipped.
    let final_elements: Arc<Mutex<std::collections::HashMap<Bytes, Vec<Bytes>>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    let quiescence: Arc<Mutex<QuiescenceSnapshots>> =
        Arc::new(Mutex::new(QuiescenceSnapshots::default()));
    let drain_out = final_elements.clone();
    let quiescence_out = quiescence.clone();
    let keys = Workload::key_space(workload.seed);
    sim.client("drainer", async move {
        tokio::time::sleep(Duration::from_millis(DRAIN_SETTLE_MS)).await;
        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = connect_with_retry(addr).await?;
        let mut buf = vec![0u8; 65536];
        let mut acc: Vec<u8> = Vec::with_capacity(4096);
        for key in &keys {
            use tokio::io::AsyncWriteExt;
            let parts: Vec<&[u8]> = vec![b"LRANGE", key.as_ref(), b"0", b"-1"];
            stream.write_all(&encode_command(&parts)).await?;
            let reply = read_reply(&mut stream, &mut buf, &mut acc).await?;
            if let OperationResult::Array(items) = reply {
                let elems: Vec<Bytes> = items
                    .into_iter()
                    .filter_map(|it| match it {
                        OperationResult::String(b) => Some(b),
                        _ => None,
                    })
                    .collect();
                drain_out.lock().unwrap().insert(key.clone(), elems);
            }
        }

        // Tier-4 quiescence probe: the workload has drained, so the four DEBUG
        // introspection commands report the settled server state. Parse their
        // RESP replies into snapshots for the invariant pipeline's stage 4.
        let locktable = debug_probe(&mut stream, &mut buf, &mut acc, b"LOCKTABLE").await?;
        let waitqueue = debug_probe(&mut stream, &mut buf, &mut acc, b"WAITQUEUE").await?;
        let memory = debug_probe(&mut stream, &mut buf, &mut acc, b"MEMORY-CHECK").await?;
        let expiry = debug_probe(&mut stream, &mut buf, &mut acc, b"EXPIRY-INDEX-CHECK").await?;
        *quiescence_out.lock().unwrap() =
            QuiescenceSnapshots::from_replies(&locktable, &waitqueue, &memory, &expiry);

        Ok::<(), Box<dyn std::error::Error>>(())
    });

    sim.run().expect("turmoil sim run failed");

    let history = history.lock().unwrap().to_testing_history();
    let final_elements = final_elements.lock().unwrap().clone();
    let mut quiescence = quiescence.lock().unwrap().clone();
    quiescence.connections_responsive = *connections_responsive.lock().unwrap();

    // Build the true registration order: invert the CLIENT ID map (conn_id →
    // client_id) and join each observed waiter ordinal onto its workload client.
    let conn_to_client: HashMap<u64, u64> = client_ids
        .lock()
        .unwrap()
        .iter()
        .map(|(&client_id, &conn_id)| (conn_id, client_id))
        .collect();
    // Key-encoding round-trip guard: the WAITQUEUE dump renders each parked key
    // via `format_key_for_display` (lossy UTF-8), which must reproduce the
    // served-key bytes the recorder joins on. The generator's keys are all
    // drawn from `key_space`, so an observed waiter key that is NOT in it means
    // the display encoding diverged and the `(served_key, client_id)` join
    // would silently miss — fail loudly instead.
    let key_space: std::collections::HashSet<Bytes> =
        Workload::key_space(workload.seed).into_iter().collect();
    let mut registration_order = WaiterRegistrationOrder::default();
    for (_shard, w) in waiter_obs.lock().unwrap().iter() {
        let key = Bytes::from(w.key.clone());
        assert!(
            key_space.contains(&key),
            "prober observed waiter on key {:?} outside the workload key space — \
             WAITQUEUE key encoding does not round-trip to the served key",
            String::from_utf8_lossy(&w.key)
        );
        if let Some(&client_id) = conn_to_client.get(&w.conn_id) {
            registration_order.insert(key, client_id, w.registration_seq);
        }
    }

    CapturedRun {
        history,
        final_elements,
        quiescence,
        registration_order,
    }
}

/// Issue `CLIENT ID` and return the integer connection id (the server-side
/// `conn_state.id()`), or `None` if the reply was not an integer.
async fn client_id_query(
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
) -> Result<Option<u64>, BoxError> {
    use tokio::io::AsyncWriteExt;
    stream
        .write_all(&encode_command(&[b"CLIENT", b"ID"]))
        .await?;
    let reply = read_reply(stream, buf, acc).await?;
    Ok(match reply {
        OperationResult::Integer(n) if n >= 0 => Some(n as u64),
        _ => None,
    })
}

/// Send `PING` on `stream` and report whether the reply was `+PONG`. Used as
/// each workload client's trailing action in the post-drain responsiveness
/// sweep (see `connections_responsive` in `run_workload_capturing`).
async fn ping_is_pong(
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
) -> Result<bool, BoxError> {
    use tokio::io::AsyncWriteExt;
    stream.write_all(&encode_command(&[b"PING"])).await?;
    let reply = read_reply(stream, buf, acc).await?;
    Ok(matches!(&reply, OperationResult::String(b) if b.as_ref() == b"PONG"))
}

/// Issue `DEBUG <subcommand>` on `stream` and return the parsed reply.
async fn debug_probe(
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
    subcommand: &[u8],
) -> Result<OperationResult, BoxError> {
    use tokio::io::AsyncWriteExt;
    stream
        .write_all(&encode_command(&[b"DEBUG", subcommand]))
        .await?;
    read_reply(stream, buf, acc).await
}

/// Execute a single scripted op on `stream`, recording invoke/return.
async fn run_op(
    history: &Arc<Mutex<OperationHistory>>,
    client_id: u64,
    op: &frogdb_testing::ScriptedOp,
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
) -> Result<(), BoxError> {
    use tokio::io::AsyncWriteExt;

    match op.command.as_str() {
        // DISCARD outside MULTI is a protocol error; the generator only ever
        // emits it standalone, so skip it (it would just record an error that
        // no model routes).
        "discard" => Ok(()),
        "exec" => run_exec(history, client_id, op, stream, buf, acc).await,
        cmd if cmd.starts_with("script_") => {
            run_script(history, client_id, op, stream, buf, acc).await
        }
        _ => {
            let op_id = {
                let mut h = history.lock().unwrap();
                h.record_invoke(client_id, op.command.to_uppercase(), op.args.clone())
            };
            let wire = wire_parts(&op.command, &op.args);
            let refs: Vec<&[u8]> = wire.iter().map(|b| b.as_ref()).collect();
            let cmd = encode_command(&refs);
            stream.write_all(&cmd).await?;
            let reply = read_reply(stream, buf, acc).await?;
            {
                let mut h = history.lock().unwrap();
                h.record_return_canonical(op_id, client_id, &op.command, reply);
            }
            Ok(())
        }
    }
}

/// Execute a pool script pseudo-op as real wire traffic, recording the
/// pseudo-op + canonical result.
///
/// The pseudo-op name (`script_getset`, …) indexes [`SCRIPT_POOL`] for the Lua
/// source; the client-side SHA-1 of that source matches FrogDB's digest scheme
/// (`frogdb_core::scripting::cache::compute_sha` — standard SHA-1, lowercase
/// hex), so an EVALSHA issued before the script has ever been EVAL'd genuinely
/// races the server's SHA cache and returns NOSCRIPT, which we recover from by
/// EVAL of the source (which also warms the cache for later EVALSHA hits).
///
/// EVAL vs EVALSHA is chosen **deterministically** from the seeded op (a stable
/// FNV-1a parity over `client_id` + args) — no runtime randomness, so a given
/// seed reproduces byte-identical wire traffic while still exercising both the
/// cold NOSCRIPT→EVAL path and the warm-cache EVALSHA path.
async fn run_script(
    history: &Arc<Mutex<OperationHistory>>,
    client_id: u64,
    op: &frogdb_testing::ScriptedOp,
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
) -> Result<(), BoxError> {
    use tokio::io::AsyncWriteExt;

    let src = frogdb_testing::workload::SCRIPT_POOL
        .iter()
        .find(|(n, _)| *n == op.command)
        .map(|(_, s)| *s)
        .expect("unknown script pseudo-op");
    let key = op.args[0].clone();
    let argv: Vec<Bytes> = op.args[1..].to_vec();
    let numkeys = Bytes::from("1");

    let op_id = {
        let mut h = history.lock().unwrap();
        h.record_invoke(client_id, op.command.to_uppercase(), op.args.clone())
    };

    let reply = if prefer_evalsha(client_id, op) {
        // EVALSHA first: hits the SHA cache when warm, else NOSCRIPT → EVAL.
        let sha = hex_sha1(src.as_bytes());
        let mut evalsha: Vec<&[u8]> =
            vec![b"EVALSHA", sha.as_bytes(), numkeys.as_ref(), key.as_ref()];
        evalsha.extend(argv.iter().map(|a| a.as_ref()));
        stream.write_all(&encode_command(&evalsha)).await?;
        let reply = read_reply(stream, buf, acc).await?;
        if matches!(&reply, OperationResult::Error(e) if e.starts_with("NOSCRIPT")) {
            eval_source(src, &numkeys, &key, &argv, stream, buf, acc).await?
        } else {
            reply
        }
    } else {
        // Plain EVAL: always executes and warms the cache as a side effect.
        eval_source(src, &numkeys, &key, &argv, stream, buf, acc).await?
    };

    {
        let mut h = history.lock().unwrap();
        h.record_return_canonical(op_id, client_id, &op.command, reply);
    }
    Ok(())
}

/// Issue `EVAL <src> <numkeys> <key> <argv…>` and return the parsed reply.
async fn eval_source(
    src: &str,
    numkeys: &Bytes,
    key: &Bytes,
    argv: &[Bytes],
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
) -> Result<OperationResult, BoxError> {
    use tokio::io::AsyncWriteExt;
    let mut eval: Vec<&[u8]> = vec![b"EVAL", src.as_bytes(), numkeys.as_ref(), key.as_ref()];
    eval.extend(argv.iter().map(|a| a.as_ref()));
    stream.write_all(&encode_command(&eval)).await?;
    read_reply(stream, buf, acc).await
}

/// Lowercase-hex SHA-1 of `src`, matching FrogDB's EVALSHA digest scheme
/// (`frogdb_core::scripting::cache::sha_to_hex`).
fn hex_sha1(src: &[u8]) -> String {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(src);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Deterministic per-op choice of EVALSHA-first vs plain EVAL. FNV-1a over
/// `client_id` and the op args — a pure function of the seeded workload, so the
/// wire traffic is reproducible for a given seed (no runtime randomness).
fn prefer_evalsha(client_id: u64, op: &frogdb_testing::ScriptedOp) -> bool {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut mix = |b: u8| {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    };
    for b in client_id.to_le_bytes() {
        mix(b);
    }
    for a in &op.args {
        for &b in a.iter() {
            mix(b);
        }
    }
    h & 1 == 0
}

/// Drive an EXEC transaction: MULTI → queued sub-commands → EXEC, recording via
/// `record_exec_invoke`/`record_exec_return`.
async fn run_exec(
    history: &Arc<Mutex<OperationHistory>>,
    client_id: u64,
    op: &frogdb_testing::ScriptedOp,
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
) -> Result<(), BoxError> {
    use tokio::io::AsyncWriteExt;

    let cmds = parse_exec_commands(&op.args);
    if cmds.is_empty() {
        return Ok(());
    }

    let op_id = {
        let mut h = history.lock().unwrap();
        h.record_exec_invoke(client_id, &cmds)
    };

    // MULTI
    stream
        .write_all(&encode_command(&[b"MULTI" as &[u8]]))
        .await?;
    let _ = read_reply(stream, buf, acc).await?;

    // Queue each sub-command.
    for (name, cargs) in &cmds {
        let mut parts: Vec<&[u8]> = vec![name.as_bytes()];
        parts.extend(cargs.iter().map(|b| b.as_ref()));
        stream.write_all(&encode_command(&parts)).await?;
        let _ = read_reply(stream, buf, acc).await?; // +QUEUED
    }

    // EXEC → array of sub-results (or nil if aborted by WATCH).
    stream
        .write_all(&encode_command(&[b"EXEC" as &[u8]]))
        .await?;
    let reply = read_reply(stream, buf, acc).await?;
    {
        let mut h = history.lock().unwrap();
        record_exec_reply(&mut h, op_id, client_id, reply);
    }
    Ok(())
}

/// Record the reply to an EXEC. A WATCH-aborted EXEC returns RESP nil and MUST
/// be recorded as `None` (an aborted transaction) — NOT as an empty array,
/// which `record_exec_return` would encode to `Some("")`. The empty-string
/// encoding is misread by `KVModel::exec` as a committed-but-empty EXEC and by
/// `check_watch_no_false_negative` as a committed transaction, manufacturing
/// false non-linearizability and false WATCH violations.
fn record_exec_reply(h: &mut OperationHistory, op_id: u64, client_id: u64, reply: OperationResult) {
    match reply {
        OperationResult::Nil => h.record_return(op_id, client_id, OperationResult::Nil),
        OperationResult::Array(items) => h.record_exec_return(op_id, client_id, &items),
        other => h.record_exec_return(op_id, client_id, &[other]),
    }
}

/// Build the on-the-wire command parts for a model-level op. Only XREAD needs
/// massaging: the model args are `[key, after_id]` but the wire form is
/// `XREAD STREAMS key after_id`. Everything else is `[COMMAND, args...]`.
fn wire_parts(command: &str, args: &[Bytes]) -> Vec<Bytes> {
    if command == "xread" && args.len() == 2 {
        return vec![
            Bytes::from("XREAD"),
            Bytes::from("STREAMS"),
            args[0].clone(),
            args[1].clone(),
        ];
    }
    let mut parts = Vec::with_capacity(args.len() + 1);
    parts.push(Bytes::from(command.to_uppercase()));
    parts.extend(args.iter().cloned());
    parts
}

/// Parse the `[num_cmds, name, num_args, args...]` EXEC encoding into
/// `(name, args)` sub-commands (mirrors `partition::parse_exec_commands`).
fn parse_exec_commands(args: &[Bytes]) -> Vec<(String, Vec<Bytes>)> {
    let mut out = Vec::new();
    let Some(num) = args
        .first()
        .and_then(|b| String::from_utf8_lossy(b).parse::<usize>().ok())
    else {
        return out;
    };
    let mut idx = 1;
    for _ in 0..num {
        let Some(name) = args.get(idx) else {
            break;
        };
        let name = String::from_utf8_lossy(name).to_string();
        idx += 1;
        let Some(na) = args
            .get(idx)
            .and_then(|b| String::from_utf8_lossy(b).parse::<usize>().ok())
        else {
            break;
        };
        idx += 1;
        let mut cargs = Vec::with_capacity(na);
        for _ in 0..na {
            let Some(a) = args.get(idx) else {
                break;
            };
            cargs.push(a.clone());
            idx += 1;
        }
        out.push((name, cargs));
    }
    out
}

/// Read one complete RESP reply from `stream`, buffering across reads.
async fn read_reply(
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
) -> Result<OperationResult, BoxError> {
    use tokio::io::AsyncReadExt;

    loop {
        if let Some((value, consumed)) = try_parse(acc) {
            acc.drain(..consumed);
            return Ok(value);
        }
        let n = stream.read(buf).await?;
        if n == 0 {
            // Connection closed with an incomplete reply.
            return Ok(OperationResult::Error("connection closed".into()));
        }
        acc.extend_from_slice(&buf[..n]);
    }
}

/// Try to parse a single RESP2 value from the front of `data`. Returns
/// `Some((value, bytes_consumed))` or `None` if `data` is incomplete.
fn try_parse(data: &[u8]) -> Option<(OperationResult, usize)> {
    parse_at(data, 0)
}

fn find_crlf(data: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < data.len() {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_at(d: &[u8], pos: usize) -> Option<(OperationResult, usize)> {
    if pos >= d.len() {
        return None;
    }
    let cr = find_crlf(d, pos + 1)?;
    let header = &d[pos + 1..cr];
    let after = cr + 2;
    match d[pos] {
        b'+' => {
            let s = String::from_utf8_lossy(header).to_string();
            let v = if s == "OK" {
                OperationResult::Ok
            } else {
                OperationResult::String(Bytes::from(s))
            };
            Some((v, after))
        }
        b'-' => Some((
            OperationResult::Error(String::from_utf8_lossy(header).to_string()),
            after,
        )),
        b':' => {
            let n = String::from_utf8_lossy(header).parse::<i64>().unwrap_or(0);
            Some((OperationResult::Integer(n), after))
        }
        b'$' => {
            let n = String::from_utf8_lossy(header).parse::<i64>().ok()?;
            if n < 0 {
                return Some((OperationResult::Nil, after));
            }
            let start = after;
            let end = start + n as usize;
            if end + 2 > d.len() {
                return None; // incomplete
            }
            Some((
                OperationResult::String(Bytes::copy_from_slice(&d[start..end])),
                end + 2,
            ))
        }
        b'*' => {
            let n = String::from_utf8_lossy(header).parse::<i64>().ok()?;
            if n < 0 {
                return Some((OperationResult::Nil, after));
            }
            let mut items = Vec::with_capacity(n as usize);
            let mut p = after;
            for _ in 0..n {
                let (v, np) = parse_at(d, p)?;
                items.push(v);
                p = np;
            }
            Some((OperationResult::Array(items), p))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_testing::Profile;

    #[test]
    fn tiny_workload_runs_and_records() {
        let w = Workload::generate(1, Profile::Mixed, 2, 5);
        let history = run_workload(&w, 1, true);
        assert!(history.is_complete(), "every invoke must have a return");
        assert!(!history.completed_operations().is_empty());
    }

    #[test]
    fn tiny_script_workload_runs() {
        let w = Workload::generate(2, Profile::Mixed, 2, 8);
        let history = run_workload(&w, 1, true);
        assert!(history.is_complete());
    }

    #[test]
    fn run_workload_is_deterministic() {
        let w = Workload::generate(3, Profile::Mixed, 2, 6);
        let a = run_workload(&w, 1, true);
        let b = run_workload(&w, 1, true);
        let tuples = |h: &frogdb_testing::History| -> Vec<(u64, String, Option<Vec<u8>>)> {
            h.completed_operations()
                .iter()
                .map(|c| {
                    (
                        c.client_id,
                        c.function.clone(),
                        c.result.as_ref().map(|b| b.to_vec()),
                    )
                })
                .collect()
        };
        assert_eq!(tuples(&a), tuples(&b));
    }

    #[test]
    fn resp_parser_handles_nested_arrays() {
        // *1 -> *2 -> [$2 st, *1 -> *2 -> [$3 1-1, *2 -> f, v]]
        let reply =
            b"*1\r\n*2\r\n$2\r\nst\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n";
        let (val, consumed) = try_parse(reply).expect("complete");
        assert_eq!(consumed, reply.len());
        assert!(matches!(val, OperationResult::Array(_)));
    }

    #[test]
    fn resp_parser_reports_incomplete() {
        assert!(try_parse(b"$3\r\nab").is_none());
    }

    #[test]
    fn aborted_exec_records_none_not_empty() {
        // A WATCH-aborted EXEC (RESP nil) must land in the history as an
        // aborted transaction (result None), never as Some("").
        let mut h = OperationHistory::new();
        let op = h.record_exec_invoke(
            1,
            &[("set".to_string(), vec![Bytes::from("k"), Bytes::from("v")])],
        );
        record_exec_reply(&mut h, op, 1, OperationResult::Nil);

        let th = h.to_testing_history();
        let completed = th.completed_operations();
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].result, None,
            "aborted EXEC must record None, not Some(\"\")"
        );
    }

    #[test]
    fn committed_exec_records_encoded_results() {
        // A committed EXEC (array reply) still records the pipe-encoded results.
        let mut h = OperationHistory::new();
        let op = h.record_exec_invoke(
            1,
            &[("set".to_string(), vec![Bytes::from("k"), Bytes::from("v")])],
        );
        record_exec_reply(
            &mut h,
            op,
            1,
            OperationResult::Array(vec![OperationResult::Ok]),
        );

        let th = h.to_testing_history();
        let completed = th.completed_operations();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].result.as_deref(), Some(&b"OK"[..]));
    }
}
