//! Turmoil harness v2: execute a generated [`Workload`] as N concurrent sim
//! clients over turmoil TCP, recording a canonical `OperationHistory`, and
//! return the completed `frogdb_testing::History`.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use frogdb_testing::workload::Workload;
use turmoil::net::TcpStream;

use super::quiescence_probe::QuiescenceSnapshots;
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
}

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

    for script in &workload.clients {
        let h = history.clone();
        let script = script.clone();
        let client_name = format!("client{}", script.client_id);
        sim.client(client_name, async move {
            let addr = turmoil::lookup(SERVER_HOST);
            let mut stream = connect_with_retry(addr).await?;
            let mut buf = vec![0u8; 65536];
            let mut acc: Vec<u8> = Vec::with_capacity(4096);

            for op in &script.ops {
                if op.think_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(op.think_ms)).await;
                }
                run_op(&h, script.client_id, op, &mut stream, &mut buf, &mut acc).await?;
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        });
    }

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
    let quiescence = quiescence.lock().unwrap().clone();
    CapturedRun {
        history,
        final_elements,
        quiescence,
    }
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
