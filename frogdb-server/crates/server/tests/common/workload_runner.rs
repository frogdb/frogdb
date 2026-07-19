//! Turmoil harness v2: execute a generated [`Workload`] as N concurrent sim
//! clients over turmoil TCP, recording a canonical `OperationHistory`, and
//! return the completed `frogdb_testing::History`.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use frogdb_testing::workload::Workload;
use turmoil::net::TcpStream;

use super::sim_harness::{OperationHistory, OperationResult, SimConfig, build_sim};
use super::sim_helpers::{
    SERVER_HOST, SERVER_PORT, encode_command, real_frogdb_server,
    real_frogdb_server_fake_persistence,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

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

    for script in &workload.clients {
        let h = history.clone();
        let script = script.clone();
        let client_name = format!("client{}", script.client_id);
        sim.client(client_name, async move {
            let addr = turmoil::lookup(SERVER_HOST);
            let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
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

    sim.run().expect("turmoil sim run failed");

    let history = history.lock().unwrap();
    history.to_testing_history()
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
    let results: Vec<OperationResult> = match reply {
        OperationResult::Array(items) => items,
        OperationResult::Nil => Vec::new(),
        other => vec![other],
    };
    {
        let mut h = history.lock().unwrap();
        h.record_exec_return(op_id, client_id, &results);
    }
    Ok(())
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
}
