//! Turmoil pub/sub harness (Phase 4c): drive a seeded [`PubSubWorkload`] against
//! the real server, recording a [`PubSubHistory`] for the pub/sub oracle.
//!
//! Pub/sub does not fit the request/reply `workload_runner` shape: a subscriber
//! connection, after SUBSCRIBE, only *receives* asynchronous frames. So this is
//! a dedicated runner — subscriber tasks subscribe (recording each confirmation
//! as a window-open), then read delivered frames until a drain deadline
//! (recording each as a Receive), then unsubscribe (window-close); publisher
//! tasks issue uniquely-tagged PUBLISHes bracketed by send/reply sequences.
//!
//! All records funnel through one `Arc<Mutex<PubSubHistory>>`, so the global
//! recording sequence is a linearization of the real interleaving under the
//! deterministic sim — exactly what the oracle's window brackets consume.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use frogdb_testing::{PubSubHistory, PubSubWorkload, SubKind};
use turmoil::net::TcpStream;

use super::sim_harness::{SimConfig, build_sim};
use super::sim_helpers::{
    SERVER_HOST, SERVER_PORT, encode_command, real_frogdb_server_fake_persistence,
};

type BoxError = Box<dyn std::error::Error>;

/// Sim-ms a subscriber keeps reading delivered frames before it unsubscribes.
/// Chosen well past every publisher's finish time (start delay ≤ ~260ms +
/// bounded per-message thinks), leaving a long idle tail to drain every
/// in-window message before the window closes.
const READ_DEADLINE_MS: u64 = 3_000;

/// Per-read timeout while polling a subscriber connection for the next frame.
const READ_STEP_MS: u64 = 100;

/// Final-drain idle timeout: after the deadline, keep reading until this long
/// passes with no further frame, so no already-delivered message is missed.
const DRAIN_IDLE_MS: u64 = 400;

/// Run `workload` against a real server in a seeded turmoil sim and return the
/// recorded pub/sub history.
pub fn run_pubsub_workload(workload: &PubSubWorkload, num_shards: usize) -> PubSubHistory {
    frogdb_core::shard::FakeWalRegistry::clear();

    let mut sim = build_sim(&SimConfig {
        seed: workload.seed,
        num_shards,
        ..SimConfig::default()
    });
    sim.host(SERVER_HOST, move || {
        real_frogdb_server_fake_persistence(num_shards)
    });

    let history = Arc::new(Mutex::new(PubSubHistory::new()));

    // Subscriber tasks.
    for sub in &workload.subscribers {
        let h = history.clone();
        let sub = sub.clone();
        sim.client(format!("sub{}", sub.client_id), async move {
            let addr = turmoil::lookup(SERVER_HOST);
            let mut stream = connect_with_retry(addr).await?;
            let mut buf = vec![0u8; 65536];
            let mut acc: Vec<u8> = Vec::with_capacity(4096);

            if sub.subscribe_think_ms > 0 {
                tokio::time::sleep(Duration::from_millis(sub.subscribe_think_ms)).await;
            }

            // SUBSCRIBE all channels in one command, then read exactly one
            // confirmation frame per channel, recording each window-open.
            if !sub.channels.is_empty() {
                let mut parts: Vec<&[u8]> = vec![b"SUBSCRIBE"];
                parts.extend(sub.channels.iter().map(|c| c.as_ref()));
                use tokio::io::AsyncWriteExt;
                stream.write_all(&encode_command(&parts)).await?;
                for _ in 0..sub.channels.len() {
                    if let Some(frame) =
                        read_frame(&mut stream, &mut buf, &mut acc, READ_STEP_MS * 4).await
                        && let Some((kind, _ch, _payload)) = classify_frame(&frame)
                        && let FrameKind::SubscribeConfirm(channel) = kind
                    {
                        h.lock()
                            .unwrap()
                            .record_subscribe_ack(sub.client_id, SubKind::Channel(channel));
                    }
                }
            }

            // PSUBSCRIBE each pattern, reading its confirmation.
            for pat in &sub.patterns {
                use tokio::io::AsyncWriteExt;
                stream
                    .write_all(&encode_command(&[b"PSUBSCRIBE", pat.as_ref()]))
                    .await?;
                if let Some(frame) =
                    read_frame(&mut stream, &mut buf, &mut acc, READ_STEP_MS * 4).await
                    && let Some((FrameKind::PSubscribeConfirm(pattern), _, _)) =
                        classify_frame(&frame)
                {
                    h.lock()
                        .unwrap()
                        .record_subscribe_ack(sub.client_id, SubKind::Pattern(pattern));
                }
            }

            // Read delivered frames until the deadline, recording each Receive.
            let start = tokio::time::Instant::now();
            while start.elapsed() < Duration::from_millis(READ_DEADLINE_MS) {
                match read_frame(&mut stream, &mut buf, &mut acc, READ_STEP_MS).await {
                    Some(frame) => record_message(&h, sub.client_id, &frame),
                    None => continue, // timeout: re-check the deadline
                }
            }

            // Final drain: keep reading until DRAIN_IDLE_MS passes with no
            // frame, so every already-delivered in-window message is recorded
            // BEFORE the window closes.
            while let Some(frame) = read_frame(&mut stream, &mut buf, &mut acc, DRAIN_IDLE_MS).await
            {
                record_message(&h, sub.client_id, &frame);
            }

            // Window close: record unsubscribe for every subscription, then
            // send the wire UNSUBSCRIBE/PUNSUBSCRIBE for realism.
            {
                let mut hist = h.lock().unwrap();
                for ch in &sub.channels {
                    hist.record_unsubscribe(sub.client_id, SubKind::Channel(ch.clone()));
                }
                for pat in &sub.patterns {
                    hist.record_unsubscribe(sub.client_id, SubKind::Pattern(pat.clone()));
                }
            }
            use tokio::io::AsyncWriteExt;
            let _ = stream.write_all(&encode_command(&[b"UNSUBSCRIBE"])).await;
            let _ = stream.write_all(&encode_command(&[b"PUNSUBSCRIBE"])).await;

            Ok::<(), BoxError>(())
        });
    }

    // Publisher tasks.
    for publisher in &workload.publishers {
        let h = history.clone();
        let publisher = publisher.clone();
        sim.client(format!("pub{}", publisher.client_id), async move {
            let addr = turmoil::lookup(SERVER_HOST);
            let mut stream = connect_with_retry(addr).await?;
            let mut buf = vec![0u8; 65536];
            let mut acc: Vec<u8> = Vec::with_capacity(4096);

            if publisher.start_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(publisher.start_delay_ms)).await;
            }

            for m in &publisher.messages {
                if m.think_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(m.think_ms)).await;
                }
                // Stamp send_seq under the lock, then write, then read the
                // integer reply and stamp reply_seq.
                let idx = h.lock().unwrap().record_publish_send(
                    publisher.client_id,
                    m.channel.clone(),
                    m.message.clone(),
                );
                use tokio::io::AsyncWriteExt;
                stream
                    .write_all(&encode_command(&[
                        b"PUBLISH",
                        m.channel.as_ref(),
                        m.message.as_ref(),
                    ]))
                    .await?;
                // The PUBLISH reply is a single integer frame.
                let _ = read_frame(&mut stream, &mut buf, &mut acc, READ_STEP_MS * 8).await;
                h.lock().unwrap().record_publish_reply(idx);
            }

            Ok::<(), BoxError>(())
        });
    }

    sim.run().expect("turmoil pub/sub sim run failed");
    history.lock().unwrap().clone()
}

/// Record a delivered `message`/`pmessage` frame as a `Receive`.
fn record_message(h: &Arc<Mutex<PubSubHistory>>, sub_client: u64, frame: &RespVal) {
    if let Some((kind, channel, payload)) = classify_frame(frame) {
        match kind {
            FrameKind::Message => {
                if let (Some(channel), Some(payload)) = (channel, payload) {
                    h.lock()
                        .unwrap()
                        .record_receive(sub_client, channel, payload, None);
                }
            }
            FrameKind::PMessage(pattern) => {
                if let (Some(channel), Some(payload)) = (channel, payload) {
                    h.lock()
                        .unwrap()
                        .record_receive(sub_client, channel, payload, Some(pattern));
                }
            }
            // Confirmations / integers / other frames are not deliveries.
            _ => {}
        }
    }
}

/// Connect with retry (the server host may not have bound its listener yet
/// under `enable_random_order`).
async fn connect_with_retry(addr: std::net::IpAddr) -> Result<TcpStream, BoxError> {
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

// ---- Minimal RESP2 frame reader ----------------------------------------

/// A parsed RESP2 value (only the shapes pub/sub traffic uses).
#[derive(Debug, Clone)]
pub enum RespVal {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Bytes>),
    Array(Vec<RespVal>),
}

/// Read the next complete RESP2 frame from `stream`, with a per-attempt
/// timeout. Returns `None` on timeout, EOF, or transport error — the caller
/// re-checks its own deadline. Partial bytes persist in `acc` across calls, so
/// a timed-out read never loses data.
async fn read_frame(
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
    step_ms: u64,
) -> Option<RespVal> {
    use tokio::io::AsyncReadExt;
    loop {
        if let Some((val, consumed)) = parse_at(acc, 0) {
            acc.drain(..consumed);
            return Some(val);
        }
        match tokio::time::timeout(Duration::from_millis(step_ms), stream.read(buf)).await {
            Ok(Ok(0)) => return None, // closed
            Ok(Ok(n)) => acc.extend_from_slice(&buf[..n]),
            Ok(Err(_)) => return None, // transport error
            Err(_) => return None,     // timeout
        }
    }
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

fn parse_at(d: &[u8], pos: usize) -> Option<(RespVal, usize)> {
    if pos >= d.len() {
        return None;
    }
    let cr = find_crlf(d, pos + 1)?;
    let header = &d[pos + 1..cr];
    let after = cr + 2;
    match d[pos] {
        b'+' => Some((
            RespVal::Simple(String::from_utf8_lossy(header).to_string()),
            after,
        )),
        b'-' => Some((
            RespVal::Error(String::from_utf8_lossy(header).to_string()),
            after,
        )),
        b':' => {
            let n = String::from_utf8_lossy(header).parse::<i64>().unwrap_or(0);
            Some((RespVal::Int(n), after))
        }
        // RESP3 Push frames (`>`) carry the same items as an Array — treat them
        // identically so a RESP3-negotiating server would still parse.
        b'$' => {
            let n = String::from_utf8_lossy(header).parse::<i64>().ok()?;
            if n < 0 {
                return Some((RespVal::Bulk(None), after));
            }
            let start = after;
            let end = start + n as usize;
            if end + 2 > d.len() {
                return None;
            }
            Some((
                RespVal::Bulk(Some(Bytes::copy_from_slice(&d[start..end]))),
                end + 2,
            ))
        }
        b'*' | b'>' => {
            let n = String::from_utf8_lossy(header).parse::<i64>().ok()?;
            if n < 0 {
                return Some((RespVal::Array(Vec::new()), after));
            }
            let mut items = Vec::with_capacity(n as usize);
            let mut p = after;
            for _ in 0..n {
                let (v, np) = parse_at(d, p)?;
                items.push(v);
                p = np;
            }
            Some((RespVal::Array(items), p))
        }
        _ => None,
    }
}

/// The pub/sub-relevant classification of a frame, plus its channel and payload
/// where applicable.
enum FrameKind {
    SubscribeConfirm(Bytes),
    PSubscribeConfirm(Bytes),
    Message,
    PMessage(Bytes),
    Other,
}

/// Classify a pub/sub frame array by its leading keyword.
/// Returns `(kind, channel, payload)` where `channel`/`payload` are the
/// message's channel and payload for delivery frames.
fn classify_frame(frame: &RespVal) -> Option<(FrameKind, Option<Bytes>, Option<Bytes>)> {
    let RespVal::Array(items) = frame else {
        return None;
    };
    let head = match items.first() {
        Some(RespVal::Bulk(Some(b))) => b.clone(),
        _ => return None,
    };
    match head.as_ref() {
        b"subscribe" => {
            let ch = bulk_at(items, 1)?;
            Some((FrameKind::SubscribeConfirm(ch), None, None))
        }
        b"psubscribe" => {
            let pat = bulk_at(items, 1)?;
            Some((FrameKind::PSubscribeConfirm(pat), None, None))
        }
        b"message" => {
            let ch = bulk_at(items, 1);
            let payload = bulk_at(items, 2);
            Some((FrameKind::Message, ch, payload))
        }
        b"pmessage" => {
            let pat = bulk_at(items, 1)?;
            let ch = bulk_at(items, 2);
            let payload = bulk_at(items, 3);
            Some((FrameKind::PMessage(pat), ch, payload))
        }
        _ => Some((FrameKind::Other, None, None)),
    }
}

fn bulk_at(items: &[RespVal], idx: usize) -> Option<Bytes> {
    match items.get(idx) {
        Some(RespVal::Bulk(Some(b))) => Some(b.clone()),
        _ => None,
    }
}
