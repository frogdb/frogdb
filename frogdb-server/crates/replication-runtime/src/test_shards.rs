//! Fake shards for this crate's own unit tests.
//!
//! Every seam here reaches its shards through [`ShardSender`] and nothing else,
//! so a test can stand a whole seam up by holding the other end of those
//! channels itself: no shard worker, no store, no server, no socket. Nothing is
//! spawned either — the test serves each message by hand, so the order a seam
//! produces is the order the test observes, and a seam that sends *nothing*
//! fails on a bounded wait instead of hanging the suite.

#![allow(dead_code)] // one helper surface shared by three test modules

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{
    CoreMsg, Envelope, ReplicationMsg, ShardMessage, ShardSender, SnapshotEntry, TransactionResult,
    Value,
};
use frogdb_protocol::{ParsedCommand, Response};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Bound on how long a test waits for a message a seam owes a shard. Only a
/// seam that never sends one ever spends it.
const WAIT: Duration = Duration::from_secs(5);

/// The shard side of the channels a seam under test holds.
pub struct FakeShards {
    senders: Arc<Vec<ShardSender>>,
    /// `None` once the worker is "gone" — see [`FakeShards::disconnect`].
    rxs: Vec<Option<mpsc::Receiver<Envelope>>>,
}

/// `count` shards, wired but with no worker behind them.
pub fn fake_shards(count: usize) -> FakeShards {
    let mut senders = Vec::with_capacity(count);
    let mut rxs = Vec::with_capacity(count);
    for _ in 0..count {
        let (tx, rx) = mpsc::channel(64);
        senders.push(ShardSender::new(tx));
        rxs.push(Some(rx));
    }
    FakeShards {
        senders: Arc::new(senders),
        rxs,
    }
}

impl FakeShards {
    /// The handle a seam is constructed with.
    pub fn senders(&self) -> Arc<Vec<ShardSender>> {
        self.senders.clone()
    }

    /// The receiving end of one shard, to serve a message by hand. Carries its
    /// own id so a stalled wait on the result can name which shard it was.
    pub fn shard(&mut self, id: usize) -> ShardRx<'_> {
        let rx = self.rxs[id]
            .as_mut()
            .unwrap_or_else(|| panic!("shard {id} was disconnected by the test"));
        ShardRx { id, rx }
    }

    /// Drop this shard's receiver: the worker is gone (shutdown, promotion), so
    /// every later send to it fails.
    pub fn disconnect(&mut self, id: usize) {
        self.rxs[id] = None;
    }

    /// Whether this shard was sent nothing at all. A closed channel is not
    /// evidence of that — it can only mean the seam is done sending, touched or
    /// not — so it fails loudly instead of reading as a pass.
    pub fn untouched(&mut self, id: usize) -> bool {
        match self.rxs[id].as_mut() {
            Some(rx) => match rx.try_recv() {
                Err(mpsc::error::TryRecvError::Empty) => true,
                Ok(_) => false,
                Err(mpsc::error::TryRecvError::Disconnected) => panic!(
                    "shard {id}'s channel was disconnected, not left empty — \
                     \"untouched\" cannot be verified"
                ),
            },
            None => panic!("shard {id} was disconnected by the test"),
        }
    }
}

/// One shard's receiver, labeled with the shard id it belongs to.
pub struct ShardRx<'a> {
    id: usize,
    rx: &'a mut mpsc::Receiver<Envelope>,
}

/// What a seam sent a shard, as the shard saw it.
#[derive(Debug)]
pub enum Seen {
    /// One command, executed directly.
    Execute {
        command: ParsedCommand,
        conn_id: u64,
        txid: Option<u64>,
        track_reads: bool,
        no_touch: bool,
    },
    /// A group, executed atomically.
    Transaction {
        commands: Vec<ParsedCommand>,
        watches: usize,
        conn_id: u64,
    },
}

/// How the fake shard answers the command(s) it is handed.
pub enum Reply {
    /// Applied cleanly.
    Ok,
    /// Refused with an `-ERR`-shaped response (a `TransactionResult::Error` for
    /// a group).
    Error(&'static str),
    /// Refused with a RESP3 blob error.
    BlobError(&'static str),
    /// The group aborted on a `WATCH` conflict.
    WatchAborted,
    /// The shard drops the response channel without answering — the shape a
    /// worker that dies mid-apply presents.
    Silent,
}

/// The next message this shard is sent, or a panic if none arrives. The
/// timeout panic states only what was observed — no request arrived — since
/// the same wait elapses whether the seam never sent one or the test's own
/// responder on another shard stalled first.
async fn next(rx: ShardRx<'_>) -> ShardMessage {
    let id = rx.id;
    timeout(WAIT, rx.rx.recv())
        .await
        .unwrap_or_else(|_| panic!("shard {id}: no request arrived within {WAIT:?}"))
        .unwrap_or_else(|| panic!("shard {id}: channel was closed before a request arrived"))
        .message
}

/// Serve the one command message the executor seam sends, and report its shape.
pub async fn serve_command(rx: ShardRx<'_>, reply: Reply) -> Seen {
    match next(rx).await {
        ShardMessage::Core(CoreMsg::Execute {
            command,
            conn_id,
            txid,
            track_reads,
            no_touch,
            response_tx,
            ..
        }) => {
            match reply {
                Reply::Ok => {
                    let _ = response_tx.send(Response::Simple(Bytes::from_static(b"OK")));
                }
                Reply::Error(e) => {
                    let _ = response_tx.send(Response::Error(Bytes::from_static(e.as_bytes())));
                }
                Reply::BlobError(e) => {
                    let _ = response_tx.send(Response::BlobError(Bytes::from_static(e.as_bytes())));
                }
                Reply::WatchAborted => panic!("a single command has no WATCH set to abort on"),
                Reply::Silent => drop(response_tx),
            }
            Seen::Execute {
                command: (*command).clone(),
                conn_id,
                txid,
                track_reads,
                no_touch,
            }
        }
        ShardMessage::Core(CoreMsg::ExecTransaction {
            commands,
            watches,
            conn_id,
            response_tx,
            ..
        }) => {
            match reply {
                Reply::Ok => {
                    let _ = response_tx.send(TransactionResult::Success(Vec::new()));
                }
                Reply::Error(e) | Reply::BlobError(e) => {
                    let _ = response_tx.send(TransactionResult::Error(e.to_string()));
                }
                Reply::WatchAborted => {
                    let _ = response_tx.send(TransactionResult::WatchAborted);
                }
                Reply::Silent => drop(response_tx),
            }
            Seen::Transaction {
                commands,
                watches: watches.len(),
                conn_id,
            }
        }
        other => panic!("the executor seam sent an unexpected message: {other:?}"),
    }
}

/// Answer the export this shard is asked for.
pub async fn serve_export(rx: ShardRx<'_>, blob: Result<Vec<u8>, String>) {
    match next(rx).await {
        ShardMessage::Replication(ReplicationMsg::ExportSnapshot { response_tx }) => {
            let _ = response_tx.send(blob);
        }
        other => panic!("the export seam sent an unexpected message: {other:?}"),
    }
}

/// Take the export request and drop its ack without answering — the shape a
/// shard that dies mid-export presents.
pub async fn drop_export_ack(rx: ShardRx<'_>) {
    match next(rx).await {
        ShardMessage::Replication(ReplicationMsg::ExportSnapshot { response_tx }) => {
            drop(response_tx);
        }
        other => panic!("the export seam sent an unexpected message: {other:?}"),
    }
}

/// Take the entries this shard is being handed, and ack the install.
pub async fn serve_install(rx: ShardRx<'_>) -> Vec<SnapshotEntry> {
    match next(rx).await {
        ShardMessage::Replication(ReplicationMsg::InstallSnapshot {
            entries,
            response_tx,
        }) => {
            let _ = response_tx.send(());
            entries
        }
        other => panic!("the install seam sent an unexpected message: {other:?}"),
    }
}

/// Take the entries but drop the ack without answering.
pub async fn drop_install_ack(rx: ShardRx<'_>) {
    match next(rx).await {
        ShardMessage::Replication(ReplicationMsg::InstallSnapshot { response_tx, .. }) => {
            drop(response_tx);
        }
        other => panic!("the install seam sent an unexpected message: {other:?}"),
    }
}

/// A parsed command, as a replicated frame would decode to.
pub fn cmd(name: &str, args: &[&str]) -> ParsedCommand {
    ParsedCommand::new(
        Bytes::copy_from_slice(name.as_bytes()),
        args.iter()
            .map(|a| Bytes::copy_from_slice(a.as_bytes()))
            .collect(),
    )
}

/// A command rendered as `NAME arg arg`, for readable assertions.
pub fn render(command: &ParsedCommand) -> String {
    let mut out = String::from_utf8_lossy(&command.name).into_owned();
    for arg in &command.args {
        out.push(' ');
        out.push_str(&String::from_utf8_lossy(arg));
    }
    out
}

/// A string value's text, for asserting on a restored keyspace.
pub fn text(value: &Value) -> String {
    match value {
        Value::String(s) => String::from_utf8_lossy(&s.as_bytes()).into_owned(),
        other => panic!("expected a string value, got {other:?}"),
    }
}

/// The keys of a shard's slice, in the order the shard was handed them.
pub fn keys(entries: &[SnapshotEntry]) -> Vec<String> {
    entries
        .iter()
        .map(|e| String::from_utf8_lossy(&e.key).into_owned())
        .collect()
}
