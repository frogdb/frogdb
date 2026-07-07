//! Replica apply: honoring the primary's atomicity + routing contract.
//!
//! The primary frames a replicated transaction group as `MULTI … EXEC` and tags
//! every frame with the shard the write executed on (see
//! [`crate::frame::ReplicationFrame`]). This module owns the *consumer* side of
//! that contract:
//!
//! - **Transaction reconstruction.** Frames arrive one command per frame. The
//!   consume loop groups `MULTI … EXEC` back into a single unit and hands it to
//!   the applier as one atomic apply, so the replica never observes intermediate
//!   state — the promise `broadcast_transaction` makes.
//! - **Tagged routing, not re-derivation.** Each frame carries its origin shard,
//!   so the replica applies on *that* shard instead of re-deriving routing from
//!   `args[0]` (which sent keyless commands and the literal `MULTI`/`EXEC` frames
//!   to shard 0, diverging the replica).
//! - **Result checking.** A failed apply is surfaced (logged + counted as a
//!   divergence signal) instead of being silently dropped.
//!
//! The shard-touching work lives behind the [`ReplicaApplier`] seam, implemented
//! by the server (which owns the shard channels). This module — and therefore
//! transaction reconstruction and result-checking — is unit-testable against a
//! mock applier, with no full server required.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::BytesMut;
use frogdb_protocol::ParsedCommand;
use redis_protocol::resp2::decode::decode_bytes_mut;
use tokio::sync::{RwLock, mpsc};

use crate::frame::ReplicationFrame;
use crate::state::ReplicationState;

/// Error returned by a [`ReplicaApplier`] when a replicated group cannot be
/// applied — a divergence signal the consume loop surfaces rather than drops.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// The tagged origin shard is not a valid shard on this replica.
    #[error("origin shard {0} out of range ({1} shards)")]
    ShardOutOfRange(u16, usize),

    /// The shard worker channel is closed (shutdown / promotion).
    #[error("shard {0} channel closed")]
    ShardUnavailable(u16),

    /// The shard applied the command(s) but returned an error response — the
    /// replica has diverged from the primary for this write.
    #[error("shard {shard} rejected replicated apply: {detail}")]
    Rejected { shard: u16, detail: String },
}

/// The server-side seam for applying replicated writes on a specific shard.
///
/// `replication` owns transaction reconstruction and result-checking; the
/// implementor (the server) owns only the mechanical "route this group of
/// commands to shard `shard_id` and report whether they applied cleanly". The
/// future is required to be `Send` so the consume loop can be spawned.
pub trait ReplicaApplier: Send + Sync {
    /// Apply a group of commands atomically on `shard_id`.
    ///
    /// A single replicated command is a group of length 1. A `MULTI … EXEC`
    /// transaction is the inner commands (framing stripped) applied as one
    /// atomic unit on the tagged shard. Returns `Err` if the group could not be
    /// applied cleanly (surfaced by the caller as a divergence).
    fn apply_group(
        &self,
        shard_id: u16,
        commands: Vec<ParsedCommand>,
    ) -> impl std::future::Future<Output = Result<(), ApplyError>> + Send;
}

/// Parse a RESP-encoded command from a replication frame payload.
pub fn parse_frame_payload(payload: &[u8]) -> Result<ParsedCommand, String> {
    let mut buf = BytesMut::from(payload);
    match decode_bytes_mut(&mut buf) {
        Ok(Some((frame, _, _))) => ParsedCommand::try_from(frame).map_err(|e| format!("{:?}", e)),
        Ok(None) => Err("incomplete frame".to_string()),
        Err(e) => Err(format!("{}", e)),
    }
}

/// In-progress `MULTI … EXEC` reconstruction: the origin shard captured at
/// `MULTI` and the inner commands accumulated until `EXEC`.
struct PendingTxn {
    shard_id: u16,
    commands: Vec<ParsedCommand>,
}

/// Consume replication frames from the primary and apply them, honoring the
/// atomicity + routing contract.
///
/// The loop:
/// 1. stops if the node was promoted to primary;
/// 2. parses each frame's RESP payload;
/// 3. handles control commands inline (`REPLCONF` skipped; `FROGDB.FINALIZE`
///    updates the replica's `active_version` — never shard-routed);
/// 4. reconstructs `MULTI … EXEC` into one atomic [`ReplicaApplier::apply_group`]
///    on the frame's tagged shard; a bare command is a group of one;
/// 5. surfaces any apply/parse error as a divergence signal (logged + counted).
pub async fn consume_frames<A: ReplicaApplier>(
    mut frame_rx: mpsc::Receiver<ReplicationFrame>,
    applier: A,
    is_replica_flag: Arc<AtomicBool>,
    replication_state: Option<Arc<RwLock<ReplicationState>>>,
) {
    tracing::info!("Replica frame consumer started");

    let mut frames_processed: u64 = 0;
    let mut errors: u64 = 0;
    let mut pending: Option<PendingTxn> = None;

    while let Some(frame) = frame_rx.recv().await {
        // Stop consuming frames if we've been promoted to primary.
        if !is_replica_flag.load(Ordering::Relaxed) {
            tracing::info!("Replica promoted to primary, stopping frame consumer");
            break;
        }

        let cmd = match parse_frame_payload(&frame.payload) {
            Ok(cmd) => cmd,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    sequence = frame.sequence,
                    payload_len = frame.payload.len(),
                    "Failed to parse replication frame"
                );
                errors += 1;
                continue;
            }
        };

        let cmd_name = cmd.name_uppercase_string();

        // --- Control commands: handled inline, never shard-routed. ---

        // REPLCONF GETACK is a control message, not a data command.
        if cmd_name == "REPLCONF" {
            continue;
        }

        // FROGDB.FINALIZE is replicated through the WAL stream after
        // finalization; the replica applies it to its own replication state
        // (active version) rather than routing it to a shard.
        if cmd_name == "FROGDB.FINALIZE" {
            if let Some(ref state) = replication_state
                && let Some(version_arg) = cmd.args.first()
            {
                let version = String::from_utf8_lossy(version_arg).to_string();
                tracing::info!(
                    version = %version,
                    "Applying replicated FROGDB.FINALIZE — active version updated"
                );
                state.write().await.active_version = Some(version);
            }
            frames_processed += 1;
            continue;
        }

        // --- Transaction reconstruction. ---

        match cmd_name.as_str() {
            "MULTI" => {
                if pending.is_some() {
                    tracing::warn!("Nested MULTI in replication stream; resetting group");
                    errors += 1;
                }
                // The whole group runs on the shard the MULTI frame is tagged
                // with (all frames of a group carry the same origin shard).
                pending = Some(PendingTxn {
                    shard_id: frame.shard_id,
                    commands: Vec::new(),
                });
            }
            "EXEC" => match pending.take() {
                Some(txn) => {
                    let n = txn.commands.len();
                    if let Err(e) = applier.apply_group(txn.shard_id, txn.commands).await {
                        tracing::error!(
                            error = %e,
                            shard = txn.shard_id,
                            commands = n,
                            sequence = frame.sequence,
                            "Replicated transaction diverged: apply failed"
                        );
                        errors += 1;
                    } else {
                        frames_processed += 1;
                    }
                }
                None => {
                    tracing::warn!("EXEC without MULTI in replication stream; ignoring");
                    errors += 1;
                }
            },
            _ => {
                if let Some(txn) = pending.as_mut() {
                    // Inside a MULTI/EXEC: buffer for the atomic apply.
                    txn.commands.push(cmd);
                } else {
                    // Bare command: a group of one on its tagged shard.
                    if let Err(e) = applier.apply_group(frame.shard_id, vec![cmd]).await {
                        tracing::error!(
                            error = %e,
                            shard = frame.shard_id,
                            command = %cmd_name,
                            sequence = frame.sequence,
                            "Replicated command diverged: apply failed"
                        );
                        errors += 1;
                    } else {
                        frames_processed += 1;
                    }
                }
            }
        }
    }

    tracing::info!(
        frames_processed = frames_processed,
        errors = errors,
        "Replica frame consumer shutting down"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::serialize_command_to_resp;
    use bytes::Bytes;
    use std::sync::Mutex;

    /// Records each applied group as `(shard_id, [command names])`, and can be
    /// told to reject a specific command name to exercise divergence surfacing.
    #[derive(Default)]
    struct MockApplier {
        groups: Mutex<Vec<(u16, Vec<String>)>>,
        reject: Option<String>,
    }

    impl ReplicaApplier for MockApplier {
        async fn apply_group(
            &self,
            shard_id: u16,
            commands: Vec<ParsedCommand>,
        ) -> Result<(), ApplyError> {
            let names: Vec<String> = commands.iter().map(|c| c.name_uppercase_string()).collect();
            if let Some(ref bad) = self.reject
                && names.iter().any(|n| n == bad)
            {
                return Err(ApplyError::Rejected {
                    shard: shard_id,
                    detail: format!("rejecting {bad}"),
                });
            }
            self.groups.lock().unwrap().push((shard_id, names));
            Ok(())
        }
    }

    fn frame_on(shard: u16, seq: u64, name: &str, args: &[&str]) -> ReplicationFrame {
        let args: Vec<Bytes> = args
            .iter()
            .map(|a| Bytes::copy_from_slice(a.as_bytes()))
            .collect();
        ReplicationFrame::new_on_shard(seq, shard, serialize_command_to_resp(name, &args))
    }

    // The consume loop takes the applier by value, so the test harness shares
    // the recording `MockApplier` through an `Arc` and inspects it afterwards.
    #[derive(Clone, Default)]
    struct SharedApplier(Arc<MockApplier>);

    impl ReplicaApplier for SharedApplier {
        async fn apply_group(
            &self,
            shard_id: u16,
            commands: Vec<ParsedCommand>,
        ) -> Result<(), ApplyError> {
            self.0.apply_group(shard_id, commands).await
        }
    }

    async fn drive(frames: Vec<ReplicationFrame>, applier: Arc<MockApplier>) {
        let (tx, rx) = mpsc::channel(64);
        for f in frames {
            tx.send(f).await.unwrap();
        }
        drop(tx);
        let flag = Arc::new(AtomicBool::new(true));
        consume_frames(rx, SharedApplier(applier), flag, None).await;
    }

    #[tokio::test]
    async fn transaction_group_applied_atomically_on_tagged_shard() {
        // A MULTI/EXEC group tagged shard 3, plus a bare command tagged shard 1.
        let frames = vec![
            frame_on(3, 1, "MULTI", &[]),
            frame_on(3, 2, "SET", &["a", "1"]),
            frame_on(3, 3, "SET", &["b", "2"]),
            frame_on(3, 4, "EXEC", &[]),
            frame_on(1, 5, "SET", &["c", "3"]),
        ];
        let applier = Arc::new(MockApplier::default());
        drive(frames, applier.clone()).await;

        let groups = applier.groups.lock().unwrap();
        // The transaction is ONE atomic group on shard 3 (MULTI/EXEC stripped),
        // and the bare command is its own group on shard 1 — routing came from
        // the frame tag, not args[0].
        assert_eq!(
            *groups,
            vec![
                (3, vec!["SET".to_string(), "SET".to_string()]),
                (1, vec!["SET".to_string()]),
            ]
        );
    }

    #[tokio::test]
    async fn replconf_is_skipped_and_not_routed() {
        let frames = vec![
            frame_on(crate::frame::CONTROL_SHARD, 1, "REPLCONF", &["GETACK", "*"]),
            frame_on(0, 2, "SET", &["k", "v"]),
        ];
        let applier = Arc::new(MockApplier::default());
        drive(frames, applier.clone()).await;
        let groups = applier.groups.lock().unwrap();
        assert_eq!(*groups, vec![(0, vec!["SET".to_string()])]);
    }

    #[tokio::test]
    async fn failed_apply_is_surfaced_not_silently_dropped() {
        // The applier rejects DEL; the group must NOT be recorded as applied.
        let frames = vec![frame_on(2, 1, "DEL", &["k"])];
        let applier = Arc::new(MockApplier {
            reject: Some("DEL".to_string()),
            ..Default::default()
        });
        drive(frames, applier.clone()).await;
        assert!(
            applier.groups.lock().unwrap().is_empty(),
            "a rejected apply must not be counted as applied"
        );
    }
}
