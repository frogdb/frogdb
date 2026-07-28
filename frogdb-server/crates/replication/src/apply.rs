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
//!   state — the promise `broadcast_transaction_on_shard` makes.
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
use parking_lot::RwLock;
use redis_protocol::resp2::decode::decode_bytes_mut;
use tokio::sync::mpsc;

use crate::frame::ReplicationFrame;
use crate::replica::ReplicaApplyStint;
use crate::state::ReplicationState;

/// Claim `bytes` of consumed stream — directly when no transaction is open, or
/// onto the open group so the whole span is claimed together at `EXEC`.
///
/// `false` means the stint may no longer move the applied offset (a promotion
/// froze it, or a newer stream retired it) and the consume loop must stop.
fn claim(stint: &ReplicaApplyStint, pending: &mut Option<PendingTxn>, bytes: u64) -> bool {
    match pending {
        Some(txn) => {
            txn.bytes += bytes;
            true
        }
        None => stint.claim(bytes),
    }
}

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
    /// Stream bytes consumed by the group so far (the `MULTI` frame plus every
    /// buffered command). Claimed against the applied offset only at `EXEC`, as
    /// the group goes to its shard — an interrupted group must never leave the
    /// applied offset claiming data no shard ever saw.
    bytes: u64,
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
/// 5. surfaces any apply/parse error as a divergence signal (logged + counted);
/// 6. claims the frame's stream bytes against `stint` — the offset of the data
///    this node holds — and stops when the claim is refused.
///
/// ## Why the claim comes first, and why refusing it is the stop signal
///
/// The claim is taken *before* the group reaches its shard, and a promotion
/// freezes the counter under the same lock ([`crate::replica::AppliedOffset`]).
/// That makes the promotion boundary exact: a group is either claimed before
/// the freeze — inside the boundary, and this loop finishes applying it — or
/// refused after it and never applied at all. Nothing lands above the boundary.
///
/// Which is also why this loop is stopped by *refusing its claims* rather than
/// by `abort()`. An abort takes effect at this task's next poll, and that may
/// be inside `apply_group().await` with the shard message already dispatched:
/// the write reaches the keyspace and its bytes are never counted, leaving data
/// above the offset the node vouches for, in no backlog and outside every
/// replication-id window — the same silent divergence the received/applied
/// split exists to prevent, one group wide.
///
/// ## Why `applied` moves here and not at decode time
///
/// The streaming path advances the *received* head as soon as a frame is decoded
/// off the socket, then queues the frame here. Between the two sits a
/// 10k-deep channel. A promotion freezes its replication-id window and backlog
/// floor on `applied`, so it must count only frames that reached the keyspace:
/// anything still queued (or dropped when this loop stops on promotion) is
/// deliberately left uncounted. Freezing the boundary too low costs a sibling a
/// full resync; freezing it too high grants `+CONTINUE` over a hole.
///
/// Errors still advance the offset. A parse/apply failure means this node has
/// already diverged for that write (surfaced above as a divergence signal) —
/// stalling the offset on top of that would desynchronise the node's stream
/// position from the primary's for every later frame, which is how Redis treats
/// it too (the replica's offset counts stream bytes consumed).
pub async fn consume_frames<A: ReplicaApplier>(
    mut frame_rx: mpsc::Receiver<ReplicationFrame>,
    applier: A,
    is_replica_flag: Arc<AtomicBool>,
    replication_state: Option<Arc<RwLock<ReplicationState>>>,
    stint: ReplicaApplyStint,
) {
    tracing::info!("Replica frame consumer started");

    let mut frames_processed: u64 = 0;
    let mut errors: u64 = 0;
    let mut pending: Option<PendingTxn> = None;

    while let Some(frame) = frame_rx.recv().await {
        // Stop consuming frames if we've been promoted to primary. Acquire pairs
        // with the promotion's Release store, so a consumer that sees the flip
        // also sees the minted identity behind it.
        if !is_replica_flag.load(Ordering::Acquire) {
            tracing::info!("Replica promoted to primary, stopping frame consumer");
            break;
        }

        // Stream bytes this frame accounts for, claimed before it touches the
        // keyspace (or, inside a MULTI, when the group EXECs).
        let frame_bytes = frame.stream_advance();

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
                if !claim(&stint, &mut pending, frame_bytes) {
                    break;
                }
                continue;
            }
        };

        let cmd_name = cmd.name_uppercase_string();

        // --- Control commands: handled inline, never shard-routed. ---

        // REPLCONF GETACK is a control message, not a data command. It still
        // occupies stream bytes the primary counted, so it advances the offset.
        if cmd_name == "REPLCONF" {
            if !claim(&stint, &mut pending, frame_bytes) {
                break;
            }
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
                state.write().active_version = Some(version);
            }
            frames_processed += 1;
            if !claim(&stint, &mut pending, frame_bytes) {
                break;
            }
            continue;
        }

        // --- Transaction reconstruction. ---

        match cmd_name.as_str() {
            "MULTI" => {
                if let Some(abandoned) = pending.take() {
                    tracing::warn!("Nested MULTI in replication stream; resetting group");
                    errors += 1;
                    // The abandoned group never applied, but its bytes were
                    // consumed from the stream: claim them so the offset keeps
                    // tracking the primary's.
                    if !stint.claim(abandoned.bytes) {
                        break;
                    }
                }
                // The whole group runs on the shard the MULTI frame is tagged
                // with (all frames of a group carry the same origin shard).
                pending = Some(PendingTxn {
                    shard_id: frame.shard_id,
                    commands: Vec::new(),
                    // The MULTI frame's own bytes ride with the group and are
                    // claimed with it at EXEC.
                    bytes: frame_bytes,
                });
            }
            "EXEC" => match pending.take() {
                Some(txn) => {
                    let n = txn.commands.len();
                    // The group's whole byte span (MULTI + inner commands + this
                    // EXEC) is claimed as it goes to the shard, never after: an
                    // apply this loop has started always completes, but the
                    // promotion boundary is frozen without waiting for it.
                    if !stint.claim(txn.bytes + frame_bytes) {
                        break;
                    }
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
                    if !stint.claim(frame_bytes) {
                        break;
                    }
                }
            },
            _ => {
                if let Some(txn) = pending.as_mut() {
                    // Inside a MULTI/EXEC: buffer for the atomic apply. The
                    // bytes ride with the group until EXEC.
                    txn.commands.push(cmd);
                    txn.bytes += frame_bytes;
                } else {
                    // Bare command: a group of one on its tagged shard, claimed
                    // on the way in for the same reason as a transaction.
                    if !stint.claim(frame_bytes) {
                        break;
                    }
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
    use crate::replica::AppliedOffset;
    use bytes::Bytes;
    use std::sync::Mutex;

    /// Records each applied group as `(shard_id, [command names])`, and can be
    /// told to reject a specific command name to exercise divergence surfacing.
    #[derive(Default)]
    struct MockApplier {
        groups: Mutex<Vec<(u16, Vec<String>)>>,
        reject: Option<String>,
        /// When set, every apply parks on this gate until the test opens it —
        /// the "apply in flight" state a promotion has to be exact about.
        gate: Option<Arc<tokio::sync::Semaphore>>,
        /// Signalled as an apply enters the gate, so a test can promote at
        /// exactly the moment a group is mid-flight.
        entered: Option<Arc<tokio::sync::Notify>>,
    }

    impl ReplicaApplier for MockApplier {
        async fn apply_group(
            &self,
            shard_id: u16,
            commands: Vec<ParsedCommand>,
        ) -> Result<(), ApplyError> {
            let names: Vec<String> = commands.iter().map(|c| c.name_uppercase_string()).collect();
            if let Some(ref entered) = self.entered {
                entered.notify_one();
            }
            if let Some(ref gate) = self.gate {
                gate.acquire().await.expect("gate closed").forget();
            }
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

    /// Drive the consume loop over `frames` and return the applied offset it
    /// reached (the frames' total stream bytes when everything applies).
    async fn drive(frames: Vec<ReplicationFrame>, applier: Arc<MockApplier>) -> u64 {
        let (tx, rx) = mpsc::channel(64);
        for f in frames {
            tx.send(f).await.unwrap();
        }
        drop(tx);
        let flag = Arc::new(AtomicBool::new(true));
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        consume_frames(rx, SharedApplier(applier), flag, None, stint).await;
        applied.current()
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
        let total: u64 = frames.iter().map(|f| f.stream_advance()).sum();
        let applier = Arc::new(MockApplier::default());
        let applied = drive(frames, applier.clone()).await;
        // Every frame is behind us, transaction framing included.
        assert_eq!(applied, total);

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
        let total: u64 = frames.iter().map(|f| f.stream_advance()).sum();
        let applier = Arc::new(MockApplier::default());
        let applied = drive(frames, applier.clone()).await;
        // A skipped control frame is still stream bytes the primary counted.
        assert_eq!(applied, total);
        let groups = applier.groups.lock().unwrap();
        assert_eq!(*groups, vec![(0, vec!["SET".to_string()])]);
    }

    #[tokio::test]
    async fn failed_apply_is_surfaced_not_silently_dropped() {
        // The applier rejects DEL; the group must NOT be recorded as applied.
        let frames = vec![frame_on(2, 1, "DEL", &["k"])];
        let total: u64 = frames.iter().map(|f| f.stream_advance()).sum();
        let applier = Arc::new(MockApplier {
            reject: Some("DEL".to_string()),
            ..Default::default()
        });
        let applied = drive(frames, applier.clone()).await;
        assert!(
            applier.groups.lock().unwrap().is_empty(),
            "a rejected apply must not be counted as applied"
        );
        // The offset still advances: the divergence is surfaced separately, and
        // stalling here would desynchronise every later frame's position.
        assert_eq!(applied, total);
    }

    #[tokio::test]
    async fn promotion_stops_the_consumer_and_leaves_queued_frames_uncounted() {
        // The CRITICAL failure this split exists to prevent: frames decoded off
        // the socket (received offset already advanced) but never applied must
        // NOT be counted, or a promotion freezes its window over a hole.
        let (tx, rx) = mpsc::channel(64);
        let applied_frame = frame_on(0, 1, "SET", &["a", "1"]);
        let applied_bytes = applied_frame.stream_advance();

        let flag = Arc::new(AtomicBool::new(true));
        let applier = Arc::new(MockApplier::default());
        let applied = AppliedOffset::detached(0);
        let flip = flag.clone();
        let recorded = applier.clone();
        let stint = applied.begin_replica_stint();
        let consumer = tokio::spawn(async move {
            consume_frames(rx, SharedApplier(recorded), flip, None, stint).await;
        });

        // Frame 1 is applied while the node is still a replica.
        tx.send(applied_frame).await.unwrap();
        while applier.groups.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        // Promote, THEN hand over frame 2: the loop sees the flipped flag and
        // stops with that frame consumed but never applied — exactly the state a
        // real promotion leaves the 10k-deep frame channel in.
        flag.store(false, Ordering::Release);
        tx.send(frame_on(0, 2, "SET", &["b", "2"])).await.unwrap();
        drop(tx);
        consumer.await.unwrap();

        assert_eq!(
            applier.groups.lock().unwrap().len(),
            1,
            "only the pre-promotion frame applied"
        );
        assert_eq!(
            applied.current(),
            applied_bytes,
            "the queued, never-applied frame must not move the applied offset"
        );
    }

    #[tokio::test]
    async fn an_interrupted_transaction_credits_nothing() {
        // A MULTI group whose EXEC never arrives applied nothing, so none of its
        // bytes may reach the applied offset.
        let frames = vec![
            frame_on(1, 1, "MULTI", &[]),
            frame_on(1, 2, "SET", &["a", "1"]),
        ];
        let applier = Arc::new(MockApplier::default());
        let applied = drive(frames, applier.clone()).await;
        assert!(applier.groups.lock().unwrap().is_empty());
        assert_eq!(applied, 0, "an unfinished group claims no applied data");
    }

    /// Spawn a consumer over `applied` whose applies park until the returned
    /// gate is released, plus a notifier that fires as each apply parks.
    #[allow(clippy::type_complexity)]
    fn parked_consumer(
        applied: &AppliedOffset,
    ) -> (
        mpsc::Sender<ReplicationFrame>,
        Arc<MockApplier>,
        Arc<tokio::sync::Semaphore>,
        Arc<tokio::sync::Notify>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::channel(64);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let applier = Arc::new(MockApplier {
            gate: Some(gate.clone()),
            entered: Some(entered.clone()),
            ..Default::default()
        });
        let stint = applied.begin_replica_stint();
        let flag = Arc::new(AtomicBool::new(true));
        let recorded = applier.clone();
        let consumer = tokio::spawn(async move {
            consume_frames(rx, SharedApplier(recorded), flag, None, stint).await
        });
        (tx, applier, gate, entered, consumer)
    }

    #[tokio::test]
    async fn a_freeze_during_an_in_flight_apply_covers_that_group_and_refuses_the_next() {
        // The narrowed race: the promotion lands while a group is inside
        // `apply_group().await`. The write WILL reach the keyspace, so the
        // frozen boundary must already cover it — and nothing after it may be
        // applied, since those bytes would sit above the boundary in no backlog
        // and outside every replication-id window.
        let applied = AppliedOffset::detached(0);
        let (tx, applier, gate, entered, consumer) = parked_consumer(&applied);

        let in_flight = frame_on(0, 1, "SET", &["a", "1"]);
        let in_flight_bytes = in_flight.stream_advance();
        tx.send(in_flight).await.unwrap();
        // Wait until the apply is genuinely in flight (parked inside the gate).
        entered.notified().await;

        // Promote *now*, mid-apply.
        let boundary = applied.freeze();
        assert_eq!(
            boundary, in_flight_bytes,
            "the boundary must cover the group already on its way to the shard"
        );

        // Let the in-flight apply finish, then offer another frame.
        gate.add_permits(1);
        tx.send(frame_on(0, 2, "SET", &["b", "2"])).await.unwrap();
        drop(tx);
        consumer.await.unwrap();

        assert_eq!(
            *applier.groups.lock().unwrap(),
            vec![(0, vec!["SET".to_string()])],
            "the in-flight group lands; the post-freeze frame never applies"
        );
        assert_eq!(
            applied.current(),
            boundary,
            "no claim may move the offset past a frozen boundary"
        );
    }

    #[tokio::test]
    async fn a_newer_stint_retires_the_previous_consumer() {
        // The demotion mirror: a new inbound stream retires the applier behind
        // the old one, so stale frames still queued from the previous primary
        // are not applied on top of the new history — and the old consumer stops
        // on its own rather than being cancelled mid-apply.
        let applied = AppliedOffset::detached(0);
        let (tx, applier, gate, entered, consumer) = parked_consumer(&applied);

        let in_flight = frame_on(0, 1, "SET", &["a", "1"]);
        let in_flight_bytes = in_flight.stream_advance();
        tx.send(in_flight).await.unwrap();
        entered.notified().await;

        // A new stream opens while the old consumer is mid-apply.
        let _next = applied.begin_replica_stint();

        gate.add_permits(1);
        tx.send(frame_on(0, 2, "SET", &["b", "2"])).await.unwrap();
        drop(tx);
        consumer.await.unwrap();

        assert_eq!(
            applier.groups.lock().unwrap().len(),
            1,
            "the retired consumer applies nothing after the stint changed"
        );
        assert_eq!(applied.current(), in_flight_bytes);
    }
}
