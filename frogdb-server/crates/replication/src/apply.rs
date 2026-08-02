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
//! - **Result checking.** A failed apply is an admitted divergence, not a log
//!   line: it is latched on the applying stint, which refuses every further
//!   claim on that history and wakes the connection to force the link back
//!   through a full resync (issue 08).
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
use crate::replica::{Claim, ReplicaApplyStint};
use crate::state::ReplicationState;

/// What the replica's frame channel carries: a decoded frame plus the **history
/// epoch** the decode loop was on when it read it.
///
/// Not a wire type — the epoch is local bookkeeping and never leaves the node.
/// It exists because the channel and its consumer outlive the connection that
/// fills them: `ReplicaReplicationHandler::start` reconnects in a loop into the
/// same 10k-deep channel, so a link that drops mid-stream leaves decoded frames
/// (and possibly an open `MULTI` group) queued for the *next* stint. When that
/// stint comes back `+FULLRESYNC` and installs a fresh dataset, those leftovers
/// describe a keyspace that no longer exists. Stamping them lets the consumer
/// tell the two histories apart in the one place that matters — the claim
/// (issue 06).
#[derive(Debug, Clone)]
pub struct StreamedFrame {
    /// The value of [`crate::replica::AppliedOffset::epoch`] when the frame was
    /// decoded.
    pub epoch: u64,
    pub frame: ReplicationFrame,
}

impl StreamedFrame {
    pub fn new(epoch: u64, frame: ReplicationFrame) -> Self {
        Self { epoch, frame }
    }
}

/// Claim `bytes` of consumed stream — directly when no transaction is open, or
/// onto the open group so the whole span is claimed together at `EXEC`.
///
/// An open group is claimed for without re-checking the gate because its frames
/// were already admitted under `epoch`: the top of the consume loop drops a
/// group the moment a frame of a newer history arrives, so a group can never
/// straddle a resync.
fn claim(
    stint: &ReplicaApplyStint,
    pending: &mut Option<PendingTxn>,
    epoch: u64,
    bytes: u64,
) -> Claim {
    match pending {
        Some(txn) => {
            txn.bytes += bytes;
            Claim::Granted
        }
        None => stint.claim(epoch, bytes),
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
    /// The history the `MULTI` opened under (see [`StreamedFrame`]). A group is
    /// abandoned rather than continued when the frames that follow belong to a
    /// newer history, so a resync can never be papered over by an `EXEC` from
    /// the other side of it.
    epoch: u64,
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
/// 1. drops any frame stamped with a history this node has replaced (see
///    [`StreamedFrame`]), along with the group it belonged to;
/// 2. parses each frame's RESP payload;
/// 3. handles control commands inline (`REPLCONF` skipped; `FROGDB.FINALIZE`
///    updates the replica's `active_version` — never shard-routed);
/// 4. reconstructs `MULTI … EXEC` into one atomic [`ReplicaApplier::apply_group`]
///    on the frame's tagged shard; a bare command is a group of one;
/// 5. admits a failed apply as a divergence — latched on the stint, which ends
///    the history rather than merely logging it (see below);
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
/// ## What a failed apply costs the history it happened on
///
/// The failing frame's own bytes stay claimed: they were claimed on the way in,
/// and un-claiming them would desynchronise this node's stream position from the
/// primary's, which is how Redis treats it too (the replica's offset counts
/// stream bytes consumed). What does *not* happen is business as usual. An `Err`
/// out of `apply_group` is proof the keyspace no longer matches the primary's,
/// so it is latched on the stint ([`ReplicaApplyStint::admit_divergence`]) and
/// the history ends there: every later claim on it is refused, so nothing is
/// applied on top of the hole and no further byte is vouched for, and the
/// connection task — woken through [`crate::replica::AppliedOffset::divergence`]
/// — drops the link and rewinds so the reconnect is answered `+FULLRESYNC`.
/// Without that, a provably diverged node kept serving reads *and*, once
/// promoted, handed siblings `+CONTINUE` at an offset covering a write it never
/// applied (issue 08).
///
/// The consumer is not retired by any of this. It outlives connections by
/// design (issue 06), so retiring it would stop this node applying
/// anything ever again; instead it idles, discarding the doomed history's
/// frames, until the resync installs a fresh dataset and bumps the epoch — at
/// which point it applies again.
///
/// A *parse* failure is not treated as a divergence: an undecodable payload is
/// as likely a frame this build does not know as a corrupted one, and it is
/// counted and stepped over as before.
pub async fn consume_frames<A: ReplicaApplier>(
    mut frame_rx: mpsc::Receiver<StreamedFrame>,
    applier: A,
    is_replica_flag: Arc<AtomicBool>,
    replication_state: Option<Arc<RwLock<ReplicationState>>>,
    stint: ReplicaApplyStint,
) {
    tracing::info!("Replica frame consumer started");

    let mut frames_processed: u64 = 0;
    let mut errors: u64 = 0;
    let mut discarded: u64 = 0;
    let mut pending: Option<PendingTxn> = None;

    while let Some(StreamedFrame { epoch, frame }) = frame_rx.recv().await {
        // Stop consuming frames if we've been promoted to primary. Acquire pairs
        // with the promotion's Release store, so a consumer that sees the flip
        // also sees the minted identity behind it.
        if !is_replica_flag.load(Ordering::Acquire) {
            tracing::info!("Replica promoted to primary, stopping frame consumer");
            break;
        }

        // A frame from a history this node has since replaced: a full resync
        // adopted a fresh dataset and offset after this frame was decoded, so
        // applying it now would write the old primary's stream onto the new
        // keyspace and credit the new history with its bytes. Drop it, and with
        // it any group it was part of — the group's remaining frames are behind
        // it in this same channel and will be dropped the same way. Cheap
        // pre-check; the claim re-checks under the gate, which is what makes it
        // race-free.
        if epoch != stint.epoch() {
            discarded += 1;
            pending = None;
            continue;
        }

        // The frame is current, but the group in hand is not: the link dropped
        // mid-`MULTI` and the retry resynced, so this frame is the new history's
        // first. Abandon the group — continuing it would apply the old primary's
        // half-transaction onto the installed dataset, with an `EXEC` from the
        // other side of the resync closing it.
        if pending.as_ref().is_some_and(|txn| txn.epoch != epoch) {
            discarded += 1;
            pending = None;
        }

        /// Claim `$bytes` for the frame in hand and act on the verdict: apply it,
        /// drop it with the history it belonged to, or stop the loop.
        ///
        /// A macro rather than a function because two of the three verdicts are
        /// control flow (`continue` / `break`) at seven call sites, and spelling
        /// them out at each one is how a site ends up with the wrong one.
        macro_rules! claim_or_stop {
            ($bytes:expr) => {
                match claim(&stint, &mut pending, epoch, $bytes) {
                    Claim::Granted => {}
                    Claim::Stale => {
                        discarded += 1;
                        pending = None;
                        continue;
                    }
                    Claim::Retired => break,
                }
            };
        }

        /// Admit that the group just handed to a shard did not apply: this node
        /// has diverged from the primary at this offset.
        ///
        /// Latching it on the stint is the consequence the bare log used to be
        /// missing (issue 08): every further claim on this
        /// history is refused, and the connection task drops its link and
        /// rewinds so the reconnect comes back through a full resync.
        macro_rules! diverged {
            ($e:expr, $what:expr) => {
                errors += 1;
                stint.admit_divergence(epoch);
                tracing::error!(
                    error = %$e,
                    shard = frame.shard_id,
                    sequence = frame.sequence,
                    epoch = epoch,
                    group = %$what,
                    "Replicated apply failed: this replica has diverged from its \
                     primary. Refusing further applies on this history and \
                     forcing the link back through a full resync."
                );
            };
        }

        /// Report the claimed head as landed — everything claimed has reached a
        /// shard (or was never going to). A no-op while a group is open: its
        /// bytes are not claimed until `EXEC`, so there is nothing to report.
        macro_rules! settled {
            () => {
                if pending.is_none() {
                    stint.land();
                }
            };
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
                claim_or_stop!(frame_bytes);
                settled!();
                continue;
            }
        };

        let cmd_name = cmd.name_uppercase_string();

        // --- Control commands: handled inline, never shard-routed. ---

        // REPLCONF GETACK is a control message, not a data command. It still
        // occupies stream bytes the primary counted, so it advances the offset.
        if cmd_name == "REPLCONF" {
            claim_or_stop!(frame_bytes);
            // Landed as soon as it is claimed: a GETACK touches no shard, and
            // its own bytes are part of the offset the answer it solicits must
            // cover (issue 09).
            settled!();
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
            claim_or_stop!(frame_bytes);
            settled!();
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
                    claim_or_stop!(abandoned.bytes);
                    settled!();
                }
                // The whole group runs on the shard the MULTI frame is tagged
                // with (all frames of a group carry the same origin shard).
                pending = Some(PendingTxn {
                    shard_id: frame.shard_id,
                    commands: Vec::new(),
                    epoch,
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
                    claim_or_stop!(txn.bytes + frame_bytes);
                    if let Err(e) = applier.apply_group(txn.shard_id, txn.commands).await {
                        diverged!(e, format!("MULTI/EXEC of {n} commands"));
                    } else {
                        frames_processed += 1;
                    }
                    settled!();
                }
                None => {
                    tracing::warn!("EXEC without MULTI in replication stream; ignoring");
                    errors += 1;
                    claim_or_stop!(frame_bytes);
                    settled!();
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
                    claim_or_stop!(frame_bytes);
                    if let Err(e) = applier.apply_group(frame.shard_id, vec![cmd]).await {
                        diverged!(e, cmd_name);
                    } else {
                        frames_processed += 1;
                    }
                    settled!();
                }
            }
        }
    }

    tracing::info!(
        frames_processed = frames_processed,
        errors = errors,
        discarded = discarded,
        "Replica frame consumer shutting down"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::serialize_command_to_resp;
    use crate::replica::AppliedOffset;
    use crate::replica::offset::ReplicaOffset;
    use bytes::Bytes;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;

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

    /// A frame as the decode loop hands it over on the history the node is
    /// already on — epoch 0, which is where a freshly built `AppliedOffset`
    /// starts and where every test stays unless it resyncs on purpose.
    fn live(frame: ReplicationFrame) -> StreamedFrame {
        StreamedFrame::new(0, frame)
    }

    /// Drive the consume loop over `frames` and return the applied offset it
    /// reached (the frames' total stream bytes when everything applies).
    async fn drive(frames: Vec<ReplicationFrame>, applier: Arc<MockApplier>) -> u64 {
        let (tx, rx) = mpsc::channel(64);
        for f in frames {
            tx.send(live(f)).await.unwrap();
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
        // The failing frame's own bytes stay claimed — stalling here would
        // desynchronise the node's stream position from the primary's. What the
        // divergence costs is everything *after* it, which
        // `a_failed_apply_stops_the_history_it_happened_on` covers.
        assert_eq!(applied, total);
    }

    /// Issue 08: a failed apply used to be logged and stepped over, so the loop
    /// went on writing to a keyspace it had proved wrong and went on claiming
    /// bytes it would vouch for once promoted. The admitted divergence ends the
    /// history instead: the next frame is refused, not applied.
    // FM-REPLICATION-010
    #[tokio::test]
    async fn a_failed_apply_stops_the_history_it_happened_on() {
        let frames = vec![
            frame_on(0, 1, "SET", &["a", "1"]),
            frame_on(0, 2, "DEL", &["k"]), // rejected: this node has diverged
            frame_on(0, 3, "SET", &["b", "2"]), // must never reach a shard
        ];
        // The failing frame's own bytes stay claimed — un-claiming them would
        // desynchronise the stream position — but nothing after it does.
        let through_the_failure = frames[0].stream_advance() + frames[1].stream_advance();
        let applier = Arc::new(MockApplier {
            reject: Some("DEL".to_string()),
            ..Default::default()
        });
        let applied = drive(frames, applier.clone()).await;

        assert_eq!(
            *applier.groups.lock().unwrap(),
            vec![(0, vec!["SET".to_string()])],
            "a frame after an admitted divergence reached the keyspace"
        );
        assert_eq!(
            applied, through_the_failure,
            "the applied offset kept advancing over a history known to be wrong"
        );
    }

    /// The consumer outlives connections, so a divergence must not retire it:
    /// it idles through the doomed history and applies again the moment the
    /// forced full resync installs a fresh dataset.
    // FM-REPLICATION-010
    #[tokio::test]
    async fn a_diverged_applier_resumes_on_the_history_a_resync_installs() {
        let (stint, offsets, applied) = resyncable();
        let (tx, rx) = mpsc::channel(64);
        let applier = Arc::new(MockApplier {
            reject: Some("DEL".to_string()),
            ..Default::default()
        });
        let flag = Arc::new(AtomicBool::new(true));
        let recorded = applier.clone();
        let consumer = tokio::spawn(async move {
            consume_frames(rx, SharedApplier(recorded), flag, None, stint).await
        });

        tx.send(live(frame_on(0, 1, "DEL", &["k"]))).await.unwrap();
        while !applied.has_diverged() {
            tokio::task::yield_now().await;
        }

        // The connection's response: rewind, reconnect, full resync. The
        // consumer is untouched throughout.
        assert!(offsets.reset_to(0), "the rewind must be accepted");
        assert!(offsets.reset_to(5_000), "the install must be accepted");
        let fresh = frame_on(0, 1, "SET", &["new", "1"]);
        let received = offsets.frame_advance(&fresh);
        tx.send(StreamedFrame::new(applied.epoch(), fresh))
            .await
            .unwrap();
        drop(tx);
        consumer.await.unwrap();

        assert_eq!(
            *applier.groups.lock().unwrap(),
            vec![(0, vec!["SET".to_string()])],
            "the consumer was retired by the divergence and applied nothing after it"
        );
        assert_eq!(applied.current(), received);
        assert!(!applied.has_diverged());
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
        tx.send(live(applied_frame)).await.unwrap();
        while applier.groups.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        // Promote, THEN hand over frame 2: the loop sees the flipped flag and
        // stops with that frame consumed but never applied — exactly the state a
        // real promotion leaves the 10k-deep frame channel in.
        flag.store(false, Ordering::Release);
        tx.send(live(frame_on(0, 2, "SET", &["b", "2"])))
            .await
            .unwrap();
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

    // ---- frames that outlive their history (issue 06) ---------------------

    /// The resync harness: the stint the consumer claims through, the
    /// `ReplicaOffset` a connection resyncs on, and the applied head both share.
    ///
    /// Order matters and is the one the real wiring uses — the stint is opened
    /// before the connection is built, so `reset_to` is not refused as coming
    /// from a retired stream.
    fn resyncable() -> (ReplicaApplyStint, ReplicaOffset, AppliedOffset) {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(
            Arc::new(RwLock::new(ReplicationState::new())),
            Arc::new(AtomicU64::new(0)),
            applied.clone(),
        );
        (stint, offsets, applied)
    }

    /// Run the consume loop to channel close and report what applied.
    async fn consume(
        rx: mpsc::Receiver<StreamedFrame>,
        stint: ReplicaApplyStint,
    ) -> Vec<(u16, Vec<String>)> {
        let applier = Arc::new(MockApplier::default());
        let flag = Arc::new(AtomicBool::new(true));
        consume_frames(rx, SharedApplier(applier.clone()), flag, None, stint).await;
        applier.groups.lock().unwrap().clone()
    }

    /// Issue 06: the frame channel and its consumer outlive the connection that
    /// fills them, so a link that drops mid-stream leaves decoded frames queued
    /// for the next stint. If that stint comes back `+FULLRESYNC`, those frames
    /// describe a keyspace the install just replaced: applying them writes the
    /// old primary's stream onto the new dataset, and claiming their bytes
    /// credits the *new* history with data it does not hold.
    // FM-REPLICATION-007
    #[tokio::test]
    async fn a_full_resync_discards_the_frames_queued_from_the_previous_history() {
        let (stint, offsets, applied) = resyncable();
        let (tx, rx) = mpsc::channel(64);

        // Decoded under the old history, still queued when the link drops.
        tx.send(live(frame_on(0, 1, "SET", &["old", "1"])))
            .await
            .unwrap();

        // The retry is granted a full resync: the installed dataset moves both
        // heads and starts a new history.
        assert!(offsets.reset_to(5_000), "the reset must be accepted");
        let fresh = frame_on(0, 1, "DEL", &["new"]);
        // As the decode loop does: the received head moves when the frame is
        // read off the socket, the applied head only when it is claimed.
        let received = offsets.frame_advance(&fresh);
        tx.send(StreamedFrame::new(applied.epoch(), fresh))
            .await
            .unwrap();
        drop(tx);

        assert_eq!(
            consume(rx, stint).await,
            vec![(0, vec!["DEL".to_string()])],
            "a frame from the replaced history reached the keyspace"
        );
        assert_eq!(
            applied.current(),
            received,
            "the void frame's bytes were credited to the new history"
        );
        assert_eq!(
            applied.current(),
            offsets.current(),
            "the applied head ran past the head this node has received"
        );
    }

    /// The same hazard one level in: the link drops *inside* a `MULTI` group, so
    /// the next history's first frames land on an open group. Continuing it
    /// would apply the old primary's half-transaction — closed by an `EXEC`
    /// from the other side of the resync — onto the installed dataset.
    // FM-REPLICATION-007
    #[tokio::test]
    async fn a_multi_group_left_open_by_a_dropped_link_is_never_closed_by_the_next_history() {
        let (stint, offsets, applied) = resyncable();
        let (tx, rx) = mpsc::channel(64);

        for frame in [
            frame_on(3, 1, "MULTI", &[]),
            frame_on(3, 2, "SET", &["old", "1"]),
        ] {
            tx.send(live(frame)).await.unwrap();
        }

        assert!(offsets.reset_to(5_000));
        let epoch = applied.epoch();
        // The new history resumes mid-transaction from *its* primary's point of
        // view: a bare command and the `EXEC` that closes the group this
        // replica never saw opened. Neither may touch the group the resync
        // voided — the bare command is a group of one on its own tagged shard,
        // and the `EXEC` closes nothing.
        let fresh = [frame_on(1, 1, "DEL", &["new"]), frame_on(1, 2, "EXEC", &[])];
        let fresh_bytes: u64 = fresh.iter().map(|f| f.stream_advance()).sum();
        for frame in fresh {
            tx.send(StreamedFrame::new(epoch, frame)).await.unwrap();
        }
        drop(tx);

        assert_eq!(
            consume(rx, stint).await,
            vec![(1, vec!["DEL".to_string()])],
            "the abandoned group's commands (or its shard) survived the resync"
        );
        assert_eq!(
            applied.current(),
            5_000 + fresh_bytes,
            "the abandoned group's bytes were claimed by the new history"
        );
    }

    /// The other half of the rule: a `+CONTINUE` resume installs no dataset and
    /// resets no head, so it starts no new history — the frames it left queued
    /// are still the ones this keyspace expects, including a `MULTI` group split
    /// across the reconnect.
    // FM-REPLICATION-007
    #[tokio::test]
    async fn a_continue_resume_still_applies_the_frames_it_left_queued() {
        let (stint, _offsets, applied) = resyncable();
        let (tx, rx) = mpsc::channel(64);

        let frames = [
            frame_on(2, 1, "MULTI", &[]),
            frame_on(2, 2, "SET", &["a", "1"]),
            // --- link drops here; the retry is granted +CONTINUE ---
            frame_on(2, 3, "SET", &["b", "2"]),
            frame_on(2, 4, "EXEC", &[]),
        ];
        let total: u64 = frames.iter().map(|f| f.stream_advance()).sum();
        let epoch = applied.epoch();
        for frame in frames {
            tx.send(StreamedFrame::new(epoch, frame)).await.unwrap();
        }
        drop(tx);

        assert_eq!(
            applied.epoch(),
            0,
            "a resume that adopts no dataset must not start a new history"
        );
        assert_eq!(
            consume(rx, stint).await,
            vec![(2, vec!["SET".to_string(), "SET".to_string()])],
            "a group split across a +CONTINUE reconnect must still apply whole"
        );
        assert_eq!(applied.current(), total);
    }

    /// Spawn a consumer over `applied` whose applies park until the returned
    /// gate is released, plus a notifier that fires as each apply parks.
    #[allow(clippy::type_complexity)]
    fn parked_consumer(
        applied: &AppliedOffset,
    ) -> (
        mpsc::Sender<StreamedFrame>,
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

    /// Issue 76: the consume loop claims a group before dispatching it, so
    /// between the claim and the shard's reply the claimed head describes data
    /// no shard has. The offset the replica ACKs — and therefore what `WAIT`
    /// counts — must not move until the apply returns.
    // FM-REPLICATION-008
    #[tokio::test]
    async fn a_group_in_flight_to_its_shard_is_claimed_but_not_yet_ackable() {
        let applied = AppliedOffset::detached(0);
        let (tx, _applier, gate, entered, consumer) = parked_consumer(&applied);

        let in_flight = frame_on(0, 1, "SET", &["a", "1"]);
        let in_flight_bytes = in_flight.stream_advance();
        tx.send(live(in_flight)).await.unwrap();
        entered.notified().await;

        assert_eq!(
            applied.current(),
            in_flight_bytes,
            "the boundary must cover the group already on its way to the shard"
        );
        assert_eq!(
            applied.landed(),
            0,
            "WAIT was satisfied by a write still in flight to its shard"
        );

        // The shard replies: the landed head catches up, and the wait the ACK
        // path parks on resolves at the same offset.
        gate.add_permits(1);
        assert_eq!(
            applied.wait_until_applied(in_flight_bytes).await,
            in_flight_bytes
        );
        drop(tx);
        consumer.await.unwrap();
        assert_eq!(applied.landed(), applied.current());
    }

    /// A `REPLCONF GETACK` reaches no shard, so it lands the moment it is
    /// claimed — otherwise the answer it solicits, which covers its own bytes,
    /// could never be given.
    // FM-REPLICATION-008
    #[tokio::test]
    async fn a_frame_that_touches_no_shard_lands_as_it_is_claimed() {
        let applied = AppliedOffset::detached(0);
        let (tx, _applier, _gate, _entered, consumer) = parked_consumer(&applied);

        let getack = frame_on(0, 1, "REPLCONF", &["GETACK", "*"]);
        let bytes = getack.stream_advance();
        tx.send(live(getack)).await.unwrap();

        assert_eq!(applied.wait_until_applied(bytes).await, bytes);
        drop(tx);
        consumer.await.unwrap();
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
        tx.send(live(in_flight)).await.unwrap();
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
        tx.send(live(frame_on(0, 2, "SET", &["b", "2"])))
            .await
            .unwrap();
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
        tx.send(live(in_flight)).await.unwrap();
        entered.notified().await;

        // A new stream opens while the old consumer is mid-apply.
        let _next = applied.begin_replica_stint();

        gate.add_permits(1);
        tx.send(live(frame_on(0, 2, "SET", &["b", "2"])))
            .await
            .unwrap();
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
