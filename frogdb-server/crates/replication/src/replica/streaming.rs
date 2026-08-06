//! WAL frame streaming and ACK handling for the replica side.
//!
//! What the replica ACKs is the **landed** offset — the data a shard has
//! actually applied — not the received head and not the claimed head. WAIT on
//! the primary counts these ACKs, so an ACK is a durability claim: every frame
//! at or below the acked offset has been applied to its shard, and would still
//! be there if this node were promoted a moment later.
//!
//! Neither of the other two heads can say that. The received head counts frames
//! still sitting in the 10k-deep channel — a promotion settles the node at its
//! applied offset and discards them, so ACKing it would let `WAIT 1` report
//! success for a write the promoted node then threw away. The claimed head
//! counts the group in flight between the applier's claim and the shard's reply:
//! it is what the node *will* hold, which is the right answer for the promotion
//! boundary and the wrong one for a durability claim. Redis has the same rule
//! for the same reason: its replicas ack after executing the command stream,
//! not on receipt (issue 76).

use super::connection::ReplicaConnection;
use super::offset::AppliedOffset;
use crate::apply::StreamedFrame;
use crate::frame::{ReplconfCodec, ReplicationFrameCodec};
use bytes::BytesMut;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::codec::Decoder;

/// The owed-ACK branch of the streaming loop: resolves at the offset a
/// `REPLCONF GETACK` solicited once the applier reaches it, and parks forever
/// when nothing is owed.
///
/// A free function because a disabled `select!` branch still *evaluates* its
/// expression (it only skips polling it), so the "no solicitation pending" case
/// has to be a future rather than a precondition.
async fn solicited_ack(applied: &AppliedOffset, pending: Option<u64>) -> u64 {
    match pending {
        Some(target) => applied.wait_until_applied(target).await,
        None => std::future::pending().await,
    }
}

impl ReplicaConnection {
    pub(crate) async fn stream_replication(
        &mut self,
        frame_tx: &mpsc::Sender<StreamedFrame>,
    ) -> io::Result<()> {
        tracing::info!("Starting replication stream");
        let mut codec = ReplicationFrameCodec::new();
        // Seeded, not fresh: a full sync's trailer and the first live frames
        // routinely land in the same read, so the payload reader hands over
        // whatever it buffered past the trailer. Starting from an empty buffer
        // here is what silently dropped those frames (hardening issue 01).
        let mut buf = self.take_pending_stream_bytes();
        // Capacity hint only — `BytesMut` grows on demand, so every value here
        // decodes the same bytes into the same frames and differs only in how
        // often the buffer reallocates. Documented equivalent mutant: no test
        // can observe the arithmetic in this argument.
        buf.reserve(64 * 1024);
        // Cloned out of `self` so the solicited-ACK branch below does not borrow
        // the connection the socket-read branch holds mutably.
        let applied = self.offsets.applied().clone();
        // The history this stream belongs to, read once: every reset that could
        // bump it (the FULLRESYNC grant, the checkpoint install) has already
        // happened by the time PSYNC hands over to streaming, and nothing resets
        // the heads again until the next connection attempt. Stamped on each
        // frame so a consumer that outlives this stream can tell them from the
        // ones a previous stint left in the channel (issue 06).
        let epoch = applied.epoch();
        // The offset a `REPLCONF GETACK` is owed an answer at, while the applier
        // is still short of it. Held out here, and answered by a `select!` branch
        // below, rather than awaited where the GETACK is decoded: the wait can
        // last as long as the applier is behind, and a decode loop that spends
        // it not polling the socket stops reading the link it is being asked
        // about — TCP backpressure onto the primary's shared broadcast, a
        // skipped ACK tick, and the catch-up it is waiting for noticed late
        // (issue 09).
        let mut pending_ack: Option<u64> = None;
        // A divergence admitted under a previous connection and never acted on
        // (the link dropped before the applier's signal reached it) would
        // otherwise let this stream resume — possibly on a `+CONTINUE` — over a
        // keyspace known to be wrong. The latch outlives connections precisely
        // so this check can be the first thing a new one does.
        if applied.has_diverged() {
            return Err(self.abandon_diverged_link());
        }
        // Drain them before the first read: they may be whole frames, and the
        // primary owes this replica nothing further until they are ACKed — a
        // decode that waited for the next byte off the socket could wait
        // forever.
        if !self
            .drain_frames(&mut codec, &mut buf, frame_tx, epoch, &mut pending_ack)
            .await?
        {
            return Ok(());
        }
        // The spontaneous ACK cadence comes from config (`replication.ack-interval-ms`,
        // Redis `repl-ping-replica-period`), threaded in at connection construction.
        let mut ack_interval = tokio::time::interval(self.ack_interval);
        loop {
            tokio::select! {
                result = self.stream.read_buf(&mut buf) => {
                    match result {
                        Ok(0) => { tracing::info!("Primary connection closed"); return Ok(()); }
                        Ok(_) => {
                            if !self.drain_frames(&mut codec, &mut buf, frame_tx, epoch, &mut pending_ack).await? {
                                return Ok(());
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                _ = ack_interval.tick() => {
                    // The landed head, never the claimed one: an ACK is a
                    // durability claim about data a shard has actually applied
                    let offset = applied.landed();
                    self.send_ack(offset).await?;
                }
                reached = solicited_ack(&applied, pending_ack) => {
                    pending_ack = None;
                    self.send_ack(reached).await?;
                }
                // The applier could not apply a replicated write, so this
                // node's keyspace has provably diverged from the primary's.
                // A `select!` branch rather than a check between frames: the
                // applier is a separate task, and the divergence may be
                // discovered while this loop is idle (issue 08).
                _ = applied.divergence() => {
                    return Err(self.abandon_diverged_link());
                }
            }
        }
    }

    /// End this link after an admitted divergence, and set the reconnect up to
    /// come back through a full resync.
    ///
    /// Rewinding the received head to 0 is what does it: `psync_request_args`
    /// sends `PSYNC ? -1` at offset 0, so the primary can only answer
    /// `+FULLRESYNC` — never a `+CONTINUE` over the write this node failed to
    /// apply. The same rewind is how a failed checkpoint install retries its
    /// sync. The rewind also clears the divergence latch (it is a `reset_pair`,
    /// i.e. a fresh history), which is why nothing else has to.
    ///
    /// Reported as an **error** rather than a clean close so
    /// `ReplicaReplicationHandler::start` takes its backing-off retry path: a
    /// primary whose stream deterministically fails to apply here would
    /// otherwise be re-full-resynced every 100ms for as long as it kept sending
    /// the offending write.
    fn abandon_diverged_link(&self) -> io::Error {
        tracing::error!(
            applied = self.offsets.applied().current(),
            received = self.offsets.current(),
            "Replica has diverged from its primary; dropping the link and \
             rewinding so the reconnect asks for a full resync"
        );
        // Refused only if a promotion froze the heads or a newer stream took
        // over — in both cases this connection is finished anyway, and the latch
        // stays set so the *next* stream re-runs this path.
        let _ = self.offsets.reset_to(0);
        io::Error::other("replica diverged from its primary; forcing a full resync")
    }

    /// Decode every complete frame sitting in `buf`, advance the received head
    /// and queue each frame for the applier, recording any GETACK as an answer
    /// owed at the offset it covers.
    ///
    /// Returns `false` when the frame channel has closed, which ends the stream.
    async fn drain_frames(
        &mut self,
        codec: &mut ReplicationFrameCodec,
        buf: &mut BytesMut,
        frame_tx: &mpsc::Sender<StreamedFrame>,
        epoch: u64,
        pending_ack: &mut Option<u64>,
    ) -> io::Result<bool> {
        // The frame lane (hardening issue 29): `decode` shrinks `buf` by
        // exactly the bytes it consumed on each successful frame, so a single
        // before/after diff over the whole drain covers every frame decoded
        // here — live frames and a full sync's over-read carryover alike —
        // with no risk of double-counting the full-sync payload itself, which
        // is recorded separately (and earlier) from `FullSyncMetadata::rdb_size`.
        let starting_len = buf.len();
        while let Some(frame) = codec.decode(buf)? {
            // Advance the live applied offset (also the cluster-bus handle).
            // The advance unit is the RESP payload only (see
            // `ReplicationFrame::stream_advance`), the same unit the primary
            // advances by, so the replica's ACK is directly comparable.
            let received = self.offsets.frame_advance(&frame);
            tracing::trace!(
                sequence = frame.sequence,
                offset = received,
                "Received replication frame"
            );
            // The primary's GETACK is an ack solicitation (sent by WAIT):
            // answer immediately instead of waiting for the next 1-second
            // spontaneous ACK tick, matching Redis replicas. The solicited
            // offset covers the GETACK frame itself, as in Redis.
            let solicited = ReplconfCodec::is_getack(&frame.payload);
            if frame_tx
                .send(StreamedFrame::new(epoch, frame))
                .await
                .is_err()
            {
                tracing::warn!("Frame channel closed");
                self.net_bytes
                    .record_input((starting_len - buf.len()) as u64);
                return Ok(false);
            }
            if solicited {
                // ACKs are cumulative, so a solicitation arriving while one is
                // already owed raises the target instead of queueing a second
                // answer: one ACK at the newer offset answers both.
                *pending_ack = Some(pending_ack.unwrap_or(0).max(received));
            }
        }
        self.net_bytes
            .record_input((starting_len - buf.len()) as u64);
        Ok(true)
    }

    async fn send_ack(&mut self, offset: u64) -> io::Result<()> {
        self.stream
            .write_all(&ReplconfCodec::encode_ack(offset))
            .await?;
        tracing::trace!(offset = offset, "Sent ACK to primary");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::ReplicationFrame;
    use crate::net_bytes::{NetByteCounters, NetByteCountersSnapshot};
    use crate::replica::connection::{ConnectionState, ReplicaConnection};
    use crate::replica::offset::{AppliedOffset, Claim, ReplicaApplyStint, ReplicaOffset};
    use crate::state::ReplicationState;
    use bytes::{Buf, Bytes};
    use parking_lot::RwLock;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::io::DuplexStream;
    use tokio::task::JoinHandle;

    /// One cadence, long enough that the `interval`'s immediate first tick is
    /// the only spontaneous ACK a test sees: every later ACK is attributable to
    /// a solicitation.
    const CADENCE: Duration = Duration::from_secs(60);

    /// A replica streaming loop wired to a duplex stand-in for the primary.
    ///
    /// These tests run **no** frame consumer on purpose: the applied head only
    /// moves when a test moves it, which is precisely the "applier is behind the
    /// received head" condition this mode is about.
    struct Link {
        primary: DuplexStream,
        applied: AppliedOffset,
        /// The applier's stint, opened *before* the connection is built exactly
        /// as the real wiring does — so the connection's own `reset_to` is not
        /// refused as coming from a retired stream.
        stint: ReplicaApplyStint,
        /// The received head the streaming loop advances (the connection's own
        /// `ReplicaOffset` is moved into it).
        received: Arc<AtomicU64>,
        state: Arc<RwLock<ReplicationState>>,
        frames: mpsc::Receiver<StreamedFrame>,
        acks: BytesMut,
        task: JoinHandle<io::Result<()>>,
        /// The connection's net-byte input tally (hardening issue 29), kept
        /// alongside it here the same way `received`/`applied` are: the
        /// connection itself is moved into `task`, so anything a test needs to
        /// read back has to be captured before the move.
        net_bytes: Arc<NetByteCounters>,
    }

    impl Link {
        fn start() -> Self {
            let applied = AppliedOffset::detached(0);
            let stint = applied.begin_replica_stint();
            Self::connect(
                Arc::new(RwLock::new(ReplicationState::new())),
                applied,
                stint,
                Arc::new(AtomicU64::new(0)),
            )
        }

        /// A second connection over the same node — same heads, same applier
        /// stint — as `ReplicaReplicationHandler`'s reconnect loop builds after
        /// a link drops.
        fn reconnect(&self) -> Self {
            Self::connect(
                self.state.clone(),
                self.applied.clone(),
                self.stint.clone(),
                self.received.clone(),
            )
        }

        fn connect(
            state: Arc<RwLock<ReplicationState>>,
            applied: AppliedOffset,
            stint: ReplicaApplyStint,
            received: Arc<AtomicU64>,
        ) -> Self {
            let (primary, replica) = tokio::io::duplex(64 * 1024);
            let offsets = ReplicaOffset::new(state.clone(), received.clone(), applied.clone());
            let net_bytes = Arc::new(NetByteCounters::default());
            let mut conn = ReplicaConnection {
                stream: Box::new(replica),
                _primary_addr: "127.0.0.1:6379".parse().unwrap(),
                state: state.clone(),
                connection_state: ConnectionState::Streaming,
                data_dir: PathBuf::from("/tmp/frogdb-test"),
                offsets,
                link_up: Arc::new(AtomicBool::new(true)),
                ack_interval: CADENCE,
                snapshot_installer: None,
                sync_refusal: Arc::new(RwLock::new(None)),
                pending_stream_bytes: BytesMut::new(),
                net_bytes: net_bytes.clone(),
            };
            let (frame_tx, frames) = mpsc::channel(16);
            let task = tokio::spawn(async move { conn.stream_replication(&frame_tx).await });
            Self {
                primary,
                applied,
                stint,
                received,
                state,
                frames,
                acks: BytesMut::new(),
                task,
                net_bytes,
            }
        }

        /// The next frame handed to the applier, unwrapped from its history
        /// stamp (which these tests never change).
        async fn next_frame(&mut self) -> ReplicationFrame {
            self.frames.recv().await.expect("the link closed").frame
        }

        /// Put one frame on the wire, as the primary's broadcast would.
        async fn send(&mut self, sequence: u64, payload: Bytes) {
            let frame = ReplicationFrame::new(sequence, payload);
            self.primary
                .write_all(&frame.encode().unwrap())
                .await
                .unwrap();
        }

        async fn next_ack(&mut self) -> u64 {
            loop {
                if let Some((offset, consumed)) = ReplconfCodec::parse_ack(&self.acks) {
                    self.acks.advance(consumed);
                    return offset;
                }
                let n = self.primary.read_buf(&mut self.acks).await.unwrap();
                assert!(n > 0, "the replica closed the link");
            }
        }
    }

    /// A `ReplicaConnection` wired the same way `Link::connect` wires one, but
    /// with nothing spawned to drive it — for tests that call `drain_frames`
    /// directly and control `buf` and the frame channel by hand, with no
    /// task-scheduling race to reason about (unlike driving the same branch
    /// through `Link` and `stream_replication`, where the moment a spawned
    /// task actually gets polled relative to the test's own `.await` points
    /// is not something a test can pin down).
    fn bare_connection() -> (ReplicaConnection, Arc<NetByteCounters>) {
        let state = Arc::new(RwLock::new(ReplicationState::new()));
        let offsets = ReplicaOffset::new(
            state.clone(),
            Arc::new(AtomicU64::new(0)),
            AppliedOffset::detached(0),
        );
        let net_bytes = Arc::new(NetByteCounters::default());
        let (_unused, replica) = tokio::io::duplex(64);
        let conn = ReplicaConnection {
            stream: Box::new(replica),
            _primary_addr: "127.0.0.1:6379".parse().unwrap(),
            state,
            connection_state: ConnectionState::Streaming,
            data_dir: PathBuf::from("/tmp/frogdb-test"),
            offsets,
            link_up: Arc::new(AtomicBool::new(true)),
            ack_interval: CADENCE,
            snapshot_installer: None,
            sync_refusal: Arc::new(RwLock::new(None)),
            pending_stream_bytes: BytesMut::new(),
            net_bytes: net_bytes.clone(),
        };
        (conn, net_bytes)
    }

    /// A GETACK solicitation as it appears on the stream. Its own payload is
    /// part of the stream, so the answer it solicits covers it
    /// (`payload.len()`, per `frame_advance`).
    fn getack() -> Bytes {
        ReplconfCodec::encode_getack()
    }

    /// Issue 09: a solicited ACK is answered off the decode path, so the frames
    /// the primary already streamed behind the GETACK keep being decoded while
    /// the applier catches up.
    ///
    /// Before the fix the answer was awaited *inline* in `drain_frames`: the
    /// loop stopped polling the socket for as long as the applier was behind
    /// (bounded only by the ACK cadence), so a `WAIT` against a busy replica
    /// throttled the very stream it was waiting on.
    // FM-REPLICATION-006
    #[tokio::test(start_paused = true)]
    async fn a_solicited_ack_does_not_stall_the_decode_loop() {
        let mut link = Link::start();
        let getack = getack();
        link.send(0, getack).await;
        link.send(1, Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"))
            .await;

        let start = tokio::time::Instant::now();
        let first = link.next_frame().await;
        let second = link.next_frame().await;
        let stalled = start.elapsed();

        assert!(ReplconfCodec::is_getack(&first.payload));
        assert_eq!(second.payload, Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));
        assert_eq!(
            stalled,
            Duration::ZERO,
            "the decode loop waited on the applier before reading the next frame"
        );
    }

    /// The prompt answer itself: the moment the applier reaches the solicited
    /// offset the ACK goes out, without waiting for the next spontaneous tick.
    // FM-REPLICATION-006
    #[tokio::test(start_paused = true)]
    async fn a_solicited_ack_is_sent_as_soon_as_the_applier_catches_up() {
        let mut link = Link::start();
        // The cadence's immediate first tick: the replica has applied nothing.
        assert_eq!(link.next_ack().await, 0);

        let getack = getack();
        let target = getack.len() as u64;
        link.send(0, getack).await;
        let frame = link.next_frame().await;
        assert!(ReplconfCodec::is_getack(&frame.payload));

        let start = tokio::time::Instant::now();
        link.applied.frame_applied(&frame);
        assert_eq!(link.next_ack().await, target, "the ACK covers the GETACK");
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "the answer waited for the spontaneous cadence instead of the applier"
        );
    }

    /// ACKs are cumulative, so a second solicitation raises the target of the
    /// one already owed rather than queueing a second answer.
    // FM-REPLICATION-006
    #[tokio::test(start_paused = true)]
    async fn a_second_getack_raises_the_target_to_the_newer_offset() {
        let mut link = Link::start();
        assert_eq!(link.next_ack().await, 0);

        let getack = getack();
        let target = 2 * getack.len() as u64;
        link.send(0, getack.clone()).await;
        let first = link.next_frame().await;
        link.send(1, getack).await;
        let second = link.next_frame().await;

        let start = tokio::time::Instant::now();
        link.applied.frame_applied(&first);
        link.applied.frame_applied(&second);
        assert_eq!(link.next_ack().await, target);
        assert_eq!(start.elapsed(), Duration::ZERO);

        // One answer, not two: the next ACK is the spontaneous one, a whole
        // cadence away.
        let quiet = tokio::time::timeout(CADENCE / 2, link.next_ack()).await;
        assert!(
            quiet.is_err(),
            "a second ACK went out for a solicitation the first already covered"
        );
    }

    /// The reason the owed answer needs no timeout: an applier that never
    /// catches up parks one branch of the loop, and the spontaneous cadence
    /// keeps reporting the same truthful, lower head a timeout would have made
    /// the solicited ACK carry.
    // FM-REPLICATION-006
    #[tokio::test(start_paused = true)]
    async fn the_ack_cadence_survives_a_solicitation_that_can_never_be_answered() {
        let mut link = Link::start();
        assert_eq!(link.next_ack().await, 0);

        link.send(0, getack()).await;
        link.next_frame().await;
        // The applier is wedged: nothing ever moves the applied head.

        let start = tokio::time::Instant::now();
        assert_eq!(link.next_ack().await, 0);
        assert_eq!(
            start.elapsed(),
            CADENCE,
            "the spontaneous ACK tick stopped while an answer was owed"
        );
    }

    /// Issue 76: an ACK is a durability claim, so both branches report the
    /// **landed** head — what a shard has applied — never the claimed head the
    /// applier moves before it dispatches the group.
    ///
    /// The gap is one group wide and is exactly where `WAIT` used to overstate:
    /// the applier claims, `WAIT` is satisfied, and the write is still in flight
    /// (or, before the received/applied split, still queued) when the replica is
    /// killed.
    // FM-REPLICATION-008
    #[tokio::test(start_paused = true)]
    async fn an_ack_reports_the_landed_head_not_the_claimed_one() {
        let mut link = Link::start();
        // The cadence's immediate first tick: nothing claimed, nothing landed.
        assert_eq!(link.next_ack().await, 0);
        let stint = link.stint.clone();

        let getack = getack();
        let target = getack.len() as u64;
        link.send(0, getack).await;
        assert!(ReplconfCodec::is_getack(&link.next_frame().await.payload));

        // The applier has claimed the frame's bytes — the promotion boundary
        // covers them — but the shard has not replied yet.
        assert_eq!(stint.claim(stint.epoch(), target), Claim::Granted);
        let start = tokio::time::Instant::now();
        assert_eq!(
            link.next_ack().await,
            0,
            "the ACK covered a group still in flight to its shard"
        );
        assert_eq!(
            start.elapsed(),
            CADENCE,
            "that was the solicited answer, not the spontaneous tick"
        );

        // The shard applied it: now, and only now, the answer goes out.
        let start = tokio::time::Instant::now();
        stint.land();
        assert_eq!(link.next_ack().await, target);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    /// Issue 08: an apply that came back `Err` is proof this node's keyspace no
    /// longer matches the primary's. The connection is the task that has to act
    /// on it — drop the link and rewind, so the reconnect can only be answered
    /// `+FULLRESYNC` and never a `+CONTINUE` over the write that failed.
    // FM-REPLICATION-010
    #[tokio::test(start_paused = true)]
    async fn an_admitted_divergence_drops_the_link_and_rewinds_for_a_full_resync() {
        let mut link = Link::start();
        assert_eq!(link.next_ack().await, 0);

        let payload = Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
        link.send(0, payload.clone()).await;
        let frame = link.next_frame().await;
        assert_eq!(link.received.load(Ordering::Acquire), payload.len() as u64);

        // The applier claimed it, dispatched it, and the shard rejected it.
        assert_eq!(
            link.stint
                .claim(link.applied.epoch(), frame.stream_advance()),
            Claim::Granted
        );
        link.stint.admit_divergence(link.applied.epoch());

        let outcome = link.task.await.expect("the streaming task panicked");
        assert!(
            outcome.is_err(),
            "a divergence ended the link as a clean close, which the reconnect \
             loop retries without backing off"
        );
        assert_eq!(
            link.received.load(Ordering::Acquire),
            0,
            "the received head was left where a `+CONTINUE` could resume from"
        );
        assert_eq!(link.applied.current(), 0);
        assert!(
            !link.applied.has_diverged(),
            "the rewind is a fresh history, so it clears the latch"
        );
    }

    /// A divergence admitted after the link had already dropped must not be
    /// lost: the latch outlives connections, so the next stream re-runs the
    /// same abandonment before it decodes a single frame — a `+CONTINUE` must
    /// never resume over a keyspace already known to be wrong.
    // FM-REPLICATION-010
    #[tokio::test(start_paused = true)]
    async fn a_divergence_outstanding_at_connect_abandons_the_new_link_at_once() {
        let link = Link::start();
        // The link drops before the applier's signal can reach it, so nothing
        // rewound and nothing cleared the latch.
        link.task.abort();
        link.stint.admit_divergence(link.applied.epoch());
        link.received.store(4_096, Ordering::Release);

        let next = link.reconnect();
        assert!(
            next.task.await.unwrap().is_err(),
            "a stream resumed over a keyspace already admitted diverged"
        );
        assert_eq!(
            next.received.load(Ordering::Acquire),
            0,
            "the reconnect was left able to ask for a `+CONTINUE`"
        );
    }

    // FM-REPLICATION-063
    /// Hardening issue 29: the frame lane of `total_net_repl_input_bytes` —
    /// bytes `drain_frames` actually decoded off the wire, not a value
    /// derived from the applied offset (which counts RESP payload only, not
    /// the transport header every frame also carries).
    #[tokio::test]
    async fn repl_input_bytes_grow_on_the_replica_as_it_applies() {
        let mut link = Link::start();
        assert_eq!(
            link.net_bytes.snapshot(),
            NetByteCountersSnapshot::default()
        );

        let payload = Bytes::from_static(b"*1\r\n$4\r\nPING\r\n");
        let expected = ReplicationFrame::new(0, payload.clone()).encoded_size() as u64;
        link.send(0, payload).await;
        let frame = link.next_frame().await;
        link.applied.frame_applied(&frame);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let seen = link.net_bytes.snapshot();
            if seen.input == expected {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "input bytes never reached {expected}; last read {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            link.net_bytes.snapshot(),
            NetByteCountersSnapshot {
                output: 0,
                input: expected
            },
            "the replica records input, never output, for a received frame"
        );
    }

    // FM-REPLICATION-063
    /// `drain_frames`' success exit (the tally taken after the decode loop
    /// runs dry) must record only the bytes of the frames it actually
    /// decoded, not the partial next frame's carryover bytes still sitting in
    /// `buf` — the same carryover the full-sync-over-read comment on
    /// `drain_frames` describes. With nothing left over, `starting_len -
    /// buf.len()`, `starting_len + buf.len()`, and (with `buf.len() == 0`) a
    /// division all read the same number, which is exactly why a prior full
    /// pass of this suite left the `-`-to-`+` mutation at this line
    /// unnoticed; a real partial trailing frame is required to tell them
    /// apart.
    #[tokio::test]
    async fn repl_input_bytes_count_only_whole_frames_leaving_partial_carryover_uncounted() {
        let mut link = Link::start();

        let frame = ReplicationFrame::new(0, Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));
        let expected = frame.encoded_size() as u64;
        let encoded = frame.encode().unwrap();
        // A truncated second frame: enough bytes to be non-empty carryover,
        // never enough to decode as a whole frame.
        let mut wire = encoded.to_vec();
        wire.extend_from_slice(&encoded[..4]);
        link.primary.write_all(&wire).await.unwrap();
        let received = link.next_frame().await;
        link.applied.frame_applied(&received);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let seen = link.net_bytes.snapshot().input;
            if seen == expected {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "input bytes never settled at {expected} (the one whole frame); \
                 last read {seen} — the partial trailing bytes leaked into (or \
                 were dropped from) the tally"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    // FM-REPLICATION-063
    /// `drain_frames`' frame-channel-closed exit records only the bytes of
    /// the frame whose send actually failed — never a second frame's bytes
    /// still sitting undecoded in `buf` when the receiver drops mid-drain.
    /// Exercises the branch `repl_input_bytes_grow_on_the_replica_as_it_applies`
    /// never reaches (its channel never closes), which is why a `-`-to-`+`/`-`-to-`/`
    /// mutation at this exit survived a prior pass: with the receiver open the
    /// branch is dead code from that test's point of view.
    ///
    /// Drives `drain_frames` directly (see `bare_connection`) rather than
    /// through a spawned `stream_replication` task: closing the channel and
    /// decoding the buffer both happen synchronously in this test's own
    /// control flow, so there is no task-scheduling order to get right.
    #[tokio::test]
    async fn repl_input_bytes_count_only_the_frame_actually_sent_when_the_channel_closes_mid_drain()
    {
        let (mut conn, net_bytes) = bare_connection();
        let mut codec = ReplicationFrameCodec::new();

        let frame1 = ReplicationFrame::new(0, Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));
        let frame2 = ReplicationFrame::new(1, Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));
        let expected = frame1.encoded_size() as u64;
        let mut wire = frame1.encode().unwrap().to_vec();
        wire.extend_from_slice(&frame2.encode().unwrap());
        let mut buf = BytesMut::from(&wire[..]);

        // Dropped before `drain_frames` is even called, so the very first
        // successful decode's send fails deterministically — no timing to
        // race, unlike closing it out from under an already-spawned task.
        let (frame_tx, frame_rx) = mpsc::channel(16);
        drop(frame_rx);

        let mut pending_ack = None;
        let ok = conn
            .drain_frames(&mut codec, &mut buf, &frame_tx, 0, &mut pending_ack)
            .await
            .unwrap();

        assert!(!ok, "drain_frames must report the channel closed");
        assert_eq!(
            net_bytes.snapshot().input,
            expected,
            "only the frame whose send failed should be counted, not the \
             second, undecoded frame still sitting in `buf`"
        );
    }
}
