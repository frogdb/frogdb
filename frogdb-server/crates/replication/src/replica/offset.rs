//! The replica-side live offset, owned in one place.

use crate::frame::ReplicationFrame;
use crate::fullsync::ShardCoverage;
use crate::offset_coordinator::OffsetCoordinator;
use crate::state::ReplicationState;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Where the offsets a node is measuring a window grant against came from.
///
/// The distinction matters only for `+CONTINUE` grants that *replace* the
/// history (FM-REPLICATION-066 / TR-REPLICATION-034): such a grant resumes the
/// stream over whatever the node already holds, so it is safe only when the node
/// can say that what it holds stops at its claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetProvenance {
    /// Established in this process by a full-sync install: the offsets and the
    /// keyspace were adopted together from one payload, and the payload's
    /// coverage vector describes exactly what sits above the claim.
    Installed,
    /// Seeded from the persisted replication state at boot. The persisted offset
    /// lags the applied head by construction (it is reconciled at save points),
    /// so a keyspace recovered alongside it may hold effects above the claim —
    /// and nothing recovered with it describes *which*. Until issue 36 pairs the
    /// two atomically, such a node refuses window grants outright (ruling R16);
    /// the same-history restart bias is a documented gap owned by that issue.
    Recovered,
}

/// The applier's admission gate, guarding the counter it is paired with.
///
/// One mutex, two decisions, both taken while the counter is being read or
/// moved — which is what makes the promotion boundary exact. See
/// [`AppliedOffset::freeze`] and [`ReplicaApplyStint`].
struct ApplyGate {
    /// Set by a promotion. No stint may claim past this point.
    frozen: bool,
    /// The current replica stint. A consumer left over from an earlier stream
    /// holds an older number and is refused, so it stops without being
    /// cancelled mid-apply.
    stint: u64,
}

/// [`AppliedOffset::diverged`] when no divergence is outstanding. A real epoch
/// can never reach it: it would take 2^64 full resyncs.
const NO_DIVERGENCE: u64 = u64::MAX;

/// The Replica-side **applied** offset: how far the frames this node received
/// have actually been applied to its keyspace, plus the gate that decides who
/// may move it.
///
/// Split from the received head ([`ReplicaOffset`]) because the two are not the
/// same number on a Replica: `streaming.rs` advances the received head the
/// moment a frame is decoded off the socket and then queues the frame on a
/// 10k-deep channel, while [`crate::consume_frames`] moves *this* counter as it
/// applies. A promotion that froze its boundary at the received head would claim
/// history for every frame still sitting in that channel — a sibling replica at
/// the same offset would then be granted `+CONTINUE` over data the promoted node
/// never applied, with contiguous offsets hiding the hole. Everything that
/// describes *held* data (the promotion boundary, the persisted
/// `offset_at_save`) reads this counter.
///
/// The counter alone is not enough to make the boundary exact, because "apply,
/// then credit" is two steps and a promotion can land between them. So a replica
/// applier does not advance the counter — it *claims* through a
/// [`ReplicaApplyStint`] before applying, against the same lock
/// [`Self::freeze`] takes.
#[derive(Clone)]
pub struct AppliedOffset {
    applied: Arc<AtomicU64>,
    /// The offset the applier has actually *landed* on a shard: the claimed head
    /// minus whatever group is in flight between the claim and the shard's
    /// reply. Never above [`Self::current`], and the only offset this node ACKs
    /// (see [`Self::landed`]).
    ///
    /// Node-local: not the shared identity atomic, because everything that
    /// describes the *boundary* — the promotion freeze, `INFO`, the persisted
    /// `offset_at_save` — must keep reading the claimed head, which is the offset
    /// this node will hold once the group in flight finishes.
    landed: Arc<AtomicU64>,
    gate: Arc<Mutex<ApplyGate>>,
    /// Woken every time the counter moves, so [`Self::wait_until_applied`] does
    /// not have to poll it.
    progress: Arc<tokio::sync::Notify>,
    /// How many times this node has adopted a fresh stream position — the
    /// **history epoch**. Bumped by [`Self::reset_pair`] only, i.e. exactly when
    /// a full resync replaces the dataset, and written while the gate is held so
    /// a claim can check it atomically. Read lock-free everywhere else.
    ///
    /// The decode loop stamps it on every frame it queues, which is what lets
    /// the consumer tell the frames of the history it is applying from the ones
    /// a dropped link left behind (see [`ReplicaApplyStint::claim`]).
    epoch: Arc<AtomicU64>,
    /// The history epoch on which a replicated apply failed, or
    /// [`NO_DIVERGENCE`]. Written under the gate (like [`Self::epoch`]) and read
    /// lock-free.
    ///
    /// An `Err` out of `apply_group` is proof this node's keyspace no longer
    /// matches the primary's at that offset. Latched here so the two tasks that
    /// have to react can see it: the applier refuses every further claim on that
    /// history (so nothing else is applied on top of the hole, and nothing else
    /// is vouched for), and the connection drops its link and rewinds so the
    /// reconnect comes back through a full resync
    /// ([`crate::replica::ReplicaConnection`]).
    ///
    /// Cleared by [`Self::reset_pair`] and nowhere else: only a fresh dataset
    /// replaces the keyspace the divergence happened in, so the latch survives
    /// a reconnect, a `+CONTINUE`, and a promotion/demotion round trip.
    diverged: Arc<AtomicU64>,
    /// Woken when [`Self::diverged`] is latched, so the connection's streaming
    /// loop can park on it as a `select!` branch instead of polling.
    divergence: Arc<tokio::sync::Notify>,
    /// How many frames this node has ignored because its applied head already
    /// covered them (FM-REPLICATION-065). Node-wide and cumulative: the
    /// per-stint tally lives on `ConsumeStats`, and this is what makes the same
    /// evidence readable while a stint is still running.
    ///
    /// Deliberately **not** cleared by [`Self::reset_pair`]: a full resync
    /// replaces the history, not the record that this node was re-sent data it
    /// already held. Every increment is a sender-side accounting bug, so the
    /// number is only ever interesting as a total.
    skipped: Arc<AtomicU64>,
    /// The per-shard skip floors the most recent full-sync payload installed —
    /// its [`ShardCoverage`] vector (FM-REPLICATION-066).
    ///
    /// Written only by [`Self::reset_pair`], under the gate and in the same
    /// critical section as the offset pair, because the two describe one
    /// dataset: an install that adopted the offset without the floors would
    /// replay the overshipped range verbatim, and floors without their offset
    /// would be read against the wrong head.
    /// Where this node's offsets came from — see [`OffsetProvenance`]. Shared
    /// with the counter it describes, because the question the refusal asks is
    /// about *these* offsets, not about the connection reading them.
    recovered: Arc<AtomicBool>,
    floors: Arc<Mutex<ShardCoverage>>,
    /// `max(Y_s)` over [`Self::floors`], or `0` when no floors are installed.
    ///
    /// The fast path: the floors are consulted per frame, and once the applied
    /// head reaches this ceiling no frame can be at or below any floor, so the
    /// applier retires the vector and stops taking the lock. `0` is unambiguous
    /// as "no floors" — a real ceiling is the end offset of a frame, which is
    /// positive.
    floor_ceiling: Arc<AtomicU64>,
    /// How many frames this node stepped over because the installed payload
    /// already contained their effect (FM-REPLICATION-066). Node-wide and
    /// cumulative, like [`Self::skipped`], and deliberately not cleared by
    /// [`Self::reset_pair`].
    ///
    /// Separate from [`Self::skipped`] because the two mean opposite things: an
    /// FM-065 skip is evidence of a sender-side accounting bug and is logged as
    /// one, while a floor skip is the expected, healthy outcome of mending a
    /// full-sync overship. Folding them together would make the bug counter fire
    /// on every full sync.
    floor_skipped: Arc<AtomicU64>,
}

/// What the applier should do with one frame, decided before it is parsed.
///
/// The three answers are mutually exclusive and the order they are tried in is
/// load-bearing — see [`frame_disposition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDisposition {
    /// Nothing on this node covers the frame: parse it, claim it, apply it.
    Apply,
    /// The applied head already covers the frame's whole byte span. It claims
    /// nothing and lands nothing — the head is already past it
    /// (FM-REPLICATION-065).
    CoveredByHead,
    /// The frame is above the applied head, but the full-sync payload this node
    /// installed already contains its effect. Its bytes *are* claimed — this
    /// node really does hold them — but the command is not re-executed
    /// (FM-REPLICATION-066).
    CoveredByFloor,
}

/// Decide one frame's disposition from the three numbers that determine it.
///
/// Pure, and separate from the state it reads, so the rule can be tested at its
/// boundaries without a stint, a channel or a keyspace.
///
/// **Head before floor.** The two answers differ in what they do with the
/// frame's bytes — a head-covered frame claims nothing, a floor-covered frame
/// claims all of them — so a frame that is both must take the head's branch, or
/// its bytes would be claimed twice and push this node's offset past the
/// primary's.
///
/// **`floor` is an `Option`, and `None` means apply.** A shard with no watermark
/// is a shard the payload makes no claim about: a control-shard frame, or a
/// sender with fewer shards than the frame's tag indexes. Reading an absent
/// watermark as "not covered" costs at worst a re-apply; reading it as "covered"
/// drops a write. The type makes only the first reading available.
///
/// **Both bounds are inclusive.** A frame spans
/// `(end_offset - stream_advance(), end_offset]` (FM-REPLICATION-031), so a
/// frame ending exactly at the head, or exactly at `Y_s`, has nothing of itself
/// above that bound.
pub fn frame_disposition(
    applied_head: u64,
    floor: Option<u64>,
    end_offset: u64,
) -> FrameDisposition {
    if end_offset <= applied_head {
        return FrameDisposition::CoveredByHead;
    }
    match floor {
        Some(watermark) if end_offset <= watermark => FrameDisposition::CoveredByFloor,
        _ => FrameDisposition::Apply,
    }
}

impl AppliedOffset {
    /// Adopt the node's shared applied atomic (from
    /// [`crate::ReplicationIdentity::applied`]).
    pub(crate) fn over(applied: Arc<AtomicU64>) -> Self {
        Self::over_with(applied, OffsetProvenance::Installed)
    }

    /// [`Self::over`] for the boot path: these offsets were recovered from the
    /// persisted state, not established alongside a keyspace
    /// ([`OffsetProvenance::Recovered`], ruling R16).
    ///
    /// A zero seed is exempt: a node that has never held anything has nothing
    /// above its claim to be wrong about, and marking it recovered would refuse
    /// window grants for the rest of a fresh node's life.
    pub(crate) fn recovered_over(applied: Arc<AtomicU64>, coverage: ShardCoverage) -> Self {
        let provenance = if applied.load(Ordering::Acquire) == 0 {
            OffsetProvenance::Installed
        } else {
            OffsetProvenance::Recovered
        };
        let offsets = Self::over_with(applied, provenance);
        // The floors that were in force at the last save point come back with
        // the offset they were saved next to (ruling R15): a crash between an
        // install and the reconcile leaves a keyspace holding effects above its
        // claim, and the reconnect replays exactly the range they describe.
        let ceiling = coverage.max();
        *offsets.floors.lock() = coverage;
        offsets.floor_ceiling.store(ceiling, Ordering::Release);
        offsets
    }

    fn over_with(applied: Arc<AtomicU64>, provenance: OffsetProvenance) -> Self {
        Self {
            recovered: Arc::new(AtomicBool::new(provenance == OffsetProvenance::Recovered)),
            landed: Arc::new(AtomicU64::new(applied.load(Ordering::Acquire))),
            applied,
            gate: Arc::new(Mutex::new(ApplyGate {
                frozen: false,
                stint: 0,
            })),
            progress: Arc::new(tokio::sync::Notify::new()),
            epoch: Arc::new(AtomicU64::new(0)),
            diverged: Arc::new(AtomicU64::new(NO_DIVERGENCE)),
            divergence: Arc::new(tokio::sync::Notify::new()),
            skipped: Arc::new(AtomicU64::new(0)),
            floors: Arc::new(Mutex::new(ShardCoverage::none())),
            floor_ceiling: Arc::new(AtomicU64::new(0)),
            floor_skipped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A standalone counter with no node behind it — unit tests and wiring that
    /// has no identity to share. The seed is treated as a position this counter
    /// established itself; a counter that stands in for a *restarted* node is
    /// built with [`Self::recovered`].
    pub fn detached(seed: u64) -> Self {
        Self::over(Arc::new(AtomicU64::new(seed)))
    }

    /// [`Self::detached`] positioned as a node that recovered `seed` from the
    /// persisted state rather than installing it (ruling R16).
    pub fn recovered(seed: u64) -> Self {
        Self::recovered_with(seed, ShardCoverage::none())
    }

    /// [`Self::recovered`] carrying the floors that were persisted next to the
    /// seed (ruling R15).
    pub fn recovered_with(seed: u64, coverage: ShardCoverage) -> Self {
        Self::recovered_over(Arc::new(AtomicU64::new(seed)), coverage)
    }

    /// Advance by `n` stream bytes of this node's **own** writes — the primary
    /// path, where the write is applied on its shard before it is broadcast.
    /// Returns the new applied offset. `n` is measured in the one advance unit
    /// ([`OffsetCoordinator::frame_advance`]).
    ///
    /// Ungated on purpose: the gate exists to stop a *replica* applier from
    /// moving the counter across a promotion boundary. A promoted primary's own
    /// writes must keep advancing it, or the persisted offset would freeze at
    /// the promotion point and a restart would resume below its own data.
    pub fn advance_by(&self, n: u64) -> u64 {
        let advanced = self.applied.fetch_add(n, Ordering::Release) + n;
        // These bytes are already on their shard — the primary path applies
        // first and counts after — so the landed head moves with them. Keeping
        // it in step also means a node demoted later starts ACKing from its real
        // position instead of the last thing its previous replica stint landed.
        self.landed.fetch_max(advanced, Ordering::Release);
        self.progress.notify_waiters();
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_view_clean(&self.view(), "AppliedOffset::advance_by");
        advanced
    }

    /// This pair's contribution to the invariant projection: the applied/landed
    /// heads and the gate that admits movement of them.
    ///
    /// No live head — this type does not hold one. `INV-OFFSET-1` therefore
    /// checks the pair alone here; see its doc comment.
    pub fn view(&self) -> crate::view::ReplicationView {
        crate::view::ReplicationView::empty()
            .with_applied_pair(self.current(), self.landed())
            .with_apply_gate(self.gate_view())
    }

    /// The gate's state, read under its own lock so `frozen` and `stint` are a
    /// consistent pair rather than two separate samples.
    pub fn gate_view(&self) -> crate::view::ApplyGateView {
        let gate = self.gate.lock();
        crate::view::ApplyGateView {
            frozen: gate.frozen,
            stint: gate.stint,
            epoch: self.epoch.load(Ordering::Acquire),
            diverged: match self.diverged.load(Ordering::Acquire) {
                NO_DIVERGENCE => None,
                epoch => Some(epoch),
            },
        }
    }

    /// Wait until the **landed** head reaches `target`, and return it.
    ///
    /// Used by the ACK path: the replica ACKs what a shard has applied
    /// ([`Self::landed`]), and the bytes a solicited ACK is
    /// asked about are already decoded, so the wait is for the frame channel to
    /// drain and the last group to reach its shard. Unbounded, and cancel-safe so it can be a
    /// `select!` branch — which is the only sanctioned way to await it. The
    /// applier can stop for good (a promotion freezes the gate, a newer stream
    /// retires it), so a caller that awaits this *inline* would stop doing
    /// whatever else it owes; parked as one branch of a loop that keeps its
    /// spontaneous ACK cadence running, an answer that never comes costs
    /// nothing, because the cadence reports the same applied head a timeout here
    /// would have reported.
    pub async fn wait_until_applied(&self, target: u64) -> u64 {
        loop {
            // Register before re-reading: an advance between the read and the
            // registration would otherwise be missed, and this wait would park
            // with its target already met.
            let notified = self.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let landed = self.landed();
            if landed >= target {
                return landed;
            }
            notified.await;
        }
    }

    /// Advance by one applied frame's payload unit.
    pub fn frame_applied(&self, frame: &ReplicationFrame) -> u64 {
        self.advance_by(OffsetCoordinator::frame_advance(frame))
    }

    /// The offset of the data this node holds — everything claimed, including
    /// the group in flight to a shard, which this loop always finishes.
    pub fn current(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// The offset of the data a shard has actually applied — what the replica
    /// ACKs, and therefore what `WAIT` counts.
    ///
    /// Lags [`Self::current`] by at most the group in flight, and only while it
    /// is in flight: every apply is awaited before the next frame is claimed.
    pub fn landed(&self) -> u64 {
        self.landed.load(Ordering::Acquire)
    }

    /// How many frames this node has ignored as already covered by its applied
    /// head, since the process started (FM-REPLICATION-065).
    ///
    /// Cumulative across resyncs, stints and role changes: the count is
    /// evidence about the *sender*, and a resync does not undo it. Non-zero
    /// means some primary re-shipped a range this node had already applied.
    pub fn skipped(&self) -> u64 {
        self.skipped.load(Ordering::Acquire)
    }

    /// Open a replica applying stint and hand out the token the frame consumer
    /// claims through. Retires every earlier stint (see
    /// [`Self::retire_replica_applies`]) and re-opens a gate a previous
    /// promotion froze, so a demoted node can apply again.
    ///
    /// Called where a consumer is spawned — before the spawn, so the ordering of
    /// stints follows the ordering of streams rather than of task scheduling.
    pub fn begin_replica_stint(&self) -> ReplicaApplyStint {
        let mut gate = self.gate.lock();
        gate.frozen = false;
        gate.stint += 1;
        ReplicaApplyStint {
            offset: self.clone(),
            stint: gate.stint,
        }
    }

    /// Retire the current replica stint without opening a new one: every claim
    /// from the consumer that is running now will be refused, so it stops at its
    /// next frame having applied everything it claimed and nothing more.
    ///
    /// This is how a role change retires an applier instead of aborting it. An
    /// abort lands at the task's next poll, which may be *inside*
    /// `apply_group().await` with the shard message already dispatched: the
    /// write reaches the keyspace but its bytes are never claimed, leaving data
    /// above the offset the node vouches for.
    pub fn retire_replica_applies(&self) {
        self.gate.lock().stint += 1;
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_view_clean(
            &self.view(),
            "AppliedOffset::retire_replica_applies",
        );
    }

    /// The stint number a replica stream is running under right now. Captured
    /// by [`ReplicaOffset::new`] so a connection built under one stream cannot
    /// reset the heads after a newer stream (or a promotion) has taken over.
    fn current_stint(&self) -> u64 {
        self.gate.lock().stint
    }

    /// Adopt `offset` as *both* heads for `stint`, under the gate: the pair is
    /// stored while the lock is held, so a promotion either freezes before the
    /// reset (which is then refused) or after it (and reads the adopted value).
    /// `false` means this stream may no longer move the heads.
    ///
    /// Gated for the same reason a claim is: a full resync running on the
    /// connection task can otherwise land *after* a promotion froze the
    /// boundary, rewriting the head a freshly minted window and backlog floor
    /// were just built around.
    ///
    /// `coverage` is the installed payload's per-shard watermark vector, which
    /// replaces the floors wholesale — including with the empty vector, which is
    /// what every rewind and every coverage-less install passes. Floors from a
    /// dataset that has just been thrown away would skip frames whose effects
    /// the *new* dataset does not contain, so "reset at each install" is not a
    /// convenience, it is the safety rule (FM-REPLICATION-066).
    fn reset_pair(
        &self,
        stint: u64,
        offset: u64,
        live: &AtomicU64,
        coverage: ShardCoverage,
    ) -> bool {
        let gate = self.gate.lock();
        if gate.frozen || gate.stint != stint {
            return false;
        }
        // Under the gate, with the pair: an applier that reads a floor is
        // reading the floors of the dataset whose offset it is claiming into.
        let ceiling = coverage.max();
        *self.floors.lock() = coverage;
        self.floor_ceiling.store(ceiling, Ordering::Release);
        // An install is the one event that pairs a keyspace with an offset, so
        // it is the one event that ends a recovered stint's uncertainty about
        // what sits above the claim — including a rewind to 0, which throws the
        // recovered keyspace away in favour of the full resync that follows.
        self.recovered.store(false, Ordering::Release);
        live.store(offset, Ordering::Release);
        self.applied.store(offset, Ordering::Release);
        // The installed dataset *is* applied, so the landed head is level with
        // the other two: a resync is the one place this counter may move
        // backwards, and it moves under the same lock as the pair it belongs to.
        self.landed.store(offset, Ordering::Release);
        // A new history starts here: everything the previous one left on the
        // frame channel is void, because this offset describes a dataset that
        // replaced the keyspace those frames were written against. Bumped under
        // the gate so the claim that checks it cannot straddle the reset.
        self.epoch.fetch_add(1, Ordering::Release);
        // And with it, the one thing that clears an admitted divergence: the
        // keyspace that diverged has just been replaced wholesale, so the
        // applier may apply again (issue 08).
        self.diverged.store(NO_DIVERGENCE, Ordering::Release);
        self.progress.notify_waiters();
        true
    }

    /// The history epoch the node is on right now — stamped on each frame the
    /// decode loop queues, and the discriminator the consumer claims against.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// How many frames this node has stepped over because the full-sync payload
    /// it installed already contained their effect (FM-REPLICATION-066).
    pub fn floor_skipped(&self) -> u64 {
        self.floor_skipped.load(Ordering::Relaxed)
    }

    /// `max(Y_s)` of the floors currently installed, or `0` once they have been
    /// retired (or were never installed).
    ///
    /// This is also the threshold the window-grant refusal reads: below it, this
    /// node's keyspace holds effects its claimed offset does not describe, so a
    /// `+CONTINUE` that replaces the history would resume over them.
    pub fn floor_ceiling(&self) -> u64 {
        self.floor_ceiling.load(Ordering::Acquire)
    }

    /// Where these offsets came from — the second input to the window-grant
    /// refusal (ruling R16, FM-REPLICATION-066).
    pub fn provenance(&self) -> OffsetProvenance {
        if self.recovered.load(Ordering::Acquire) {
            OffsetProvenance::Recovered
        } else {
            OffsetProvenance::Installed
        }
    }

    /// Whether floors are still in force — i.e. whether the applied head has yet
    /// to catch up with everything the installed payload covered.
    ///
    /// Retires the vector as a side effect once the head reaches the ceiling, so
    /// the steady-state applier neither takes the floors lock nor keeps a stale
    /// vector alive. Retiring on a *read* rather than on the claim that crossed
    /// the ceiling keeps the rule in one place: every consumer of the floors
    /// goes through here.
    fn floors_in_force(&self) -> bool {
        let ceiling = self.floor_ceiling.load(Ordering::Acquire);
        if ceiling == 0 {
            return false;
        }
        if self.current() >= ceiling {
            // Compare-exchange rather than a bare store: a full resync may have
            // installed a *new* vector between the load above and here, and
            // clobbering it would drop floors that are still needed.
            let _ = self.floor_ceiling.compare_exchange(
                ceiling,
                0,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            return false;
        }
        true
    }

    /// Shard `shard`'s installed floor, or `None` when no floor applies to it —
    /// no vector in force, or a vector that does not describe that shard.
    pub fn floor_for(&self, shard: u16) -> Option<u64> {
        if !self.floors_in_force() {
            return None;
        }
        self.floors.lock().watermark(shard)
    }

    /// The floors still **in force**, for persistence and for tests — empty once
    /// the applied head has caught up with the ceiling, which is what a node in
    /// steady state persists.
    pub fn floors(&self) -> ShardCoverage {
        if !self.floors_in_force() {
            return ShardCoverage::none();
        }
        self.floors.lock().clone()
    }

    /// Whether a replicated apply has failed on the history this node is
    /// currently applying — i.e. whether this node has admitted that its
    /// keyspace no longer matches the primary's (see [`Self::diverged`]).
    pub fn has_diverged(&self) -> bool {
        self.diverged.load(Ordering::Acquire) != NO_DIVERGENCE
    }

    /// Resolve as soon as a divergence is outstanding, and park forever while
    /// none is.
    ///
    /// Cancel-safe, so the replica's streaming loop can hold it as a `select!`
    /// branch: the connection is the task that has to act on a divergence (drop
    /// the link, rewind for a full resync) but the applier is the task that
    /// discovers one. Registers before re-reading the latch, so a divergence
    /// admitted between the two is not missed.
    pub async fn divergence(&self) {
        loop {
            let notified = self.divergence.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.has_diverged() {
                return;
            }
            notified.await;
        }
    }

    /// Freeze the counter against replica applies and return the boundary — the
    /// offset of everything the applier has claimed, and therefore of everything
    /// it will apply.
    ///
    /// Exact by construction: a claim and this freeze take the same lock, so a
    /// group is either claimed before the freeze (its bytes are inside the
    /// boundary, and the applier finishes it — it is never cancelled) or refused
    /// after it (never applied at all). There is no third outcome, which is what
    /// stops a write from landing above a boundary that no backlog covers and no
    /// replication-id window describes.
    pub fn freeze(&self) -> u64 {
        let boundary = {
            let mut gate = self.gate.lock();
            gate.frozen = true;
            self.applied.load(Ordering::Acquire)
        };
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_view_clean(&self.view(), "AppliedOffset::freeze");
        boundary
    }
}

/// A frame consumer's licence to move the applied offset.
///
/// Held by one [`crate::consume_frames`] loop for the life of one replica
/// stream. Every keyspace-touching step claims its stream bytes *before*
/// applying them, and a refused claim is the loop's stop signal: the stint was
/// frozen by a promotion or retired by a newer stream.
#[derive(Clone)]
pub struct ReplicaApplyStint {
    offset: AppliedOffset,
    stint: u64,
}

/// The verdict on a [`ReplicaApplyStint::claim`], and with it what the consume
/// loop must do with the frame it asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claim {
    /// The bytes are claimed: apply the frame.
    Granted,
    /// The frame belongs to a history this node no longer applies — either one
    /// a full resync replaced after the frame was decoded, or one this node
    /// admitted a divergence on ([`ReplicaApplyStint::admit_divergence`]). Drop
    /// it — including any open `MULTI` group it belonged to — and keep
    /// consuming: the frames behind it are either the new history's or bound for
    /// the same fate until the resync that ends this one lands.
    Stale,
    /// This stint may no longer touch the keyspace: a promotion froze the gate,
    /// or a newer stream retired it. Stop consuming.
    Retired,
}

impl ReplicaApplyStint {
    /// Claim `bytes` of stream about to be applied, on behalf of a frame decoded
    /// under history `epoch`.
    ///
    /// The epoch is checked under the same lock that moves the counter, so a
    /// claim cannot straddle a full resync: it is taken either wholly before the
    /// reset (its bytes are then overwritten by the reset, which adopts the
    /// payload's offset outright) or refused as [`Claim::Stale`] after it. The
    /// racing shape this closes is a consumer that read a matching epoch, was
    /// descheduled while the connection task installed a checkpoint, and then
    /// credited the new history's offset with bytes from the old one.
    pub fn claim(&self, epoch: u64, bytes: u64) -> Claim {
        let gate = self.offset.gate.lock();
        if gate.frozen || gate.stint != self.stint {
            return Claim::Retired;
        }
        if epoch != self.offset.epoch.load(Ordering::Acquire) {
            return Claim::Stale;
        }
        // The history is current, and it has been admitted diverged: the
        // keyspace under it no longer matches the primary's, so nothing more may
        // be applied on top of the hole and nothing more may be vouched for.
        // Refused rather than retired, because the applier itself is fine — it
        // goes on to apply the history the forced full resync installs
        if self.offset.diverged.load(Ordering::Acquire) != NO_DIVERGENCE {
            return Claim::Stale;
        }
        self.offset.applied.fetch_add(bytes, Ordering::Release);
        self.offset.progress.notify_waiters();
        Claim::Granted
    }

    /// Report that everything claimed so far has reached its shard — called by
    /// the consume loop as each apply returns, and immediately for the frames
    /// that never touch a shard (a `REPLCONF`, a `FROGDB.FINALIZE`, an
    /// unparseable payload).
    ///
    /// Reads the claimed head rather than taking a byte count because the loop
    /// applies one group at a time and awaits each: at every point this is
    /// called, nothing is in flight, so "landed" and "claimed" are the same
    /// number. `fetch_max` keeps that monotone against a resync that stored a
    /// lower offset in the meantime — the resync's value wins, and the frames
    /// this call was reporting are void anyway.
    pub fn land(&self) {
        let claimed = self.offset.applied.load(Ordering::Acquire);
        self.offset.landed.fetch_max(claimed, Ordering::Release);
        self.offset.progress.notify_waiters();
    }

    /// Admit that a replicated apply failed on history `epoch`: this node's
    /// keyspace no longer matches the primary's at the offset it is claiming.
    ///
    /// Latching it is what turns a logged error into a consequence. From here
    /// every claim on `epoch` is refused ([`Claim::Stale`]), so the applier
    /// stops adding to a keyspace it knows is wrong and stops crediting bytes it
    /// would later vouch for; and the connection wakes on
    /// [`AppliedOffset::divergence`], drops its link and rewinds, so the
    /// reconnect is answered `+FULLRESYNC` instead of resuming over the hole.
    /// The stint itself is deliberately *not* retired: the frame consumer
    /// outlives connections, and retiring it would stop this node applying
    /// anything ever again — issue 06 named that blunt fix and refused it.
    ///
    /// Taken under the gate for the same reason the epoch bump is, and ignored
    /// when `epoch` is no longer the current history: a full resync landing
    /// between the claim and the apply's `Err` has already replaced the keyspace
    /// the divergence was about, so latching it would force a second resync
    /// against a dataset that never diverged. Same reasoning as [`Claim::Stale`].
    pub fn admit_divergence(&self, epoch: u64) {
        {
            let _gate = self.offset.gate.lock();
            if epoch != self.offset.epoch.load(Ordering::Acquire) {
                return;
            }
            self.offset.diverged.store(epoch, Ordering::Release);
        }
        self.offset.divergence.notify_waiters();
    }

    /// The applied offset this stint is claiming into.
    pub fn current(&self) -> u64 {
        self.offset.current()
    }

    /// The history epoch the node is on right now.
    pub fn epoch(&self) -> u64 {
        self.offset.epoch()
    }

    /// Whether the applied head already covers a frame ending at `end_offset`.
    ///
    /// The receiver half of FM-REPLICATION-065's dedup. A frame spans
    /// `(end_offset - stream_advance(), end_offset]` (FM-REPLICATION-031), so
    /// the head covers *all* of it exactly when `end_offset <= current()` —
    /// inclusive, because a frame ending precisely at the head is the common
    /// re-delivery, and strictly nothing of it is new. A frame ending above the
    /// head is not covered even when its span starts below one; the honest
    /// answer there is to apply it, and the overship that produces that shape is
    /// TR-REPLICATION-034's to remove.
    ///
    /// Read outside the gate on purpose. The only writers of the applied head
    /// during a replica stint are this stint's own [`Self::claim`] (single
    /// consumer, no self-race) and [`AppliedOffset::reset_pair`], which bumps
    /// the epoch under the gate — so a frame that races a resync is refused
    /// [`Claim::Stale`] whichever side of the reset this read lands on.
    pub fn covers(&self, end_offset: u64) -> bool {
        end_offset <= self.current()
    }

    /// What to do with a frame from `shard` ending at `end_offset`: apply it,
    /// step over it as already-applied ([`FrameDisposition::CoveredByHead`]), or
    /// step over it as already-in-the-payload
    /// ([`FrameDisposition::CoveredByFloor`]).
    ///
    /// The floor half is the receiver end of FM-REPLICATION-066. A full-sync
    /// payload is cut *after* the offset the replica is granted, so the handoff
    /// replays a range the installed keyspace already partly holds; the sender's
    /// per-shard watermarks say exactly how much, per shard, and this is where
    /// that is spent.
    ///
    /// Reads the floors outside the gate for the same reason [`Self::covers`]
    /// reads the head outside it: the only writer during a stint is
    /// [`AppliedOffset::reset_pair`], which bumps the epoch under the gate, so a
    /// frame that races an install is refused [`Claim::Stale`] whichever side of
    /// the reset this read lands on.
    pub fn disposition(&self, shard: u16, end_offset: u64) -> FrameDisposition {
        frame_disposition(self.current(), self.offset.floor_for(shard), end_offset)
    }

    /// Record one frame stepped over by a full-sync floor, and return the
    /// node-wide running total. Counterpart to [`Self::record_skip`]; see
    /// [`AppliedOffset::floor_skipped`] for why the two are separate counters.
    pub fn record_floor_skip(&self) -> u64 {
        self.offset.floor_skipped.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Record one frame ignored as already covered, and return the node-wide
    /// running total.
    ///
    /// Returns the total so the caller can log it without reading the counter
    /// back — and so the increment happens outside the `tracing` macro, whose
    /// fields are not evaluated when the event is disabled. A counter that moved
    /// only at a given log level would be worse than no counter.
    pub fn record_skip(&self) -> u64 {
        self.offset.skipped.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// The Replica-side live **received** offset — the mirror of the Primary's
/// [`OffsetCoordinator`]'s `live` atomic: the single home of "how far this
/// Replica has read the stream", advanced per ingested frame, and the single
/// vendor of the shared handle the cluster bus (HealthProbe) / INFO read.
///
/// Pairs with [`AppliedOffset`], the offset of the data actually applied; the
/// received head may run ahead of it by the depth of the frame channel. Reads
/// that describe *held* data go through [`Self::applied`], never
/// [`Self::current`] — including the ACK, which is what WAIT counts and must
/// therefore describe data this node would still hold if it were promoted.
///
/// The owner does **not** mint a second atomic: it takes over maintenance of the
/// `shared_offset` atomic the scattered call sites updated before, so the
/// cluster-bus handle identity is unchanged.
#[derive(Clone)]
pub struct ReplicaOffset {
    live: Arc<AtomicU64>,
    applied: AppliedOffset,
    /// The applying stint this stream belongs to, captured at construction.
    /// [`Self::reset_to`] is refused once a newer stream opens (or a promotion
    /// freezes the gate), so a connection that outlives its stream cannot
    /// rewrite the heads. Callers must therefore open the stream's stint
    /// *before* building its connections.
    stint: u64,
    state: Arc<RwLock<ReplicationState>>,
}

impl ReplicaOffset {
    /// Adopt the Replica's shared `live` atomic — the one read by the cluster bus
    /// / INFO — plus the node's applied counter. Both are seeded from the
    /// persisted `offset_at_save` where they are minted (the handler at boot, a
    /// test fixture in unit tests), so this constructor never re-stores: a fresh
    /// connection attempt or a save point adopts the atomics that already hold
    /// the live position, and reconnecting can never rewind the head to the
    /// lagging persisted field.
    pub fn new(
        state: Arc<RwLock<ReplicationState>>,
        live: Arc<AtomicU64>,
        applied: AppliedOffset,
    ) -> Self {
        let stint = applied.current_stint();
        Self {
            live,
            applied,
            stint,
            state,
        }
    }

    /// Advance by one *received* frame's payload unit — the SAME unit as the
    /// Primary's advance (via [`OffsetCoordinator::frame_advance`]), so a Replica
    /// ACK stays directly comparable to the Primary's live head. Returns the new
    /// received offset (what the streaming path ACKs). The frame is not applied
    /// yet: [`AppliedOffset`] moves later, in the consume loop.
    pub fn frame_advance(&self, frame: &ReplicationFrame) -> u64 {
        let n = OffsetCoordinator::frame_advance(frame);
        let received = self.live.fetch_add(n, Ordering::Release) + n;
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_view_clean(&self.view(), "ReplicaOffset::frame_advance");
        received
    }

    /// This stream's contribution to the invariant projection: the full
    /// received/applied/landed triple, the applier's gate, and the identity the
    /// three are measured against.
    ///
    /// The state is sampled with `try_read` because this runs inside the ingest
    /// path, which a writer may hold the identity lock across; a debug-only
    /// projection must never be the thing that deadlocks the node, and a view
    /// without the identity simply skips the identity claims.
    pub fn view(&self) -> crate::view::ReplicationView {
        let view = crate::view::ReplicationView::empty()
            .with_offsets(
                self.current(),
                self.applied.current(),
                self.applied.landed(),
            )
            .with_apply_gate(self.applied.gate_view());
        match self.state.try_read() {
            Some(state) => view.with_state(state.clone()),
            None => view,
        }
    }

    /// The live *received* position. Replaces `state.offset_at_save` reads on the
    /// Replica ingest / reconnect-PSYNC path.
    pub fn current(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }

    /// The applied head this stream feeds — what the replica ACKs.
    pub fn applied(&self) -> &AppliedOffset {
        &self.applied
    }

    /// Adopt a fresh stream position on FULLRESYNC / staged-checkpoint install.
    /// Replaces the direct `state.replication_offset = new_offset` writes.
    ///
    /// Moves **both** heads under the applier's gate: a full resync replaces the
    /// dataset wholesale, so after it the node holds exactly the snapshot's data
    /// — received and applied are level again, and any queued frames from the
    /// previous history are void.
    ///
    /// `false` means this stream no longer owns the heads: a promotion froze
    /// them (the node is minting a window and arming a backlog floor at the
    /// frozen boundary — adopting a resync offset on top would leave INFO's
    /// `master_repl_offset` and every PSYNC window comparison describing a
    /// position unrelated to the backlog) or a newer stream retired this one.
    /// `stop()` on the handler is signal-only, so a connection mid-full-sync can
    /// reach here after either. The caller must abandon the sync.
    #[must_use = "a refused reset means the stream must abandon this sync"]
    pub fn reset_to(&self, offset: u64) -> bool {
        self.reset_to_payload(offset, ShardCoverage::none())
    }

    /// [`Self::reset_to`] for an install that carries a coverage vector: adopt
    /// the payload's offset *and* its per-shard skip floors, in one critical
    /// section (FM-REPLICATION-066).
    ///
    /// Every other reset — a rewind to `0`, a coverage-less install — goes
    /// through [`Self::reset_to`] and clears the floors, which is the same
    /// operation with an empty vector. There is deliberately no way to adopt an
    /// offset while *leaving* the floors alone: the floors describe the dataset
    /// the offset came with, and pairing them with a different one is the bug
    /// they exist to prevent.
    #[must_use = "a refused reset means the stream must abandon this sync"]
    pub fn reset_to_payload(&self, offset: u64, coverage: ShardCoverage) -> bool {
        let accepted = self
            .applied
            .reset_pair(self.stint, offset, &self.live, coverage);
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_view_clean(&self.view(), "ReplicaOffset::reset_to");
        accepted
    }

    /// Reconcile [`offset_at_save`] up to the **applied** head for persistence —
    /// monotone-guarded, symmetric to [`OffsetCoordinator::reconcile_for_persist`].
    /// On the Replica this is the persist-what-you-applied semantic in its strict
    /// form: persisting the received head instead would let a restart resume
    /// above data the node never applied.
    ///
    /// [`offset_at_save`]: ReplicationState::offset_at_save
    pub async fn reconcile_for_persist(&self) -> ReplicationState {
        let offset = self.applied.current();
        // The floors are persisted with the offset they qualify, never without
        // it: recovering an offset whose residue is undescribed is the very
        // shape FM-REPLICATION-066 refuses window grants over.
        let floors = self.applied.floors();
        let mut state = self.state.write();
        state.coverage_at_save = floors;
        // Monotone bump as a `max`, not a compare-and-assign: the two forms of
        // the guard (`>` / `>=`) differ only in whether they redundantly
        // re-store the value the field already holds.
        state.offset_at_save = state.offset_at_save.max(offset);
        state.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    fn state_with_save(offset_at_save: u64) -> Arc<RwLock<ReplicationState>> {
        let mut s = ReplicationState::new();
        s.offset_at_save = offset_at_save;
        Arc::new(RwLock::new(s))
    }

    /// Mint a live atomic seeded from `seed`, mirroring how the handler seeds
    /// the atomic from the persisted `offset_at_save` when it mints it.
    fn seeded(seed: u64) -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(seed))
    }

    /// Claim at the node's current history epoch — the ordinary case for tests
    /// about the gate rather than about a resync.
    fn claim_now(stint: &ReplicaApplyStint, bytes: u64) -> Claim {
        stint.claim(stint.epoch(), bytes)
    }

    fn frame_of(payload: &'static [u8]) -> ReplicationFrame {
        ReplicationFrame::new(0, Bytes::from_static(payload))
    }

    /// A pair whose received and applied heads both start at `seed`, plus the
    /// applied handle the consume loop would hold.
    fn offsets_at(
        state: Arc<RwLock<ReplicationState>>,
        seed: u64,
    ) -> (ReplicaOffset, AppliedOffset) {
        let applied = AppliedOffset::detached(seed);
        (
            ReplicaOffset::new(state, seeded(seed), applied.clone()),
            applied,
        )
    }

    #[tokio::test]
    async fn new_adopts_the_seeded_live_atomic() {
        let (offsets, applied) = offsets_at(state_with_save(500), 500);
        assert_eq!(offsets.current(), 500);
        assert_eq!(applied.current(), 500);
    }

    #[tokio::test]
    async fn frame_advance_advances_live_and_ack_equals_summed_payload() {
        // Replica ACK equals the received offset: the sum of payload units.
        let (offsets, _applied) = offsets_at(state_with_save(0), 0);
        assert_eq!(offsets.frame_advance(&frame_of(b"hello")), 5); // 5
        assert_eq!(offsets.frame_advance(&frame_of(b"world!")), 11); // +6
        assert_eq!(offsets.current(), 11);
    }

    #[tokio::test]
    async fn received_head_runs_ahead_of_applied_until_the_frame_is_applied() {
        // The whole reason the two counters exist: decoding a frame moves the
        // received head but claims nothing about data; the replica ACKs the
        // applied counter, which advances only when the frame is applied.
        let (offsets, applied) = offsets_at(state_with_save(0), 0);
        let frame = frame_of(b"hello");
        assert_eq!(offsets.frame_advance(&frame), 5);
        assert_eq!(applied.current(), 0, "nothing applied yet");
        assert_eq!(applied.frame_applied(&frame), 5);
        assert_eq!(applied.current(), offsets.current());
    }

    #[tokio::test]
    async fn frame_advance_counts_payload_not_header() {
        let (offsets, _applied) = offsets_at(state_with_save(0), 0);
        let frame = frame_of(b"*1\r\n$4\r\nPING\r\n");
        let advanced = offsets.frame_advance(&frame);
        assert_eq!(advanced, frame.payload.len() as u64);
        assert_ne!(advanced, frame.encoded_size() as u64);
    }

    #[tokio::test]
    async fn current_reports_live_not_persisted_offset_at_save() {
        // The reconnect-PSYNC hazard: offset_at_save lags the live applied head
        // between save points. `current()` must report the live head (N), never
        // the persisted save-point value (M < N) — otherwise a reconnect would
        // ask the primary to resume from behind where the replica has applied.
        let st = state_with_save(100); // persisted save-point M = 100
        let (offsets, _applied) = offsets_at(st.clone(), 100);
        // Apply past the save point.
        offsets.frame_advance(&frame_of(b"aaaaaaaaaa")); // +10 -> live N = 110
        assert_eq!(offsets.current(), 110);
        // The persisted field still lags at the save point.
        assert_eq!(st.read().offset_at_save, 100);
    }

    #[tokio::test]
    async fn reconcile_for_persist_is_monotonic_and_persists_what_was_applied() {
        let st = state_with_save(0);
        let (offsets, applied) = offsets_at(st.clone(), 0);

        let frame = ReplicationFrame::new(0, Bytes::from(vec![b'x'; 750]));
        offsets.frame_advance(&frame);
        // Received advanced; offset_at_save still lags until reconcile.
        assert_eq!(offsets.current(), 750);
        assert_eq!(st.read().offset_at_save, 0);

        // Received but NOT applied: a save point must not persist an offset
        // above the data on disk, or a restart resumes over a hole.
        let snapshot = offsets.reconcile_for_persist().await;
        assert_eq!(snapshot.offset_at_save, 0);

        applied.frame_applied(&frame);
        let snapshot = offsets.reconcile_for_persist().await;
        // Persist-what-you-applied: the persisted value is the applied head.
        assert_eq!(snapshot.offset_at_save, 750);
        assert_eq!(st.read().offset_at_save, 750);

        // A reconcile never moves the offset backwards.
        st.write().offset_at_save = 5000;
        let snapshot = offsets.reconcile_for_persist().await;
        assert_eq!(snapshot.offset_at_save, 5000);
    }

    // FM-REPLICATION-006
    #[tokio::test(start_paused = true)]
    async fn wait_until_applied_returns_as_soon_as_the_applier_catches_up() {
        // The solicited-ACK path: the bytes are decoded, the applier just has
        // not drained them yet. The wait must end on the advance, and on nothing
        // else — the streaming loop parks it as a `select!` branch, so a wait
        // that woke early would send an ACK claiming data that is not applied.
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        let applier = {
            let stint = stint.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                assert_eq!(claim_now(&stint, 60), Claim::Granted);
                stint.land();
                tokio::time::sleep(Duration::from_millis(5)).await;
                assert_eq!(claim_now(&stint, 40), Claim::Granted);
                stint.land();
            })
        };

        let reached = applied.wait_until_applied(100).await;
        applier.await.unwrap();

        assert_eq!(reached, 100, "woke on a partial advance");
    }

    /// A wait whose target the applier will never reach parks forever rather
    /// than reporting a lower offset: the streaming loop holds it as one
    /// `select!` branch, and its spontaneous ACK cadence — not this wait — is
    /// what keeps a frozen applier's truthful, lower head flowing to the primary.
    // FM-REPLICATION-006
    #[tokio::test(start_paused = true)]
    async fn wait_until_applied_parks_when_the_applier_can_no_longer_advance() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        assert_eq!(claim_now(&stint, 40), Claim::Granted);
        stint.land();
        // A promotion froze the gate: nothing will claim the remaining 60.
        applied.freeze();

        let parked = tokio::time::timeout(Duration::from_secs(30), applied.wait_until_applied(100));
        assert!(
            parked.await.is_err(),
            "the wait reported an unapplied offset"
        );
        assert_eq!(applied.current(), 40);
        assert_eq!(applied.landed(), 40);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_applied_returns_immediately_when_already_caught_up() {
        let applied = AppliedOffset::detached(500);
        assert_eq!(applied.wait_until_applied(500).await, 500);
    }

    /// Issue 76: the claim is taken *before* the group reaches its shard — that
    /// is what makes the promotion boundary exact — so the claimed head is not
    /// something this node may ACK. `WAIT` counts ACKs, and an ACK has to mean
    /// the data is on a shard, not that the applier is about to send it there.
    // FM-REPLICATION-008
    #[test]
    fn a_claim_alone_does_not_move_the_offset_the_replica_acks() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();

        assert_eq!(claim_now(&stint, 40), Claim::Granted);
        assert_eq!(
            applied.current(),
            40,
            "the boundary covers the group in flight"
        );
        assert_eq!(
            applied.landed(),
            0,
            "the ACK covered a group no shard has applied"
        );

        stint.land();
        assert_eq!(applied.landed(), 40);
        assert!(
            applied.landed() <= applied.current(),
            "the landed head ran past the claimed one"
        );
    }

    /// A resync levels all three heads: the installed dataset *is* applied, so
    /// the node may ACK the offset it was granted — and must not keep ACKing the
    /// higher offset a previous history landed at.
    // FM-REPLICATION-008
    #[test]
    fn a_full_resync_levels_the_landed_head_with_the_adopted_offset() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        assert_eq!(claim_now(&stint, 900), Claim::Granted);
        stint.land();

        let offsets = ReplicaOffset::new(state_with_save(0), seeded(900), applied.clone());
        assert!(offsets.reset_to(10));
        assert_eq!(
            applied.landed(),
            10,
            "the ACK still claimed the old history"
        );
        assert_eq!(applied.current(), 10);
    }

    #[test]
    fn a_frozen_gate_refuses_every_claim_and_pins_the_boundary() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        assert_eq!(claim_now(&stint, 10), Claim::Granted);
        let boundary = applied.freeze();
        assert_eq!(boundary, 10, "the boundary covers what was claimed");
        assert_eq!(
            claim_now(&stint, 5),
            Claim::Retired,
            "a frozen gate admits nothing"
        );
        assert_eq!(applied.current(), boundary);
    }

    #[test]
    fn a_new_stint_retires_the_old_one_and_reopens_a_frozen_gate() {
        let applied = AppliedOffset::detached(0);
        let old = applied.begin_replica_stint();
        applied.freeze();
        // A demotion opens a fresh stream: its stint applies again...
        let new = applied.begin_replica_stint();
        assert_eq!(claim_now(&new, 7), Claim::Granted);
        // ...while the consumer left over from the previous stream does not.
        assert_eq!(claim_now(&old, 3), Claim::Retired);
        assert_eq!(applied.current(), 7);
    }

    #[test]
    fn a_frozen_gate_refuses_a_full_resync_reset() {
        // The connection task is not stopped synchronously by a promotion
        // (`handler.stop()` is signal-only), so a full sync in flight can reach
        // `reset_to` after the boundary was frozen. It must not clobber the
        // heads the minted window and backlog floor were built around.
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        assert_eq!(claim_now(&stint, 40), Claim::Granted);
        let (offsets, applied) = (
            ReplicaOffset::new(state_with_save(0), seeded(40), applied.clone()),
            applied,
        );
        let boundary = applied.freeze();
        assert!(!offsets.reset_to(900), "a frozen gate refuses a reset");
        assert_eq!(applied.current(), boundary);
        assert_eq!(offsets.current(), 40, "the received head is untouched too");
    }

    #[test]
    fn a_stale_stream_cannot_reset_the_heads() {
        let applied = AppliedOffset::detached(0);
        let _first = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0), applied.clone());
        assert!(offsets.reset_to(10));
        // A newer stream opens: the older connection is now stale.
        let _second = applied.begin_replica_stint();
        assert!(!offsets.reset_to(999));
        assert_eq!(applied.current(), 10);
        assert_eq!(offsets.current(), 10);
    }

    /// Issue 06: the claim is where a frame's history is decided, and it is
    /// decided under the gate lock — so a full resync that lands between the
    /// consumer reading the epoch and taking the claim refuses the claim rather
    /// than crediting the new history with the old one's bytes.
    // FM-REPLICATION-007
    #[test]
    fn a_claim_stamped_before_a_resync_is_refused_after_it() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0), applied.clone());

        // The epoch the consumer read when it picked the frame up.
        let stamped = stint.epoch();
        assert_eq!(stint.claim(stamped, 10), Claim::Granted);

        // ... and the resync that lands while it is still holding that frame.
        assert!(offsets.reset_to(5_000));
        assert_eq!(
            stint.claim(stamped, 10),
            Claim::Stale,
            "a claim from the replaced history moved the new history's head"
        );
        assert_eq!(applied.current(), 5_000);

        // The stint is not retired by the resync: it is the same consumer, and
        // the frames of the new history still have to apply through it.
        assert_eq!(claim_now(&stint, 7), Claim::Granted);
        assert_eq!(applied.current(), 5_007);
    }

    /// Issue 08: an admitted divergence ends the history it happened on. Every
    /// later claim on it is refused, so the node stops applying onto a keyspace
    /// it knows is wrong and stops crediting bytes it would later vouch for —
    /// and the stint stays open, so the same consumer applies the history the
    /// forced full resync installs.
    // FM-REPLICATION-010
    #[test]
    fn a_diverged_history_is_refused_until_a_resync_replaces_it() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0), applied.clone());

        assert_eq!(claim_now(&stint, 10), Claim::Granted);
        assert!(!applied.has_diverged());

        // The apply that group went to came back `Err`.
        stint.admit_divergence(stint.epoch());
        assert!(applied.has_diverged());
        assert_eq!(
            claim_now(&stint, 10),
            Claim::Stale,
            "the applier kept claiming on a history it had admitted diverged"
        );
        assert_eq!(
            applied.current(),
            10,
            "a refused claim credited the diverged history anyway"
        );

        // The forced full resync installs a fresh dataset: the keyspace that
        // diverged is gone, so the latch clears with it and the SAME stint
        // applies again.
        assert!(offsets.reset_to(5_000));
        assert!(!applied.has_diverged());
        assert_eq!(claim_now(&stint, 7), Claim::Granted);
        assert_eq!(applied.current(), 5_007);
    }

    /// The latch is keyed to the history it was admitted on: a resync that
    /// lands between the claim and the apply's `Err` has already replaced the
    /// keyspace the failure was about, so latching it would cost a second full
    /// resync against a dataset that never diverged.
    // FM-REPLICATION-010
    #[test]
    fn a_divergence_on_a_history_a_resync_already_replaced_is_ignored() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0), applied.clone());

        let stamped = stint.epoch();
        assert_eq!(stint.claim(stamped, 10), Claim::Granted);
        assert!(offsets.reset_to(5_000));

        stint.admit_divergence(stamped);
        assert!(
            !applied.has_diverged(),
            "a divergence in a replaced history forced the new one to resync"
        );
        assert_eq!(claim_now(&stint, 7), Claim::Granted);
    }

    /// The connection task learns about a divergence by parking on it, so the
    /// wait must resolve whether the latch is set before or after it is
    /// registered, and must park while none is outstanding.
    // FM-REPLICATION-010
    #[tokio::test(start_paused = true)]
    async fn the_divergence_wait_resolves_however_it_races_the_latch() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();

        // Nothing outstanding: the branch parks rather than firing.
        assert!(
            tokio::time::timeout(Duration::from_secs(60), applied.divergence())
                .await
                .is_err(),
            "the link was dropped without a divergence"
        );

        // Admitted while the connection is elsewhere: the wait picks it up on
        // its first read, without a notification to catch.
        stint.admit_divergence(stint.epoch());
        applied.divergence().await;

        // And admitted after a waiter has already registered.
        let fresh = AppliedOffset::detached(0);
        let stint = fresh.begin_replica_stint();
        let waiter = {
            let fresh = fresh.clone();
            tokio::spawn(async move { fresh.divergence().await })
        };
        tokio::task::yield_now().await;
        stint.admit_divergence(stint.epoch());
        waiter.await.unwrap();
    }

    #[test]
    fn retiring_stops_claims_without_opening_a_new_stint() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        assert_eq!(claim_now(&stint, 4), Claim::Granted);
        applied.retire_replica_applies();
        assert_eq!(claim_now(&stint, 4), Claim::Retired);
        // The node's own writes are not gated: a promoted primary keeps counting.
        assert_eq!(applied.advance_by(6), 10);
    }

    #[test]
    fn a_stint_reports_the_head_it_is_claiming_into() {
        // The consume loop reads `current()` off the stint it holds to stamp the
        // offset a frame is being applied at; it must be the shared claimed head
        // (which a resync can move underneath it), not a per-stint counter and
        // not a constant.
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        assert_eq!(stint.current(), 0, "a fresh stint starts where it opened");
        assert_eq!(claim_now(&stint, 7), Claim::Granted);
        assert_eq!(stint.current(), 7, "and tracks each byte it claims");
        assert_eq!(stint.current(), applied.current());
        // A second claim keeps it moving — pins the value against a constant.
        assert_eq!(claim_now(&stint, 5), Claim::Granted);
        assert_eq!(stint.current(), 12);
    }

    #[tokio::test]
    async fn reset_to_adopts_a_fresh_position_visible_through_the_shared_atomic() {
        // The cluster bus reads the adopted atomic directly; every mutation
        // through the owner must be visible there in lockstep.
        let shared = seeded(0);
        let applied = AppliedOffset::detached(0);
        let offsets = ReplicaOffset::new(state_with_save(0), shared.clone(), applied.clone());
        offsets.frame_advance(&frame_of(b"aaaaaaaaaa")); // 10
        assert_eq!(offsets.current(), 10);
        assert_eq!(shared.load(Ordering::Acquire), 10);

        // A fresh FULLRESYNC resets the live position (possibly backward) and it
        // is visible through the adopted cluster-bus atomic. The applied head
        // moves with it: the installed snapshot IS the data at that offset, and
        // any frame decoded from the previous history is void.
        assert!(offsets.reset_to(3));
        assert_eq!(offsets.current(), 3);
        assert_eq!(shared.load(Ordering::Acquire), 3);
        assert_eq!(applied.current(), 3);
    }

    /// Issue 16 of `.scratch/replication-correctness/issues/`, muzzled: both
    /// reconcile paths raise `offset_at_save` with a `max` and nothing lowers
    /// it, so a node that followed a history whose head is below its own save
    /// point keeps claiming the higher offset — on disk, in `INFO`, and (after
    /// a restart seeds the live head from the file) as its own history.
    ///
    /// The invariant catalog reports the state as INV-OFFSET-2 and carries it
    /// as a documented exception citing this issue, because
    /// `primary::tests::a_promotion_persists_its_boundary_without_ever_rewinding_it`
    /// asserts today's behaviour deliberately. Un-ignore when the ruling lands.
    #[tokio::test]
    #[ignore = "issue 17: a backwards full resync leaves the save point above the live head"]
    async fn save_point_follows_a_backwards_full_resync() {
        let state = state_with_save(5_000);
        let offsets = ReplicaOffset::new(state.clone(), seeded(5_000), AppliedOffset::detached(0));

        // A new primary grants a history whose head is below where this node
        // ran: the dataset it now holds stops at 900.
        assert!(offsets.reset_to(900));
        assert_eq!(offsets.current(), 900);

        let persisted = offsets.reconcile_for_persist().await;
        assert_eq!(
            persisted.offset_at_save, 900,
            "the file must describe the data this node holds, not the one it used to"
        );
    }

    // ---------------------------------------------------------------------
    // Full-sync coverage floors (FM-REPLICATION-066).
    // ---------------------------------------------------------------------

    // FM-REPLICATION-066
    #[test]
    fn a_frame_at_or_below_its_floor_is_covered_by_it() {
        assert_eq!(
            frame_disposition(0, Some(100), 100),
            FrameDisposition::CoveredByFloor,
            "the boundary is inclusive: a frame ending exactly at Y_s is wholly inside the payload"
        );
        assert_eq!(
            frame_disposition(0, Some(100), 99),
            FrameDisposition::CoveredByFloor
        );
        assert_eq!(
            frame_disposition(0, Some(100), 101),
            FrameDisposition::Apply,
            "one byte above Y_s is a byte the payload does not contain"
        );
    }

    // FM-REPLICATION-066
    #[test]
    fn an_absent_floor_never_covers_anything() {
        // The reading that costs a re-apply, never the one that drops a write.
        assert_eq!(frame_disposition(0, None, 1), FrameDisposition::Apply);
        assert_eq!(
            frame_disposition(0, None, u64::MAX),
            FrameDisposition::Apply
        );
    }

    // FM-REPLICATION-066
    #[test]
    fn the_head_outranks_the_floor() {
        // Both cover it. The head's branch claims nothing; the floor's claims
        // everything. Taking the floor's would credit the bytes twice.
        assert_eq!(
            frame_disposition(100, Some(100), 100),
            FrameDisposition::CoveredByHead
        );
        assert_eq!(
            frame_disposition(100, Some(1_000), 50),
            FrameDisposition::CoveredByHead
        );
    }

    // FM-REPLICATION-066
    #[test]
    fn an_install_adopts_the_payloads_floors_with_its_offset() {
        let applied = AppliedOffset::detached(0);
        let _stint = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0), applied.clone());

        assert!(offsets.reset_to_payload(0, ShardCoverage::from_watermarks(vec![10, 40, 25])));
        assert_eq!(applied.floor_for(0), Some(10));
        assert_eq!(applied.floor_for(1), Some(40));
        assert_eq!(applied.floor_for(2), Some(25));
        assert_eq!(applied.floor_ceiling(), 40, "the ceiling is max(Y_s)");
    }

    // FM-REPLICATION-066
    #[test]
    fn a_shard_the_vector_does_not_describe_has_no_floor() {
        let applied = AppliedOffset::detached(0);
        let _stint = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0), applied.clone());
        assert!(offsets.reset_to_payload(0, ShardCoverage::from_watermarks(vec![10])));

        assert_eq!(
            applied.floor_for(1),
            None,
            "a shard past the end of the vector"
        );
        assert_eq!(
            applied.floor_for(crate::frame::CONTROL_SHARD),
            None,
            "a control frame is process-wide state the payload makes no claim about"
        );
    }

    // FM-REPLICATION-066
    #[test]
    fn floors_retire_once_the_head_reaches_the_ceiling() {
        let applied = AppliedOffset::detached(0);
        let stint = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0), applied.clone());
        assert!(offsets.reset_to_payload(0, ShardCoverage::from_watermarks(vec![10, 40])));

        assert!(matches!(stint.claim(applied.epoch(), 39), Claim::Granted));
        assert_eq!(applied.floor_for(1), Some(40), "still one byte short");

        assert!(matches!(stint.claim(applied.epoch(), 1), Claim::Granted));
        assert_eq!(
            applied.floor_for(1),
            None,
            "the head has caught up with everything the payload covered"
        );
        assert_eq!(applied.floor_ceiling(), 0, "and the vector is retired");
    }

    // FM-REPLICATION-066
    #[test]
    fn a_rewind_clears_the_floors() {
        let applied = AppliedOffset::detached(0);
        let _stint = applied.begin_replica_stint();
        let offsets = ReplicaOffset::new(state_with_save(0), seeded(0), applied.clone());
        assert!(offsets.reset_to_payload(0, ShardCoverage::from_watermarks(vec![10])));

        assert!(offsets.reset_to(0));
        assert_eq!(
            applied.floor_for(0),
            None,
            "the dataset those floors described has been thrown away"
        );
    }

    /// A node that boots with a persisted offset cannot say what its recovered
    /// keyspace holds above that offset, and says so.
    // FM-REPLICATION-066
    #[test]
    fn offsets_seeded_from_the_persisted_state_are_recovered() {
        assert_eq!(
            AppliedOffset::recovered(5_000).provenance(),
            OffsetProvenance::Recovered
        );
    }

    /// A zero seed is a node that has never held anything: there is no residue
    /// above a claim of nothing, so it is not treated as recovered.
    // FM-REPLICATION-066
    #[test]
    fn a_node_that_has_never_synced_is_not_recovered() {
        assert_eq!(
            AppliedOffset::recovered(0).provenance(),
            OffsetProvenance::Installed
        );
    }

    /// An install is what ends the uncertainty: it adopts a keyspace and an
    /// offset together.
    // FM-REPLICATION-066
    #[test]
    fn an_install_clears_the_recovered_provenance() {
        let applied = AppliedOffset::recovered(5_000);
        let live = seeded(5_000);
        let offsets = ReplicaOffset::new(state_with_save(5_000), live, applied.clone());
        assert!(offsets.reset_to_payload(9_000, ShardCoverage::from_watermarks(vec![9_400])));
        assert_eq!(applied.provenance(), OffsetProvenance::Installed);
    }

    /// The save point persists the floors next to the offset they qualify, so a
    /// restart recovers a claim and a description of what sits above it -- never
    /// one without the other (ruling R15).
    // FM-REPLICATION-066
    #[tokio::test]
    async fn a_save_point_persists_the_floors_in_force() {
        let state = state_with_save(0);
        let offsets = ReplicaOffset::new(state.clone(), seeded(0), AppliedOffset::detached(0));
        assert!(offsets.reset_to_payload(500, ShardCoverage::from_watermarks(vec![900, 700])));

        let saved = offsets.reconcile_for_persist().await;
        assert_eq!(saved.offset_at_save, 500);
        assert_eq!(
            saved.coverage_at_save,
            ShardCoverage::from_watermarks(vec![900, 700])
        );

        // Once the head has caught up with the ceiling there is nothing above
        // the claim left to describe, and the save point says so.
        let stint = offsets.applied().begin_replica_stint();
        assert_eq!(claim_now(&stint, 400), Claim::Granted);
        stint.land();
        let saved = offsets.reconcile_for_persist().await;
        assert_eq!(saved.offset_at_save, 900);
        assert_eq!(saved.coverage_at_save, ShardCoverage::none());
    }
}
