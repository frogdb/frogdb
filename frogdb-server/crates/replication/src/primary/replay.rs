//! Partial-sync replay: the replication backlog's role in PSYNC.
//!
//! [`PartialSyncReplay`] owns the replication backlog end to end and answers the
//! only questions PSYNC needs: "can I continue this replica from `req_offset`?"
//! and, if so, "what bytes do I stream before joining the live tail?".
//!
//! Deep by construction: the interface is a handful of methods, but behind them
//! sit the eviction-aware coverage check (the *lower* bound
//! [`crate::state::ReplicationState::window_contains`] documents but cannot make),
//! offset-ordered extraction, and the offset→data invariant the streamer relies
//! on. Callers never see the `VecDeque`, the byte accounting, or the eviction
//! rules — they see a single [`ReplayDecision`].
//!
//! ## Offset contract
//!
//! Replay is correct *given* proposal 18's offset contract: the offsets a
//! replica sends in `PSYNC <id> <offset>` and the offsets stored in the backlog
//! are the SAME unit (RESP command-stream bytes, payload only — see
//! [`crate::offset_coordinator::OffsetCoordinator`]). Both ends advance by
//! `frame_advance`, so a requested offset is directly comparable to a stored
//! one.
//!
//! ## Single global stream
//!
//! FrogDB has internal shards, but replication is a *single* global stream: one
//! `PrimaryReplicationHandler`, one `wal_broadcast`, one tracker offset, one
//! backlog. So one [`PartialSyncReplay`] suffices; there is no per-shard offset
//! to reconcile here. If replication is ever sharded, the backlog would shard
//! with it and [`PartialSyncReplay::can_replay`] would need a per-shard window.

use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::primary::ring_buffer::{BacklogConfig, ReplicationRingBuffer};
use crate::state::ReplicationState;

/// How long the backlog outlives its last replica (Redis `repl-backlog-ttl`).
///
/// The backlog is armed for the whole of a primary stint, so a node that once
/// had a replica keeps buffering every write for the rest of the process's life
/// — memory and a push per write spent on resume history nobody is waiting for.
/// This is the bound: once the replica count has been zero for `secs`, the
/// buffer is freed and the window closed. Freeing is safe because a resume
/// below the floor already degrades to a full resync; the TTL only decides how
/// long the cheap path stays on offer.
///
/// `secs == 0` disables the timer, matching Redis's `repl-backlog-ttl 0`.
///
/// Live-mutable: `CONFIG SET repl-backlog-ttl` stores into the atomic and the
/// next tick uses it, including against an idle window that is already running.
#[derive(Debug)]
pub struct BacklogTtl {
    secs: AtomicU64,
    /// When the replica count last fell to zero. `None` whenever a replica is
    /// attached, so a reconnect inside the window restarts the clock rather
    /// than resuming it.
    idle_since: Mutex<Option<Instant>>,
}

impl BacklogTtl {
    /// Seed the TTL (0 = never free).
    pub fn new(secs: u64) -> Self {
        Self {
            secs: AtomicU64::new(secs),
            idle_since: Mutex::new(None),
        }
    }

    /// Seconds with zero replicas before the backlog is freed (0 = disabled).
    pub fn secs(&self) -> u64 {
        self.secs.load(Ordering::Relaxed)
    }

    /// Retune the TTL. Reachable from `ConfigManager` for
    /// `CONFIG SET repl-backlog-ttl`.
    pub fn set_secs(&self, secs: u64) {
        self.secs.store(secs, Ordering::Relaxed);
    }

    /// Advance the idle clock and report whether the backlog is now due to be
    /// freed. Returns `true` exactly once per idle window: the clock is cleared
    /// as it fires, so a still-replica-less primary does not free again on
    /// every subsequent tick.
    fn due(&self, replica_count: usize, now: Instant) -> bool {
        let secs = self.secs();
        let mut idle = self.idle_since.lock();
        if secs == 0 || replica_count > 0 {
            *idle = None;
            return false;
        }
        match *idle {
            None => {
                *idle = Some(now);
                false
            }
            Some(start) => {
                if now.duration_since(start) >= Duration::from_secs(secs) {
                    *idle = None;
                    true
                } else {
                    false
                }
            }
        }
    }
}

/// Owns the replication backlog and the partial-sync grant decision.
///
/// Shares one backlog with split-brain reconciliation (both want the same data:
/// recent commands + offsets), so there is exactly one place that knows what the
/// backlog contains and what offsets it can still serve.
pub struct PartialSyncReplay {
    /// The backlog of recent RESP-encoded commands keyed by their *end* offset.
    backlog: ReplicationRingBuffer,
    /// Whether the backlog is populated. When `false`, [`Self::record`] is a
    /// no-op and every grant falls back to a full resync ([`FullResyncReason::Disabled`]).
    enabled: bool,
    /// How long the window stays open with no replicas attached (see
    /// [`BacklogTtl`]). Shared by `Arc` with `ConfigManager` so
    /// `CONFIG SET repl-backlog-ttl` retunes it live.
    ttl: Arc<BacklogTtl>,
}

/// The decision PSYNC acts on. Total: every PSYNC resolves to exactly one arm.
#[derive(Debug)]
pub enum ReplayDecision {
    /// The window fits AND the backlog still covers `(req_offset, current]`.
    /// Reply `+CONTINUE` and replay the backlog tail, then join the live tail.
    Continue(ReplayGrant),
    /// Window / backlog / replid insufficient — the caller must `+FULLRESYNC`.
    FullResync(FullResyncReason),
}

/// A granted partial resync.
///
/// `frames` is computed at decision time for the unit-test contract (and as a
/// record of what the grant covers). The *live* streamer re-extracts the tail
/// after subscribing to `wal_broadcast` — see
/// `ReplicaSession::start_streaming` — so the handshake window (writes between
/// the grant and the subscribe) cannot slip through the gap between the backlog
/// tail and the live stream. `replay_from` is what the streamer needs.
#[derive(Debug)]
pub struct ReplayGrant {
    /// The replica's offset; the streamer replays `(replay_from, current]`.
    pub replay_from: u64,
    /// RESP-encoded backlog tail `(replay_from, current]` at decision time,
    /// offset-ordered. Each entry is `(offset, origin_shard, resp_bytes)`.
    pub frames: Vec<(u64, u16, Bytes)>,
    /// Offset the replica holds once the (decision-time) tail is applied
    /// (== the live offset observed at grant time).
    pub resume_offset: u64,
}

/// Why a full resync is required — surfaced for logs/metrics so operators can
/// see *why* a replica fell back (Redis tracks `sync_partial_ok`/`sync_partial_err`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullResyncReason {
    /// `PSYNC ? -1` — the replica has no prior offset.
    InitialSync,
    /// The requested replid matches neither `master_replid` nor `master_replid2`.
    ReplidMismatch,
    /// `req_offset > current_offset` — the replica claims to be ahead of the
    /// primary (an impossible continuable window).
    OffsetAhead,
    /// `req_offset < oldest retained offset` — the resume point was evicted.
    BacklogEvicted,
    /// The backlog is disabled by config; no replay is possible.
    Disabled,
}

impl PartialSyncReplay {
    /// Build the replay owner from the backlog config. The backlog is always
    /// constructed (so accessors are infallible); writes are gated on `enabled`.
    pub fn new(config: &BacklogConfig) -> Self {
        Self {
            backlog: ReplicationRingBuffer::new(config.max_entries, config.max_bytes),
            enabled: config.enabled,
            ttl: Arc::new(BacklogTtl::new(config.ttl_secs)),
        }
    }

    /// The live TTL seam. Handed out as an `Arc` so `ConfigManager` can retune
    /// `repl-backlog-ttl` without routing through the handler, the same way it
    /// holds [`crate::primary::LagThresholds`].
    pub fn backlog_ttl(&self) -> Arc<BacklogTtl> {
        self.ttl.clone()
    }

    /// Record one broadcast command into the backlog. Called by
    /// `broadcast_command` (and GETACK) in place of a direct `push`. `shard_id`
    /// is the origin shard the command executed on ([`crate::frame::CONTROL_SHARD`]
    /// for control frames), preserved so a backlog-replayed frame tags the same
    /// shard the live frame did.
    pub fn record(&self, offset: u64, shard_id: u16, resp_bytes: Bytes) {
        if self.enabled {
            self.backlog.push(offset, shard_id, resp_bytes);
        }
    }

    /// Recent divergent writes for split-brain reconciliation — the backlog's
    /// *other* role. Returns `(offset, RESP)` pairs with `offset > last`.
    pub fn extract_divergent_writes(&self, last_replicated_offset: u64) -> Vec<(u64, Bytes)> {
        self.backlog
            .extract_divergent_writes(last_replicated_offset)
    }

    /// The replay tail `(start, end]`, offset-ordered — each entry
    /// `(offset, origin_shard, resp_bytes)`. Only call after [`Self::can_replay`]
    /// has confirmed coverage.
    pub fn extract_backlog(&self, start: u64, end: u64) -> Vec<(u64, u16, Bytes)> {
        self.backlog.extract_backlog(start, end)
    }

    /// End offset of the oldest *retained entry*. Informational (tests, logs);
    /// the resume bound is [`Self::backlog_start`].
    pub fn oldest_offset(&self) -> Option<u64> {
        self.backlog.oldest_offset()
    }

    /// Lowest offset a `+CONTINUE` may resume from (Redis `repl_backlog_off`),
    /// or `None` while this node claims no replication history at all.
    pub fn backlog_start(&self) -> Option<u64> {
        self.enabled.then(|| self.backlog.start_offset()).flatten()
    }

    /// Open the backlog window at `offset` — this node starts claiming history
    /// from here. Called when a primary stint begins (boot recovery, promotion).
    /// See [`crate::primary::ring_buffer::ReplicationRingBuffer::arm_start`].
    pub fn arm_backlog_floor(&self, offset: u64) {
        if self.enabled {
            self.backlog.arm_start(offset);
        }
    }

    /// Throw away the backlog and close the window — this node claims no
    /// replayable history until a stint arms it again.
    ///
    /// Called at both ends of a primary stint: on demotion (the buffered
    /// commands belong to a stint that just ended, and a node that is following
    /// someone else must not keep a window it could serve on re-promotion) and
    /// again when the next stint begins (belt and braces: between the two the
    /// node may have full-resynced to an offset *below* those entries, and a
    /// `fetch_max` floor cannot follow an offset rewind down).
    ///
    /// The split-brain audit reads the backlog through
    /// [`Self::extract_divergent_writes`] as part of the demotion itself, so the
    /// reset must run after that capture — see `RoleManager::demote`.
    pub fn reset_backlog(&self) {
        self.backlog.reset();
    }

    /// Whether this node holds a resume point a reconnecting replica could be
    /// continued from — Redis's "the replication backlog exists".
    ///
    /// True from the moment the window is armed, not from the first buffered
    /// write: an armed-but-empty backlog is exactly the state a just-promoted (or
    /// just-restarted) primary is in, and it *can* serve a caught-up replica an
    /// empty tail. The primary must keep stamping offsets and recording writes
    /// for as long as this holds, even with zero connected replicas: a write that
    /// skips the backlog while a replica is away leaves a hole no later
    /// `+CONTINUE` can fill, so the replica would resume at a stale offset and
    /// silently diverge. See [`crate::ReplicationBroadcaster::is_active`].
    pub fn has_resume_history(&self) -> bool {
        self.backlog_start().is_some()
    }

    /// One tick of the backlog TTL: free the buffer and close the window if it
    /// has now gone [`BacklogTtl::secs`] seconds without a replica. Returns
    /// whether it freed.
    ///
    /// The `now` is a parameter rather than an `Instant::now()` inside so the
    /// timer is testable without sleeping.
    ///
    /// A node with no window armed never starts the clock: there is nothing to
    /// free, and the window a later promotion arms should get its own full TTL
    /// rather than inherit one that has been running since boot.
    ///
    /// Freeing touches the ring buffer and nothing else. It is not a stint
    /// change: the replication id, the failover window and every offset stay
    /// exactly where they were, so a replica that comes back afterwards is
    /// answered `+FULLRESYNC` (no armed floor) rather than a `+CONTINUE` over
    /// history that has been dropped.
    pub fn expire_backlog_if_idle(&self, replica_count: usize, now: Instant) -> bool {
        if !self.has_resume_history() {
            return false;
        }
        if !self.ttl.due(replica_count, now) {
            return false;
        }
        self.reset_backlog();
        true
    }

    /// The single entry point. A pure decision over `(state, req_offset, current)`
    /// plus the backlog's current contents; performs no I/O. PSYNC turns the
    /// result into the `+CONTINUE`/`+FULLRESYNC` reply.
    pub fn handle_partial_sync_request(
        &self,
        state: &ReplicationState,
        requested_id: &str,
        req_offset: u64,
        current_offset: u64,
    ) -> ReplayDecision {
        match self.can_replay(state, requested_id, req_offset, current_offset) {
            Err(reason) => ReplayDecision::FullResync(reason),
            Ok(()) => ReplayDecision::Continue(ReplayGrant {
                replay_from: req_offset,
                frames: self.extract_backlog(req_offset, current_offset),
                resume_offset: current_offset,
            }),
        }
    }

    /// Both bounds. Composes the existing upper-bound window check
    /// ([`ReplicationState::window_contains`], which also validates the replid
    /// against `master_replid` / `master_replid2`) with the NEW lower-bound
    /// (eviction) check only the backlog can answer.
    fn can_replay(
        &self,
        state: &ReplicationState,
        requested_id: &str,
        req_offset: u64,
        current_offset: u64,
    ) -> Result<(), FullResyncReason> {
        if !self.enabled {
            return Err(FullResyncReason::Disabled);
        }
        // `PSYNC ? -1` (and any "?" id) is the initial-sync sentinel.
        if requested_id == "?" {
            return Err(FullResyncReason::InitialSync);
        }
        // Upper bound + replid — reuse the state primitive, don't re-derive it.
        if !state.window_contains(requested_id, req_offset, current_offset) {
            // Classify *why* it missed, for observability.
            let replid_known = requested_id == state.replication_id
                || state.secondary_id.as_deref() == Some(requested_id);
            if !replid_known {
                return Err(FullResyncReason::ReplidMismatch);
            }
            // replid is known, so the miss is an offset past the upper bound.
            return Err(FullResyncReason::OffsetAhead);
        }
        // Lower bound: the check `window_contains` documents but cannot make.
        //
        // The floor is the single authority — deliberately NOT "empty backlog ⇒
        // grant when `req == current`". That shortcut is safe only at offset 0
        // (nothing to miss); at any nonzero offset it hands a `+CONTINUE` to a
        // replica whose gap was never buffered, and the replica silently resumes
        // on a divergent dataset. An armed floor already covers the legitimate
        // caught-up case (`req == current >= floor`) without the hazard.
        match self.backlog_start() {
            // The window is open and still holds the resume point.
            Some(floor) if req_offset >= floor => Ok(()),
            // The resume point predates the window — replaying would truncate.
            Some(_) => Err(FullResyncReason::BacklogEvicted),
            // No window armed: this node claims no replayable history.
            None => Err(FullResyncReason::BacklogEvicted),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::serialize_command_to_resp;

    fn enabled_replay() -> PartialSyncReplay {
        PartialSyncReplay::new(&BacklogConfig {
            enabled: true,
            max_entries: 1000,
            max_bytes: 64 * 1024 * 1024,
            ttl_secs: 0,
        })
    }

    /// Same, with the backlog TTL set (0 = never free).
    fn ttl_replay(ttl_secs: u64) -> PartialSyncReplay {
        PartialSyncReplay::new(&BacklogConfig {
            enabled: true,
            max_entries: 1000,
            max_bytes: 64 * 1024 * 1024,
            ttl_secs,
        })
    }

    /// Push `n` commands, advancing the offset by each command's payload length,
    /// and return the resulting list of `(end_offset, bytes)` plus the head.
    fn seed(replay: &PartialSyncReplay, n: usize) -> (Vec<(u64, Bytes)>, u64) {
        let mut offset = 0u64;
        let mut pushed = Vec::new();
        for i in 0..n {
            let resp = serialize_command_to_resp(
                "SET",
                &[Bytes::from(format!("k{i}")), Bytes::from(format!("v{i}"))],
            );
            offset += resp.len() as u64;
            // Tag each seeded command with a distinct origin shard so the
            // replay tail can be asserted to preserve it.
            let shard = (i % 4) as u16;
            replay.record(offset, shard, resp.clone());
            pushed.push((offset, resp));
        }
        (pushed, offset)
    }

    fn assert_full(decision: ReplayDecision, reason: FullResyncReason) {
        match decision {
            ReplayDecision::FullResync(r) => assert_eq!(r, reason, "wrong full-resync reason"),
            ReplayDecision::Continue(_) => panic!("expected FullResync({reason:?}), got Continue"),
        }
    }

    fn assert_continue(decision: ReplayDecision) -> ReplayGrant {
        match decision {
            ReplayDecision::Continue(grant) => grant,
            ReplayDecision::FullResync(r) => panic!("expected Continue, got FullResync({r:?})"),
        }
    }

    #[test]
    fn window_fit_continues_with_ordered_tail() {
        let replay = enabled_replay();
        let state = ReplicationState::new();
        let (pushed, head) = seed(&replay, 5);
        // Request from the 2nd command's end offset; expect (req, head] in order.
        let req = pushed[1].0;
        let grant = assert_continue(replay.handle_partial_sync_request(
            &state,
            &state.replication_id,
            req,
            head,
        ));
        assert_eq!(grant.replay_from, req);
        assert_eq!(grant.resume_offset, head);
        let offsets: Vec<u64> = grant.frames.iter().map(|(o, _, _)| *o).collect();
        assert_eq!(offsets, vec![pushed[2].0, pushed[3].0, pushed[4].0]);
        // Strictly ascending.
        assert!(offsets.windows(2).all(|w| w[0] < w[1]));
        // The origin shard tag survives the replay tail (seed tags i%4).
        let shards: Vec<u16> = grant.frames.iter().map(|(_, s, _)| *s).collect();
        assert_eq!(shards, vec![2, 3, 0]);
    }

    #[test]
    fn offset_ahead_falls_back_to_full() {
        let replay = enabled_replay();
        let state = ReplicationState::new();
        let (_pushed, head) = seed(&replay, 3);
        assert_full(
            replay.handle_partial_sync_request(&state, &state.replication_id, head + 1, head),
            FullResyncReason::OffsetAhead,
        );
    }

    #[test]
    fn unknown_replid_falls_back_to_full() {
        let replay = enabled_replay();
        let state = ReplicationState::new();
        let (_pushed, head) = seed(&replay, 3);
        assert_full(
            replay.handle_partial_sync_request(&state, "deadbeef", 0, head),
            FullResyncReason::ReplidMismatch,
        );
    }

    #[test]
    fn initial_sync_sentinel_falls_back_to_full() {
        let replay = enabled_replay();
        let state = ReplicationState::new();
        let (_pushed, head) = seed(&replay, 3);
        assert_full(
            replay.handle_partial_sync_request(&state, "?", 0, head),
            FullResyncReason::InitialSync,
        );
    }

    #[test]
    fn secondary_id_within_window_continues() {
        let replay = enabled_replay();
        let mut state = ReplicationState::new();
        // A promotion at offset 4096: the id rotates, the old id becomes the
        // failover window frozen at the live offset, and the backlog floor is
        // armed at the same boundary. A sibling replica that had applied exactly
        // up to the boundary resumes with an empty tail.
        let boundary = 4096u64;
        let old_id = state.replication_id.clone();
        state.new_replication_id(boundary);
        replay.arm_backlog_floor(boundary);
        let grant = assert_continue(
            replay.handle_partial_sync_request(&state, &old_id, boundary, boundary),
        );
        assert_eq!(grant.replay_from, boundary);
        assert_eq!(grant.resume_offset, boundary);
        assert!(grant.frames.is_empty());
    }

    #[test]
    fn unarmed_backlog_at_nonzero_offset_falls_back_to_full() {
        // The floor is the sole authority for the lower bound. Without it, an
        // empty backlog at a nonzero head would hand out a `+CONTINUE` for a gap
        // that was never buffered — the replica would resume on a divergent
        // dataset. No floor => no partial sync, whatever the requested offset.
        let replay = enabled_replay();
        let state = ReplicationState::new();
        assert!(replay.backlog_start().is_none());
        assert_full(
            replay.handle_partial_sync_request(&state, &state.replication_id, 4096, 4096),
            FullResyncReason::BacklogEvicted,
        );
    }

    #[test]
    fn request_below_armed_floor_falls_back_to_full() {
        let replay = enabled_replay();
        let state = ReplicationState::new();
        replay.arm_backlog_floor(4096);
        assert_eq!(replay.backlog_start(), Some(4096));
        assert_full(
            replay.handle_partial_sync_request(&state, &state.replication_id, 4095, 4096),
            FullResyncReason::BacklogEvicted,
        );
    }

    #[test]
    fn evicted_offset_falls_back_to_full_not_truncated() {
        // Tight entry cap so early commands are evicted; requesting an offset
        // below the new oldest must FULLRESYNC, never a truncated Continue.
        let replay = PartialSyncReplay::new(&BacklogConfig {
            enabled: true,
            max_entries: 3,
            max_bytes: 64 * 1024 * 1024,
            ttl_secs: 0,
        });
        let state = ReplicationState::new();
        let (pushed, head) = seed(&replay, 10);
        // The first 7 were evicted; oldest is pushed[7].
        let oldest = replay.oldest_offset().unwrap();
        assert_eq!(oldest, pushed[7].0);
        // Request below the oldest retained offset.
        assert_full(
            replay.handle_partial_sync_request(&state, &state.replication_id, pushed[2].0, head),
            FullResyncReason::BacklogEvicted,
        );
    }

    #[test]
    fn boundary_req_equals_oldest_continues() {
        let replay = PartialSyncReplay::new(&BacklogConfig {
            enabled: true,
            max_entries: 3,
            max_bytes: 64 * 1024 * 1024,
            ttl_secs: 0,
        });
        let state = ReplicationState::new();
        let (pushed, head) = seed(&replay, 10);
        let oldest = replay.oldest_offset().unwrap();
        // req == oldest is the lowest grantable offset.
        let grant = assert_continue(replay.handle_partial_sync_request(
            &state,
            &state.replication_id,
            oldest,
            head,
        ));
        // The tail starts strictly after `oldest`.
        assert_eq!(grant.frames.first().map(|(o, _, _)| *o), Some(pushed[8].0));
        assert_eq!(grant.frames.last().map(|(o, _, _)| *o), Some(head));
    }

    #[test]
    fn boundary_req_equals_current_grants_empty_tail() {
        let replay = enabled_replay();
        let state = ReplicationState::new();
        let (_pushed, head) = seed(&replay, 5);
        let grant = assert_continue(replay.handle_partial_sync_request(
            &state,
            &state.replication_id,
            head,
            head,
        ));
        assert!(grant.frames.is_empty(), "caught-up replica replays nothing");
        assert_eq!(grant.resume_offset, head);
    }

    /// Freeing is on a clock, not on the disconnect: the window survives the
    /// last replica by exactly `repl-backlog-ttl` seconds, then the buffer is
    /// emptied and the floor disarmed.
    // FM-REPLICATION-009
    #[test]
    fn an_idle_backlog_is_freed_once_its_ttl_elapses() {
        let replay = ttl_replay(60);
        let (_pushed, head) = seed(&replay, 5);
        assert!(replay.has_resume_history());
        let t0 = Instant::now();
        // First idle tick only starts the clock.
        assert!(!replay.expire_backlog_if_idle(0, t0));
        assert!(!replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(59)));
        assert!(replay.backlog_start().is_some(), "still inside the window");
        // At the deadline it frees: buffer empty, window closed.
        assert!(replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(60)));
        assert_eq!(replay.backlog_start(), None);
        assert_eq!(replay.oldest_offset(), None);
        assert!(replay.extract_backlog(0, head).is_empty());
    }

    /// The clock is idle time, not age: any tick that sees a replica clears it,
    /// so a replica that reconnects inside the window still gets its
    /// `+CONTINUE`, and the window it leaves behind starts over.
    // FM-REPLICATION-009
    #[test]
    fn a_replica_reconnecting_before_the_ttl_still_resumes() {
        let replay = ttl_replay(60);
        let state = ReplicationState::new();
        let (pushed, head) = seed(&replay, 5);
        let t0 = Instant::now();
        assert!(!replay.expire_backlog_if_idle(0, t0));
        // The replica comes back at t0+30 and the resume is granted.
        assert!(!replay.expire_backlog_if_idle(1, t0 + Duration::from_secs(30)));
        let grant = assert_continue(replay.handle_partial_sync_request(
            &state,
            &state.replication_id,
            pushed[1].0,
            head,
        ));
        assert_eq!(grant.resume_offset, head);
        // It leaves again at t0+31; the deadline is 31+60, not 0+60.
        assert!(!replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(31)));
        assert!(!replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(90)));
        assert!(replay.backlog_start().is_some());
        assert!(replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(91)));
    }

    /// A replica attached the whole time never lets the clock start, whatever
    /// the tick count.
    // FM-REPLICATION-009
    #[test]
    fn a_connected_replica_never_starts_the_ttl_clock() {
        let replay = ttl_replay(1);
        let (_pushed, _head) = seed(&replay, 3);
        let t0 = Instant::now();
        for tick in 0..600u64 {
            assert!(
                !replay.expire_backlog_if_idle(1, t0 + Duration::from_secs(tick)),
                "freed while a replica was attached (tick {tick})"
            );
        }
        assert!(replay.backlog_start().is_some());
    }

    /// `repl-backlog-ttl 0` parks the timer (Redis's disable value), and the
    /// knob is live: a `CONFIG SET` retunes a window that is already idling.
    // FM-REPLICATION-009
    #[test]
    fn a_ttl_of_zero_never_frees_the_backlog() {
        let replay = ttl_replay(0);
        let (_pushed, _head) = seed(&replay, 3);
        let t0 = Instant::now();
        for hour in 0..48u64 {
            assert!(!replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(hour * 3600)));
        }
        assert!(replay.backlog_start().is_some());
        // Live retune: enabling it mid-idle-window frees on the next deadline
        // (the clock starts from the tick that follows the CONFIG SET, since a
        // 0 TTL keeps clearing it).
        replay.backlog_ttl().set_secs(60);
        let t1 = t0 + Duration::from_secs(48 * 3600);
        assert!(!replay.expire_backlog_if_idle(0, t1));
        assert!(replay.expire_backlog_if_idle(0, t1 + Duration::from_secs(60)));
        // And back off again: a re-armed window with the TTL at 0 stays.
        replay.arm_backlog_floor(4096);
        replay.backlog_ttl().set_secs(0);
        assert!(!replay.expire_backlog_if_idle(0, t1 + Duration::from_secs(120)));
        assert_eq!(replay.backlog_start(), Some(4096));
    }

    /// The point of the free: the floor is disarmed with the buffer, so the
    /// next PSYNC is answered `+FULLRESYNC` rather than a `+CONTINUE` over a
    /// hole — including from the replica that was exactly caught up.
    // FM-REPLICATION-009
    #[test]
    fn a_freed_backlog_full_resyncs_the_next_psync() {
        let replay = ttl_replay(60);
        let state = ReplicationState::new();
        let (pushed, head) = seed(&replay, 5);
        let t0 = Instant::now();
        assert!(!replay.expire_backlog_if_idle(0, t0));
        assert!(replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(60)));
        for req in [0, pushed[1].0, head] {
            assert_full(
                replay.handle_partial_sync_request(&state, &state.replication_id, req, head),
                FullResyncReason::BacklogEvicted,
            );
        }
    }

    /// A node with no window armed has nothing to free and must not start a
    /// clock: the window a later stint arms gets its own full TTL rather than
    /// inheriting one that has been running since boot.
    // FM-REPLICATION-009
    #[test]
    fn an_unarmed_backlog_never_starts_the_ttl_clock() {
        let replay = ttl_replay(60);
        assert!(!replay.has_resume_history());
        let t0 = Instant::now();
        for tick in 0..200u64 {
            assert!(!replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(tick)));
        }
        // A stint arms the window at t0+200; its deadline is 200+60.
        replay.arm_backlog_floor(4096);
        assert!(!replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(200)));
        assert!(!replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(259)));
        assert!(replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(260)));
    }

    /// The tick runs at 1 Hz forever; the free is a transition, so it reports
    /// `true` exactly once per idle window and stays quiet afterwards (the
    /// caller logs on `true`).
    // FM-REPLICATION-009
    #[test]
    fn the_ttl_fires_once_per_idle_window_not_once_per_tick() {
        let replay = ttl_replay(5);
        seed(&replay, 3);
        let t0 = Instant::now();
        let mut fired = 0;
        for tick in 0..100u64 {
            if replay.expire_backlog_if_idle(0, t0 + Duration::from_secs(tick)) {
                fired += 1;
            }
        }
        assert_eq!(fired, 1, "the free is a transition, not a per-tick action");
        // A new stint arms a fresh window; that one is freed once too.
        replay.arm_backlog_floor(4096);
        let t1 = t0 + Duration::from_secs(100);
        let mut fired = 0;
        for tick in 0..100u64 {
            if replay.expire_backlog_if_idle(0, t1 + Duration::from_secs(tick)) {
                fired += 1;
            }
        }
        assert_eq!(fired, 1);
    }

    #[test]
    fn disabled_backlog_always_full_resyncs() {
        let replay = PartialSyncReplay::new(&BacklogConfig {
            enabled: false,
            max_entries: 1000,
            max_bytes: 64 * 1024 * 1024,
            ttl_secs: 0,
        });
        let state = ReplicationState::new();
        // record is a no-op when disabled.
        replay.record(100, 0, Bytes::from_static(b"x"));
        assert!(replay.oldest_offset().is_none());
        assert_full(
            replay.handle_partial_sync_request(&state, &state.replication_id, 0, 0),
            FullResyncReason::Disabled,
        );
    }
}
