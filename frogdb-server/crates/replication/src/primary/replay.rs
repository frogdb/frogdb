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

use crate::primary::ring_buffer::{ReplicationRingBuffer, SplitBrainBufferConfig};
use crate::state::ReplicationState;

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
    pub fn new(config: &SplitBrainBufferConfig) -> Self {
        Self {
            backlog: ReplicationRingBuffer::new(config.max_entries, config.max_bytes),
            enabled: config.enabled,
        }
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

    /// Oldest offset the backlog can still serve (Redis `repl_backlog_off`).
    pub fn oldest_offset(&self) -> Option<u64> {
        self.backlog.oldest_offset()
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
        match self.oldest_offset() {
            // The backlog still holds the resume point.
            Some(oldest) if req_offset >= oldest => Ok(()),
            // The resume point was evicted — replaying would silently truncate.
            Some(_) => Err(FullResyncReason::BacklogEvicted),
            // Empty backlog: a fully caught-up replica (req == current) needs an
            // empty tail, which we can serve; anything older was evicted.
            None => {
                if req_offset == current_offset {
                    Ok(())
                } else {
                    Err(FullResyncReason::BacklogEvicted)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::serialize_command_to_resp;

    fn enabled_replay() -> PartialSyncReplay {
        PartialSyncReplay::new(&SplitBrainBufferConfig {
            enabled: true,
            max_entries: 1000,
            max_bytes: 64 * 1024 * 1024,
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
        // Establish a failover boundary: rotate the id, freezing secondary at 0
        // (the live offset here). Seed the backlog so the lower bound holds.
        let old_id = state.replication_id.clone();
        state.new_replication_id(0);
        // Backlog must cover the requested offset; an empty backlog with req==0
        // and current==0 still grants an empty tail.
        let grant = assert_continue(replay.handle_partial_sync_request(&state, &old_id, 0, 0));
        assert_eq!(grant.replay_from, 0);
        assert!(grant.frames.is_empty());
    }

    #[test]
    fn evicted_offset_falls_back_to_full_not_truncated() {
        // Tight entry cap so early commands are evicted; requesting an offset
        // below the new oldest must FULLRESYNC, never a truncated Continue.
        let replay = PartialSyncReplay::new(&SplitBrainBufferConfig {
            enabled: true,
            max_entries: 3,
            max_bytes: 64 * 1024 * 1024,
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
        let replay = PartialSyncReplay::new(&SplitBrainBufferConfig {
            enabled: true,
            max_entries: 3,
            max_bytes: 64 * 1024 * 1024,
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

    #[test]
    fn disabled_backlog_always_full_resyncs() {
        let replay = PartialSyncReplay::new(&SplitBrainBufferConfig {
            enabled: false,
            max_entries: 1000,
            max_bytes: 64 * 1024 * 1024,
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
