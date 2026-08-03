use crate::primary::ring_buffer::ReplicationRingBuffer;
use bytes::Bytes;

/// Build a primary handler with an enabled split-brain backlog for the
/// divergence-record tests. No I/O beyond a temp state path; the handler's own
/// `offsets` coordinator + `replay` backlog are the only inputs exercised.
#[cfg(test)]
fn divergence_handler(dir: &std::path::Path) -> crate::primary::PrimaryReplicationHandler {
    use crate::identity::ReplicationIdentity;
    use crate::primary::PrimaryReplicationHandler;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use crate::{BacklogConfig, LagThresholdConfig};
    use std::sync::Arc;
    use std::time::Duration;

    let tracker = Arc::new(ReplicationTrackerImpl::new());
    PrimaryReplicationHandler::new(
        ReplicationIdentity::adopting(ReplicationState::new(), &tracker),
        dir.join("replication_state.json"),
        tracker,
        None,
        dir.to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        BacklogConfig {
            enabled: true,
            max_entries: 1000,
            max_bytes: 64 * 1024 * 1024,
            ttl_secs: 0,
        },
        0,
    )
}

/// Broadcast one command through the primary path (advances the live offset AND
/// records into the backlog, exactly as production writes do) and return the new
/// live offset.
#[cfg(test)]
fn push_write(handler: &crate::primary::PrimaryReplicationHandler, key: &str, val: &str) -> u64 {
    handler.broadcast_command(
        "SET",
        &[Bytes::from(key.to_string()), Bytes::from(val.to_string())],
    )
}

/// Register a streaming replica and ack it at `acked`, so it contributes to
/// `min_acked_offset`.
#[cfg(test)]
fn streaming_replica_acked_at(
    handler: &crate::primary::PrimaryReplicationHandler,
    addr: &str,
    acked: u64,
) {
    use crate::replica_session::Phase;
    let session = handler.tracker.register_replica(addr.parse().unwrap());
    session.force_phase_for_test(Phase::Streaming);
    handler.offsets.ingest_replica_ack(session.id(), acked);
}

/// The broadcast gate stays OPEN after the last replica disconnects, for as
/// long as the backlog can still serve a `+CONTINUE`.
///
/// Regression guard for silent divergence: the gate used to be
/// `replica_count() > 0`, so a primary that dropped a laggy replica stopped
/// stamping offsets and recording writes. The replica then reconnected inside
/// the (stale) window, was granted a partial resync, and resumed past every
/// write made while it was away — `master_link_status:up`, WAIT acking, and a
/// keyspace missing thousands of keys.
#[test]
fn broadcast_gate_stays_open_while_backlog_can_resume() {
    use crate::ReplicationBroadcaster;
    use frogdb_types::ReplicationTracker;

    let dir = tempfile::TempDir::new().unwrap();
    let handler = divergence_handler(dir.path());

    // A primary that never had a replica records nothing and stays inactive, so
    // standalone writes pay no replication cost.
    assert!(
        !handler.is_active(),
        "no replica and an empty backlog ⇒ inactive"
    );

    // A replica connects; the writes it streams populate the backlog.
    let session = handler
        .tracker
        .register_replica("127.0.0.1:6380".parse().unwrap());
    session.force_phase_for_test(crate::replica_session::Phase::Streaming);
    assert!(handler.is_active(), "a connected replica opens the gate");
    let before_drop = push_write(&handler, "k0", "v0");

    // ... and then drops.
    handler.tracker.unregister_replica(session.id());
    assert_eq!(handler.tracker.replica_count(), 0);

    assert!(
        handler.is_active(),
        "zero replicas but a live resume window ⇒ the gate must stay open"
    );

    // The writes made while nobody is connected still advance the offset and
    // land in the backlog, so the replay tail covers them when the replica
    // returns.
    let after_drop = push_write(&handler, "k1", "v1");
    assert!(
        after_drop > before_drop,
        "an unreplicated write must still advance the offset"
    );
    let tail = handler.replay.extract_backlog(before_drop, after_drop);
    assert_eq!(
        tail.len(),
        1,
        "the write made with no replica connected must be replayable, got {tail:?}"
    );
}

/// `end == min_acked` ⇒ `None` (pins the `end > start` gate — `current > min_acked`
/// today). A fully caught-up demoted primary diverged from nothing.
#[test]
fn divergence_record_none_when_caught_up() {
    let dir = tempfile::TempDir::new().unwrap();
    let handler = divergence_handler(dir.path());

    push_write(&handler, "k0", "v0");
    let current = push_write(&handler, "k1", "v1");
    // One streaming replica acked exactly at the live head.
    streaming_replica_acked_at(&handler, "127.0.0.1:6380", current);

    assert_eq!(handler.offsets.min_acked(), Some(current));
    assert_eq!(handler.offsets.current(), current);
    assert!(
        handler.divergence_record().is_none(),
        "a caught-up primary (end == start) did not diverge"
    );
}

/// `end > start` but no backlog writes past `start` ⇒ `None` (pins the
/// `!writes.is_empty()` gate). The live offset advanced without any recorded
/// command past the acked point, so there is nothing to surrender.
#[test]
fn divergence_record_none_when_backlog_empty_past_start() {
    let dir = tempfile::TempDir::new().unwrap();
    let handler = divergence_handler(dir.path());

    // Advance the live offset directly (no backlog record), so `current` moves
    // ahead of `min_acked` (0, no streaming replicas) but the backlog holds
    // nothing with `offset > 0`.
    let current = handler.offsets.advance(&Bytes::from(vec![b'x'; 64]));
    assert_eq!(handler.offsets.min_acked(), None);
    assert!(current > 0);
    assert!(handler.replay.extract_divergent_writes(0).is_empty());

    assert!(
        handler.divergence_record().is_none(),
        "no backlog write past start ⇒ nothing diverged"
    );
}

/// Acks at `min_acked`, several writes past it ⇒
/// `Some { start == min_acked, end == current, writes == (start, current] }`,
/// offset-ordered — the exact fact no prior test covered.
#[test]
fn divergence_record_window_and_writes() {
    let dir = tempfile::TempDir::new().unwrap();
    let handler = divergence_handler(dir.path());

    // Writes the cluster had acknowledged.
    push_write(&handler, "k0", "v0");
    let acked = push_write(&handler, "k1", "v1");
    // Two streaming replicas, both acked at `acked` — the min is `acked`.
    streaming_replica_acked_at(&handler, "127.0.0.1:6380", acked);
    streaming_replica_acked_at(&handler, "127.0.0.1:6381", acked);

    // Divergent writes committed past the acked point.
    let o2 = push_write(&handler, "k2", "v2");
    let o3 = push_write(&handler, "k3", "v3");
    let current = o3;

    let record = handler
        .divergence_record()
        .expect("primary committed writes past the acked offset ⇒ diverged");
    assert_eq!(record.start, acked, "lower bound is the min acked offset");
    assert_eq!(
        record.end, current,
        "upper bound is the live write position"
    );
    let offsets: Vec<u64> = record.writes.iter().map(|(o, _)| *o).collect();
    assert_eq!(
        offsets,
        vec![o2, o3],
        "only writes with offset > start, in order"
    );
    assert!(offsets.windows(2).all(|w| w[0] < w[1]), "offset-ordered");
    // The lower-bound write (== start) is excluded; the head (== end) is included.
    assert!(record.writes.iter().all(|(o, _)| *o > acked));
}

/// `min_acked()` is `None` (no streaming replicas) ⇒ `start == 0`, the whole
/// backlog is divergent (pins the `unwrap_or(0)` floor).
#[test]
fn divergence_record_no_streaming_replicas_uses_zero_floor() {
    let dir = tempfile::TempDir::new().unwrap();
    let handler = divergence_handler(dir.path());

    let o0 = push_write(&handler, "k0", "v0");
    let o1 = push_write(&handler, "k1", "v1");
    let current = push_write(&handler, "k2", "v2");
    assert_eq!(handler.offsets.min_acked(), None);

    let record = handler
        .divergence_record()
        .expect("no acked floor ⇒ the whole backlog is divergent");
    assert_eq!(record.start, 0, "no streaming replicas ⇒ zero floor");
    assert_eq!(record.end, current);
    let offsets: Vec<u64> = record.writes.iter().map(|(o, _)| *o).collect();
    assert_eq!(
        offsets,
        vec![o0, o1, current],
        "entire backlog is divergent"
    );
}

// The `parse_replconf_ack` unit tests moved to the `ReplconfCodec` golden
// round-trip suite in `frame.rs` (the codec now owns the ACK/GETACK grammar).

#[test]
fn test_ring_buffer_push_and_extract() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.push(10, 0, Bytes::from("cmd1"));
    rb.push(20, 0, Bytes::from("cmd2"));
    rb.push(30, 0, Bytes::from("cmd3"));
    let writes = rb.extract_divergent_writes(0);
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0], (10, Bytes::from("cmd1")));
    assert_eq!(writes[1], (20, Bytes::from("cmd2")));
    assert_eq!(writes[2], (30, Bytes::from("cmd3")));
    let writes = rb.extract_divergent_writes(20);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0], (30, Bytes::from("cmd3")));
    let writes = rb.extract_divergent_writes(30);
    assert!(writes.is_empty());
}

#[test]
fn test_ring_buffer_entry_limit_eviction() {
    let rb = ReplicationRingBuffer::new(3, 1024 * 1024);
    rb.push(10, 0, Bytes::from("cmd1"));
    rb.push(20, 0, Bytes::from("cmd2"));
    rb.push(30, 0, Bytes::from("cmd3"));
    rb.push(40, 0, Bytes::from("cmd4"));
    let writes = rb.extract_divergent_writes(0);
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0].0, 20);
    assert_eq!(writes[2].0, 40);
}

#[test]
fn test_ring_buffer_byte_limit_eviction() {
    let rb = ReplicationRingBuffer::new(100, 10);
    rb.push(10, 0, Bytes::from("abcde"));
    rb.push(20, 0, Bytes::from("fghij"));
    rb.push(30, 0, Bytes::from("klmno"));
    let writes = rb.extract_divergent_writes(0);
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0].0, 20);
    assert_eq!(writes[1].0, 30);
}

#[test]
fn test_ring_buffer_empty() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    let writes = rb.extract_divergent_writes(0);
    assert!(writes.is_empty());
}

#[test]
fn test_ring_buffer_extract_is_nondestructive() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.push(10, 0, Bytes::from("cmd1"));
    let w1 = rb.extract_divergent_writes(0);
    let w2 = rb.extract_divergent_writes(0);
    assert_eq!(w1.len(), 1);
    assert_eq!(w2.len(), 1);
}

#[test]
fn ring_buffer_reset_closes_the_window_and_lets_the_floor_move_down() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.arm_start(1000);
    rb.push(1010, 0, Bytes::from("cmd1"));
    assert_eq!(rb.start_offset(), Some(1000));

    rb.reset();

    assert_eq!(rb.start_offset(), None, "a reset buffer claims no history");
    assert_eq!(rb.oldest_offset(), None);
    assert!(rb.extract_divergent_writes(0).is_empty());
    // The floor is `fetch_max`, so only a reset can bring it back below a
    // previous stint's head — the case a re-promotion after a rewinding full
    // resync lands in.
    rb.arm_start(50);
    assert_eq!(rb.start_offset(), Some(50));
}

#[test]
fn ring_buffer_push_into_an_unarmed_buffer_opens_the_window_at_the_entry_start() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.push(1010, 0, Bytes::from("0123456789"));
    assert_eq!(
        rb.start_offset(),
        Some(1000),
        "the pushed entry itself must be replayable"
    );
}

#[test]
fn test_ring_buffer_oldest_offset_tracks_eviction() {
    let rb = ReplicationRingBuffer::new(3, 1024 * 1024);
    assert_eq!(rb.oldest_offset(), None);
    rb.push(10, 0, Bytes::from("cmd1"));
    assert_eq!(rb.oldest_offset(), Some(10));
    rb.push(20, 0, Bytes::from("cmd2"));
    rb.push(30, 0, Bytes::from("cmd3"));
    assert_eq!(rb.oldest_offset(), Some(10));
    // Eviction raises the oldest retained offset (Redis repl_backlog_off).
    rb.push(40, 0, Bytes::from("cmd4"));
    assert_eq!(rb.oldest_offset(), Some(20));
}

#[test]
fn test_ring_buffer_extract_backlog_is_contiguous_and_bounded() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.push(10, 0, Bytes::from("cmd1"));
    rb.push(20, 0, Bytes::from("cmd2"));
    rb.push(30, 0, Bytes::from("cmd3"));
    rb.push(40, 0, Bytes::from("cmd4"));
    // (start, end] — exclusive lower, inclusive upper.
    let tail = rb.extract_backlog(10, 30);
    assert_eq!(
        tail,
        vec![(20, 0, Bytes::from("cmd2")), (30, 0, Bytes::from("cmd3"))]
    );
    // start == end yields an empty (caught-up) tail.
    assert!(rb.extract_backlog(40, 40).is_empty());
    // Whole tail above start.
    let all = rb.extract_backlog(0, 40);
    assert_eq!(all.len(), 4);
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
}

#[tokio::test]
async fn save_state_persists_tracker_offset() {
    use crate::primary::PrimaryReplicationHandler;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use crate::{BacklogConfig, LagThresholdConfig};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let state_path = dir.path().join("replication_state.json");
    let state = ReplicationState::new();
    let replid = state.replication_id.clone();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let handler = PrimaryReplicationHandler::new(
        crate::identity::ReplicationIdentity::adopting(state, &tracker),
        state_path.clone(),
        tracker.clone(),
        None,
        dir.path().to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        BacklogConfig {
            enabled: false,
            max_entries: 0,
            max_bytes: 0,
            ttl_secs: 0,
        },
        0,
    );

    // The offset on disk starts at 0 even though it was never explicitly saved.
    // Advance through the one advance gate, the way `broadcast_command` does —
    // which moves the applied offset with the live head, since a primary
    // broadcasts only what its shard already applied. A save point persists the
    // applied offset, so a bare `tracker.set_offset` would not be a faithful
    // stand-in here.
    assert_eq!(handler.offsets.advance(&Bytes::from(vec![b'x'; 987])), 987);
    handler.save_state().unwrap();

    // A restart seeds the tracker from this file, so the offset must survive
    // and keep the same replication id.
    let reloaded = ReplicationState::load_or_create(&state_path).unwrap();
    assert_eq!(reloaded.offset_at_save, 987);
    assert_eq!(reloaded.replication_id, replid);
}

/// Regression pin for the WAIT → GETACK wiring: a blocking WAIT with a lagging
/// streaming replica must broadcast a `REPLCONF GETACK *` frame, stamped with
/// the (advanced) live offset like any other command-stream frame.
#[tokio::test]
async fn wait_with_lagging_replica_broadcasts_a_stamped_getack() {
    use crate::primary::PrimaryReplicationHandler;
    use crate::replica_session::Phase;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use crate::wait_coordinator::WaitVerdict;
    use crate::{BacklogConfig, LagThresholdConfig};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let handler = PrimaryReplicationHandler::new(
        crate::identity::ReplicationIdentity::adopting(ReplicationState::new(), &tracker),
        dir.path().join("replication_state.json"),
        tracker.clone(),
        None,
        dir.path().to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        BacklogConfig {
            enabled: false,
            max_entries: 0,
            max_bytes: 0,
            ttl_secs: 0,
        },
        0,
    );

    // One streaming replica that has acked nothing while the stream is ahead.
    let session = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
    session.force_phase_for_test(Phase::Streaming);
    let target = handler.offsets.advance(&Bytes::from(vec![b'x'; 100]));

    let mut frames = handler.wal_broadcast.subscribe();

    let wait = handler.wait_coordinator();
    let verdict = wait
        .wait_for_replicas(
            wait.role_fence(),
            target,
            1,
            Some(tokio::time::Instant::now() + Duration::from_millis(30)),
            &handler,
        )
        .await;
    assert_eq!(verdict, WaitVerdict::TimedOut(0));

    // The solicitation frame went out on the WAL broadcast, advanced the live
    // offset, and self-describes its end offset in the sequence field.
    let frame = frames.try_recv().expect("WAIT must broadcast a GETACK");
    assert!(
        frame
            .payload
            .starts_with(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n"),
        "expected REPLCONF GETACK, got {:?}",
        frame.payload
    );
    assert_eq!(frame.sequence, handler.offsets.current());
    assert!(frame.sequence > target, "GETACK advances the offset");
}

// ============================================================================
// Primary stint boundaries: promotion freezes the APPLIED offset, demotion
// closes the window.
// ============================================================================

/// A handler over a caller-supplied identity, so a test can hold the node's
/// `live`/`applied` atomics and drive them the way a replica stream would.
#[cfg(test)]
fn stint_handler(
    dir: &std::path::Path,
    identity: crate::identity::ReplicationIdentity,
) -> crate::primary::PrimaryReplicationHandler {
    use crate::primary::PrimaryReplicationHandler;
    use crate::tracker::ReplicationTrackerImpl;
    use crate::{BacklogConfig, LagThresholdConfig};
    use std::sync::Arc;
    use std::time::Duration;

    let tracker = Arc::new(ReplicationTrackerImpl::new());
    PrimaryReplicationHandler::new(
        identity,
        dir.join("replication_state.json"),
        tracker,
        None,
        dir.to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        BacklogConfig {
            enabled: true,
            max_entries: 1000,
            max_bytes: 64 * 1024 * 1024,
            ttl_secs: 0,
        },
        0,
    )
}

/// The replica-side offset pair over a node identity: what `streaming.rs` and
/// `consume_frames` drive in production.
#[cfg(test)]
fn replica_heads(
    identity: &crate::identity::ReplicationIdentity,
) -> (
    crate::replica::offset::ReplicaOffset,
    crate::replica::AppliedOffset,
) {
    use crate::replica::offset::ReplicaOffset;
    let applied = identity.applied();
    (
        ReplicaOffset::new(identity.state(), identity.live(), applied.clone()),
        applied,
    )
}

#[cfg(test)]
fn frame_of(len: usize) -> crate::frame::ReplicationFrame {
    crate::frame::ReplicationFrame::new(0, Bytes::from(vec![b'x'; len]))
}

/// CRITICAL: the promotion boundary is the offset of data this node HOLDS.
///
/// The replica stream advances the received head at decode time and queues the
/// frame on a 10k-deep channel; a promotion that froze the received head would
/// claim history for frames the keyspace never saw. A sibling replica sitting at
/// that same received offset would then be granted `+CONTINUE` over a hole, with
/// contiguous offsets hiding the divergence forever.
#[tokio::test]
async fn promotion_freezes_the_window_at_the_applied_offset_not_the_received_head() {
    use crate::identity::ReplicationIdentity;
    use crate::primary::replay::ReplayDecision;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let identity = ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
    let (received, applied) = replica_heads(&identity);
    let inherited_id = identity.state().read().replication_id.clone();
    let handler = stint_handler(dir.path(), identity);

    // Two frames off the socket; only the first ever reaches the keyspace.
    let landed = frame_of(100);
    let queued = frame_of(40);
    received.frame_advance(&landed);
    received.frame_advance(&queued);
    applied.frame_applied(&landed);
    assert_eq!(received.current(), 140, "received head counts both frames");

    let (boundary, snapshot) = handler.begin_primary_stint().unwrap();

    assert_eq!(boundary, 100, "the window freezes at the applied offset");
    assert_eq!(
        snapshot.secondary_offset, 100,
        "the inherited id's window ends at the data this node holds"
    );
    assert_eq!(
        handler.current_offset(),
        100,
        "the new primary's stream head is rewound to its data, so its own \
         writes continue from there"
    );
    assert_eq!(handler.replay.backlog_start(), Some(100));

    // A downstream replica that read as far as the *received* head asks to
    // continue from 140 under the inherited id. It must be told to full resync:
    // this node never applied those 40 bytes.
    let state = handler.state();
    assert!(
        matches!(
            handler
                .replay
                .handle_partial_sync_request(&state, &inherited_id, 140, boundary),
            ReplayDecision::FullResync(_)
        ),
        "a replica ahead of the promoted node's data must NOT get +CONTINUE"
    );
    // At the applied boundary the same replica is exactly caught up and is
    // continuable with an empty tail — the point of arming the floor.
    assert!(matches!(
        handler
            .replay
            .handle_partial_sync_request(&state, &inherited_id, 100, boundary),
        ReplayDecision::Continue(_)
    ));
}

/// MAJOR: the backlog and its floor are scoped to a single primary stint.
///
/// A demoted node's buffered commands describe a history it no longer heads, and
/// the resync that follows can rewind its offset *below* them. Left in place,
/// the `fetch_max` floor could never follow the rewind down, so a re-promoted
/// node would claim a window it cannot serve.
#[tokio::test]
async fn a_re_promotion_at_a_lower_offset_re_arms_the_floor_from_scratch() {
    use crate::identity::ReplicationIdentity;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let identity = ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
    let applied = identity.applied();
    let handler = stint_handler(dir.path(), identity.clone());

    // Stint one: promote, then take writes as a primary.
    handler.begin_primary_stint().unwrap();
    push_write(&handler, "k1", "v1");
    let high = push_write(&handler, "k2", "v2");
    assert!(high > 0);
    assert!(
        handler.replay.oldest_offset().is_some(),
        "backlog populated"
    );

    // Demotion: the window closes and the buffered history goes with it.
    handler.end_primary_stint();
    assert_eq!(
        handler.replay.backlog_start(),
        None,
        "a node that follows someone else claims no replayable history"
    );
    assert_eq!(handler.replay.oldest_offset(), None, "backlog is empty");

    // The new primary full-resyncs it to an offset BELOW where it had been. The
    // demotion path opens a fresh applying stint before dialing (see
    // `RealReplicaStreamer::start`), which re-opens the gate the promotion
    // froze; the connection built under it then adopts the resync offset.
    let _stint = applied.begin_replica_stint();
    let (received, _applied) = replica_heads(&identity);
    assert!(received.reset_to(50));

    let (boundary, _) = handler.begin_primary_stint().unwrap();

    assert_eq!(
        boundary, 50,
        "the second stint starts at the resynced offset"
    );
    assert_eq!(
        handler.replay.backlog_start(),
        Some(50),
        "the floor follows the rewind down instead of staying at the old head"
    );
    assert_eq!(handler.replay.oldest_offset(), None);
}

/// MEDIUM: a promotion that cannot be written to disk must not happen.
///
/// Persisting is what makes the mint survive a restart; a node that reported a
/// successful promotion and then came back advertising the id it replaced would
/// hand `+CONTINUE` to replicas following the new one.
#[tokio::test]
async fn a_promotion_that_cannot_persist_leaves_the_identity_untouched() {
    use crate::identity::ReplicationIdentity;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    // A directory where the state file belongs: the atomic rename over it fails.
    std::fs::create_dir_all(dir.path().join("replication_state.json")).unwrap();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let identity = ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
    let inherited_id = identity.state().read().replication_id.clone();
    let handler = stint_handler(dir.path(), identity);

    handler
        .begin_primary_stint()
        .expect_err("an unwritable state file must abort the promotion");

    let state = handler.state();
    assert_eq!(
        state.replication_id, inherited_id,
        "the failed mint must be rolled back, not left half-applied"
    );
    assert_eq!(state.secondary_id, None, "no failover window was opened");
    assert_eq!(
        handler.replay.backlog_start(),
        None,
        "an un-promoted node must not claim history"
    );
}

/// A demotion drops the replicas that were following the ended stint — Redis's
/// `replicationSetMaster` → `disconnectSlaves`.
#[tokio::test]
async fn ending_a_stint_disconnects_downstream_replicas() {
    use crate::identity::ReplicationIdentity;
    use crate::replica_session::Phase;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let identity = ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
    let handler = stint_handler(dir.path(), identity);
    for addr in ["127.0.0.1:6380", "127.0.0.1:6381"] {
        let session = handler.tracker.register_replica(addr.parse().unwrap());
        session.force_phase_for_test(Phase::Streaming);
    }

    assert_eq!(
        handler.end_primary_stint(),
        2,
        "every registered session must be signalled to tear down"
    );
}

/// Freeing the backlog is a memory decision, not a history change: the buffered
/// commands go, and nothing else moves. A stint change (promotion, demotion)
/// rotates the replication id and freezes a failover window; the TTL must do
/// none of that, or a node that sat idle overnight would come back claiming a
/// different history than the one its data is on.
// FM-REPLICATION-009
#[test]
fn freeing_the_backlog_moves_no_offset_and_no_replication_id() {
    use crate::ReplicationBroadcaster;

    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());
    handler.begin_primary_stint().unwrap();
    push_write(&handler, "k1", "v1");
    let head = push_write(&handler, "k2", "v2");

    let before = handler.state();
    assert!(handler.replay.has_resume_history());

    // The TTL elapses with no replica attached.
    let t0 = std::time::Instant::now();
    assert!(handler.replay.backlog_ttl().secs() == 0);
    handler.replay.backlog_ttl().set_secs(60);
    assert!(!handler.replay.expire_backlog_if_idle(0, t0));
    assert!(
        handler
            .replay
            .expire_backlog_if_idle(0, t0 + std::time::Duration::from_secs(60))
    );

    let after = handler.state();
    assert_eq!(after.replication_id, before.replication_id);
    assert_eq!(after.secondary_id, before.secondary_id);
    assert_eq!(after.secondary_offset, before.secondary_offset);
    assert_eq!(handler.current_offset(), head, "the live offset stands");
    assert_eq!(handler.replication_id(), before.replication_id);
    // Only the resume window is gone.
    assert!(!handler.replay.has_resume_history());
    // …and with it the reason to keep buffering: a node with no window and no
    // replica is idle again, which is the whole point of the TTL. Nothing is
    // lost by that — the next PSYNC full-resyncs either way.
    assert!(!handler.is_active());
}
