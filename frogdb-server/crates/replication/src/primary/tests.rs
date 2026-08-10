use crate::primary::ring_buffer::{BacklogTruncated, ReplicationRingBuffer};
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
        crate::feed_gate::ReplicaFeedGate::open(),
    )
}

/// Broadcast one command through the primary path (advances the live offset AND
/// records into the backlog, exactly as production writes do) and return the new
/// live offset.
#[cfg(test)]
fn push_write(handler: &crate::primary::PrimaryReplicationHandler, key: &str, val: &str) -> u64 {
    handler.broadcast_control_command(
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
    let tail = handler
        .replay
        .extract_backlog(before_drop, after_drop)
        .expect("the resume point is still inside the window");
    assert_eq!(
        tail.len(),
        1,
        "the write made with no replica connected must be replayable, got {tail:?}"
    );
}

// FM-REPLICATION-024
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

// FM-REPLICATION-024
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

// FM-REPLICATION-024
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

// FM-REPLICATION-024
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

// FM-REPLICATION-016
#[test]
fn test_ring_buffer_push_and_extract() {
    // Offsets advance by exactly the pushed payload's length, because the one
    // caller (`broadcast_tagged`) advances the offset with the same bytes it
    // records. A fixture that skips bytes describes a ring with holes in it,
    // which `INV-BACKLOG-1` (rightly) reports as a defect.
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.push(4, 0, Bytes::from("cmd1"));
    rb.push(8, 0, Bytes::from("cmd2"));
    rb.push(12, 0, Bytes::from("cmd3"));
    let writes = rb.extract_divergent_writes(0);
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0], (4, Bytes::from("cmd1")));
    assert_eq!(writes[1], (8, Bytes::from("cmd2")));
    assert_eq!(writes[2], (12, Bytes::from("cmd3")));
    let writes = rb.extract_divergent_writes(8);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0], (12, Bytes::from("cmd3")));
    let writes = rb.extract_divergent_writes(12);
    assert!(writes.is_empty());
}

// FM-REPLICATION-016
#[test]
fn test_ring_buffer_entry_limit_eviction() {
    let rb = ReplicationRingBuffer::new(3, 1024 * 1024);
    rb.push(4, 0, Bytes::from("cmd1"));
    rb.push(8, 0, Bytes::from("cmd2"));
    rb.push(12, 0, Bytes::from("cmd3"));
    rb.push(16, 0, Bytes::from("cmd4"));
    let writes = rb.extract_divergent_writes(0);
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0].0, 8);
    assert_eq!(writes[2].0, 16);
}

// FM-REPLICATION-016
#[test]
fn test_ring_buffer_byte_limit_eviction() {
    let rb = ReplicationRingBuffer::new(100, 10);
    rb.push(5, 0, Bytes::from("abcde"));
    rb.push(10, 0, Bytes::from("fghij"));
    rb.push(15, 0, Bytes::from("klmno"));
    let writes = rb.extract_divergent_writes(0);
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0].0, 10);
    assert_eq!(writes[1].0, 15);
}

// FM-REPLICATION-016
#[test]
fn test_ring_buffer_empty() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    let writes = rb.extract_divergent_writes(0);
    assert!(writes.is_empty());
}

// FM-REPLICATION-047
/// A degenerate cap must bound the buffer, never wedge the writer.
///
/// `push` holds the entries lock across its eviction loop, so a loop that
/// cannot terminate does not merely lose the backlog — it parks every later
/// write behind a mutex that is never released. `max_entries == 0` used to do
/// exactly that: `entries.len() >= 0` is true on an empty deque, `pop_front()`
/// returns `None`, and the body was a no-op forever.
///
/// Validation now refuses `0` from a config file, but the loop is the
/// load-bearing half: it is the only guard on the callers that build a
/// `BacklogConfig` directly (tests, and any future in-process construction).
/// Each case runs on its own thread behind a `recv_timeout` so a regression
/// fails this test rather than wedging the whole suite.
#[test]
fn ring_buffer_push_terminates_under_a_degenerate_cap() {
    for (label, max_entries, max_bytes) in [
        ("max_entries = 0", 0usize, 1024usize),
        ("max_bytes = 0", 4, 0),
        ("both caps 0", 0, 0),
    ] {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rb = ReplicationRingBuffer::new(max_entries, max_bytes);
            rb.push(4, 0, Bytes::from("cmd1"));
            rb.push(8, 0, Bytes::from("cmd2"));
            let _ = tx.send(rb.extract_divergent_writes(0));
        });
        let retained = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap_or_else(|_| {
                panic!(
                    "{label}: push never returned — the eviction loop cannot drain an empty deque"
                )
            });
        // The empty-deque guard is what stops the loop, so the newest entry
        // always survives: the buffer is bounded at one, not emptied and not
        // grown. Zero retained would mean `push` had become a no-op.
        assert_eq!(
            retained.len(),
            1,
            "{label}: a degenerate cap must retain exactly the newest command"
        );
        assert_eq!(
            retained[0].0, 8,
            "{label}: the survivor is the newest entry"
        );
    }
}

// FM-REPLICATION-015
#[test]
fn test_ring_buffer_extract_is_nondestructive() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.push(10, 0, Bytes::from("cmd1"));
    let w1 = rb.extract_divergent_writes(0);
    let w2 = rb.extract_divergent_writes(0);
    assert_eq!(w1.len(), 1);
    assert_eq!(w2.len(), 1);
}

// FM-REPLICATION-014
#[test]
fn ring_buffer_reset_closes_the_window_and_lets_the_floor_move_down() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.arm_start(1000);
    rb.push(1004, 0, Bytes::from("cmd1"));
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

// FM-REPLICATION-014
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

// FM-REPLICATION-059
/// Constructing the handler publishes its ring to the tracker, so the object
/// both INFO renderers reach can answer for the backlog without a route to the
/// handler. Live, not copied: a write moves the reported window.
#[test]
fn the_handler_publishes_its_backlog_to_the_tracker() {
    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());

    let published = handler.tracker.backlog_geometry();
    assert_eq!(
        published.size_bytes,
        64 * 1024 * 1024,
        "the tracker must report the cap the handler's ring was built with, \
         not a default"
    );
    assert!(!published.active, "nothing armed it yet");

    let offset = push_write(&handler, "k", "v");
    let after = handler.tracker.backlog_geometry();
    assert!(after.active, "the write opened the window");
    assert_eq!(
        after.first_byte_offset + after.histlen,
        offset,
        "first_byte_offset + histlen must land on the live head"
    );
}

// FM-REPLICATION-059
/// The geometry INFO renders is read off the ring, not off a default: the byte
/// cap is the one the ring was built with, and the reported floor is the floor
/// `extract_backlog` refuses below.
#[test]
fn backlog_geometry_reports_the_configured_cap_and_the_armed_floor() {
    let rb = ReplicationRingBuffer::new(100, 4096);
    let unarmed = rb.geometry(0);
    assert!(!unarmed.active, "an unarmed ring claims no window");
    assert_eq!(unarmed.first_byte_offset, 0);
    assert_eq!(unarmed.histlen, 0);
    assert_eq!(
        unarmed.size_bytes, 4096,
        "capacity is reported even with no window open — it is the number an \
         operator tuned"
    );

    rb.arm_start(1000);
    let armed = rb.geometry(1500);
    assert!(armed.active);
    assert_eq!(armed.first_byte_offset, 1000);
    assert_eq!(
        armed.histlen, 500,
        "first_byte_offset + histlen == the head"
    );
    assert_eq!(armed.size_bytes, 4096);
}

// FM-REPLICATION-059
/// Eviction raises the reported first byte offset, because it raises the one
/// floor there is (FM-REPLICATION-014). The pre-fix render printed `0` here,
/// which claims the backlog can serve from the beginning of history — exactly
/// the claim the floor exists to deny.
#[test]
fn backlog_geometry_first_byte_offset_follows_eviction() {
    let rb = ReplicationRingBuffer::new(2, 1024 * 1024);
    rb.push(10, 0, Bytes::from("0123456789"));
    assert_eq!(rb.geometry(10).first_byte_offset, 0);

    rb.push(20, 0, Bytes::from("0123456789"));
    rb.push(30, 0, Bytes::from("0123456789"));

    let geometry = rb.geometry(30);
    assert_eq!(
        geometry.first_byte_offset,
        rb.start_offset().expect("armed"),
        "INFO must report the same floor a `+CONTINUE` is judged against"
    );
    assert_eq!(geometry.first_byte_offset, 10);
    assert_eq!(geometry.histlen, 20);
}

// FM-REPLICATION-059
/// A reset closes the window, so the geometry stops claiming one — while still
/// reporting the capacity, which is config and did not change.
#[test]
fn backlog_geometry_after_a_reset_claims_no_window() {
    let rb = ReplicationRingBuffer::new(100, 8192);
    rb.arm_start(1000);
    rb.push(1004, 0, Bytes::from("cmd1"));
    assert!(rb.geometry(1004).active);

    rb.reset();

    let geometry = rb.geometry(1004);
    assert!(!geometry.active);
    assert_eq!(geometry.first_byte_offset, 0);
    assert_eq!(geometry.histlen, 0);
    assert_eq!(geometry.size_bytes, 8192);
}

// FM-REPLICATION-059
/// A floor armed above the live offset (a promotion that armed from a recovered
/// position before the counter caught up) reports an empty window, never a
/// window longer than the stream.
#[test]
fn backlog_geometry_histlen_never_underflows() {
    let rb = ReplicationRingBuffer::new(100, 1024);
    rb.arm_start(5000);
    assert_eq!(rb.geometry(10).histlen, 0);
}

// FM-REPLICATION-016
#[test]
fn test_ring_buffer_oldest_offset_tracks_eviction() {
    let rb = ReplicationRingBuffer::new(3, 1024 * 1024);
    assert_eq!(rb.oldest_offset(), None);
    rb.push(4, 0, Bytes::from("cmd1"));
    assert_eq!(rb.oldest_offset(), Some(4));
    rb.push(8, 0, Bytes::from("cmd2"));
    rb.push(12, 0, Bytes::from("cmd3"));
    assert_eq!(rb.oldest_offset(), Some(4));
    // Eviction raises the oldest retained offset (Redis repl_backlog_off).
    rb.push(16, 0, Bytes::from("cmd4"));
    assert_eq!(rb.oldest_offset(), Some(8));
}

// FM-REPLICATION-015
#[test]
fn test_ring_buffer_extract_backlog_is_contiguous_and_bounded() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    // Arm at 0 so the whole range is inside the window; without this the first
    // push opens the window at its own *start* offset (4 - len("cmd1")).
    rb.arm_start(0);
    rb.push(4, 0, Bytes::from("cmd1"));
    rb.push(8, 0, Bytes::from("cmd2"));
    rb.push(12, 0, Bytes::from("cmd3"));
    rb.push(16, 0, Bytes::from("cmd4"));
    // (start, end] — exclusive lower, inclusive upper.
    let tail = rb.extract_backlog(4, 12).expect("inside the window");
    assert_eq!(
        tail,
        vec![(8, 0, Bytes::from("cmd2")), (12, 0, Bytes::from("cmd3"))]
    );
    // start == end yields an empty (caught-up) tail.
    assert!(
        rb.extract_backlog(16, 16)
            .expect("inside the window")
            .is_empty()
    );
    // Whole tail above start.
    let all = rb.extract_backlog(0, 16).expect("inside the window");
    assert_eq!(all.len(), 4);
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
}

/// The eviction check is on the extraction, not only on the grant: a resume
/// point the window no longer covers is an error, never a shorter vector.
///
/// Pre-fix this returned the three retained entries and dropped `(0, 10]` on the
/// floor — the caller could not tell that apart from "the replica was already
/// caught up on that range".
// FM-REPLICATION-012
#[test]
fn an_evicted_resume_point_is_refused_not_truncated() {
    let rb = ReplicationRingBuffer::new(3, 1024 * 1024);
    rb.arm_start(0);
    rb.push(4, 0, Bytes::from("cmd1"));
    rb.push(8, 0, Bytes::from("cmd2"));
    rb.push(12, 0, Bytes::from("cmd3"));
    // The fourth push evicts `cmd1` and raises the floor to where it ended.
    rb.push(16, 0, Bytes::from("cmd4"));
    assert_eq!(rb.start_offset(), Some(4));

    assert_eq!(
        rb.extract_backlog(0, 16),
        Err(BacklogTruncated {
            requested: 0,
            floor: Some(4)
        }),
        "a resume below the floor must be refused, not served short"
    );
    // The boundary is inclusive: a replica sitting exactly on the floor is the
    // lowest resume the window can still serve.
    let tail = rb
        .extract_backlog(4, 16)
        .expect("floor == start is servable");
    assert_eq!(
        tail.iter().map(|(o, _, _)| *o).collect::<Vec<_>>(),
        vec![8, 12, 16]
    );
}

/// A window that was closed (a TTL free, a stint boundary) refuses every
/// extraction rather than answering with the empty tail of a caught-up replica.
// FM-REPLICATION-012
#[test]
fn a_closed_window_refuses_every_extraction() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.arm_start(0);
    rb.push(4, 0, Bytes::from("cmd1"));
    rb.reset();

    for requested in [0, 2, 3] {
        assert_eq!(
            rb.extract_backlog(requested, 4),
            Err(BacklogTruncated {
                requested,
                floor: None
            }),
            "an unarmed window claims no history at all"
        );
    }
    // ... except for the empty range, which needs no history to serve.
    assert_eq!(rb.extract_backlog(4, 4), Ok(Vec::new()));
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
        crate::feed_gate::ReplicaFeedGate::open(),
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
        crate::feed_gate::ReplicaFeedGate::open(),
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
        crate::feed_gate::ReplicaFeedGate::open(),
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

// FM-REPLICATION-019
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

// FM-REPLICATION-019
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

// FM-REPLICATION-020
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

// FM-REPLICATION-022
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

// ============================================================================
// The handler's wiring seams: the accessors callers reach the live state
// through, and the setters that install the hooks it runs.
// ============================================================================

/// The hooks are how this crate reaches state it does not own (the function
/// library lives in the server crate), so a setter that dropped its argument —
/// or a getter that answered `None` — would silently ship a full-syncing replica
/// a keyspace with no libraries behind it.
#[test]
fn the_function_snapshot_hook_is_installed_and_handed_back_callable() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());

    assert!(
        handler.function_snapshot_hook().is_none(),
        "nothing is shipped until the owner of the registry wires it"
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    handler.set_function_snapshot_hook(Arc::new(move |_handler| {
        counter.fetch_add(1, Ordering::SeqCst);
    }));

    let hook = handler
        .function_snapshot_hook()
        .expect("the installed hook must be handed back to the full-sync path");
    hook(&handler);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the hook that ran is ours");

    // Re-installing supersedes rather than stacks.
    let second = Arc::new(AtomicUsize::new(0));
    let counter = second.clone();
    handler.set_function_snapshot_hook(Arc::new(move |_handler| {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    handler.function_snapshot_hook().unwrap()(&handler);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the first hook is gone");
    assert_eq!(second.load(Ordering::SeqCst), 1);
}

/// `tracker()` vends the handler's own registry, not a fresh one: WAIT counting,
/// INFO's `connected_slaves` and the lag monitor all read the sessions this
/// handler registered.
#[test]
fn tracker_vends_the_handlers_own_session_registry() {
    use crate::replica_session::Phase;
    use frogdb_types::ReplicationTracker;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());
    let vended = handler.tracker();
    assert!(
        Arc::ptr_eq(&vended, &handler.tracker),
        "the vended handle must be the registry the handler registers into"
    );

    let session = handler
        .tracker
        .register_replica("127.0.0.1:6380".parse().unwrap());
    session.force_phase_for_test(Phase::Streaming);
    assert_eq!(
        vended.replica_count(),
        1,
        "a replica registered on the handler must be visible through the vended handle"
    );
}

/// `CONFIG SET replication-lag-threshold-*` retunes the live thresholds every
/// streaming session reads; a setter that dropped its argument would leave the
/// operator's new value invisible while `CONFIG GET` reported it applied.
#[test]
fn setting_a_lag_threshold_retunes_the_live_thresholds() {
    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());
    let live = handler.lag_thresholds();
    assert_eq!(live.threshold_bytes(), 0, "seeded from the config");
    assert_eq!(live.threshold_secs(), 0);

    handler.set_lag_threshold_bytes(8 * 1024 * 1024);
    handler.set_lag_threshold_secs(45);

    // Read through the handle `ConfigManager` holds, which is the same object
    // the sessions consult — not a copy taken at construction.
    assert_eq!(live.threshold_bytes(), 8 * 1024 * 1024);
    assert_eq!(live.threshold_secs(), 45);
    assert_eq!(handler.lag_thresholds().threshold_bytes(), 8 * 1024 * 1024);
}

/// One tick of the backlog TTL, driven through the production entry point the
/// maintenance task calls (which reads the clock itself). It must answer `false`
/// while the window is still owed its TTL and `true` on the tick that frees it —
/// the caller logs the transition off that boolean, so a stuck answer either
/// hides the free or reports one on every tick forever.
#[test]
fn one_ttl_tick_reports_only_the_tick_that_freed_the_backlog() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());
    handler.begin_primary_stint().unwrap();
    push_write(&handler, "k1", "v1");
    assert!(handler.replay.has_resume_history());

    handler.replay.backlog_ttl().set_secs(60);
    assert!(
        !handler.expire_idle_backlog(),
        "the first idle tick only starts the clock"
    );
    assert!(handler.replay.has_resume_history(), "nothing freed yet");

    handler
        .replay
        .backlog_ttl()
        .backdate_idle_clock_for_test(Duration::from_secs(61));
    assert!(
        handler.expire_idle_backlog(),
        "the tick that finds the TTL elapsed with no replica must report the free"
    );
    assert!(!handler.replay.has_resume_history(), "the window closed");

    assert!(
        !handler.expire_idle_backlog(),
        "a freed backlog has nothing left to free"
    );
}

/// `replica_count()` is what INFO's `connected_slaves` and the TTL tick read.
#[test]
fn replica_count_reports_every_registered_session() {
    use crate::replica_session::Phase;

    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());
    assert_eq!(handler.replica_count(), 0);

    let mut ids = Vec::new();
    for addr in ["127.0.0.1:6380", "127.0.0.1:6381", "127.0.0.1:6382"] {
        let session = handler.tracker.register_replica(addr.parse().unwrap());
        session.force_phase_for_test(Phase::Streaming);
        ids.push(session.id());
    }
    assert_eq!(handler.replica_count(), 3);

    handler.tracker.unregister_replica(ids[0]);
    assert_eq!(handler.replica_count(), 2);
}

/// INFO's `master_replid` reads the handler's state through `shared_state()`; a
/// handle onto a *different* state would report an identity this node does not
/// have — the exact confusion a promotion's mint exists to avoid.
#[test]
fn shared_state_is_the_handlers_live_state_not_a_copy() {
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());
    let shared = handler.shared_state();
    assert!(Arc::ptr_eq(&shared, &handler.state));
    assert_eq!(shared.read().replication_id, handler.replication_id());

    // A promotion mints a new id under the handler's own lock; the shared handle
    // must observe it.
    handler.begin_primary_stint().unwrap();
    assert_eq!(shared.read().replication_id, handler.replication_id());
    assert!(
        shared.read().secondary_id.is_some(),
        "the failover window the mint opened is visible through the shared handle"
    );
}

/// The cluster bus's HealthProbe answers off `shared_offset()`. A handle that was
/// not the atomic the advance gate writes would advertise a frozen zero, and the
/// failure detector would judge this node's progress on it.
#[test]
fn shared_offset_tracks_the_gate_the_primary_advances() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());
    let probe = handler.shared_offset();
    assert_eq!(probe.load(Ordering::Acquire), 0);

    let head = push_write(&handler, "k", "v");
    assert!(head > 1, "the write advances past a constant answer");
    assert_eq!(probe.load(Ordering::Acquire), head);
    assert_eq!(probe.load(Ordering::Acquire), handler.current_offset());
    assert!(
        Arc::ptr_eq(&probe, &handler.shared_offset()),
        "there is one atomic, vended twice"
    );
}

/// The `ReplicationBroadcaster` impl is what the command path calls for every
/// write. Its return value is the offset the write landed at — WAIT blocks on
/// exactly that number — and `current_offset` is the head WAIT compares against.
#[test]
fn the_broadcaster_impl_returns_the_offset_the_write_landed_at() {
    use crate::ReplicationBroadcaster;
    use crate::frame::serialize_command_to_resp;

    let dir = tempfile::tempdir().unwrap();
    let handler = divergence_handler(dir.path());
    let mut frames = handler.wal_broadcast.subscribe();
    assert_eq!(ReplicationBroadcaster::current_offset(&handler), 0);

    let args = [Bytes::from_static(b"k"), Bytes::from_static(b"v")];
    let expected = serialize_command_to_resp("SET", &args).len() as u64;
    let landed = handler.broadcast_command_on_shard(7, "SET", &args);

    assert_eq!(
        landed, expected,
        "the returned offset is the stream position after this command"
    );
    assert_eq!(ReplicationBroadcaster::current_offset(&handler), landed);
    assert_eq!(handler.current_offset(), landed);

    // The write really went out, tagged with the shard it executed on and
    // stamped at the offset that was returned.
    let frame = frames.try_recv().expect("the write must be broadcast");
    assert_eq!(frame.sequence, landed);
    assert_eq!(frame.shard_id, 7);

    // A second write advances again, so the number is a running head rather than
    // a constant.
    let second = handler.broadcast_command_on_shard(7, "SET", &args);
    assert_eq!(second, landed + expected);
    assert_eq!(ReplicationBroadcaster::current_offset(&handler), second);
}

/// A promotion persists the boundary it froze: the offset the node vouches for
/// must survive a restart, or the reboot resumes below its own backlog floor.
/// Monotone — a boundary below an already-persisted save point never rewinds it.
#[tokio::test]
async fn a_promotion_persists_its_boundary_without_ever_rewinding_it() {
    use crate::identity::ReplicationIdentity;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let identity = ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
    let (received, applied) = replica_heads(&identity);
    let handler = stint_handler(dir.path(), identity);

    let landed = frame_of(900);
    received.frame_advance(&landed);
    applied.frame_applied(&landed);

    let (boundary, snapshot) = handler.begin_primary_stint().unwrap();
    assert_eq!(boundary, 900);
    assert_eq!(
        snapshot.offset_at_save, 900,
        "the promotion boundary is what a restart resumes from"
    );
    let reloaded =
        ReplicationState::load_or_create(&dir.path().join("replication_state.json")).unwrap();
    assert_eq!(reloaded.offset_at_save, 900, "and it is on disk");

    // A later stint whose boundary sits *below* the persisted save point must not
    // drag it back down: the file describes data this node still holds.
    handler.end_primary_stint();
    // A save point above the boundary the next stint settles at: what a node
    // that ran to 5000, followed a shorter history, and is now promoted again
    // holds. INV-OFFSET-2 reports it as a violation and is tiered as a
    // documented exception for exactly this state — see issue 17.
    handler.state.write().offset_at_save = 5_000;
    let (second_boundary, snapshot) = handler.begin_primary_stint().unwrap();
    assert_eq!(second_boundary, 900, "the applied head has not moved");
    assert_eq!(
        snapshot.offset_at_save, 5_000,
        "a lower boundary must never rewind the persisted offset"
    );
}
