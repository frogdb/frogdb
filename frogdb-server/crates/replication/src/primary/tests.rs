use crate::primary::ring_buffer::ReplicationRingBuffer;
use crate::replica_session::create_minimal_rdb;
use bytes::Bytes;

/// Build a primary handler with an enabled split-brain backlog for the
/// divergence-record tests. No I/O beyond a temp state path; the handler's own
/// `offsets` coordinator + `replay` backlog are the only inputs exercised.
#[cfg(test)]
fn divergence_handler(dir: &std::path::Path) -> crate::primary::PrimaryReplicationHandler {
    use crate::primary::PrimaryReplicationHandler;
    use crate::state::ReplicationState;
    use crate::tracker::ReplicationTrackerImpl;
    use crate::{LagThresholdConfig, SplitBrainBufferConfig};
    use std::sync::Arc;
    use std::time::Duration;

    PrimaryReplicationHandler::new(
        ReplicationState::new(),
        dir.join("replication_state.json"),
        Arc::new(ReplicationTrackerImpl::new()),
        None,
        dir.to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        SplitBrainBufferConfig {
            enabled: true,
            max_entries: 1000,
            max_bytes: 64 * 1024 * 1024,
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

#[test]
fn test_create_minimal_rdb() {
    let rdb = create_minimal_rdb();
    assert_eq!(&rdb[0..5], b"REDIS");
    assert_eq!(&rdb[5..9], b"0011");
    assert!(rdb.contains(&0xFF));
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
    use crate::{LagThresholdConfig, SplitBrainBufferConfig};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let state_path = dir.path().join("replication_state.json");
    let state = ReplicationState::new();
    let replid = state.replication_id.clone();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let handler = PrimaryReplicationHandler::new(
        state,
        state_path.clone(),
        tracker.clone(),
        None,
        dir.path().to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        SplitBrainBufferConfig {
            enabled: false,
            max_entries: 0,
            max_bytes: 0,
        },
        0,
    );

    // The offset on disk starts at 0 even though it was never explicitly saved.
    // Advance the tracker the way `broadcast_command` does, then persist.
    tracker.set_offset(987);
    handler.save_state().await.unwrap();

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
    use crate::{LagThresholdConfig, SplitBrainBufferConfig};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    let handler = PrimaryReplicationHandler::new(
        ReplicationState::new(),
        dir.path().join("replication_state.json"),
        tracker.clone(),
        None,
        dir.path().to_path_buf(),
        LagThresholdConfig {
            threshold_bytes: 0,
            threshold_secs: 0,
            cooldown: Duration::from_secs(0),
        },
        SplitBrainBufferConfig {
            enabled: false,
            max_entries: 0,
            max_bytes: 0,
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
            target,
            1,
            Some(Instant::now() + Duration::from_millis(30)),
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
