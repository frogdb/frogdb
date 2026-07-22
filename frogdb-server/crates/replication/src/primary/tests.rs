use crate::primary::ring_buffer::ReplicationRingBuffer;
use crate::replica_session::create_minimal_rdb;
use bytes::Bytes;

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
