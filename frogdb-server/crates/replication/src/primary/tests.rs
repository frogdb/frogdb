use super::parse_replconf_ack;
use crate::checkpoint_stream::create_minimal_rdb;
use crate::primary::ring_buffer::ReplicationRingBuffer;
use bytes::Bytes;

#[test]
fn test_create_minimal_rdb() {
    let rdb = create_minimal_rdb();
    assert_eq!(&rdb[0..5], b"REDIS");
    assert_eq!(&rdb[5..9], b"0011");
    assert!(rdb.contains(&0xFF));
}

#[test]
fn test_parse_replconf_ack() {
    let data = b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$5\r\n12345\r\n";
    assert_eq!(parse_replconf_ack(data), Some(12345));
    let data = b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$10\r\n1234567890\r\n";
    assert_eq!(parse_replconf_ack(data), Some(1234567890));
    let data = b"INVALID";
    assert_eq!(parse_replconf_ack(data), None);
}

#[test]
fn test_ring_buffer_push_and_extract() {
    let rb = ReplicationRingBuffer::new(100, 1024 * 1024);
    rb.push(10, Bytes::from("cmd1"));
    rb.push(20, Bytes::from("cmd2"));
    rb.push(30, Bytes::from("cmd3"));
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
    rb.push(10, Bytes::from("cmd1"));
    rb.push(20, Bytes::from("cmd2"));
    rb.push(30, Bytes::from("cmd3"));
    rb.push(40, Bytes::from("cmd4"));
    let writes = rb.extract_divergent_writes(0);
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0].0, 20);
    assert_eq!(writes[2].0, 40);
}

#[test]
fn test_ring_buffer_byte_limit_eviction() {
    let rb = ReplicationRingBuffer::new(100, 10);
    rb.push(10, Bytes::from("abcde"));
    rb.push(20, Bytes::from("fghij"));
    rb.push(30, Bytes::from("klmno"));
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
    rb.push(10, Bytes::from("cmd1"));
    let w1 = rb.extract_divergent_writes(0);
    let w2 = rb.extract_divergent_writes(0);
    assert_eq!(w1.len(), 1);
    assert_eq!(w2.len(), 1);
}
