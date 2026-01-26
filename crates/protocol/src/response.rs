//! Response types for RESP2/RESP3.

use bytes::Bytes;
use bytes_utils::Str;
use redis_protocol::resp2::types::BytesFrame as Resp2BytesFrame;
use redis_protocol::resp3::types::BytesFrame as Resp3BytesFrame;

/// Re-export RESP2 frame type for backwards compatibility.
pub type BytesFrame = Resp2BytesFrame;

/// Direction for list operations (BLPOP, BRPOP, BLMOVE, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Pop/push from the left (front).
    Left,
    /// Pop/push from the right (back).
    Right,
}

impl Direction {
    /// Parse a direction from a byte slice.
    pub fn parse(arg: &[u8]) -> Option<Self> {
        if arg.eq_ignore_ascii_case(b"LEFT") {
            Some(Direction::Left)
        } else if arg.eq_ignore_ascii_case(b"RIGHT") {
            Some(Direction::Right)
        } else {
            None
        }
    }
}

/// Blocking operation type for responses.
///
/// This is a simplified version for use in Response::BlockingNeeded.
/// The connection handler converts this to the full BlockingOp in frogdb_core.
#[derive(Debug, Clone, PartialEq)]
pub enum BlockingOp {
    /// BLPOP operation.
    BLPop,
    /// BRPOP operation.
    BRPop,
    /// BLMOVE operation.
    BLMove {
        /// Destination key.
        dest: Bytes,
        /// Source direction (where to pop from).
        src_dir: Direction,
        /// Destination direction (where to push to).
        dest_dir: Direction,
    },
    /// BLMPOP operation.
    BLMPop {
        /// Pop direction.
        direction: Direction,
        /// Number of elements to pop.
        count: usize,
    },
    /// BZPOPMIN operation.
    BZPopMin,
    /// BZPOPMAX operation.
    BZPopMax,
    /// BZMPOP operation.
    BZMPop {
        /// Whether to pop minimum (true) or maximum (false).
        min: bool,
        /// Number of elements to pop.
        count: usize,
    },
    /// XREAD blocking operation.
    XRead {
        /// Stream IDs to read after (ms, seq) tuples - resolved from $ at block time.
        after_ids: Vec<(u64, u64)>,
        /// Maximum entries per stream.
        count: Option<usize>,
    },
    /// XREADGROUP blocking operation.
    XReadGroup {
        /// Consumer group name.
        group: bytes::Bytes,
        /// Consumer name.
        consumer: bytes::Bytes,
        /// Skip PEL updates (NOACK flag).
        noack: bool,
        /// Maximum entries to return.
        count: Option<usize>,
    },
    /// WAIT command - wait for replica acknowledgments.
    Wait {
        /// Number of replicas that must acknowledge.
        num_replicas: u32,
        /// Timeout in milliseconds (0 = block forever).
        timeout_ms: u64,
    },
}

/// Raft cluster operation types for Response::RaftNeeded.
///
/// This is a serializable representation of cluster commands that lives in the
/// protocol crate (which cannot depend on core). The connection handler converts
/// these to the appropriate core `ClusterCommand` types.
#[derive(Debug, Clone, PartialEq)]
pub enum RaftClusterOp {
    /// Add a node to the cluster.
    AddNode {
        /// Node ID.
        node_id: u64,
        /// Client-facing address (ip:port).
        addr: std::net::SocketAddr,
        /// Cluster bus address (ip:cluster_port).
        cluster_addr: std::net::SocketAddr,
    },
    /// Remove a node from the cluster.
    RemoveNode {
        /// Node ID to remove.
        node_id: u64,
    },
    /// Assign slots to a node.
    AssignSlots {
        /// Target node ID.
        node_id: u64,
        /// Slot numbers to assign.
        slots: Vec<u16>,
    },
    /// Remove slot assignments from a node.
    RemoveSlots {
        /// Target node ID.
        node_id: u64,
        /// Slot numbers to remove.
        slots: Vec<u16>,
    },
    /// Set a node's role.
    SetRole {
        /// Target node ID.
        node_id: u64,
        /// Whether the node is a replica (true) or primary (false).
        is_replica: bool,
        /// Primary ID if this is a replica.
        primary_id: Option<u64>,
    },
    /// Begin slot migration.
    BeginSlotMigration {
        /// Slot being migrated.
        slot: u16,
        /// Source node ID.
        source_node: u64,
        /// Target node ID.
        target_node: u64,
    },
    /// Complete slot migration.
    CompleteSlotMigration {
        /// Slot being migrated.
        slot: u16,
        /// Source node ID.
        source_node: u64,
        /// Target node ID.
        target_node: u64,
    },
    /// Cancel slot migration.
    CancelSlotMigration {
        /// Slot whose migration to cancel.
        slot: u16,
    },
    /// Increment the config epoch.
    IncrementEpoch,
}

/// Response types that can be sent to clients.
///
/// Includes both RESP2 types (fully implemented) and RESP3 types
/// (defined for future use).
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    // === RESP2 Types (Implemented) ===
    /// Simple string (+OK\r\n)
    Simple(Bytes),

    /// Error (-ERR message\r\n)
    Error(Bytes),

    /// Integer (:1000\r\n)
    Integer(i64),

    /// Bulk string ($5\r\nhello\r\n) or null ($-1\r\n)
    Bulk(Option<Bytes>),

    /// Array (*2\r\n...)
    Array(Vec<Response>),

    // === RESP3 Types ===
    /// Null (_\r\n)
    Null,

    /// Double (,3.14159\r\n)
    Double(f64),

    /// Boolean (#t\r\n or #f\r\n)
    Boolean(bool),

    /// Blob error (!<len>\r\n<bytes>\r\n)
    BlobError(Bytes),

    /// Verbatim string (=<len>\r\n<fmt>:<data>\r\n)
    VerbatimString {
        format: [u8; 3],
        data: Bytes,
    },

    /// Map (%<count>\r\n<key><value>...)
    Map(Vec<(Response, Response)>),

    /// Set (~<count>\r\n<elements>...)
    Set(Vec<Response>),

    /// Attribute (|<count>\r\n<attr-map><data>)
    Attribute(Box<Response>),

    /// Push (><count>\r\n<elements>...)
    Push(Vec<Response>),

    /// Big number ((<big-integer>\r\n)
    BigNumber(Bytes),

    // === Internal Types (Not Wire-Serialized) ===
    /// Signal that a blocking command needs to wait for data.
    /// This is intercepted by the connection handler and never sent on the wire.
    BlockingNeeded {
        /// Keys to wait on.
        keys: Vec<Bytes>,
        /// Timeout in seconds (0 = block forever).
        timeout: f64,
        /// The blocking operation to perform when data arrives.
        op: BlockingOp,
    },

    /// Signal that a Raft cluster command needs to be executed.
    /// This is intercepted by the connection handler and never sent on the wire.
    RaftNeeded {
        /// The Raft cluster operation to execute.
        op: RaftClusterOp,
        /// For AddNode: register in NetworkFactory after commit.
        register_node: Option<(u64, std::net::SocketAddr)>,
        /// For RemoveNode: unregister from NetworkFactory after commit.
        unregister_node: Option<u64>,
    },

    /// Signal that a MIGRATE command needs to be executed.
    /// This is intercepted by the connection handler and never sent on the wire.
    MigrateNeeded {
        /// The raw arguments for the MIGRATE command.
        args: Vec<Bytes>,
    },
}

impl Response {
    /// Create a simple "OK" response.
    pub fn ok() -> Self {
        Response::Simple(Bytes::from_static(b"OK"))
    }

    /// Create an error response.
    pub fn error(msg: impl Into<Bytes>) -> Self {
        Response::Error(msg.into())
    }

    /// Create a null bulk string response.
    pub fn null() -> Self {
        Response::Bulk(None)
    }

    /// Create a bulk string response.
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Response::Bulk(Some(data.into()))
    }

    /// Create a "PONG" response.
    pub fn pong() -> Self {
        Response::Simple(Bytes::from_static(b"PONG"))
    }

    /// Create a "QUEUED" response (for transactions).
    pub fn queued() -> Self {
        Response::Simple(Bytes::from_static(b"QUEUED"))
    }

    /// Convert to a RESP2 frame.
    ///
    /// RESP3-only types are converted to their RESP2 equivalents:
    /// - Map → flattened Array of alternating keys/values
    /// - Set → Array
    /// - Double → BulkString (formatted as string)
    /// - Boolean → Integer (1 or 0)
    /// - Null → Null bulk string
    /// - Push → Array
    pub fn to_resp2_frame(self) -> Resp2BytesFrame {
        match self {
            Response::Simple(s) => Resp2BytesFrame::SimpleString(s),
            Response::Error(e) => {
                Resp2BytesFrame::Error(Str::from_inner(e).expect("error messages must be valid UTF-8"))
            }
            Response::Integer(i) => Resp2BytesFrame::Integer(i),
            Response::Bulk(Some(b)) => Resp2BytesFrame::BulkString(b),
            Response::Bulk(None) => Resp2BytesFrame::Null,
            Response::Array(items) => {
                Resp2BytesFrame::Array(items.into_iter().map(|r| r.to_resp2_frame()).collect())
            }
            // RESP3 types with RESP2 fallbacks
            Response::Null => Resp2BytesFrame::Null,
            Response::Double(d) => Resp2BytesFrame::BulkString(Bytes::from(format_float(d))),
            Response::Boolean(b) => Resp2BytesFrame::Integer(if b { 1 } else { 0 }),
            Response::BlobError(e) => {
                // Convert blob error to simple error (truncate if needed)
                let msg = String::from_utf8_lossy(&e);
                Resp2BytesFrame::Error(Str::from_inner(Bytes::from(msg.into_owned())).expect("error must be valid UTF-8"))
            }
            Response::VerbatimString { data, .. } => {
                // Strip format prefix, return as bulk string
                Resp2BytesFrame::BulkString(data)
            }
            Response::Map(pairs) => {
                // Flatten map to array: [k1, v1, k2, v2, ...]
                let mut items = Vec::with_capacity(pairs.len() * 2);
                for (k, v) in pairs {
                    items.push(k.to_resp2_frame());
                    items.push(v.to_resp2_frame());
                }
                Resp2BytesFrame::Array(items)
            }
            Response::Set(items) => {
                // Convert set to array
                Resp2BytesFrame::Array(items.into_iter().map(|r| r.to_resp2_frame()).collect())
            }
            Response::Attribute(inner) => {
                // Just return the inner value (attributes are metadata)
                inner.to_resp2_frame()
            }
            Response::Push(items) => {
                // Convert push to array
                Resp2BytesFrame::Array(items.into_iter().map(|r| r.to_resp2_frame()).collect())
            }
            Response::BigNumber(n) => {
                // Convert big number to bulk string
                Resp2BytesFrame::BulkString(n)
            }
            // Internal types - should never reach serialization
            Response::BlockingNeeded { .. } => {
                panic!("BlockingNeeded response should be intercepted by connection handler")
            }
            Response::RaftNeeded { .. } => {
                panic!("RaftNeeded response should be intercepted by connection handler")
            }
            Response::MigrateNeeded { .. } => {
                panic!("MigrateNeeded response should be intercepted by connection handler")
            }
        }
    }

    /// Convert to a RESP3 frame.
    ///
    /// All types are encoded with their native RESP3 representations.
    pub fn to_resp3_frame(self) -> Resp3BytesFrame {
        match self {
            Response::Simple(s) => Resp3BytesFrame::SimpleString {
                data: s,
                attributes: None,
            },
            Response::Error(e) => Resp3BytesFrame::SimpleError {
                data: Str::from_inner(e).expect("error messages must be valid UTF-8"),
                attributes: None,
            },
            Response::Integer(i) => Resp3BytesFrame::Number {
                data: i,
                attributes: None,
            },
            Response::Bulk(Some(b)) => Resp3BytesFrame::BlobString {
                data: b,
                attributes: None,
            },
            Response::Bulk(None) => Resp3BytesFrame::Null,
            Response::Array(items) => Resp3BytesFrame::Array {
                data: items.into_iter().map(|r| r.to_resp3_frame()).collect(),
                attributes: None,
            },
            Response::Null => Resp3BytesFrame::Null,
            Response::Double(d) => Resp3BytesFrame::Double {
                data: d,
                attributes: None,
            },
            Response::Boolean(b) => Resp3BytesFrame::Boolean {
                data: b,
                attributes: None,
            },
            Response::BlobError(e) => Resp3BytesFrame::BlobError {
                data: e,
                attributes: None,
            },
            Response::VerbatimString { format: _, data } => {
                // VerbatimString format is limited to txt/mkd in redis-protocol
                // We default to text format
                Resp3BytesFrame::VerbatimString {
                    data,
                    format: redis_protocol::resp3::types::VerbatimStringFormat::Text,
                    attributes: None,
                }
            }
            Response::Map(pairs) => Resp3BytesFrame::Map {
                data: pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_resp3_frame(), v.to_resp3_frame()))
                    .collect(),
                attributes: None,
            },
            Response::Set(items) => Resp3BytesFrame::Set {
                data: items.into_iter().map(|r| r.to_resp3_frame()).collect(),
                attributes: None,
            },
            Response::Attribute(inner) => {
                // Attributes in RESP3 are handled at the frame level
                // For now, just return the inner value
                inner.to_resp3_frame()
            }
            Response::Push(items) => Resp3BytesFrame::Push {
                data: items.into_iter().map(|r| r.to_resp3_frame()).collect(),
                attributes: None,
            },
            Response::BigNumber(n) => Resp3BytesFrame::BigNumber {
                data: n,
                attributes: None,
            },
            // Internal types - should never reach serialization
            Response::BlockingNeeded { .. } => {
                panic!("BlockingNeeded response should be intercepted by connection handler")
            }
            Response::RaftNeeded { .. } => {
                panic!("RaftNeeded response should be intercepted by connection handler")
            }
            Response::MigrateNeeded { .. } => {
                panic!("MigrateNeeded response should be intercepted by connection handler")
            }
        }
    }
}

/// Format a float for Redis compatibility.
fn format_float(f: f64) -> String {
    if f == f64::INFINITY {
        return "inf".to_string();
    }
    if f == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if f.is_nan() {
        return "nan".to_string();
    }
    if f == 0.0 {
        return "0".to_string();
    }

    if f.fract() == 0.0 && f.abs() < 1e15 {
        return format!("{:.0}", f);
    }

    let s = format!("{:.17}", f);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}

impl From<Response> for BytesFrame {
    fn from(response: Response) -> Self {
        response.to_resp2_frame()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_ok() {
        let resp = Response::ok();
        assert!(matches!(resp, Response::Simple(s) if s.as_ref() == b"OK"));
    }

    #[test]
    fn test_response_to_frame() {
        let resp = Response::Integer(42);
        let frame: BytesFrame = resp.into();
        assert!(matches!(frame, BytesFrame::Integer(42)));
    }

    #[test]
    fn test_response_null() {
        let resp = Response::null();
        let frame: BytesFrame = resp.into();
        assert!(matches!(frame, BytesFrame::Null));
    }

    // === RESP3 Encoding Tests ===

    #[test]
    fn test_map_to_resp3_frame() {
        let resp = Response::Map(vec![
            (
                Response::Bulk(Some(Bytes::from("field1"))),
                Response::Bulk(Some(Bytes::from("value1"))),
            ),
            (
                Response::Bulk(Some(Bytes::from("field2"))),
                Response::Integer(42),
            ),
        ]);
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::Map { data, attributes } => {
                assert_eq!(data.len(), 2);
                assert!(attributes.is_none());
            }
            _ => panic!("Expected Map frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_map_to_resp2_flattens() {
        let resp = Response::Map(vec![
            (
                Response::Bulk(Some(Bytes::from("field1"))),
                Response::Bulk(Some(Bytes::from("value1"))),
            ),
            (
                Response::Bulk(Some(Bytes::from("field2"))),
                Response::Integer(42),
            ),
        ]);
        let frame = resp.to_resp2_frame();
        match frame {
            Resp2BytesFrame::Array(items) => {
                // Map with 2 pairs should flatten to 4 elements
                assert_eq!(items.len(), 4);
                // Check first key
                assert!(matches!(&items[0], Resp2BytesFrame::BulkString(b) if b.as_ref() == b"field1"));
                // Check first value
                assert!(matches!(&items[1], Resp2BytesFrame::BulkString(b) if b.as_ref() == b"value1"));
                // Check second key
                assert!(matches!(&items[2], Resp2BytesFrame::BulkString(b) if b.as_ref() == b"field2"));
                // Check second value (integer)
                assert!(matches!(&items[3], Resp2BytesFrame::Integer(42)));
            }
            _ => panic!("Expected Array frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_set_to_resp3_frame() {
        let resp = Response::Set(vec![
            Response::Bulk(Some(Bytes::from("member1"))),
            Response::Bulk(Some(Bytes::from("member2"))),
            Response::Bulk(Some(Bytes::from("member3"))),
        ]);
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::Set { data, attributes } => {
                assert_eq!(data.len(), 3);
                assert!(attributes.is_none());
            }
            _ => panic!("Expected Set frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_set_to_resp2_array() {
        let resp = Response::Set(vec![
            Response::Bulk(Some(Bytes::from("member1"))),
            Response::Bulk(Some(Bytes::from("member2"))),
        ]);
        let frame = resp.to_resp2_frame();
        match frame {
            Resp2BytesFrame::Array(items) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected Array frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_double_to_resp3_frame() {
        let resp = Response::Double(3.14159);
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::Double { data, attributes } => {
                assert!((data - 3.14159).abs() < f64::EPSILON);
                assert!(attributes.is_none());
            }
            _ => panic!("Expected Double frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_double_to_resp2_string() {
        let resp = Response::Double(3.14159);
        let frame = resp.to_resp2_frame();
        match frame {
            Resp2BytesFrame::BulkString(data) => {
                let s = String::from_utf8(data.to_vec()).unwrap();
                // Should be a valid float string
                let parsed: f64 = s.parse().unwrap();
                assert!((parsed - 3.14159).abs() < 1e-10);
            }
            _ => panic!("Expected BulkString frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_double_special_values() {
        // Test infinity
        let resp = Response::Double(f64::INFINITY);
        let frame = resp.to_resp2_frame();
        match frame {
            Resp2BytesFrame::BulkString(data) => {
                assert_eq!(data.as_ref(), b"inf");
            }
            _ => panic!("Expected BulkString frame"),
        }

        // Test negative infinity
        let resp = Response::Double(f64::NEG_INFINITY);
        let frame = resp.to_resp2_frame();
        match frame {
            Resp2BytesFrame::BulkString(data) => {
                assert_eq!(data.as_ref(), b"-inf");
            }
            _ => panic!("Expected BulkString frame"),
        }

        // Test NaN
        let resp = Response::Double(f64::NAN);
        let frame = resp.to_resp2_frame();
        match frame {
            Resp2BytesFrame::BulkString(data) => {
                assert_eq!(data.as_ref(), b"nan");
            }
            _ => panic!("Expected BulkString frame"),
        }
    }

    #[test]
    fn test_push_to_resp3_frame() {
        let resp = Response::Push(vec![
            Response::Bulk(Some(Bytes::from("message"))),
            Response::Bulk(Some(Bytes::from("channel"))),
            Response::Bulk(Some(Bytes::from("payload"))),
        ]);
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::Push { data, attributes } => {
                assert_eq!(data.len(), 3);
                assert!(attributes.is_none());
            }
            _ => panic!("Expected Push frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_push_to_resp2_array() {
        let resp = Response::Push(vec![
            Response::Bulk(Some(Bytes::from("message"))),
            Response::Bulk(Some(Bytes::from("channel"))),
        ]);
        let frame = resp.to_resp2_frame();
        match frame {
            Resp2BytesFrame::Array(items) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected Array frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_boolean_to_resp3_frame() {
        // Test true
        let resp = Response::Boolean(true);
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::Boolean { data, attributes } => {
                assert!(data);
                assert!(attributes.is_none());
            }
            _ => panic!("Expected Boolean frame, got {:?}", frame),
        }

        // Test false
        let resp = Response::Boolean(false);
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::Boolean { data, attributes } => {
                assert!(!data);
                assert!(attributes.is_none());
            }
            _ => panic!("Expected Boolean frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_boolean_to_resp2_integer() {
        // True becomes 1
        let resp = Response::Boolean(true);
        let frame = resp.to_resp2_frame();
        assert!(matches!(frame, Resp2BytesFrame::Integer(1)));

        // False becomes 0
        let resp = Response::Boolean(false);
        let frame = resp.to_resp2_frame();
        assert!(matches!(frame, Resp2BytesFrame::Integer(0)));
    }

    #[test]
    fn test_null_to_resp3_frame() {
        let resp = Response::Null;
        let frame = resp.to_resp3_frame();
        assert!(matches!(frame, Resp3BytesFrame::Null));
    }

    #[test]
    fn test_null_to_resp2_frame() {
        let resp = Response::Null;
        let frame = resp.to_resp2_frame();
        assert!(matches!(frame, Resp2BytesFrame::Null));
    }

    #[test]
    fn test_blob_error_to_resp3_frame() {
        let resp = Response::BlobError(Bytes::from("ERR some long error message"));
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::BlobError { data, attributes } => {
                assert_eq!(data.as_ref(), b"ERR some long error message");
                assert!(attributes.is_none());
            }
            _ => panic!("Expected BlobError frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_big_number_to_resp3_frame() {
        let resp = Response::BigNumber(Bytes::from("3492890328409238509324850943850943825024385"));
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::BigNumber { data, attributes } => {
                assert_eq!(
                    data.as_ref(),
                    b"3492890328409238509324850943850943825024385"
                );
                assert!(attributes.is_none());
            }
            _ => panic!("Expected BigNumber frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_verbatim_string_to_resp3_frame() {
        let resp = Response::VerbatimString {
            format: *b"txt",
            data: Bytes::from("Some text content"),
        };
        let frame = resp.to_resp3_frame();
        match frame {
            Resp3BytesFrame::VerbatimString {
                data, attributes, ..
            } => {
                assert_eq!(data.as_ref(), b"Some text content");
                assert!(attributes.is_none());
            }
            _ => panic!("Expected VerbatimString frame, got {:?}", frame),
        }
    }
}
