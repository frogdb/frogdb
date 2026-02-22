//! Response types for RESP2/RESP3.
//!
//! This module provides a type-safe separation between wire-serializable responses
//! and internal control-flow responses:
//!
//! - [`WireResponse`] - Only contains types that can be serialized to RESP2/RESP3.
//!   The `to_resp2_frame()` and `to_resp3_frame()` methods CANNOT panic.
//!
//! - [`InternalAction`] - Control-flow signals that must be intercepted before
//!   serialization (blocking commands, Raft operations, migrations).
//!
//! - [`Response`] - Union type for command handlers that can return either wire
//!   responses or internal actions. Use `into_wire()` to safely extract.

use bytes::Bytes;
use bytes_utils::Str;
use redis_protocol::resp2::types::BytesFrame as Resp2BytesFrame;
use redis_protocol::resp3::types::BytesFrame as Resp3BytesFrame;

/// Re-export RESP2 frame type for backwards compatibility.
pub type BytesFrame = Resp2BytesFrame;

// =============================================================================
// Internal Action Types (non-wire control flow)
// =============================================================================

/// Internal action that must be handled by the connection layer.
///
/// These are control-flow signals returned by command handlers that require
/// special handling. They CANNOT be serialized to the wire protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum InternalAction {
    /// Signal that a blocking command needs to wait for data.
    BlockingNeeded {
        /// Keys to wait on.
        keys: Vec<Bytes>,
        /// Timeout in seconds (0 = block forever).
        timeout: f64,
        /// The blocking operation to perform when data arrives.
        op: BlockingOp,
    },

    /// Signal that a Raft cluster command needs to be executed.
    RaftNeeded {
        /// The Raft cluster operation to execute.
        op: Box<RaftClusterOp>,
        /// For AddNode: register in NetworkFactory after commit.
        register_node: Option<(u64, std::net::SocketAddr)>,
        /// For RemoveNode: unregister from NetworkFactory after commit.
        unregister_node: Option<u64>,
    },

    /// Signal that a MIGRATE command needs to be executed.
    MigrateNeeded {
        /// The raw arguments for the MIGRATE command.
        args: Vec<Bytes>,
    },
}

// =============================================================================
// Wire Response Type (safe to serialize)
// =============================================================================

/// Response types that can be safely serialized to the wire protocol.
///
/// This enum ONLY contains types that can be encoded as RESP2 or RESP3 frames.
/// The `to_resp2_frame()` and `to_resp3_frame()` methods will NEVER panic.
#[derive(Debug, Clone, PartialEq)]
pub enum WireResponse {
    // === RESP2 Types ===
    /// Simple string (+OK\r\n)
    Simple(Bytes),

    /// Error (-ERR message\r\n)
    Error(Bytes),

    /// Integer (:1000\r\n)
    Integer(i64),

    /// Bulk string ($5\r\nhello\r\n) or null ($-1\r\n)
    Bulk(Option<Bytes>),

    /// Array (*2\r\n...)
    Array(Vec<WireResponse>),

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
    VerbatimString { format: [u8; 3], data: Bytes },

    /// Map (%<count>\r\n<key><value>...)
    Map(Vec<(WireResponse, WireResponse)>),

    /// Set (~<count>\r\n<elements>...)
    Set(Vec<WireResponse>),

    /// Attribute (|<count>\r\n<attr-map><data>)
    Attribute(Box<WireResponse>),

    /// Push (><count>\r\n<elements>...)
    Push(Vec<WireResponse>),

    /// Big number ((<big-integer>\r\n)
    BigNumber(Bytes),
}

impl WireResponse {
    /// Create a simple "OK" response.
    pub fn ok() -> Self {
        WireResponse::Simple(Bytes::from_static(b"OK"))
    }

    /// Create an error response.
    pub fn error(msg: impl Into<Bytes>) -> Self {
        WireResponse::Error(msg.into())
    }

    /// Create a null bulk string response.
    pub fn null() -> Self {
        WireResponse::Bulk(None)
    }

    /// Create a bulk string response.
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        WireResponse::Bulk(Some(data.into()))
    }

    /// Create a "PONG" response.
    pub fn pong() -> Self {
        WireResponse::Simple(Bytes::from_static(b"PONG"))
    }

    /// Create a "QUEUED" response (for transactions).
    pub fn queued() -> Self {
        WireResponse::Simple(Bytes::from_static(b"QUEUED"))
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
    ///
    /// This method CANNOT panic - all variants are wire-serializable.
    pub fn to_resp2_frame(self) -> Resp2BytesFrame {
        match self {
            WireResponse::Simple(s) => Resp2BytesFrame::SimpleString(s),
            WireResponse::Error(e) => Resp2BytesFrame::Error(
                Str::from_inner(e).expect("error messages must be valid UTF-8"),
            ),
            WireResponse::Integer(i) => Resp2BytesFrame::Integer(i),
            WireResponse::Bulk(Some(b)) => Resp2BytesFrame::BulkString(b),
            WireResponse::Bulk(None) => Resp2BytesFrame::Null,
            WireResponse::Array(items) => {
                Resp2BytesFrame::Array(items.into_iter().map(|r| r.to_resp2_frame()).collect())
            }
            // RESP3 types with RESP2 fallbacks
            WireResponse::Null => Resp2BytesFrame::Null,
            WireResponse::Double(d) => Resp2BytesFrame::BulkString(Bytes::from(format_float(d))),
            WireResponse::Boolean(b) => Resp2BytesFrame::Integer(if b { 1 } else { 0 }),
            WireResponse::BlobError(e) => {
                // Convert blob error to simple error (truncate if needed)
                let msg = String::from_utf8_lossy(&e);
                Resp2BytesFrame::Error(
                    Str::from_inner(Bytes::from(msg.into_owned()))
                        .expect("error must be valid UTF-8"),
                )
            }
            WireResponse::VerbatimString { data, .. } => {
                // Strip format prefix, return as bulk string
                Resp2BytesFrame::BulkString(data)
            }
            WireResponse::Map(pairs) => {
                // Flatten map to array: [k1, v1, k2, v2, ...]
                let mut items = Vec::with_capacity(pairs.len() * 2);
                for (k, v) in pairs {
                    items.push(k.to_resp2_frame());
                    items.push(v.to_resp2_frame());
                }
                Resp2BytesFrame::Array(items)
            }
            WireResponse::Set(items) => {
                // Convert set to array
                Resp2BytesFrame::Array(items.into_iter().map(|r| r.to_resp2_frame()).collect())
            }
            WireResponse::Attribute(inner) => {
                // Just return the inner value (attributes are metadata)
                inner.to_resp2_frame()
            }
            WireResponse::Push(items) => {
                // Convert push to array
                Resp2BytesFrame::Array(items.into_iter().map(|r| r.to_resp2_frame()).collect())
            }
            WireResponse::BigNumber(n) => {
                // Convert big number to bulk string
                Resp2BytesFrame::BulkString(n)
            }
        }
    }

    /// Convert to a RESP3 frame.
    ///
    /// All types are encoded with their native RESP3 representations.
    ///
    /// This method CANNOT panic - all variants are wire-serializable.
    pub fn to_resp3_frame(self) -> Resp3BytesFrame {
        match self {
            WireResponse::Simple(s) => Resp3BytesFrame::SimpleString {
                data: s,
                attributes: None,
            },
            WireResponse::Error(e) => Resp3BytesFrame::SimpleError {
                data: Str::from_inner(e).expect("error messages must be valid UTF-8"),
                attributes: None,
            },
            WireResponse::Integer(i) => Resp3BytesFrame::Number {
                data: i,
                attributes: None,
            },
            WireResponse::Bulk(Some(b)) => Resp3BytesFrame::BlobString {
                data: b,
                attributes: None,
            },
            WireResponse::Bulk(None) => Resp3BytesFrame::Null,
            WireResponse::Array(items) => Resp3BytesFrame::Array {
                data: items.into_iter().map(|r| r.to_resp3_frame()).collect(),
                attributes: None,
            },
            WireResponse::Null => Resp3BytesFrame::Null,
            WireResponse::Double(d) => Resp3BytesFrame::Double {
                data: d,
                attributes: None,
            },
            WireResponse::Boolean(b) => Resp3BytesFrame::Boolean {
                data: b,
                attributes: None,
            },
            WireResponse::BlobError(e) => Resp3BytesFrame::BlobError {
                data: e,
                attributes: None,
            },
            WireResponse::VerbatimString { format: _, data } => {
                // VerbatimString format is limited to txt/mkd in redis-protocol
                // We default to text format
                Resp3BytesFrame::VerbatimString {
                    data,
                    format: redis_protocol::resp3::types::VerbatimStringFormat::Text,
                    attributes: None,
                }
            }
            WireResponse::Map(pairs) => Resp3BytesFrame::Map {
                data: pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_resp3_frame(), v.to_resp3_frame()))
                    .collect(),
                attributes: None,
            },
            WireResponse::Set(items) => Resp3BytesFrame::Set {
                data: items.into_iter().map(|r| r.to_resp3_frame()).collect(),
                attributes: None,
            },
            WireResponse::Attribute(inner) => {
                // Attributes in RESP3 are handled at the frame level
                // For now, just return the inner value
                inner.to_resp3_frame()
            }
            WireResponse::Push(items) => Resp3BytesFrame::Push {
                data: items.into_iter().map(|r| r.to_resp3_frame()).collect(),
                attributes: None,
            },
            WireResponse::BigNumber(n) => Resp3BytesFrame::BigNumber {
                data: n,
                attributes: None,
            },
        }
    }
}

impl From<WireResponse> for BytesFrame {
    fn from(response: WireResponse) -> Self {
        response.to_resp2_frame()
    }
}

// =============================================================================
// Full Response Type (union of wire + internal)
// =============================================================================

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
    /// Failover: promote replica to primary and transfer slots.
    Failover {
        /// The replica node ID to promote.
        replica_id: u64,
        /// The primary node ID to take over from.
        primary_id: u64,
        /// Whether to force failover even if primary is unreachable.
        force: bool,
    },
    /// Mark a node as failed.
    MarkNodeFailed {
        /// Node ID to mark as failed.
        node_id: u64,
    },
    /// Mark a node as recovered.
    MarkNodeRecovered {
        /// Node ID to mark as recovered.
        node_id: u64,
    },
}

/// Response types that can be sent to clients.
///
/// This is the union type that command handlers return. It can contain either:
/// - Wire-serializable responses (can be converted to `WireResponse`)
/// - Internal control-flow signals (must be handled before serialization)
///
/// Use `into_wire()` to safely extract a wire response or an internal action.
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
    VerbatimString { format: [u8; 3], data: Bytes },

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

/// Result of converting a Response to a WireResponse.
///
/// Used to safely separate wire-serializable responses from internal actions.
pub type WireResult = Result<WireResponse, InternalAction>;

/// Check if a Response is an internal action (not wire-serializable).
impl Response {
    /// Returns true if this response is an internal action that must be handled
    /// before serialization.
    pub fn is_internal(&self) -> bool {
        matches!(
            self,
            Response::BlockingNeeded { .. }
                | Response::RaftNeeded { .. }
                | Response::MigrateNeeded { .. }
        )
    }

    /// Convert to a WireResponse, returning an error for internal actions.
    ///
    /// This is the type-safe way to convert a Response before serialization.
    /// Internal actions are returned as `Err(InternalAction)` and must be
    /// handled by the connection layer.
    pub fn into_wire(self) -> WireResult {
        match self {
            // Wire-serializable types
            Response::Simple(s) => Ok(WireResponse::Simple(s)),
            Response::Error(e) => Ok(WireResponse::Error(e)),
            Response::Integer(i) => Ok(WireResponse::Integer(i)),
            Response::Bulk(b) => Ok(WireResponse::Bulk(b)),
            Response::Array(items) => {
                let wire_items: Result<Vec<_>, _> =
                    items.into_iter().map(|r| r.into_wire()).collect();
                Ok(WireResponse::Array(wire_items?))
            }
            Response::Null => Ok(WireResponse::Null),
            Response::Double(d) => Ok(WireResponse::Double(d)),
            Response::Boolean(b) => Ok(WireResponse::Boolean(b)),
            Response::BlobError(e) => Ok(WireResponse::BlobError(e)),
            Response::VerbatimString { format, data } => {
                Ok(WireResponse::VerbatimString { format, data })
            }
            Response::Map(pairs) => {
                let wire_pairs: Result<Vec<_>, _> = pairs
                    .into_iter()
                    .map(|(k, v)| Ok((k.into_wire()?, v.into_wire()?)))
                    .collect();
                Ok(WireResponse::Map(wire_pairs?))
            }
            Response::Set(items) => {
                let wire_items: Result<Vec<_>, _> =
                    items.into_iter().map(|r| r.into_wire()).collect();
                Ok(WireResponse::Set(wire_items?))
            }
            Response::Attribute(inner) => {
                Ok(WireResponse::Attribute(Box::new((*inner).into_wire()?)))
            }
            Response::Push(items) => {
                let wire_items: Result<Vec<_>, _> =
                    items.into_iter().map(|r| r.into_wire()).collect();
                Ok(WireResponse::Push(wire_items?))
            }
            Response::BigNumber(n) => Ok(WireResponse::BigNumber(n)),

            // Internal actions - return as error
            Response::BlockingNeeded { keys, timeout, op } => {
                Err(InternalAction::BlockingNeeded { keys, timeout, op })
            }
            Response::RaftNeeded {
                op,
                register_node,
                unregister_node,
            } => Err(InternalAction::RaftNeeded {
                op: Box::new(op),
                register_node,
                unregister_node,
            }),
            Response::MigrateNeeded { args } => Err(InternalAction::MigrateNeeded { args }),
        }
    }

    /// Convert a WireResponse back to a Response.
    ///
    /// This is useful when you have a WireResponse and need to return it
    /// in a context that expects Response.
    pub fn from_wire(wire: WireResponse) -> Self {
        match wire {
            WireResponse::Simple(s) => Response::Simple(s),
            WireResponse::Error(e) => Response::Error(e),
            WireResponse::Integer(i) => Response::Integer(i),
            WireResponse::Bulk(b) => Response::Bulk(b),
            WireResponse::Array(items) => {
                Response::Array(items.into_iter().map(Response::from_wire).collect())
            }
            WireResponse::Null => Response::Null,
            WireResponse::Double(d) => Response::Double(d),
            WireResponse::Boolean(b) => Response::Boolean(b),
            WireResponse::BlobError(e) => Response::BlobError(e),
            WireResponse::VerbatimString { format, data } => {
                Response::VerbatimString { format, data }
            }
            WireResponse::Map(pairs) => Response::Map(
                pairs
                    .into_iter()
                    .map(|(k, v)| (Response::from_wire(k), Response::from_wire(v)))
                    .collect(),
            ),
            WireResponse::Set(items) => {
                Response::Set(items.into_iter().map(Response::from_wire).collect())
            }
            WireResponse::Attribute(inner) => {
                Response::Attribute(Box::new(Response::from_wire(*inner)))
            }
            WireResponse::Push(items) => {
                Response::Push(items.into_iter().map(Response::from_wire).collect())
            }
            WireResponse::BigNumber(n) => Response::BigNumber(n),
        }
    }
}

// Additional constructors for Response (keep backward compatibility)
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
    /// **IMPORTANT**: This method will panic if the response contains internal
    /// action types (BlockingNeeded, RaftNeeded, MigrateNeeded). Use `into_wire()`
    /// first to safely extract a WireResponse, then call `to_resp2_frame()` on that.
    ///
    /// RESP3-only types are converted to their RESP2 equivalents:
    /// - Map → flattened Array of alternating keys/values
    /// - Set → Array
    /// - Double → BulkString (formatted as string)
    /// - Boolean → Integer (1 or 0)
    /// - Null → Null bulk string
    /// - Push → Array
    ///
    /// # Panics
    ///
    /// Panics if this response is an internal action type. To avoid panics,
    /// use `into_wire()` to convert to a `WireResponse` first.
    #[deprecated(
        since = "0.1.0",
        note = "Use `into_wire()` to safely convert to WireResponse, then call `to_resp2_frame()` on that"
    )]
    pub fn to_resp2_frame(self) -> Resp2BytesFrame {
        match self.into_wire() {
            Ok(wire) => wire.to_resp2_frame(),
            Err(action) => panic!(
                "{:?} response should be intercepted by connection handler before serialization",
                std::mem::discriminant(&action)
            ),
        }
    }

    /// Convert to a RESP3 frame.
    ///
    /// **IMPORTANT**: This method will panic if the response contains internal
    /// action types (BlockingNeeded, RaftNeeded, MigrateNeeded). Use `into_wire()`
    /// first to safely extract a WireResponse, then call `to_resp3_frame()` on that.
    ///
    /// All types are encoded with their native RESP3 representations.
    ///
    /// # Panics
    ///
    /// Panics if this response is an internal action type. To avoid panics,
    /// use `into_wire()` to convert to a `WireResponse` first.
    #[deprecated(
        since = "0.1.0",
        note = "Use `into_wire()` to safely convert to WireResponse, then call `to_resp3_frame()` on that"
    )]
    pub fn to_resp3_frame(self) -> Resp3BytesFrame {
        match self.into_wire() {
            Ok(wire) => wire.to_resp3_frame(),
            Err(action) => panic!(
                "{:?} response should be intercepted by connection handler before serialization",
                std::mem::discriminant(&action)
            ),
        }
    }

    /// Safely convert to a RESP2 frame, returning None for internal actions.
    ///
    /// This is the safe alternative to `to_resp2_frame()` that won't panic.
    /// Returns None if the response is an internal action type.
    pub fn try_to_resp2_frame(self) -> Option<Resp2BytesFrame> {
        self.into_wire().ok().map(|w| w.to_resp2_frame())
    }

    /// Safely convert to a RESP3 frame, returning None for internal actions.
    ///
    /// This is the safe alternative to `to_resp3_frame()` that won't panic.
    /// Returns None if the response is an internal action type.
    pub fn try_to_resp3_frame(self) -> Option<Resp3BytesFrame> {
        self.into_wire().ok().map(|w| w.to_resp3_frame())
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
        response
            .into_wire()
            .expect("cannot convert internal action to BytesFrame")
            .to_resp2_frame()
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
        let frame = resp.into_wire().unwrap().to_resp3_frame();
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
        let frame = resp.into_wire().unwrap().to_resp2_frame();
        match frame {
            Resp2BytesFrame::Array(items) => {
                // Map with 2 pairs should flatten to 4 elements
                assert_eq!(items.len(), 4);
                // Check first key
                assert!(
                    matches!(&items[0], Resp2BytesFrame::BulkString(b) if b.as_ref() == b"field1")
                );
                // Check first value
                assert!(
                    matches!(&items[1], Resp2BytesFrame::BulkString(b) if b.as_ref() == b"value1")
                );
                // Check second key
                assert!(
                    matches!(&items[2], Resp2BytesFrame::BulkString(b) if b.as_ref() == b"field2")
                );
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
        let frame = resp.into_wire().unwrap().to_resp3_frame();
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
        let frame = resp.into_wire().unwrap().to_resp2_frame();
        match frame {
            Resp2BytesFrame::Array(items) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected Array frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_double_to_resp3_frame() {
        let resp = Response::Double(3.125);
        let frame = resp.into_wire().unwrap().to_resp3_frame();
        match frame {
            Resp3BytesFrame::Double { data, attributes } => {
                assert!((data - 3.125).abs() < f64::EPSILON);
                assert!(attributes.is_none());
            }
            _ => panic!("Expected Double frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_double_to_resp2_string() {
        let resp = Response::Double(3.125);
        let frame = resp.into_wire().unwrap().to_resp2_frame();
        match frame {
            Resp2BytesFrame::BulkString(data) => {
                let s = String::from_utf8(data.to_vec()).unwrap();
                // Should be a valid float string
                let parsed: f64 = s.parse().unwrap();
                assert!((parsed - 3.125).abs() < 1e-10);
            }
            _ => panic!("Expected BulkString frame, got {:?}", frame),
        }
    }

    #[test]
    fn test_double_special_values() {
        // Test infinity
        let resp = Response::Double(f64::INFINITY);
        let frame = resp.into_wire().unwrap().to_resp2_frame();
        match frame {
            Resp2BytesFrame::BulkString(data) => {
                assert_eq!(data.as_ref(), b"inf");
            }
            _ => panic!("Expected BulkString frame"),
        }

        // Test negative infinity
        let resp = Response::Double(f64::NEG_INFINITY);
        let frame = resp.into_wire().unwrap().to_resp2_frame();
        match frame {
            Resp2BytesFrame::BulkString(data) => {
                assert_eq!(data.as_ref(), b"-inf");
            }
            _ => panic!("Expected BulkString frame"),
        }

        // Test NaN
        let resp = Response::Double(f64::NAN);
        let frame = resp.into_wire().unwrap().to_resp2_frame();
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
        let frame = resp.into_wire().unwrap().to_resp3_frame();
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
        let frame = resp.into_wire().unwrap().to_resp2_frame();
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
        let frame = resp.into_wire().unwrap().to_resp3_frame();
        match frame {
            Resp3BytesFrame::Boolean { data, attributes } => {
                assert!(data);
                assert!(attributes.is_none());
            }
            _ => panic!("Expected Boolean frame, got {:?}", frame),
        }

        // Test false
        let resp = Response::Boolean(false);
        let frame = resp.into_wire().unwrap().to_resp3_frame();
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
        let frame = resp.into_wire().unwrap().to_resp2_frame();
        assert!(matches!(frame, Resp2BytesFrame::Integer(1)));

        // False becomes 0
        let resp = Response::Boolean(false);
        let frame = resp.into_wire().unwrap().to_resp2_frame();
        assert!(matches!(frame, Resp2BytesFrame::Integer(0)));
    }

    #[test]
    fn test_null_to_resp3_frame() {
        let resp = Response::Null;
        let frame = resp.into_wire().unwrap().to_resp3_frame();
        assert!(matches!(frame, Resp3BytesFrame::Null));
    }

    #[test]
    fn test_null_to_resp2_frame() {
        let resp = Response::Null;
        let frame = resp.into_wire().unwrap().to_resp2_frame();
        assert!(matches!(frame, Resp2BytesFrame::Null));
    }

    #[test]
    fn test_blob_error_to_resp3_frame() {
        let resp = Response::BlobError(Bytes::from("ERR some long error message"));
        let frame = resp.into_wire().unwrap().to_resp3_frame();
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
        let frame = resp.into_wire().unwrap().to_resp3_frame();
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
        let frame = resp.into_wire().unwrap().to_resp3_frame();
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

    // === Type-State Pattern Tests ===

    #[test]
    fn test_into_wire_simple_types() {
        // Test that wire-serializable types convert successfully
        let cases = vec![
            Response::ok(),
            Response::error("ERR test"),
            Response::Integer(42),
            Response::bulk("hello"),
            Response::null(),
            Response::Null,
            Response::Double(3.125),
            Response::Boolean(true),
        ];

        for resp in cases {
            assert!(
                resp.into_wire().is_ok(),
                "Expected Ok for wire-serializable type"
            );
        }
    }

    #[test]
    fn test_into_wire_arrays_and_maps() {
        // Nested wire-serializable types
        let array = Response::Array(vec![
            Response::Integer(1),
            Response::bulk("two"),
            Response::Null,
        ]);
        assert!(array.into_wire().is_ok());

        let map = Response::Map(vec![(Response::bulk("key"), Response::Integer(42))]);
        assert!(map.into_wire().is_ok());
    }

    #[test]
    fn test_into_wire_blocking_needed_returns_internal_action() {
        let resp = Response::BlockingNeeded {
            keys: vec![Bytes::from("mykey")],
            timeout: 5.0,
            op: BlockingOp::BLPop,
        };

        let result = resp.into_wire();
        assert!(result.is_err());

        match result {
            Err(InternalAction::BlockingNeeded { keys, timeout, op }) => {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0].as_ref(), b"mykey");
                assert!((timeout - 5.0).abs() < f64::EPSILON);
                assert!(matches!(op, BlockingOp::BLPop));
            }
            _ => panic!("Expected InternalAction::BlockingNeeded"),
        }
    }

    #[test]
    fn test_into_wire_raft_needed_returns_internal_action() {
        let resp = Response::RaftNeeded {
            op: RaftClusterOp::IncrementEpoch,
            register_node: None,
            unregister_node: None,
        };

        let result = resp.into_wire();
        assert!(result.is_err());

        match result {
            Err(InternalAction::RaftNeeded { op, .. }) => {
                assert!(matches!(*op, RaftClusterOp::IncrementEpoch));
            }
            _ => panic!("Expected InternalAction::RaftNeeded"),
        }
    }

    #[test]
    fn test_into_wire_migrate_needed_returns_internal_action() {
        let resp = Response::MigrateNeeded {
            args: vec![Bytes::from("host"), Bytes::from("port")],
        };

        let result = resp.into_wire();
        assert!(result.is_err());

        match result {
            Err(InternalAction::MigrateNeeded { args }) => {
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected InternalAction::MigrateNeeded"),
        }
    }

    #[test]
    fn test_is_internal() {
        // Wire-serializable types are not internal
        assert!(!Response::ok().is_internal());
        assert!(!Response::Integer(42).is_internal());
        assert!(!Response::Array(vec![]).is_internal());

        // Internal action types are internal
        assert!(
            Response::BlockingNeeded {
                keys: vec![],
                timeout: 0.0,
                op: BlockingOp::BLPop,
            }
            .is_internal()
        );

        assert!(
            Response::RaftNeeded {
                op: RaftClusterOp::IncrementEpoch,
                register_node: None,
                unregister_node: None,
            }
            .is_internal()
        );

        assert!(Response::MigrateNeeded { args: vec![] }.is_internal());
    }

    #[test]
    fn test_wire_response_constructors() {
        let ok = WireResponse::ok();
        assert!(matches!(ok, WireResponse::Simple(s) if s.as_ref() == b"OK"));

        let err = WireResponse::error("ERR test");
        assert!(matches!(err, WireResponse::Error(e) if e.as_ref() == b"ERR test"));

        let null = WireResponse::null();
        assert!(matches!(null, WireResponse::Bulk(None)));

        let bulk = WireResponse::bulk("hello");
        assert!(matches!(bulk, WireResponse::Bulk(Some(b)) if b.as_ref() == b"hello"));

        let pong = WireResponse::pong();
        assert!(matches!(pong, WireResponse::Simple(s) if s.as_ref() == b"PONG"));

        let queued = WireResponse::queued();
        assert!(matches!(queued, WireResponse::Simple(s) if s.as_ref() == b"QUEUED"));
    }

    #[test]
    fn test_wire_response_to_resp2_frame() {
        // This should NOT panic - WireResponse only contains safe types
        let wire = WireResponse::Integer(42);
        let frame = wire.to_resp2_frame();
        assert!(matches!(frame, Resp2BytesFrame::Integer(42)));

        let wire_array =
            WireResponse::Array(vec![WireResponse::Integer(1), WireResponse::bulk("two")]);
        let frame = wire_array.to_resp2_frame();
        match frame {
            Resp2BytesFrame::Array(items) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_wire_response_to_resp3_frame() {
        // This should NOT panic - WireResponse only contains safe types
        let wire = WireResponse::Double(3.125);
        let frame = wire.to_resp3_frame();
        match frame {
            Resp3BytesFrame::Double { data, .. } => {
                assert!((data - 3.125).abs() < f64::EPSILON);
            }
            _ => panic!("Expected double"),
        }
    }

    #[test]
    fn test_from_wire() {
        // Test round-trip: Response -> WireResponse -> Response
        let original = Response::Integer(42);
        let wire = original.clone().into_wire().unwrap();
        let back = Response::from_wire(wire);
        assert_eq!(original, back);

        // Complex nested type
        let original = Response::Array(vec![
            Response::Integer(1),
            Response::bulk("two"),
            Response::Map(vec![(Response::bulk("key"), Response::Boolean(true))]),
        ]);
        let wire = original.clone().into_wire().unwrap();
        let back = Response::from_wire(wire);
        assert_eq!(original, back);
    }

    #[test]
    fn test_try_to_resp2_frame() {
        // Wire-serializable type should return Some
        let resp = Response::Integer(42);
        let frame = resp.try_to_resp2_frame();
        assert!(frame.is_some());

        // Internal action should return None
        let resp = Response::BlockingNeeded {
            keys: vec![],
            timeout: 0.0,
            op: BlockingOp::BLPop,
        };
        let frame = resp.try_to_resp2_frame();
        assert!(frame.is_none());
    }

    #[test]
    fn test_try_to_resp3_frame() {
        // Wire-serializable type should return Some
        let resp = Response::Double(3.125);
        let frame = resp.try_to_resp3_frame();
        assert!(frame.is_some());

        // Internal action should return None
        let resp = Response::RaftNeeded {
            op: RaftClusterOp::IncrementEpoch,
            register_node: None,
            unregister_node: None,
        };
        let frame = resp.try_to_resp3_frame();
        assert!(frame.is_none());
    }
}
