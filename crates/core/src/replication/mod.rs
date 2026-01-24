//! Replication module for FrogDB.
//!
//! This module provides the core abstractions and implementations for
//! primary-replica replication:
//!
//! - `ReplicationState`: Tracks replication IDs and offsets
//! - `ReplicationTracker`: Concrete implementation for tracking replica ACKs
//! - Frame encoding/decoding for WAL streaming
//!
//! # Architecture
//!
//! FrogDB uses an async replication model similar to Redis:
//!
//! ```text
//! Primary                         Replica
//!    │                               │
//!    │◄──────── PSYNC ───────────────│
//!    │                               │
//!    │──── FULLRESYNC/CONTINUE ─────►│
//!    │                               │
//!    │──── WAL Frames ──────────────►│
//!    │          ...                  │
//!    │◄──────── ACK ─────────────────│
//! ```
//!
//! # Protocol
//!
//! The replication protocol follows Redis PSYNC2:
//! - PSYNC <repl_id> <offset> - Partial sync request
//! - FULLRESYNC <repl_id> <offset> - Full resync response
//! - CONTINUE - Continue with partial sync
//! - REPLCONF ACK <offset> - Replica acknowledges offset

pub mod frame;
pub mod state;
pub mod tracker;

pub use frame::{ReplicationFrame, ReplicationFrameCodec, FRAME_MAGIC, FRAME_VERSION};
pub use state::ReplicationState;
pub use tracker::{ReplicaInfo, ReplicationTrackerImpl};
