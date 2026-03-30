//! Cluster coordination module for FrogDB.
//!
//! This module provides Raft-based cluster coordination for managing:
//! - Cluster topology (nodes, roles)
//! - Slot ownership and assignment
//! - Failover coordination
//! - Configuration epochs
//!
//! # Architecture
//!
//! FrogDB uses Raft consensus for cluster metadata coordination, while data
//! replication uses the existing PSYNC protocol. This provides strongly
//! consistent cluster state while maintaining high-performance data operations.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    FrogDB Cluster                        │
//! ├─────────────────────────────────────────────────────────┤
//! │  ┌─────────────────────────────────────────────────┐    │
//! │  │     Coordination Layer (Raft Consensus)         │    │
//! │  │  - Cluster topology (nodes, roles)              │    │
//! │  │  - Slot ownership map                           │    │
//! │  │  - Failover decisions                           │    │
//! │  │  - Config epochs                                │    │
//! │  └─────────────────────────────────────────────────┘    │
//! │                          │                               │
//! │  ┌───────────┐  ┌───────────┐  ┌───────────┐           │
//! │  │  Node A   │  │  Node B   │  │  Node C   │           │
//! │  │ (Leader)  │  │ (Follower)│  │ (Follower)│           │
//! │  │  Slots    │  │  Slots    │  │  Slots    │           │
//! │  │  0-5460   │  │ 5461-10922│  │10923-16383│           │
//! │  └───────────┘  └───────────┘  └───────────┘           │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # What Raft Coordinates (Metadata Only)
//!
//! | Coordinated via Raft          | NOT via Raft (data plane)    |
//! |-------------------------------|------------------------------|
//! | Cluster membership            | Key-value data replication   |
//! | Slot ownership changes        | PSYNC/WAL streaming          |
//! | Failover decisions            | Read/write operations        |
//! | Config epochs                 | Slot migrations (data)       |

mod commands;
pub mod network;
pub mod state;
pub mod storage;
pub mod types;

pub use network::{
    ClusterNetwork, ClusterNetworkFactory, ClusterRpcRequest, ClusterRpcResponse, FramedStream,
    handle_rpc_request, new_framed, parse_rpc_message, send_rpc_response,
};
pub use state::{ClusterState, ClusterStateMachine, DemotionEvent, SlotMigrationCompleteEvent};
pub use storage::ClusterStorage;
pub use types::{
    CLUSTER_SLOTS, ClusterCommand, ClusterConfig, ClusterError, ClusterResponse, ClusterSnapshot,
    ConfigEpoch, NodeId, NodeInfo, NodeRole, SlotRange, TypeConfig,
};

use openraft::Raft;
use std::sync::Arc;

/// The Raft instance type for cluster coordination.
pub type ClusterRaft = Raft<TypeConfig>;

/// A shared reference to the cluster Raft instance.
pub type SharedClusterRaft = Arc<ClusterRaft>;
