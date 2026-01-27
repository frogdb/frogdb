//! Dependency groups for ConnectionHandler.
//!
//! This module organizes the many dependencies of ConnectionHandler into
//! logical groups, making the handler easier to construct and understand.

use std::sync::Arc;
use std::time::Duration;

use frogdb_core::{
    AclManager, ClusterNetworkFactory, ClusterRaft, ClusterState, CommandRegistry,
    MetricsRecorder, ReplicationTrackerImpl, SharedFunctionRegistry, ShardMessage,
    persistence::SnapshotCoordinator,
};
use frogdb_metrics::{HotShardConfig, LatencyBandTracker, MemoryDiagConfig, SharedTracer};
use tokio::sync::mpsc;

use frogdb_core::ClientRegistry;

use crate::config::TracingConfig;
use crate::replication::PrimaryReplicationHandler;
use crate::runtime_config::ConfigManager;

// ============================================================================
// Core Dependencies - Required for all command execution
// ============================================================================

/// Core dependencies required for command routing and execution.
///
/// These are the essential dependencies that every connection needs
/// to route commands to shards and record metrics.
#[derive(Clone)]
pub struct CoreDeps {
    /// Command registry for looking up command implementations.
    pub registry: Arc<CommandRegistry>,

    /// Shard message senders for routing commands to shards.
    pub shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// Metrics recorder for observability.
    pub metrics_recorder: Arc<dyn MetricsRecorder>,

    /// ACL manager for authentication and authorization.
    pub acl_manager: Arc<AclManager>,
}

// ============================================================================
// Admin Dependencies - For administrative commands
// ============================================================================

/// Dependencies for administrative commands like CLIENT, CONFIG, DEBUG.
///
/// These are separated from core deps because they're only needed for
/// admin functionality, not regular command execution.
#[derive(Clone)]
pub struct AdminDeps {
    /// Client registry for CLIENT commands (LIST, KILL, etc.).
    pub client_registry: Arc<ClientRegistry>,

    /// Configuration manager for CONFIG commands (GET, SET, REWRITE).
    pub config_manager: Arc<ConfigManager>,

    /// Snapshot coordinator for BGSAVE, LASTSAVE commands.
    pub snapshot_coordinator: Arc<dyn SnapshotCoordinator>,

    /// Function registry for FUNCTION/FCALL commands.
    pub function_registry: SharedFunctionRegistry,
}

// ============================================================================
// Cluster Dependencies - Only for cluster mode
// ============================================================================

/// Dependencies for cluster mode operation.
///
/// These are `None` when running in standalone mode.
#[derive(Clone, Default)]
pub struct ClusterDeps {
    /// Cluster state with slot assignments and node topology.
    pub cluster_state: Option<Arc<ClusterState>>,

    /// This node's ID in the cluster.
    pub node_id: Option<u64>,

    /// Raft instance for consensus operations.
    pub raft: Option<Arc<ClusterRaft>>,

    /// Network factory for establishing connections to other nodes.
    pub network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Replication tracker for WAIT command and replica acknowledgments.
    pub replication_tracker: Option<Arc<ReplicationTrackerImpl>>,

    /// Primary replication handler for PSYNC command.
    pub primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
}

impl ClusterDeps {
    /// Create empty cluster deps (standalone mode).
    pub fn standalone() -> Self {
        Self::default()
    }

    /// Create cluster deps with all fields populated.
    pub fn cluster(
        cluster_state: Arc<ClusterState>,
        node_id: u64,
        raft: Arc<ClusterRaft>,
        network_factory: Arc<ClusterNetworkFactory>,
        replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
        primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
    ) -> Self {
        Self {
            cluster_state: Some(cluster_state),
            node_id: Some(node_id),
            raft: Some(raft),
            network_factory: Some(network_factory),
            replication_tracker,
            primary_replication_handler,
        }
    }

    /// Check if cluster mode is enabled.
    pub fn is_cluster_mode(&self) -> bool {
        self.cluster_state.is_some()
    }
}

// ============================================================================
// Connection Configuration
// ============================================================================

/// Configuration options for connection behavior.
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Number of shards in this node.
    pub num_shards: usize,

    /// Allow scatter-gather operations across shards.
    pub allow_cross_slot: bool,

    /// Timeout for scatter-gather operations.
    pub scatter_gather_timeout: Duration,

    /// Whether this is an admin connection (from admin port).
    pub is_admin: bool,

    /// Whether admin port separation is enabled.
    pub admin_enabled: bool,

    /// Hot shard detection configuration.
    pub hotshards_config: HotShardConfig,

    /// Memory diagnostics configuration.
    pub memory_diag_config: MemoryDiagConfig,
}

impl ConnectionConfig {
    /// Create a default config for testing.
    #[cfg(test)]
    pub fn default_for_testing(num_shards: usize) -> Self {
        Self {
            num_shards,
            allow_cross_slot: false,
            scatter_gather_timeout: Duration::from_millis(5000),
            is_admin: false,
            admin_enabled: false,
            hotshards_config: HotShardConfig::default(),
            memory_diag_config: MemoryDiagConfig::default(),
        }
    }
}

// ============================================================================
// Observability Dependencies
// ============================================================================

/// Optional observability dependencies.
#[derive(Clone, Default)]
pub struct ObservabilityDeps {
    /// Shared tracer for distributed tracing.
    pub shared_tracer: Option<SharedTracer>,

    /// Tracing configuration.
    pub tracing_config: TracingConfig,

    /// Latency band tracker for SLO monitoring.
    pub band_tracker: Option<Arc<LatencyBandTracker>>,
}

// ============================================================================
// All Dependencies Bundle
// ============================================================================

/// All dependencies needed to create a ConnectionHandler.
///
/// This bundles all the dependency groups together for convenience.
#[derive(Clone)]
pub struct ConnectionDeps {
    pub core: CoreDeps,
    pub admin: AdminDeps,
    pub cluster: ClusterDeps,
    pub config: ConnectionConfig,
    pub observability: ObservabilityDeps,
}
