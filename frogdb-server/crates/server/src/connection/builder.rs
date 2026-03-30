//! Builder pattern for ConnectionHandler.
//!
//! This module provides a fluent builder API for constructing ConnectionHandler
//! instances with clear separation of required and optional parameters.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use frogdb_core::{
    AclManager, ClientHandle, ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState,
    CommandRegistry, MetricsRecorder, ReplicationTrackerImpl, ShardMessage, SharedFunctionRegistry,
    persistence::SnapshotCoordinator,
};
use frogdb_debug::{HotShardConfig, MemoryDiagConfig};
use frogdb_telemetry::SharedTracer;
use tokio::sync::mpsc;

use crate::config::TracingConfig;
use crate::net::TcpStream;
use crate::replication::PrimaryReplicationHandler;
use crate::runtime_config::ConfigManager;

use super::{
    AdminDeps, ClusterDeps, ConnectionConfig, ConnectionHandler, CoreDeps, ObservabilityDeps,
};

/// Builder for creating [`ConnectionHandler`] instances.
///
/// This provides a fluent API for constructing connection handlers with
/// clear separation between required and optional dependencies.
///
/// # Example
///
/// ```rust,ignore
/// let handler = ConnectionHandlerBuilder::new(core_deps)
///     .with_admin(admin_deps)
///     .with_cluster(cluster_deps)
///     .with_observability(observability_deps)
///     .with_config(config)
///     .build(socket, addr, conn_id, shard_id, client_handle);
/// ```
pub struct ConnectionHandlerBuilder {
    core: CoreDeps,
    admin: Option<AdminDeps>,
    cluster: ClusterDeps,
    observability: ObservabilityDeps,
    config: Option<ConnectionConfig>,
}

impl ConnectionHandlerBuilder {
    /// Create a new builder with required core dependencies.
    pub fn new(core: CoreDeps) -> Self {
        Self {
            core,
            admin: None,
            cluster: ClusterDeps::default(),
            observability: ObservabilityDeps::default(),
            config: None,
        }
    }

    /// Create a new builder with individual core dependencies.
    pub fn with_core_parts(
        registry: Arc<CommandRegistry>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        metrics_recorder: Arc<dyn MetricsRecorder>,
        acl_manager: Arc<AclManager>,
    ) -> Self {
        let mut builder = Self::new(CoreDeps {
            registry,
            shard_senders,
            acl_manager,
        });
        builder.observability.metrics_recorder = metrics_recorder;
        builder
    }

    /// Set admin dependencies.
    pub fn with_admin(mut self, admin: AdminDeps) -> Self {
        self.admin = Some(admin);
        self
    }

    /// Set admin dependencies from individual parts.
    pub fn with_admin_parts(
        mut self,
        client_registry: Arc<ClientRegistry>,
        config_manager: Arc<ConfigManager>,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        function_registry: SharedFunctionRegistry,
    ) -> Self {
        self.admin = Some(AdminDeps {
            client_registry,
            config_manager,
            snapshot_coordinator,
            function_registry,
            cursor_store: Arc::new(crate::cursor_store::AggregateCursorStore::new()),
        });
        self
    }

    /// Set cluster dependencies (for cluster mode).
    pub fn with_cluster(mut self, cluster: ClusterDeps) -> Self {
        self.cluster = cluster;
        self
    }

    /// Set cluster dependencies from individual parts.
    #[allow(clippy::too_many_arguments)]
    pub fn with_cluster_parts(
        mut self,
        cluster_state: Arc<ClusterState>,
        node_id: u64,
        raft: Arc<ClusterRaft>,
        network_factory: Arc<ClusterNetworkFactory>,
        replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
        primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
        quorum_checker: Option<Arc<dyn frogdb_core::QuorumChecker>>,
    ) -> Self {
        self.cluster = ClusterDeps {
            quorum_checker,
            ..ClusterDeps::cluster(
                cluster_state,
                node_id,
                raft,
                network_factory,
                replication_tracker,
                primary_replication_handler,
            )
        };
        self
    }

    /// Set observability dependencies.
    pub fn with_observability(mut self, observability: ObservabilityDeps) -> Self {
        self.observability = observability;
        self
    }

    /// Set shared tracer for distributed tracing.
    pub fn with_tracer(mut self, tracer: SharedTracer) -> Self {
        self.observability.shared_tracer = Some(tracer);
        self
    }

    /// Set tracing configuration.
    pub fn with_tracing_config(mut self, config: TracingConfig) -> Self {
        self.observability.tracing_config = config;
        self
    }

    /// Set connection configuration.
    pub fn with_config(mut self, config: ConnectionConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Configure connection as admin.
    pub fn as_admin(mut self) -> Self {
        if let Some(ref mut config) = self.config {
            config.is_admin = true;
        }
        self
    }

    /// Configure connection for cluster mode.
    pub fn enable_admin_separation(mut self) -> Self {
        if let Some(ref mut config) = self.config {
            config.admin_enabled = true;
        }
        self
    }

    /// Build the ConnectionHandler.
    ///
    /// # Panics
    ///
    /// Panics if admin dependencies or config are not set.
    pub fn build(
        self,
        socket: TcpStream,
        addr: SocketAddr,
        conn_id: u64,
        shard_id: usize,
        client_handle: ClientHandle,
    ) -> ConnectionHandler {
        let admin = self
            .admin
            .expect("admin dependencies must be set via with_admin()");
        let config = self.config.expect("config must be set via with_config()");

        ConnectionHandler::from_deps(
            socket,
            addr,
            conn_id,
            shard_id,
            client_handle,
            self.core,
            admin,
            self.cluster,
            config,
            self.observability,
        )
    }

    /// Try to build the ConnectionHandler, returning None if dependencies are missing.
    pub fn try_build(
        self,
        socket: TcpStream,
        addr: SocketAddr,
        conn_id: u64,
        shard_id: usize,
        client_handle: ClientHandle,
    ) -> Option<ConnectionHandler> {
        let admin = self.admin?;
        let config = self.config?;

        Some(ConnectionHandler::from_deps(
            socket,
            addr,
            conn_id,
            shard_id,
            client_handle,
            self.core,
            admin,
            self.cluster,
            config,
            self.observability,
        ))
    }
}

/// Convenience function to create a builder with default configuration.
pub fn connection_builder(
    registry: Arc<CommandRegistry>,
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    metrics_recorder: Arc<dyn MetricsRecorder>,
    acl_manager: Arc<AclManager>,
) -> ConnectionHandlerBuilder {
    ConnectionHandlerBuilder::with_core_parts(
        registry,
        shard_senders,
        metrics_recorder,
        acl_manager,
    )
}

/// Convenience function to create a simple standalone ConnectionConfig.
pub fn standalone_config(num_shards: usize) -> ConnectionConfig {
    ConnectionConfig {
        num_shards,
        allow_cross_slot: false,
        scatter_gather_timeout: Duration::from_millis(5000),
        is_admin: false,
        admin_enabled: false,
        hotshards_config: HotShardConfig::default(),
        memory_diag_config: MemoryDiagConfig::default(),
        per_request_spans: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        is_replica: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        #[cfg(feature = "turmoil")]
        chaos_config: std::sync::Arc::new(crate::config::ChaosConfig::default()),
    }
}
