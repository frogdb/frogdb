//! Replication handler setup (primary/replica/standalone).

use anyhow::Result;
use frogdb_core::persistence::RocksStore;
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{
    MetricsRecorder, NoopBroadcaster, ReplicationTrackerImpl, SharedBroadcaster,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::Config;
use crate::replication::{
    LagThresholdConfig, PrimaryReplicationHandler, ReplicaReplicationHandler,
    SplitBrainBufferConfig,
};
use crate::replication_quorum::ReplicationQuorumChecker;

/// Result of the replication initialization phase.
pub(super) struct ReplicationInitResult {
    pub replication_broadcaster: SharedBroadcaster,
    pub replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    pub replica_handler: Option<Arc<ReplicaReplicationHandler>>,
    pub replica_frame_rx: Option<mpsc::Receiver<frogdb_core::ReplicationFrame>>,
    pub primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
    pub shared_replication_offset: Option<Arc<AtomicU64>>,
    pub replication_quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>>,
}

/// Initialize replication handlers based on the server role.
pub(super) fn init_replication(
    config: &Config,
    rocks_store: &Option<Arc<RocksStore>>,
    _metrics_recorder: &Arc<dyn MetricsRecorder>,
) -> Result<ReplicationInitResult> {
    let mut replica_handler: Option<Arc<ReplicaReplicationHandler>> = None;
    let mut replica_frame_rx: Option<mpsc::Receiver<frogdb_core::ReplicationFrame>> = None;
    let mut primary_replication_handler: Option<Arc<PrimaryReplicationHandler>> = None;
    let mut shared_replication_offset: Option<Arc<AtomicU64>> = None;

    let (replication_broadcaster, replication_tracker): (
        SharedBroadcaster,
        Option<Arc<ReplicationTrackerImpl>>,
    ) = if config.replication.is_primary() {
        // Initialize PrimaryReplicationHandler for primary role
        let state_path = config
            .persistence
            .data_dir
            .join(&config.replication.state_file);
        let repl_state = frogdb_core::ReplicationState::load_or_create(&state_path)
            .map_err(|e| anyhow::anyhow!("Failed to load replication state: {}", e))?;

        info!(
            replication_id = %repl_state.replication_id,
            offset = repl_state.replication_offset,
            "Initialized primary replication state"
        );

        let tracker = Arc::new(frogdb_core::ReplicationTrackerImpl::new());
        tracker.set_offset(repl_state.replication_offset);

        // Wire up shared replication offset for cluster bus HealthProbe
        if config.cluster.enabled {
            shared_replication_offset = Some(tracker.shared_offset());
        }

        let handler = Arc::new(PrimaryReplicationHandler::new(
            repl_state,
            tracker.clone(),
            rocks_store.clone(),
            config.persistence.data_dir.clone(),
            LagThresholdConfig {
                threshold_bytes: config.replication.replication_lag_threshold_bytes,
                threshold_secs: config.replication.replication_lag_threshold_secs,
                cooldown: Duration::from_secs(config.replication.fullresync_cooldown_secs),
            },
            SplitBrainBufferConfig {
                enabled: config.replication.split_brain_log_enabled,
                max_entries: config.replication.split_brain_buffer_size,
                max_bytes: config.replication.split_brain_buffer_max_mb * 1024 * 1024,
            },
            config.replication.replica_write_timeout_ms,
        ));

        // Store a reference for PSYNC connection handoff
        primary_replication_handler = Some(handler.clone());

        (handler as SharedBroadcaster, Some(tracker))
    } else if config.replication.is_replica() {
        // Initialize ReplicaReplicationHandler for replica role
        let primary_addr = format!(
            "{}:{}",
            config.replication.primary_host, config.replication.primary_port
        )
        .parse::<std::net::SocketAddr>()
        .map_err(|e| anyhow::anyhow!("Invalid primary address: {}", e))?;

        let state_path = config
            .persistence
            .data_dir
            .join(&config.replication.state_file);
        let repl_state = frogdb_core::ReplicationState::load_or_create(&state_path)
            .map_err(|e| anyhow::anyhow!("Failed to load replication state: {}", e))?;

        info!(
            primary = %primary_addr,
            replication_id = %repl_state.replication_id,
            offset = repl_state.replication_offset,
            "Initialized replica replication state"
        );

        let (mut handler, frame_rx) = ReplicaReplicationHandler::new(
            primary_addr,
            config.server.port,
            repl_state,
            config.persistence.data_dir.clone(),
        );

        // Wire up shared replication offset for cluster bus HealthProbe
        if config.cluster.enabled {
            let offset = Arc::new(AtomicU64::new(0));
            handler.set_shared_offset(offset.clone());
            shared_replication_offset = Some(offset);
        }

        replica_handler = Some(Arc::new(handler));
        replica_frame_rx = Some(frame_rx);

        // Replicas use NoopBroadcaster (they don't broadcast to other replicas)
        (Arc::new(NoopBroadcaster), None)
    } else {
        // Standalone mode
        (Arc::new(NoopBroadcaster), None)
    };

    // Create replication quorum checker for primary self-fencing
    let replication_quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>> =
        if config.replication.is_primary() && config.replication.self_fence_on_replica_loss {
            replication_tracker.as_ref().map(|tracker| {
                Arc::new(ReplicationQuorumChecker::new(
                    tracker.clone(),
                    Duration::from_millis(config.replication.replica_freshness_timeout_ms),
                )) as Arc<dyn frogdb_core::command::QuorumChecker>
            })
        } else {
            None
        };

    Ok(ReplicationInitResult {
        replication_broadcaster,
        replication_tracker,
        replica_handler,
        replica_frame_rx,
        primary_replication_handler,
        shared_replication_offset,
        replication_quorum_checker,
    })
}
