//! Replication handler setup (primary/replica/standalone).

use anyhow::Result;
use bytes::Bytes;
use frogdb_core::persistence::RocksStore;
use frogdb_core::sync::{Arc, AtomicU64};
use frogdb_core::{
    MetricsRecorder, ReplicationBroadcaster, ReplicationTrackerImpl, SharedBroadcaster,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::Config;
use crate::replication::{
    LagThresholdConfig, PrimaryReplicationHandler, ReplicaReplicationHandler, ReplicationIdentity,
    SplitBrainBufferConfig,
};
use crate::replication_quorum::ReplicationQuorumChecker;

/// Result of the replication initialization phase.
pub(super) struct ReplicationInitResult {
    pub replication_broadcaster: SharedBroadcaster,
    /// Downstream-replica tracker. Always present (see [`init_replication`]);
    /// `Option` only so the per-connection `ClusterDeps` can keep a `Default`.
    pub replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    pub replica_handler: Option<Arc<ReplicaReplicationHandler>>,
    pub replica_frame_rx: Option<mpsc::Receiver<frogdb_core::ReplicationFrame>>,
    /// Primary-side replication handler. Always present, on every role.
    pub primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
    pub shared_replication_offset: Option<Arc<AtomicU64>>,
    pub replication_quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>>,
    /// The same checker, un-erased, so `ConfigManager` can reach its live
    /// self-fence / freshness-timeout setters (a `dyn QuorumChecker` cannot).
    /// `Some` exactly when `replication_quorum_checker` is — i.e. always.
    pub replication_self_fence: Option<Arc<ReplicationQuorumChecker>>,
    /// The resolved `replicaof` primary address when this node boots as a
    /// replica (`None` for a primary/standalone boot). Threaded into
    /// `init_cluster` so the `RoleManager` seeds `primary_target` with the
    /// same address `ROLE`/INFO report at boot — computed once here rather
    /// than re-resolved a second time.
    pub primary_addr: Option<std::net::SocketAddr>,
    /// The node's replication identity, shared by both role handlers. Handed on
    /// to the `RoleManager` so a runtime `REPLICAOF` builds its replica handler
    /// over the same cell instead of minting a fresh one.
    pub replication_identity: ReplicationIdentity,
}

/// Initialize replication handlers based on the server role.
///
/// The replication state is recovered upstream by recovery phase 5 (already
/// reconciled with any staged full-sync metadata). This phase only constructs
/// the live components from it — including seeding the in-memory tracker offset,
/// which is deliberately a wiring-layer concern kept out of the recovery seam.
///
/// # Primary-side seams exist on every role
///
/// The tracker, the [`PrimaryReplicationHandler`] and the
/// [`ReplicationQuorumChecker`] are built for *all* roles, not just a
/// boot-configured primary. A node can become a primary at runtime
/// (`REPLICAOF NO ONE`, cluster failover), and construction-time gating made
/// every primary-side seam permanently absent on such a node: `CONFIG SET
/// self-fence-on-replica-loss` / `replication-lag-threshold-*` /
/// `min-replicas-to-write` reported success with nothing behind them, the write
/// gate saw no quorum checker and accepted writes with zero replicas forever,
/// and PSYNC stayed refused. Building the seams once, up front, and gating
/// *behavior* on the live role flag makes a promotion take effect with no
/// post-construction wiring that can silently no-op — the same "decide at the
/// point of use" rule [`crate::cluster_flags`] follows.
///
/// What stays role-dependent:
/// - Broadcasting is wrapped in [`RoleGatedBroadcaster`], so a replica (or a
///   demoted former primary that still holds tracker entries) ships nothing.
/// - Persisting primary state is gated on the live flag at the call sites
///   (pre-snapshot hook, shutdown), because the primary and replica handlers
///   share one `replication.state_file`.
/// - Only a replica boot builds a [`ReplicaReplicationHandler`] and dials a
///   primary; a runtime demotion starts its own stream via the `RoleManager`.
pub(super) fn init_replication(
    config: &Config,
    recovered_replication: &frogdb_core::ReplicationState,
    rocks_store: &Option<Arc<RocksStore>>,
    shard_senders: &Arc<Vec<frogdb_core::ShardSender>>,
    _metrics_recorder: &Arc<dyn MetricsRecorder>,
    // The process-wide live role flag (minted in phase 1, owned by the
    // `RoleManager` from phase 3 on). Gates broadcasting, so promotion/demotion
    // reroutes the write stream without rebuilding any shard worker.
    is_replica_flag: &Arc<std::sync::atomic::AtomicBool>,
    #[cfg(not(feature = "turmoil"))] tls_runtime: &Option<
        Arc<crate::tls_runtime::TlsRuntimeHandle>,
    >,
) -> Result<ReplicationInitResult> {
    let mut replica_handler: Option<Arc<ReplicaReplicationHandler>> = None;
    let mut replica_frame_rx: Option<mpsc::Receiver<frogdb_core::ReplicationFrame>> = None;
    let mut primary_addr: Option<std::net::SocketAddr> = None;

    let state_path = config
        .persistence
        .data_dir
        .join(&config.replication.state_file);

    // Primary-side seams, built unconditionally (see the fn doc).
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    // One identity cell per process, built before the role branch and shared by
    // both handlers: the replication id, the failover window and the live
    // offset belong to the *node*, not to whichever role is running right now.
    // It also adopts the tracker's offset atomic, so INFO, the cluster bus's
    // HealthProbe and a promotion's window boundary all read one value.
    let identity = ReplicationIdentity::adopting(recovered_replication.clone(), &tracker);
    let primary_handler = Arc::new(PrimaryReplicationHandler::new(
        identity.clone(),
        state_path.clone(),
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
    // A FULLRESYNC checkpoint must contain every write this node has already
    // acknowledged: it is the sole carrier of the writes made before the replica
    // attached (nothing was broadcast then, so no backlog tail can replay them).
    // The replication crate owns no shards, so the drain is injected here — the
    // same quiesce the snapshot coordinator's pre-snapshot hook performs.
    {
        let senders = shard_senders.clone();
        primary_handler.set_pre_checkpoint_hook(Arc::new(move || {
            let senders = senders.clone();
            Box::pin(async move {
                super::checkpoint_quiesce::quiesce_shards_for_checkpoint(&senders).await;
            })
        }));
    }
    // Issue 67: with `persistence.enabled = false` there is no RocksDB to
    // checkpoint, and a full resync that carries no dataset leaves the replica
    // serving its own stale keyspace. Wired for every role and every config —
    // a node can be told to serve a full resync at any point, and the reader is
    // only consulted when there is no store to checkpoint.
    primary_handler.set_live_snapshot_source(crate::replication::live_snapshot_source(
        shard_senders.clone(),
    ));
    let replication_broadcaster: SharedBroadcaster = Arc::new(RoleGatedBroadcaster {
        inner: primary_handler.clone(),
        is_replica: is_replica_flag.clone(),
    });

    // The cluster-bus HealthProbe offset handle is the identity's live atomic —
    // the single one both role handlers advance — so the failure detector keeps
    // reading this node's offset across a promotion or demotion instead of a
    // handle only one role ever writes. Minted for every role, because any node
    // can become a replica at runtime.
    let shared_replication_offset: Option<Arc<AtomicU64>> =
        config.cluster.enabled.then(|| identity.live());

    if config.replication.is_replica() {
        // Initialize ReplicaReplicationHandler for replica role
        let resolved_primary_addr = format!(
            "{}:{}",
            config.replication.primary_host, config.replication.primary_port
        )
        .parse::<std::net::SocketAddr>()
        .map_err(|e| anyhow::anyhow!("Invalid primary address: {}", e))?;
        primary_addr = Some(resolved_primary_addr);

        info!(
            primary = %resolved_primary_addr,
            replication_id = %identity.replication_id(),
            offset_at_save = recovered_replication.offset_at_save,
            "Initialized replica replication state"
        );

        let (mut handler, frame_rx) = ReplicaReplicationHandler::new(
            resolved_primary_addr,
            config.server.port,
            identity.clone(),
            state_path.clone(),
            config.persistence.data_dir.clone(),
        );
        handler.set_ack_interval(config.replication.ack_interval_ms);
        // Issue 61: a received full resync must land in the live keyspace, not
        // just on disk for the next boot.
        handler.set_snapshot_installer(crate::replication::LiveSnapshotInstaller::for_config(
            config,
            shard_senders.clone(),
        ));

        // Under turmoil simulation the replica must dial its primary through
        // turmoil's simulated network — the default factory's
        // `tokio::net::TcpStream::connect` would try (and fail) to leave the
        // simulation. Mirrors the `crate::net` alias the inbound listener uses.
        #[cfg(feature = "turmoil")]
        {
            let factory: frogdb_replication::replica::ConnectFactory =
                Arc::new(|addr: std::net::SocketAddr| {
                    Box::pin(async move {
                        let stream = turmoil::net::TcpStream::connect(addr).await?;
                        Ok(Box::new(stream) as frogdb_replication::BoxedStream)
                    })
                        as std::pin::Pin<
                            Box<
                                dyn std::future::Future<
                                        Output = std::io::Result<frogdb_replication::BoxedStream>,
                                    > + Send,
                            >,
                        >
                });
            handler.set_connect_factory(factory);
        }

        // Wire up TLS connection factory for encrypted replication.
        // Captures Arc<TlsManager> (not a snapshot connector) so that
        // certificate hot-reload propagates to new outgoing connections.
        #[cfg(not(feature = "turmoil"))]
        if config.tls.enabled
            && config.tls.tls_replication
            && let Some(handle) = tls_runtime
        {
            let mgr = handle.manager().clone();
            let handshake_timeout = handle.handshake_timeout();
            let factory: frogdb_replication::replica::ConnectFactory =
                Arc::new(move |addr: std::net::SocketAddr| {
                    let mgr = mgr.clone();
                    // Timeout read per dial so a change applies without a restart.
                    let handshake_timeout = handshake_timeout.get();
                    Box::pin(async move {
                        let connector = mgr.connector().ok_or_else(|| {
                            std::io::Error::other("TLS client connector not configured")
                        })?;
                        crate::tls::tls_connect(&connector, addr, handshake_timeout).await
                    })
                        as std::pin::Pin<
                            Box<
                                dyn std::future::Future<
                                        Output = std::io::Result<frogdb_replication::BoxedStream>,
                                    > + Send,
                            >,
                        >
                });
            handler.set_connect_factory(factory);
            info!("Replication TLS enabled for outgoing replica connections");
        }

        // Adopt the HealthProbe atomic explicitly so `shared_offset()` reports
        // it as wired. It is the same `Arc` the identity already holds, so this
        // is an identity swap, not a second atomic.
        if config.cluster.enabled {
            handler.set_shared_offset(identity.live());
        }

        replica_handler = Some(Arc::new(handler));
        replica_frame_rx = Some(frame_rx);
    } else {
        info!(
            replication_id = %recovered_replication.replication_id,
            offset_at_save = recovered_replication.offset_at_save,
            standalone = config.replication.is_standalone(),
            "Initialized primary replication state"
        );
    }

    // Create the replication quorum checker for primary self-fencing. It is
    // installed regardless of the toggle *and* regardless of role: both
    // `self-fence-on-replica-loss` and `replica-freshness-timeout-ms` are live
    // atomics inside the checker, and the checker never fences until a replica
    // has actually streamed from this node (arming), so a present-but-disabled
    // — or present-on-a-replica — checker behaves exactly like the absent one it
    // replaces (`has_quorum()` is unconditionally true) while leaving `CONFIG
    // SET` a seam that survives a promotion.
    let replication_self_fence = Some(Arc::new(ReplicationQuorumChecker::new(
        tracker.clone(),
        config.replication.self_fence_on_replica_loss,
        Duration::from_millis(config.replication.replica_freshness_timeout_ms),
    )));
    let replication_quorum_checker: Option<Arc<dyn frogdb_core::command::QuorumChecker>> =
        replication_self_fence
            .clone()
            .map(|c| c as Arc<dyn frogdb_core::command::QuorumChecker>);

    Ok(ReplicationInitResult {
        replication_self_fence,
        replication_broadcaster,
        replication_tracker: Some(tracker),
        replica_handler,
        replica_frame_rx,
        primary_replication_handler: Some(primary_handler),
        shared_replication_offset,
        replication_quorum_checker,
        primary_addr,
        replication_identity: identity,
    })
}

/// Broadcaster that ships frames only while this node is a primary.
///
/// The primary handler is constructed on every role so a promotion has live
/// seams (see [`init_replication`]), which means the shard workers hold a
/// broadcaster that must stay silent until this node actually is a primary.
/// Gating here rather than at construction is what makes `REPLICAOF NO ONE`
/// reroute the write stream: the shard workers keep the same broadcaster for
/// the whole process lifetime and the role flag decides, per write, whether it
/// carries.
///
/// A demoted former primary is the reason this is not merely defensive: its
/// tracker still holds replica entries until those sessions break, so
/// `is_active()` would stay true and internal removals (expiry/eviction
/// propagation) could still be broadcast by a node that no longer owns the
/// dataset history.
struct RoleGatedBroadcaster {
    inner: Arc<PrimaryReplicationHandler>,
    is_replica: Arc<std::sync::atomic::AtomicBool>,
}

impl RoleGatedBroadcaster {
    /// `Acquire`, pairing with the `Release` store in `RoleManager::promote`: a
    /// writer that observes "primary" must also observe the freshly minted
    /// replication id and armed backlog the promotion published before the flag.
    /// A `Relaxed` load could see the flag alone and stamp a write under the
    /// inherited identity.
    fn is_primary(&self) -> bool {
        !self.is_replica.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl ReplicationBroadcaster for RoleGatedBroadcaster {
    fn broadcast_command_on_shard(&self, shard_id: u16, cmd_name: &str, args: &[Bytes]) -> u64 {
        if !self.is_primary() {
            return 0;
        }
        self.inner
            .broadcast_command_on_shard(shard_id, cmd_name, args)
    }

    fn broadcast_transaction_on_shard(&self, shard_id: u16, commands: &[(&str, &[Bytes])]) -> u64 {
        if !self.is_primary() {
            return 0;
        }
        self.inner
            .broadcast_transaction_on_shard(shard_id, commands)
    }

    /// Both conditions the write path cares about: this node is a primary *and*
    /// some replica is attached. The shard worker skips the whole propagation
    /// pipeline when this is false.
    fn is_active(&self) -> bool {
        self.is_primary() && self.inner.is_active()
    }

    fn current_offset(&self) -> u64 {
        // The inherent accessor shadows the trait method here; both read the
        // same OffsetCoordinator value.
        self.inner.current_offset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::ReplicationIdentity;
    use frogdb_replication::ReplicationState;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn handler(
        dir: &std::path::Path,
        identity: ReplicationIdentity,
    ) -> Arc<PrimaryReplicationHandler> {
        Arc::new(PrimaryReplicationHandler::new(
            identity,
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
                max_entries: 128,
                max_bytes: 1024 * 1024,
            },
            1000,
        ))
    }

    /// A replica must never originate stream bytes, whatever the backlog says
    /// (Redis `replicationFeedSlaves` returns early on `masterhost != NULL`).
    /// After promotion the freshly armed backlog floor makes the same node
    /// active with zero replicas attached — which is what stamps and buffers
    /// every post-promotion write, so a sibling's `+CONTINUE` cannot resume
    /// past a hole.
    #[test]
    fn role_gated_broadcaster_activates_only_after_promotion() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ReplicationIdentity::detached(ReplicationState::new());
        let primary = handler(dir.path(), identity);
        let is_replica = Arc::new(AtomicBool::new(true));
        let broadcaster = RoleGatedBroadcaster {
            inner: primary.clone(),
            is_replica: is_replica.clone(),
        };

        // Still a replica: silent even once the identity has a window armed.
        primary.begin_primary_stint().unwrap();
        assert!(
            !broadcaster.is_active(),
            "a replica must not originate stream bytes"
        );
        assert_eq!(broadcaster.broadcast_command_on_shard(0, "SET", &[]), 0);

        // Promotion opens the fence; the armed floor keeps it active with no
        // replicas attached.
        is_replica.store(false, Ordering::Release);
        assert!(broadcaster.is_active());
    }

    /// A node that never was a primary and never got promoted has no resume
    /// history, so writes stay unstamped and the propagation pipeline is
    /// skipped entirely.
    #[test]
    fn standalone_primary_without_history_stays_inactive() {
        let dir = tempfile::tempdir().unwrap();
        let identity = ReplicationIdentity::detached(ReplicationState::new());
        let primary = handler(dir.path(), identity);
        let broadcaster = RoleGatedBroadcaster {
            inner: primary,
            is_replica: Arc::new(AtomicBool::new(false)),
        };

        assert!(!broadcaster.is_active());
    }
}
