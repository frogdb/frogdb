//! Persistence connection commands (BGSAVE, LASTSAVE).
//!
//! These are migrated behind the [`ConnectionCommand`] seam (see
//! [`crate::connection::conn_command`] and the CONFIG executor there for the
//! template). Each command reads only the snapshot coordinator it needs through
//! [`ConnCtx::snapshot_coordinator`], instead of taking `&ConnectionHandler`,
//! so both are unit-testable in isolation (see `tests`).
//!
//! BGSAVE and LASTSAVE are two distinct commands (unlike CONFIG's single
//! subcommand dispatcher), so each is its own [`ConnectionCommand`] with its
//! own [`CommandSpec`], registered separately via
//! [`frogdb_core::CommandRegistry::register_connection`].

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnectionCommand,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// The `CommandSpec` for BGSAVE. Declared here alongside the executor (rather
/// than in a stub `Command` impl) so the connection command is a single
/// self-contained unit. Strategy is `ConnectionLevel(Persistence)`; the registry
/// validates that this agrees with the `Connection` executor variant.
static BGSAVE_SPEC: CommandSpec = CommandSpec {
    name: "BGSAVE",
    arity: Arity::Range { min: 0, max: 1 },
    flags: CommandFlags::ADMIN,
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence),
};

/// The `CommandSpec` for LASTSAVE.
static LASTSAVE_SPEC: CommandSpec = CommandSpec {
    name: "LASTSAVE",
    arity: Arity::Fixed(0),
    flags: CommandFlags::READONLY
        .union(CommandFlags::FAST)
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence),
};

/// The registrable, `'static` BGSAVE executor. Registered via
/// [`frogdb_core::CommandRegistry::register_connection`] in `server/register.rs`.
pub(crate) static BGSAVE_CONN_COMMAND: BgsaveConnCommand = BgsaveConnCommand;

/// The registrable, `'static` LASTSAVE executor.
pub(crate) static LASTSAVE_CONN_COMMAND: LastsaveConnCommand = LastsaveConnCommand;

/// BGSAVE — trigger a background snapshot.
pub(crate) struct BgsaveConnCommand;

impl ConnectionCommand for BgsaveConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &BGSAVE_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move { handle_bgsave(ctx, args) })
    }
}

/// LASTSAVE — return the Unix timestamp of the last successful save.
pub(crate) struct LastsaveConnCommand;

impl ConnectionCommand for LastsaveConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &LASTSAVE_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move { handle_lastsave(ctx) })
    }
}

/// BGSAVE `[SCHEDULE]` — start a background snapshot, or schedule one if a save
/// is already running.
fn handle_bgsave(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    // Check for SCHEDULE option.
    if !args.is_empty() {
        let opt = args[0].to_ascii_uppercase();
        if opt.as_slice() == b"SCHEDULE" {
            // BGSAVE SCHEDULE - schedule a save if one is already running,
            // otherwise start immediately.
            if ctx.snapshot_coordinator.in_progress() {
                ctx.snapshot_coordinator.schedule_snapshot();
                return Response::Simple(Bytes::from_static(b"Background saving scheduled"));
            }
            // No save in progress, fall through to start one immediately.
        }
    }

    match ctx.snapshot_coordinator.start_snapshot() {
        Ok(handle) => {
            tracing::info!(epoch = handle.epoch(), "BGSAVE started");
            Response::Simple(Bytes::from_static(b"Background saving started"))
        }
        Err(frogdb_core::persistence::SnapshotError::AlreadyInProgress) => {
            // Return a simple status like Redis does.
            Response::Simple(Bytes::from_static(b"Background save already in progress"))
        }
        Err(e) => Response::error(format!("ERR {}", e)),
    }
}

/// LASTSAVE — return the Unix timestamp of the last successful save.
fn handle_lastsave(ctx: &ConnCtx<'_>) -> Response {
    use std::time::{SystemTime, UNIX_EPOCH};

    match ctx.snapshot_coordinator.last_save_time() {
        Some(instant) => {
            // Convert Instant to Unix timestamp: work out how long ago the save
            // was and subtract from the current wall-clock time.
            let elapsed = instant.elapsed();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            let save_time = now.as_secs().saturating_sub(elapsed.as_secs());
            Response::Integer(save_time as i64)
        }
        None => {
            // No snapshot has been taken yet.
            Response::Integer(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ClusterDeps;
    use crate::connection::observability_conn_command::MemoryDiag;
    use crate::cursor_store::AggregateCursorStore;
    use frogdb_core::persistence::{NoopSnapshotCoordinator, SnapshotCoordinator};
    use frogdb_core::{
        ClientRegistry, CommandLatencyHistograms, KeyspaceStats, NoopMetricsRecorder,
        SharedHotkeySession, new_shared_hotkey_session,
    };
    use frogdb_protocol::ProtocolVersion;

    /// Build a `ConnCtx` over fixture dependencies — no socket, no
    /// `ConnectionHandler`. Only the snapshot coordinator is exercised by these
    /// commands; the rest are unused placeholders.
    struct Fixture {
        snapshot_coordinator: NoopSnapshotCoordinator,
        client_registry: ClientRegistry,
        latency_histograms: CommandLatencyHistograms,
        keyspace_stats: KeyspaceStats,
        config_manager: crate::runtime_config::ConfigManager,
        hotkey_session: SharedHotkeySession,
        cluster: ClusterDeps,
        cursor_store: AggregateCursorStore,
        metrics_recorder: NoopMetricsRecorder,
        memory_diag: MemoryDiag,
        acl_manager: std::sync::Arc<frogdb_core::AclManager>,
        command_registry: frogdb_core::CommandRegistry,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                snapshot_coordinator: NoopSnapshotCoordinator::new(),
                client_registry: ClientRegistry::new(),
                latency_histograms: CommandLatencyHistograms::new(true),
                keyspace_stats: KeyspaceStats::new(),
                config_manager: crate::runtime_config::ConfigManager::new(
                    &crate::config::Config::default(),
                ),
                hotkey_session: new_shared_hotkey_session(),
                cluster: ClusterDeps::standalone(),
                cursor_store: AggregateCursorStore::new(),
                metrics_recorder: NoopMetricsRecorder::new(),
                memory_diag: MemoryDiag(frogdb_debug::MemoryDiagConfig::default()),
                acl_manager: frogdb_core::AclManager::new(Default::default()),
                command_registry: frogdb_core::CommandRegistry::new(),
            }
        }

        fn ctx(&self) -> ConnCtx<'_> {
            ConnCtx {
                config: &self.config_manager,
                client_registry: &self.client_registry,
                latency_histograms: &self.latency_histograms,
                keyspace_stats: &self.keyspace_stats,
                shard_senders: &[],
                snapshot_coordinator: &self.snapshot_coordinator,
                hotkey_session: &self.hotkey_session,
                hotkey_cluster: &self.cluster,
                protocol_version: ProtocolVersion::Resp2,
                cursor_store: &self.cursor_store,
                metrics_recorder: &self.metrics_recorder,
                memory_diag: &self.memory_diag,
                num_shards: 0,
                max_clients: 10000,
                acl_manager: self.acl_manager.as_ref(),
                command_registry: &self.command_registry,
                username: "default",
                info: &frogdb_core::NoopInfoProvider,
                scripting: &frogdb_core::NoopScriptingProvider,
                conn_state: None,
                tracking: None,
            }
        }
    }

    fn arg(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[tokio::test]
    async fn bgsave_starts_a_snapshot() {
        let fx = Fixture::new();
        let resp = BgsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            resp,
            Response::Simple(Bytes::from_static(b"Background saving started"))
        );
    }

    #[tokio::test]
    async fn bgsave_reports_already_in_progress() {
        let fx = Fixture::new();
        // Leak a handle so the snapshot stays in progress.
        let handle = fx.snapshot_coordinator.start_snapshot().unwrap();
        std::mem::forget(handle);
        let resp = BgsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            resp,
            Response::Simple(Bytes::from_static(b"Background save already in progress"))
        );
    }

    #[tokio::test]
    async fn bgsave_schedule_while_in_progress_schedules() {
        let fx = Fixture::new();
        let handle = fx.snapshot_coordinator.start_snapshot().unwrap();
        std::mem::forget(handle);
        let resp = BgsaveConnCommand
            .execute(&mut fx.ctx(), &[arg("SCHEDULE")])
            .await;
        assert_eq!(
            resp,
            Response::Simple(Bytes::from_static(b"Background saving scheduled"))
        );
        assert!(fx.snapshot_coordinator.is_scheduled());
    }

    #[tokio::test]
    async fn lastsave_returns_zero_when_never_saved() {
        let fx = Fixture::new();
        let resp = LastsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(resp, Response::Integer(0));
    }

    #[tokio::test]
    async fn lastsave_returns_timestamp_after_save() {
        let fx = Fixture::new();
        let handle = fx.snapshot_coordinator.start_snapshot().unwrap();
        drop(handle);
        let resp = LastsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        match resp {
            Response::Integer(ts) => assert!(ts > 0, "expected a positive last-save timestamp"),
            other => panic!("expected integer, got {other:?}"),
        }
    }

    #[test]
    fn specs_are_connection_level_and_valid() {
        assert!(BGSAVE_CONN_COMMAND.spec().validate().is_ok());
        assert!(LASTSAVE_CONN_COMMAND.spec().validate().is_ok());
        assert!(matches!(
            BGSAVE_CONN_COMMAND.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence)
        ));
        assert!(matches!(
            LASTSAVE_CONN_COMMAND.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence)
        ));
    }
}
