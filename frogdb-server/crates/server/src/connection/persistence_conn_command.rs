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
    mutation: frogdb_core::ConnMutation::None,
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
    mutation: frogdb_core::ConnMutation::None,
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
    use frogdb_core::persistence::{SnapshotMode, SnapshotRequest};

    // With `SCHEDULE`, a save already in flight coalesces a follow-up; without it,
    // BGSAVE refuses without queuing. Both read the same coalesce decision through
    // one atomic seam (no caller-side check-then-act race).
    let is_schedule = !args.is_empty() && args[0].eq_ignore_ascii_case(b"SCHEDULE");
    let mode = if is_schedule {
        SnapshotMode::Schedule
    } else {
        SnapshotMode::Immediate
    };

    match ctx.snapshot_coordinator.request_snapshot(mode) {
        SnapshotRequest::Started(epoch) => {
            tracing::info!(epoch, "BGSAVE started");
            Response::Simple(Bytes::from_static(b"Background saving started"))
        }
        SnapshotRequest::Coalesced => {
            Response::Simple(Bytes::from_static(b"Background saving scheduled"))
        }
        SnapshotRequest::AlreadyRunning => {
            // Return a simple status like Redis does.
            Response::Simple(Bytes::from_static(b"Background save already in progress"))
        }
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
            ConnCtx::new(
                &self.config_manager,
                &self.client_registry,
                &self.latency_histograms,
                &self.keyspace_stats,
                &[],
                &self.snapshot_coordinator,
                &self.hotkey_session,
                &self.cluster,
                &self.cursor_store,
                &self.metrics_recorder,
                &self.memory_diag,
                self.acl_manager.as_ref(),
                &self.command_registry,
                0,
                10000,
                false,
            )
            .with_username("default")
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
    async fn bgsave_starts_each_time_under_instant_completion() {
        // The no-op coordinator now completes instantly (proposal 21): a save
        // releases the slot synchronously, so there is never a genuinely in-flight
        // save to observe. A leaked handle no longer pins `in_progress` — the
        // deliberate semantic flip — so back-to-back BGSAVEs each `Started`.
        let fx = Fixture::new();
        let first = BgsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            first,
            Response::Simple(Bytes::from_static(b"Background saving started"))
        );
        let second = BgsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            second,
            Response::Simple(Bytes::from_static(b"Background saving started"))
        );
    }

    #[tokio::test]
    async fn bgsave_schedule_starts_when_idle() {
        // With instant completion there is no save in flight to coalesce with, so
        // BGSAVE SCHEDULE `Started`s a fresh save (proposal 21). The mode split
        // (`Immediate` no-queue vs `Schedule` coalesce) is pinned at the scheduler
        // (`test_scheduler_request_mode_immediate_no_queue_vs_schedule_arms`).
        let fx = Fixture::new();
        let resp = BgsaveConnCommand
            .execute(&mut fx.ctx(), &[arg("SCHEDULE")])
            .await;
        assert_eq!(
            resp,
            Response::Simple(Bytes::from_static(b"Background saving started"))
        );
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
        // Instant completion: the save runs and stamps `last_save` synchronously
        // (the handle is a bare epoch carrier — nothing to drop/await).
        let _ = fx.snapshot_coordinator.start_snapshot().unwrap();
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
