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
use frogdb_protocol::{Response, SafeStatus};

/// The `CommandSpec` for BGSAVE. Declared here alongside the executor (rather
/// than in a stub `Command` impl) so the connection command is a single
/// self-contained unit. Strategy is `ConnectionLevel(Persistence)`; the registry
/// validates that this agrees with the `Connection` executor variant.
static BGSAVE_SPEC: CommandSpec = CommandSpec {
    name: "BGSAVE",
    docs: frogdb_core::CommandDocs {
        summary: "Asynchronously saves the database(s) to disk.",
        since: "1.0.0",
        group: "server",
        complexity: Some("O(1)"),
    },
    arity: Arity::Range { min: 0, max: 1 },
    flags: CommandFlags::ADMIN.union(CommandFlags::NOSCRIPT),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    reindex: frogdb_core::ReindexSpec::None,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Persistence),
};

/// The `CommandSpec` for LASTSAVE.
static LASTSAVE_SPEC: CommandSpec = CommandSpec {
    name: "LASTSAVE",
    docs: frogdb_core::CommandDocs {
        summary: "Returns the Unix timestamp of the last successful save to disk.",
        since: "1.0.0",
        group: "server",
        complexity: Some("O(1)"),
    },
    arity: Arity::Fixed(0),
    flags: CommandFlags::FAST
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    reindex: frogdb_core::ReindexSpec::None,
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
            Response::Simple(SafeStatus::from_static("Background saving started"))
        }
        SnapshotRequest::Coalesced => {
            Response::Simple(SafeStatus::from_static("Background saving scheduled"))
        }
        SnapshotRequest::AlreadyRunning => {
            // KNOWN DIVERGENCE FROM REDIS (pinned by
            // `bgsave_overlap_observes_already_running_on_real_coordinator` in
            // `tests` below, testing-gap issue 45 / audit D#6): Redis 8.6 rejects
            // an overlapping `BGSAVE` with a `-ERR` error reply
            // ("Background save already in progress"). FrogDB instead replies
            // with a `+` simple string here, so a RESP client that only treats
            // `-`-prefixed replies as errors will read this as a successful
            // acknowledgement rather than a rejected request. This is a real,
            // tested deviation, not a deliberate compatibility choice — pinned
            // as-is for now; switching to `Response::Error` to match Redis is a
            // follow-up decision (it changes client-visible behavior), out of
            // scope for this test-only pass.
            Response::Simple(SafeStatus::from_static(
                "Background save already in progress",
            ))
        }
    }
}

/// LASTSAVE — return the Unix timestamp of the last successful save.
fn handle_lastsave(ctx: &ConnCtx<'_>) -> Response {
    use std::time::UNIX_EPOCH;

    match ctx.snapshot_coordinator.last_save_time() {
        // The coordinator keeps wall-clock time, seeded at boot from the newest
        // complete snapshot's own `completed_at_ms`, so this is the save's real
        // timestamp even when the save predates the process. No elapsed-time
        // arithmetic, hence no truncation footgun.
        Some(saved_at) => Response::Integer(
            saved_at
                .duration_since(UNIX_EPOCH)
                .map(|since_epoch| since_epoch.as_secs() as i64)
                .unwrap_or(0),
        ),
        // No snapshot has ever been taken (Redis reports 0 here too).
        None => Response::Integer(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ClusterDeps;
    use crate::connection::observability_conn_command::MemoryDiag;
    use crate::cursor_store::AggregateCursorStore;
    use frogdb_core::persistence::{
        NoopSnapshotCoordinator, RocksConfig, RocksSnapshotCoordinator, RocksStore, SnapshotConfig,
        SnapshotCoordinator, SnapshotMode,
    };
    use frogdb_core::{
        ClientRegistry, CommandLatencyHistograms, KeyspaceStats, NoopMetricsRecorder,
        SharedHotkeySession, new_shared_hotkey_session,
    };
    use std::sync::Arc;
    use std::time::Duration;

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

    // FM-PERSISTENCE-015
    #[tokio::test]
    async fn bgsave_starts_a_snapshot() {
        let fx = Fixture::new();
        let resp = BgsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            resp,
            Response::Simple(SafeStatus::from_static("Background saving started"))
        );
    }

    // FM-PERSISTENCE-022
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
            Response::Simple(SafeStatus::from_static("Background saving started"))
        );
        let second = BgsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            second,
            Response::Simple(SafeStatus::from_static("Background saving started"))
        );
    }

    // FM-PERSISTENCE-015
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
            Response::Simple(SafeStatus::from_static("Background saving started"))
        );
    }

    // FM-PERSISTENCE-022
    #[tokio::test]
    async fn lastsave_returns_zero_when_never_saved() {
        let fx = Fixture::new();
        let resp = LastsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(resp, Response::Integer(0));
    }

    // FM-PERSISTENCE-022
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

    // ========================================================================
    // Real (non-`Noop`) coordinator tests — testing-gap issue 45 / audit D#6+D#7
    //
    // The tests above only ever exercise `NoopSnapshotCoordinator`, which
    // completes a save synchronously: there is never a save genuinely in flight
    // to overlap (`AlreadyRunning` never fires), and `last_save_time()` is always
    // "just now" so the ±1s double-truncation bug in `handle_lastsave` can never
    // be observed. `RealFixture` below wires the actual `RocksSnapshotCoordinator`
    // (a real, temp-dir-backed RocksDB) through the same `ConnCtx` seam so
    // `BgsaveConnCommand`/`LastsaveConnCommand` run unmodified against
    // production persistence.
    // ========================================================================

    /// Fixture mirroring `Fixture` field-for-field, but with a real
    /// `RocksSnapshotCoordinator` in place of `NoopSnapshotCoordinator`.
    struct RealFixture {
        // Kept alive for the fixture's lifetime (RAII cleanup on drop); never
        // read directly, hence the leading underscores.
        _db_dir: tempfile::TempDir,
        snapshot_dir: tempfile::TempDir,
        snapshot_coordinator: RocksSnapshotCoordinator,
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

    impl RealFixture {
        fn new() -> Self {
            let db_dir = tempfile::tempdir().unwrap();
            let snapshot_dir = tempfile::tempdir().unwrap();
            let rocks_store = Arc::new(
                RocksStore::open(db_dir.path(), 1, &RocksConfig::default())
                    .expect("open a fresh RocksDB for the fixture"),
            );
            let snapshot_coordinator = RocksSnapshotCoordinator::new(
                rocks_store,
                SnapshotConfig {
                    snapshot_dir: snapshot_dir.path().to_path_buf(),
                    snapshot_interval_secs: 3600,
                    max_snapshots: 5,
                },
                Arc::new(NoopMetricsRecorder::new()),
                db_dir.path().to_path_buf(),
            )
            .expect("construct a coordinator over a fresh snapshot dir");
            Self {
                _db_dir: db_dir,
                snapshot_dir,
                snapshot_coordinator,
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

    /// Poll until the coordinator reports idle (a spawned save's background task
    /// has run to completion, success or failure), or panic after a generous
    /// bound — a hung save is a real test failure, not something to swallow.
    async fn wait_for_coordinator_idle(c: &RocksSnapshotCoordinator) {
        for _ in 0..500 {
            if !c.in_progress() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("snapshot coordinator never returned to idle");
    }

    fn unix_now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    // FM-PERSISTENCE-015
    /// D#6: overlap two real `BGSAVE`s so the second genuinely observes a save
    /// in flight. `RocksSnapshotCoordinator`'s pre-snapshot hook is used as a
    /// deterministic gate — the first save's background task blocks in the hook
    /// until the test releases it, so the second `BGSAVE`, issued while the
    /// first is parked there, is guaranteed (not just likely) to land on
    /// `AlreadyRunning`. This also pins the current reply's Redis divergence
    /// (see the comment on `SnapshotRequest::AlreadyRunning` in `handle_bgsave`).
    #[tokio::test]
    async fn bgsave_overlap_observes_already_running_on_real_coordinator() {
        use tokio::sync::Notify;

        let fx = RealFixture::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        {
            let entered = entered.clone();
            let release = release.clone();
            fx.snapshot_coordinator
                .set_pre_snapshot_hook(Arc::new(move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    Box::pin(async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                }));
        }

        let first = BgsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            first,
            Response::Simple(SafeStatus::from_static("Background saving started")),
            "first BGSAVE must claim the idle slot and start"
        );

        // Block until the spawned save has actually entered (and is now parked
        // in) the pre-snapshot hook, so the second call below unambiguously
        // overlaps a save genuinely in flight rather than racing it.
        entered.notified().await;
        assert!(
            fx.snapshot_coordinator.in_progress(),
            "save must be in flight once the hook has been entered"
        );

        let second = BgsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            second,
            Response::Simple(SafeStatus::from_static(
                "Background save already in progress"
            )),
            "overlapping BGSAVE must observe AlreadyRunning's pinned (Redis-diverging) reply"
        );

        // Release the first save so it completes cleanly; nothing should be
        // left blocked in the background when the test exits.
        release.notify_one();
        wait_for_coordinator_idle(&fx.snapshot_coordinator).await;
        assert!(
            fx.snapshot_coordinator.last_save_time().is_some(),
            "the released save should have completed and stamped last_save_time"
        );
    }

    // FM-PERSISTENCE-022
    /// D#7: `LASTSAVE` against the real coordinator returns `0` before any save,
    /// the actual last-save Unix time (within the ±1s the fixed single-truncation
    /// conversion should now hold to) after a completed save, and does not
    /// advance after a save that fails.
    #[tokio::test]
    async fn lastsave_tracks_real_bgsave_and_ignores_failed_saves() {
        let fx = RealFixture::new();

        // Before any save: the documented `0` sentinel.
        let before = LastsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(before, Response::Integer(0));

        // A real, successful BGSAVE (driven directly through the coordinator, as
        // `lastsave_returns_timestamp_after_save` above does against the Noop
        // one, so the assigned epoch is known for the sabotage step below).
        let epoch1 = fx
            .snapshot_coordinator
            .request_snapshot(SnapshotMode::Immediate);
        let epoch1 = match epoch1 {
            frogdb_core::persistence::SnapshotRequest::Started(e) => e,
            other => panic!("first request must start cleanly, got {other:?}"),
        };
        wait_for_coordinator_idle(&fx.snapshot_coordinator).await;
        assert!(fx.snapshot_coordinator.last_save_time().is_some());

        let now_secs = unix_now_secs();
        let ts1 = match LastsaveConnCommand.execute(&mut fx.ctx(), &[]).await {
            Response::Integer(ts) => ts,
            other => panic!("expected integer LASTSAVE reply, got {other:?}"),
        };
        assert!(
            (now_secs - ts1).abs() <= 1,
            "LASTSAVE {ts1} should be within 1s of the real current time {now_secs}"
        );

        // Force the *next* save (epoch1 + 1) to fail before anything durable
        // changes: plant a regular file at the exact staging path
        // `SnapshotStager::run` will `create_dir_all` into.  `create_dir_all`
        // errors when a path component already exists as a non-directory, so
        // checkpoint creation aborts cleanly — the same trick
        // `snapshot::tests::test_stager_checkpoint_failure_aborts_cleanly` uses.
        // This is deliberately not permission-bit based (e.g. chmod 0): that
        // approach silently no-ops under a root test runner, while a path
        // collision fails `create_dir_all` unconditionally.
        let epoch2 = epoch1 + 1;
        let sabotage = fx
            .snapshot_dir
            .path()
            .join(format!(".snapshot_{epoch2:05}.tmp"));
        std::fs::write(&sabotage, b"blocking file, not a directory").unwrap();

        let started2 = fx
            .snapshot_coordinator
            .request_snapshot(SnapshotMode::Immediate);
        match started2 {
            frogdb_core::persistence::SnapshotRequest::Started(e) => assert_eq!(
                e, epoch2,
                "sabotage must target the epoch the coordinator actually assigns next"
            ),
            other => {
                panic!("second request must still be accepted; it fails asynchronously: {other:?}")
            }
        }
        wait_for_coordinator_idle(&fx.snapshot_coordinator).await;

        let after_failed = LastsaveConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert_eq!(
            after_failed,
            Response::Integer(ts1),
            "a failed BGSAVE must not advance LASTSAVE"
        );
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
