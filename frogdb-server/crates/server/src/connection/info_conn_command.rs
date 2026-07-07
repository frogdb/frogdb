//! INFO connection command.
//!
//! INFO is migrated behind the [`ConnectionCommand`] seam (see
//! [`crate::connection::conn_command`] and the CONFIG executor there for the
//! template). It is the widest read surface in the connection tree — it
//! aggregates observability, config, replication/cluster, and per-shard state —
//! but it is *pure* read aggregation with no connection-state mutation, so the
//! whole gather-and-render is exposed through a single read-only
//! [`frogdb_core::ConnCtx::info`] provider ([`frogdb_core::InfoProvider`]). The
//! provider is implemented for `ConnectionHandler` in
//! [`crate::connection::handlers::info`], where the server-only aggregation
//! types (`InfoSources`, `InfoBuilder`, replication/cluster state) can be named;
//! the section rendering itself lives in [`crate::info`] and is untouched by the
//! migration, so INFO's wire output is byte-for-byte identical.
//!
//! The shard-local INFO ([`crate::commands::info::InfoCommand`]) is a *separate*
//! command: `redis.call('INFO')` runs on the owning shard and reports that
//! shard's own view (a shard cannot do the fleet scatter this connection-level
//! path does). It stays registered as a `CommandImpl::Shard` executor for
//! scripts; only the connection-level entry moves behind this seam.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnectionCommand,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// The `CommandSpec` for INFO. Declared here alongside the executor (rather than
/// in a stub `Command` impl) so the connection command is a single
/// self-contained unit. These fields mirror the original INFO registration
/// exactly — arity `AtLeast(0)`, `READONLY | LOADING | STALE`, strategy
/// `ConnectionLevel(Admin)` — so `COMMAND`/`get_entry` metadata is unchanged.
/// The registry validates that the strategy agrees with the `Connection`
/// executor variant.
static INFO_SPEC: CommandSpec = CommandSpec {
    name: "INFO",
    arity: Arity::AtLeast(0),
    flags: CommandFlags::READONLY
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` INFO executor. Registered via
/// [`frogdb_core::CommandRegistry::register_connection`] in `server/register.rs`.
pub(crate) static INFO_CONN_COMMAND: InfoConnCommand = InfoConnCommand;

/// INFO — render server information across the fleet plus connection-level
/// sources. Reads only [`ConnCtx::info`], delegating the aggregation to the
/// [`frogdb_core::InfoProvider`] impl on `ConnectionHandler`.
pub(crate) struct InfoConnCommand;

impl ConnectionCommand for InfoConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &INFO_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move { ctx.info.render(args).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_is_connection_level_and_valid() {
        assert!(INFO_CONN_COMMAND.spec().validate().is_ok());
        assert!(matches!(
            INFO_CONN_COMMAND.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
        ));
    }

    #[tokio::test]
    async fn execute_delegates_to_info_provider() {
        use frogdb_core::{
            ClientRegistry, CommandLatencyHistograms, KeyspaceStats, NoopInfoProvider,
            SharedHotkeySession, new_shared_hotkey_session,
        };
        use frogdb_protocol::ProtocolVersion;

        // A ConnCtx whose `info` is the no-op provider: the executor must return
        // exactly what the provider yields (here, an empty bulk string), proving
        // it delegates rather than rendering anything itself.
        let client_registry = ClientRegistry::new();
        let latency_histograms = CommandLatencyHistograms::new(true);
        let keyspace_stats = KeyspaceStats::new();
        let snapshot_coordinator = frogdb_core::persistence::NoopSnapshotCoordinator::new();
        let hotkey_session: SharedHotkeySession = new_shared_hotkey_session();
        let cluster = crate::connection::ClusterDeps::standalone();
        let cursor_store = crate::cursor_store::AggregateCursorStore::new();
        let config = crate::runtime_config::ConfigManager::new(&crate::config::Config::default());
        let info = NoopInfoProvider;
        let acl_manager = frogdb_core::AclManager::new(Default::default());
        let command_registry = frogdb_core::CommandRegistry::new();
        let metrics_recorder = frogdb_core::NoopMetricsRecorder::new();
        let memory_diag = crate::connection::observability_conn_command::MemoryDiag(
            frogdb_debug::MemoryDiagConfig::default(),
        );

        let mut ctx = ConnCtx {
            config: &config,
            client_registry: &client_registry,
            latency_histograms: &latency_histograms,
            keyspace_stats: &keyspace_stats,
            shard_senders: &[],
            snapshot_coordinator: &snapshot_coordinator,
            hotkey_session: &hotkey_session,
            hotkey_cluster: &cluster,
            protocol_version: ProtocolVersion::Resp2,
            cursor_store: &cursor_store,
            metrics_recorder: &metrics_recorder,
            memory_diag: &memory_diag,
            num_shards: 0,
            max_clients: 10000,
            acl_manager: acl_manager.as_ref(),
            command_registry: &command_registry,
            username: "default",
            info: &info,
            conn_state: None,
            tracking: None,
        };

        let resp = InfoConnCommand.execute(&mut ctx, &[]).await;
        assert_eq!(resp, Response::bulk(Bytes::new()));
    }
}
