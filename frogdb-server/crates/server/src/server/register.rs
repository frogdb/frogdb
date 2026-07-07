use frogdb_core::CommandRegistry;

/// Register all built-in commands.
pub fn register_commands(registry: &mut CommandRegistry) {
    // Register data-structure commands from the frogdb-commands crate.
    frogdb_commands::register_all(registry);

    // =====================================================================
    // Server-specific commands (depend on server infrastructure)
    // =====================================================================

    // AUTH / HELLO: migrated behind the ConnCtx seam as mutating connection
    // commands (they change per-connection auth/protocol state via
    // `ConnCtx::conn_state`). Registered as CommandImpl::Connection executors and
    // dispatched through the registry union — but intercepted *early*, before the
    // NOAUTH pre-check, so a not-yet-authenticated client can still run them.
    registry.register_connection(&crate::connection::auth_conn_command::AUTH_CONN_COMMAND);
    registry.register_connection(&crate::connection::auth_conn_command::HELLO_CONN_COMMAND);

    // Persistence commands. BGSAVE/LASTSAVE are migrated behind the ConnCtx seam
    // (registered as CommandImpl::Connection executors, dispatched through the
    // registry union rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::persistence_conn_command::BGSAVE_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::persistence_conn_command::LASTSAVE_CONN_COMMAND);
    registry.register(crate::commands::persistence::DumpCommand);
    registry.register(crate::commands::persistence::RestoreCommand);

    // Migration commands
    registry.register(crate::commands::migrate_cmd::MigrateCommand);

    // Transaction commands
    registry.register(crate::commands::transaction::MultiCommand);
    registry.register(crate::commands::transaction::ExecCommand);
    registry.register(crate::commands::transaction::DiscardCommand);
    registry.register(crate::commands::transaction::WatchCommand);
    registry.register(crate::commands::transaction::UnwatchCommand);

    // Scripting commands
    registry.register(crate::commands::scripting::EvalCommand);
    registry.register(crate::commands::scripting::EvalshaCommand);
    registry.register(crate::commands::scripting::EvalRoCommand);
    registry.register(crate::commands::scripting::EvalshaRoCommand);
    registry.register(crate::commands::scripting::ScriptCommand);

    // Server commands
    registry.register(crate::commands::server::DbsizeCommand);
    registry.register(crate::commands::server::FlushdbCommand);
    registry.register(crate::commands::server::FlushallCommand);
    registry.register(crate::commands::server::TimeCommand);
    registry.register(crate::commands::server::ShutdownCommand);
    registry.register(crate::commands::server::LolwutCommand);

    // Info command. Two distinct executors share the name "INFO":
    //   - The shard-local `InfoCommand` (registered here as a
    //     `CommandImpl::Shard`) serves `redis.call('INFO')` inside scripts, which
    //     runs on the owning shard and reports that shard's own view. Scripts
    //     resolve it through the `commands` map (`CommandRegistry::get`).
    //   - The connection-level `INFO_CONN_COMMAND` (registered just below as a
    //     `CommandImpl::Connection`) does the fleet scatter + connection-level
    //     aggregation for clients, dispatched through the registry union.
    // `register_connection` must run *after* `register` so the shared `entries`
    // slot for "INFO" ends up holding the `Connection` executor (connection-level
    // dispatch), while `commands["INFO"]` keeps the shard executor (scripts).
    registry.register(crate::commands::info::InfoCommand);
    registry.register_connection(&crate::connection::info_conn_command::INFO_CONN_COMMAND);

    // CLIENT: migrated behind the ConnCtx seam as a mutating connection command
    // (it changes per-connection name/reply/tracking/caching state via
    // `ConnCtx::conn_state` and drives tracking IO via `ConnCtx::tracking`).
    // Registered as a CommandImpl::Connection executor, dispatched through the
    // registry union rather than the legacy router→connection-handler path.
    registry.register_connection(&crate::connection::client_conn_command::CLIENT_CONN_COMMAND);

    // Config commands (migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::conn_command::CONFIG_CONN_COMMAND);

    // Slowlog commands (migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry
        .register_connection(&crate::connection::observability_conn_command::SLOWLOG_CONN_COMMAND);

    // ACL migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path.
    registry.register_connection(&crate::connection::acl_conn_command::ACL_CONN_COMMAND);

    // Pub/Sub commands (migrated behind the ConnCtx seam: registered as
    // CommandImpl::Connection executors, dispatched through the registry union
    // via the multi-response `execute_multi` seam — SUBSCRIBE/UNSUBSCRIBE emit
    // one confirmation per channel — rather than the legacy
    // router→connection-handler path).
    registry.register_connection(&crate::connection::pubsub_conn_command::SUBSCRIBE_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::PSUBSCRIBE_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::SSUBSCRIBE_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::UNSUBSCRIBE_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::pubsub_conn_command::PUNSUBSCRIBE_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::pubsub_conn_command::SUNSUBSCRIBE_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::PUBLISH_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::SPUBLISH_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::PUBSUB_CONN_COMMAND);

    // Cluster
    registry.register(crate::commands::cluster::ClusterCommand);

    // Connection-state commands (RESET / ASKING / READONLY / READWRITE): migrated
    // behind the ConnCtx seam as *mutating* connection commands (they change
    // per-connection state via `ConnCtx::conn_state`). Registered as
    // CommandImpl::Connection executors and dispatched through the mutable
    // `conn_ctx_authmut` view rather than the legacy router→connection-handler
    // path. RESET is intercepted early (direct dispatch, never queued/paused);
    // ASKING/READONLY/READWRITE dispatch through the mutable registry union.
    registry
        .register_connection(&crate::connection::connection_state_conn_command::RESET_CONN_COMMAND);
    registry.register_connection(
        &crate::connection::connection_state_conn_command::ASKING_CONN_COMMAND,
    );
    registry.register_connection(
        &crate::connection::connection_state_conn_command::READONLY_CONN_COMMAND,
    );
    registry.register_connection(
        &crate::connection::connection_state_conn_command::READWRITE_CONN_COMMAND,
    );

    // Replication
    registry.register(crate::commands::replication::ReplicaofCommand);
    registry.register(crate::commands::replication::SlaveofCommand);
    registry.register(crate::commands::replication::WaitCommand);
    registry.register(crate::commands::stub::WaitaofCommand);
    registry.register(crate::commands::replication::PsyncCommand);
    registry.register(crate::commands::replication::ReplconfCommand);
    registry.register(crate::commands::replication::RoleCommand);

    // Memory (migrated behind the ConnCtx seam).
    registry
        .register_connection(&crate::connection::observability_conn_command::MEMORY_CONN_COMMAND);

    // Latency (migrated behind the ConnCtx seam).
    registry
        .register_connection(&crate::connection::observability_conn_command::LATENCY_CONN_COMMAND);

    // Hotkeys (migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::handlers::hotkeys::HOTKEYS_CONN_COMMAND);

    // Status (migrated behind the ConnCtx seam).
    registry
        .register_connection(&crate::connection::observability_conn_command::STATUS_CONN_COMMAND);

    // Module
    registry.register(crate::commands::stub::ModuleCommand);

    // Function
    registry.register(crate::commands::function::FunctionCommand);
    registry.register(crate::commands::function::FcallCommand);
    registry.register(crate::commands::function::FcallRoCommand);

    // Search commands
    registry.register(crate::commands::search::FtCreateCommand);
    registry.register(crate::commands::search::FtAlterCommand);
    registry.register(crate::commands::search::FtSearchCommand);
    registry.register(crate::commands::search::FtDropIndexCommand);
    registry.register(crate::commands::search::FtInfoCommand);
    registry.register(crate::commands::search::FtListCommand);
    registry.register(crate::commands::search::FtAggregateCommand);
    registry.register(crate::commands::search::FtHybridCommand);
    registry.register(crate::commands::search::FtSynupdateCommand);
    registry.register(crate::commands::search::FtSyndumpCommand);
    registry.register(crate::commands::search::FtSugaddCommand);
    registry.register(crate::commands::search::FtSuggetCommand);
    registry.register(crate::commands::search::FtSugdelCommand);
    registry.register(crate::commands::search::FtSuglenCommand);
    registry.register(crate::commands::search::FtAliasaddCommand);
    registry.register(crate::commands::search::FtAliasdelCommand);
    registry.register(crate::commands::search::FtAliasupdateCommand);
    registry.register(crate::commands::search::FtTagvalsCommand);
    registry.register(crate::commands::search::FtDictaddCommand);
    registry.register(crate::commands::search::FtDictdelCommand);
    registry.register(crate::commands::search::FtDictdumpCommand);
    registry.register(crate::commands::search::FtConfigCommand);
    registry.register(crate::commands::search::FtSpellcheckCommand);
    // FT.CURSOR: migrated behind the ConnCtx seam (registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::conn_command::FT_CURSOR_CONN_COMMAND);
    registry.register(crate::commands::search::FtExplainCommand);
    registry.register(crate::commands::search::FtExplainCliCommand);
    registry.register(crate::commands::search::FtProfileCommand);

    // Version / upgrade commands
    registry.register(crate::commands::version::FrogdbVersionCommand);
    registry.register(crate::commands::version::FrogdbFinalizeCommand);

    // Connection/Stubs
    registry.register(crate::commands::stub::SelectCommand);
    registry.register(crate::commands::stub::SwapdbCommand);
    // RESET is registered above alongside the other connection-state commands.
    registry.register(crate::commands::stub::BgrewriteaofCommand);
    // RPOPLPUSH now registered via frogdb_commands::list::RpoplpushCommand
    registry.register(crate::commands::stub::SyncCommand);
    registry.register(crate::commands::stub::SaveCommand);
    registry.register_metadata(crate::commands::metadata::MonitorMetadata);
    registry.register(crate::commands::stub::MoveCommand);
}

#[cfg(test)]
mod spec_exhaustiveness {
    //! Registry-wide machine-checks that the declarative CommandSpec stays
    //! complete. These make the silent omissions that motivated the spec
    //! (missing keyspace event, missing waiter wake, missing WAL strategy)
    //! impossible to land without a failing test.
    use super::*;
    use frogdb_core::{CommandFlags, EventSpec, WaiterKind, WaiterWake, WalStrategy};

    /// WRITE commands that legitimately persist nothing through the key WAL:
    /// FLUSH* clear via RocksDB, FT.* persist through the search index, MIGRATE
    /// streams via async external I/O, and MOVE/SWAPDB reject before writing.
    const WAL_NOOP_ALLOWLIST: &[&str] = &[
        "FLUSHDB",
        "FLUSHALL",
        "FROGDB.FINALIZE",
        "FT.ALIASADD",
        "FT.ALIASDEL",
        "FT.ALIASUPDATE",
        "FT.ALTER",
        "FT.CREATE",
        "FT.DICTADD",
        "FT.DICTDEL",
        "FT.DROPINDEX",
        "FT.SUGADD",
        "FT.SUGDEL",
        "FT.SYNUPDATE",
        "MIGRATE",
        "MOVE",
        "SWAPDB",
    ];

    fn full_registry() -> CommandRegistry {
        let mut r = CommandRegistry::new();
        register_commands(&mut r);
        r
    }

    /// Every full command's spec satisfies its cross-field invariants
    /// (Dynamic keys iff MOVABLEKEYS, arity covers the keys it reads,
    /// Suppressed only on WRITE, WRITE declares an event).
    #[test]
    fn every_full_command_spec_validates() {
        for (name, entry) in full_registry().iter() {
            if let Some(cmd) = entry.as_command() {
                cmd.spec()
                    .validate()
                    .unwrap_or_else(|e| panic!("{name}: invalid spec: {e}"));
            }
        }
    }

    /// A WRITE command must declare a keyspace event (Emits or, explicitly,
    /// Suppressed) — never the read-command default NotApplicable.
    #[test]
    fn every_write_command_declares_event() {
        for (name, entry) in full_registry().iter() {
            if let Some(cmd) = entry.as_command() {
                let spec = cmd.spec();
                if spec.flags.contains(CommandFlags::WRITE) {
                    assert!(
                        !matches!(spec.event, EventSpec::NotApplicable),
                        "{name}: WRITE command must declare Emits or Suppressed"
                    );
                }
            }
        }
    }

    /// A WRITE command must declare a WAL strategy unless it is on the explicit
    /// allowlist of commands that persist through another mechanism.
    #[test]
    fn every_write_command_declares_wal() {
        for (name, entry) in full_registry().iter() {
            if let Some(cmd) = entry.as_command() {
                let spec = cmd.spec();
                if spec.flags.contains(CommandFlags::WRITE) && matches!(spec.wal, WalStrategy::NoOp)
                {
                    assert!(
                        WAL_NOOP_ALLOWLIST.contains(&name),
                        "{name}: WRITE command must declare a WAL strategy (or be allowlisted)"
                    );
                }
            }
        }
    }

    /// Regression guard for the waiter-wake bug class: commands that add data
    /// to a structure with blocking readers must wake the matching waiter kind.
    /// LREM/RPOPLPUSH/LMOVE/ZINCRBY were all silent omissions before the spec.
    #[test]
    fn data_adding_commands_wake_blocked_clients() {
        let expected: &[(&str, WaiterKind)] = &[
            ("LPUSH", WaiterKind::List),
            ("RPUSH", WaiterKind::List),
            ("LINSERT", WaiterKind::List),
            ("RPOPLPUSH", WaiterKind::List),
            ("LMOVE", WaiterKind::List),
            ("ZADD", WaiterKind::SortedSet),
            ("ZINCRBY", WaiterKind::SortedSet),
            ("XADD", WaiterKind::Stream),
        ];
        let r = full_registry();
        for (name, kind) in expected {
            let entry = r
                .get_entry(name)
                .unwrap_or_else(|| panic!("{name} not registered"));
            let cmd = entry.as_command().unwrap();
            assert_eq!(
                cmd.spec().wakes,
                WaiterWake::Kind(*kind),
                "{name} adds data and must wake {kind:?} waiters"
            );
        }
    }
}
