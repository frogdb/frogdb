use frogdb_core::CommandRegistry;

/// Register all built-in commands.
pub fn register_commands(registry: &mut CommandRegistry) {
    // Register data-structure commands from the frogdb-commands crate.
    frogdb_commands::register_all(registry);

    // =====================================================================
    // Server-specific commands (depend on server infrastructure)
    // =====================================================================

    // Hello (protocol negotiation)
    registry.register(crate::commands::HelloCommand);

    // Persistence commands. BGSAVE/LASTSAVE are migrated behind the ConnCtx seam
    // (registered as CommandImpl::Connection executors, dispatched through the
    // registry union rather than the legacy router→connection-handler path).
    registry.register_connection(
        &crate::connection::persistence_conn_command::BGSAVE_CONN_COMMAND,
    );
    registry.register_connection(
        &crate::connection::persistence_conn_command::LASTSAVE_CONN_COMMAND,
    );
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

    // Info command
    registry.register(crate::commands::info::InfoCommand);

    // Client commands
    registry.register(crate::commands::client::ClientCommand);

    // Config commands (migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::conn_command::CONFIG_CONN_COMMAND);

    // Slowlog commands
    registry.register(crate::commands::slowlog::SlowlogCommand);

    // Auth/ACL commands
    registry.register(crate::commands::auth::Auth);
    registry.register(crate::commands::acl::Acl);

    // Pub/Sub commands (metadata-only, handled at connection level)
    registry.register_metadata(crate::commands::metadata::SubscribeMetadata);
    registry.register_metadata(crate::commands::metadata::PsubscribeMetadata);
    registry.register_metadata(crate::commands::metadata::SsubscribeMetadata);
    registry.register_metadata(crate::commands::metadata::UnsubscribeMetadata);
    registry.register_metadata(crate::commands::metadata::PunsubscribeMetadata);
    registry.register_metadata(crate::commands::metadata::SunsubscribeMetadata);
    registry.register_metadata(crate::commands::metadata::PublishMetadata);
    registry.register_metadata(crate::commands::metadata::SpublishMetadata);
    registry.register_metadata(crate::commands::metadata::PubsubMetadata);

    // Cluster
    registry.register(crate::commands::cluster::ClusterCommand);
    registry.register(crate::commands::cluster::AskingCommand);
    registry.register(crate::commands::cluster::ReadonlyCommand);
    registry.register(crate::commands::cluster::ReadwriteCommand);

    // Replication
    registry.register(crate::commands::replication::ReplicaofCommand);
    registry.register(crate::commands::replication::SlaveofCommand);
    registry.register(crate::commands::replication::WaitCommand);
    registry.register(crate::commands::stub::WaitaofCommand);
    registry.register(crate::commands::replication::PsyncCommand);
    registry.register(crate::commands::replication::ReplconfCommand);
    registry.register(crate::commands::replication::RoleCommand);

    // Memory
    registry.register(crate::commands::memory::MemoryCommand);

    // Latency
    registry.register(crate::commands::latency::LatencyCommand);

    // Hotkeys
    registry.register(crate::commands::hotkeys::HotkeysCommand);

    // Status
    registry.register(crate::commands::status::StatusCommand);

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
    registry.register(crate::commands::search::FtCursorCommand);
    registry.register(crate::commands::search::FtExplainCommand);
    registry.register(crate::commands::search::FtExplainCliCommand);
    registry.register(crate::commands::search::FtProfileCommand);

    // Version / upgrade commands
    registry.register(crate::commands::version::FrogdbVersionCommand);
    registry.register(crate::commands::version::FrogdbFinalizeCommand);

    // Connection/Stubs
    registry.register(crate::commands::stub::SelectCommand);
    registry.register(crate::commands::stub::SwapdbCommand);
    registry.register_metadata(crate::commands::metadata::ResetMetadata);
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
