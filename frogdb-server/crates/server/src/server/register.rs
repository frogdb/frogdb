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

    // Persistence commands
    registry.register(crate::commands::persistence::BgsaveCommand);
    registry.register(crate::commands::persistence::LastsaveCommand);
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

    // Config commands
    registry.register(crate::commands::config::ConfigCommand);

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
