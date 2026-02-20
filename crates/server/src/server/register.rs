use frogdb_core::CommandRegistry;

/// Register all built-in commands.
pub fn register_commands(registry: &mut CommandRegistry) {
    // Connection commands
    registry.register(crate::commands::basic::PingCommand);
    registry.register(crate::commands::basic::EchoCommand);
    registry.register(crate::commands::basic::QuitCommand);
    registry.register(crate::commands::basic::CommandCommand);
    registry.register(crate::commands::HelloCommand);

    // String commands (basic)
    registry.register(crate::commands::basic::GetCommand);
    registry.register(crate::commands::basic::SetCommand);
    registry.register(crate::commands::basic::DelCommand);
    registry.register(crate::commands::basic::ExistsCommand);

    // String commands (extended)
    registry.register(crate::commands::string::SetnxCommand);
    registry.register(crate::commands::string::SetexCommand);
    registry.register(crate::commands::string::PsetexCommand);
    registry.register(crate::commands::string::AppendCommand);
    registry.register(crate::commands::string::StrlenCommand);
    registry.register(crate::commands::string::GetrangeCommand);
    registry.register(crate::commands::string::SetrangeCommand);
    registry.register(crate::commands::string::GetdelCommand);
    registry.register(crate::commands::string::GetexCommand);

    // Numeric commands
    registry.register(crate::commands::string::IncrCommand);
    registry.register(crate::commands::string::DecrCommand);
    registry.register(crate::commands::string::IncrbyCommand);
    registry.register(crate::commands::string::DecrbyCommand);
    registry.register(crate::commands::string::IncrbyfloatCommand);

    // Multi-key string commands
    registry.register(crate::commands::string::MgetCommand);
    registry.register(crate::commands::string::MsetCommand);
    registry.register(crate::commands::string::MsetnxCommand);

    // TTL/Expiry commands
    registry.register(crate::commands::expiry::ExpireCommand);
    registry.register(crate::commands::expiry::PexpireCommand);
    registry.register(crate::commands::expiry::ExpireatCommand);
    registry.register(crate::commands::expiry::PexpireatCommand);
    registry.register(crate::commands::expiry::TtlCommand);
    registry.register(crate::commands::expiry::PttlCommand);
    registry.register(crate::commands::expiry::PersistCommand);
    registry.register(crate::commands::expiry::ExpiretimeCommand);
    registry.register(crate::commands::expiry::PexpiretimeCommand);

    // Generic commands
    registry.register(crate::commands::generic::TypeCommand);
    registry.register(crate::commands::generic::RenameCommand);
    registry.register(crate::commands::generic::RenamenxCommand);
    registry.register(crate::commands::generic::TouchCommand);
    registry.register(crate::commands::generic::UnlinkCommand);
    registry.register(crate::commands::generic::ObjectCommand);
    registry.register(crate::commands::generic::DebugCommand);

    // Sorted set commands - basic
    registry.register(crate::commands::sorted_set::ZaddCommand);
    registry.register(crate::commands::sorted_set::ZremCommand);
    registry.register(crate::commands::sorted_set::ZscoreCommand);
    registry.register(crate::commands::sorted_set::ZmscoreCommand);
    registry.register(crate::commands::sorted_set::ZcardCommand);
    registry.register(crate::commands::sorted_set::ZincrbyCommand);

    // Sorted set commands - ranking
    registry.register(crate::commands::sorted_set::ZrankCommand);
    registry.register(crate::commands::sorted_set::ZrevrankCommand);

    // Sorted set commands - range queries
    registry.register(crate::commands::sorted_set::ZrangeCommand);
    registry.register(crate::commands::sorted_set::ZrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZrevrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZrangebylexCommand);
    registry.register(crate::commands::sorted_set::ZrevrangebylexCommand);
    registry.register(crate::commands::sorted_set::ZcountCommand);
    registry.register(crate::commands::sorted_set::ZlexcountCommand);

    // Sorted set commands - pop & random
    registry.register(crate::commands::sorted_set::ZpopminCommand);
    registry.register(crate::commands::sorted_set::ZpopmaxCommand);
    registry.register(crate::commands::sorted_set::ZmpopCommand);
    registry.register(crate::commands::sorted_set::ZrandmemberCommand);

    // Sorted set commands - set operations
    registry.register(crate::commands::sorted_set::ZunionCommand);
    registry.register(crate::commands::sorted_set::ZunionstoreCommand);
    registry.register(crate::commands::sorted_set::ZinterCommand);
    registry.register(crate::commands::sorted_set::ZinterstoreCommand);
    registry.register(crate::commands::sorted_set::ZintercardCommand);
    registry.register(crate::commands::sorted_set::ZdiffCommand);
    registry.register(crate::commands::sorted_set::ZdiffstoreCommand);

    // Sorted set commands - other
    registry.register(crate::commands::sorted_set::ZscanCommand);
    registry.register(crate::commands::sorted_set::ZrangestoreCommand);
    registry.register(crate::commands::sorted_set::ZremrangebyrankCommand);
    registry.register(crate::commands::sorted_set::ZremrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZremrangebylexCommand);

    // Persistence commands
    registry.register(crate::commands::persistence::BgsaveCommand);
    registry.register(crate::commands::persistence::LastsaveCommand);
    registry.register(crate::commands::persistence::DumpCommand);
    registry.register(crate::commands::persistence::RestoreCommand);

    // Migration commands
    registry.register(crate::commands::migrate_cmd::MigrateCommand);

    // Hash commands
    registry.register(crate::commands::hash::HsetCommand);
    registry.register(crate::commands::hash::HsetnxCommand);
    registry.register(crate::commands::hash::HgetCommand);
    registry.register(crate::commands::hash::HdelCommand);
    registry.register(crate::commands::hash::HmsetCommand);
    registry.register(crate::commands::hash::HmgetCommand);
    registry.register(crate::commands::hash::HgetallCommand);
    registry.register(crate::commands::hash::HkeysCommand);
    registry.register(crate::commands::hash::HvalsCommand);
    registry.register(crate::commands::hash::HexistsCommand);
    registry.register(crate::commands::hash::HlenCommand);
    registry.register(crate::commands::hash::HincrbyCommand);
    registry.register(crate::commands::hash::HincrbyfloatCommand);
    registry.register(crate::commands::hash::HstrlenCommand);
    registry.register(crate::commands::hash::HscanCommand);
    registry.register(crate::commands::hash::HrandfieldCommand);

    // Set commands
    registry.register(crate::commands::set::SaddCommand);
    registry.register(crate::commands::set::SremCommand);
    registry.register(crate::commands::set::SmembersCommand);
    registry.register(crate::commands::set::SismemberCommand);
    registry.register(crate::commands::set::SmismemberCommand);
    registry.register(crate::commands::set::ScardCommand);
    registry.register(crate::commands::set::SunionCommand);
    registry.register(crate::commands::set::SinterCommand);
    registry.register(crate::commands::set::SdiffCommand);
    registry.register(crate::commands::set::SunionstoreCommand);
    registry.register(crate::commands::set::SinterstoreCommand);
    registry.register(crate::commands::set::SdiffstoreCommand);
    registry.register(crate::commands::set::SintercardCommand);
    registry.register(crate::commands::set::SrandmemberCommand);
    registry.register(crate::commands::set::SpopCommand);
    registry.register(crate::commands::set::SmoveCommand);
    registry.register(crate::commands::set::SscanCommand);

    // List commands
    registry.register(crate::commands::list::LpushCommand);
    registry.register(crate::commands::list::RpushCommand);
    registry.register(crate::commands::list::LpushxCommand);
    registry.register(crate::commands::list::RpushxCommand);
    registry.register(crate::commands::list::LpopCommand);
    registry.register(crate::commands::list::RpopCommand);
    registry.register(crate::commands::list::LlenCommand);
    registry.register(crate::commands::list::LrangeCommand);
    registry.register(crate::commands::list::LindexCommand);
    registry.register(crate::commands::list::LsetCommand);
    registry.register(crate::commands::list::LinsertCommand);
    registry.register(crate::commands::list::LremCommand);
    registry.register(crate::commands::list::LtrimCommand);
    registry.register(crate::commands::list::LposCommand);
    registry.register(crate::commands::list::LmoveCommand);
    registry.register(crate::commands::list::LmpopCommand);

    // Blocking commands (list and sorted set)
    registry.register(crate::commands::blocking::BlpopCommand);
    registry.register(crate::commands::blocking::BrpopCommand);
    registry.register(crate::commands::blocking::BlmoveCommand);
    registry.register(crate::commands::blocking::BlmpopCommand);
    registry.register(crate::commands::blocking::BzpopminCommand);
    registry.register(crate::commands::blocking::BzpopmaxCommand);
    registry.register(crate::commands::blocking::BzmpopCommand);
    registry.register(crate::commands::blocking::BrpoplpushCommand);

    // Transaction commands
    registry.register(crate::commands::transaction::MultiCommand);
    registry.register(crate::commands::transaction::ExecCommand);
    registry.register(crate::commands::transaction::DiscardCommand);
    registry.register(crate::commands::transaction::WatchCommand);
    registry.register(crate::commands::transaction::UnwatchCommand);

    // Scripting commands
    registry.register(crate::commands::scripting::EvalCommand);
    registry.register(crate::commands::scripting::EvalshaCommand);
    registry.register(crate::commands::scripting::ScriptCommand);

    // Scan commands
    registry.register(crate::commands::scan::ScanCommand);
    registry.register(crate::commands::scan::KeysCommand);

    // Server commands
    registry.register(crate::commands::server::DbsizeCommand);
    registry.register(crate::commands::server::FlushdbCommand);
    registry.register(crate::commands::server::FlushallCommand);
    registry.register(crate::commands::server::TimeCommand);
    registry.register(crate::commands::server::ShutdownCommand);

    // Info command
    registry.register(crate::commands::info::InfoCommand);

    // Client commands (handled specially in connection.rs, but registered for introspection)
    registry.register(crate::commands::client::ClientCommand);

    // Config commands (handled specially in connection.rs, but registered for introspection)
    registry.register(crate::commands::config::ConfigCommand);

    // Slowlog commands (handled specially in connection.rs, but registered for introspection)
    registry.register(crate::commands::slowlog::SlowlogCommand);

    // Stream commands
    registry.register(crate::commands::stream::XaddCommand);
    registry.register(crate::commands::stream::XlenCommand);
    registry.register(crate::commands::stream::XrangeCommand);
    registry.register(crate::commands::stream::XrevrangeCommand);
    registry.register(crate::commands::stream::XdelCommand);
    registry.register(crate::commands::stream::XtrimCommand);
    registry.register(crate::commands::stream::XreadCommand);
    registry.register(crate::commands::stream::XgroupCommand);
    registry.register(crate::commands::stream::XreadgroupCommand);
    registry.register(crate::commands::stream::XackCommand);
    registry.register(crate::commands::stream::XpendingCommand);
    registry.register(crate::commands::stream::XclaimCommand);
    registry.register(crate::commands::stream::XautoclaimCommand);
    registry.register(crate::commands::stream::XinfoCommand);
    registry.register(crate::commands::stream::XsetidCommand);

    // Auth/ACL commands (handled specially in connection.rs, but registered for introspection)
    registry.register(crate::commands::auth::Auth);
    registry.register(crate::commands::acl::Acl);

    // Bitmap commands
    registry.register(crate::commands::bitmap::SetbitCommand);
    registry.register(crate::commands::bitmap::GetbitCommand);
    registry.register(crate::commands::bitmap::BitcountCommand);
    registry.register(crate::commands::bitmap::BitopCommand);
    registry.register(crate::commands::bitmap::BitposCommand);
    registry.register(crate::commands::bitmap::BitfieldCommand);
    registry.register(crate::commands::bitmap::BitfieldRoCommand);

    // Geo commands
    registry.register(crate::commands::geo::GeoaddCommand);
    registry.register(crate::commands::geo::GeodistCommand);
    registry.register(crate::commands::geo::GeohashCommand);
    registry.register(crate::commands::geo::GeoposCommand);
    registry.register(crate::commands::geo::GeosearchCommand);
    registry.register(crate::commands::geo::GeosearchstoreCommand);
    registry.register(crate::commands::geo::GeoradiusCommand);
    registry.register(crate::commands::geo::GeoradiusbymemberCommand);

    // Bloom filter commands
    registry.register(crate::commands::bloom::BfReserve);
    registry.register(crate::commands::bloom::BfAdd);
    registry.register(crate::commands::bloom::BfMadd);
    registry.register(crate::commands::bloom::BfExists);
    registry.register(crate::commands::bloom::BfMexists);
    registry.register(crate::commands::bloom::BfInsert);
    registry.register(crate::commands::bloom::BfInfo);
    registry.register(crate::commands::bloom::BfCard);
    registry.register(crate::commands::bloom::BfScandump);
    registry.register(crate::commands::bloom::BfLoadchunk);

    // HyperLogLog commands
    registry.register(crate::commands::hyperloglog::PfaddCommand);
    registry.register(crate::commands::hyperloglog::PfcountCommand);
    registry.register(crate::commands::hyperloglog::PfmergeCommand);
    registry.register(crate::commands::hyperloglog::PfdebugCommand);
    registry.register(crate::commands::hyperloglog::PfselftestCommand);

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
    registry.register(crate::commands::stub::WaitaofCommand); // WAITAOF not yet implemented
    registry.register(crate::commands::replication::PsyncCommand);
    registry.register(crate::commands::replication::ReplconfCommand);

    // Memory (handled specially in connection.rs for scatter-gather)
    registry.register(crate::commands::memory::MemoryCommand);

    // Latency (handled specially in connection.rs for scatter-gather)
    registry.register(crate::commands::latency::LatencyCommand);

    // Status (handled specially in connection.rs)
    registry.register(crate::commands::status::StatusCommand);

    // Module
    registry.register(crate::commands::stub::ModuleCommand);

    // Function
    registry.register(crate::commands::function::FunctionCommand);
    registry.register(crate::commands::function::FcallCommand);
    registry.register(crate::commands::function::FcallRoCommand);

    // Debug (connection-level dispatch for TRACING, VLL, BUNDLE; shard-level for OBJECT, SET)
    registry.register(crate::commands::generic::DebugCommand);

    // Generic/Keys
    registry.register(crate::commands::generic::CopyCommand);
    registry.register(crate::commands::generic::RandomkeyCommand);
    registry.register(crate::commands::sort::SortCommand);
    registry.register(crate::commands::sort::SortRoCommand);
    registry.register(crate::commands::stub::MoveCommand);

    // Connection
    registry.register(crate::commands::stub::SelectCommand);
    registry.register(crate::commands::stub::SwapdbCommand);
    // RESET is handled at connection level, use metadata
    registry.register_metadata(crate::commands::metadata::ResetMetadata);

    // Server
    registry.register(crate::commands::stub::BgrewriteaofCommand);
    registry.register(crate::commands::server::LolwutCommand);
    registry.register(crate::commands::replication::RoleCommand);
    registry.register(crate::commands::stub::SaveCommand);
    registry.register(crate::commands::stub::MonitorCommand);

    // String (deprecated/missing)
    registry.register(crate::commands::stub::GetsetCommand);
    registry.register(crate::commands::string::LcsCommand);
    registry.register(crate::commands::stub::SubstrCommand);

    // List (deprecated)
    registry.register(crate::commands::stub::RpoplpushCommand);

    // Replication (additional)
    registry.register(crate::commands::stub::SyncCommand);

    // TimeSeries commands
    registry.register(crate::commands::timeseries::TsCreateCommand);
    registry.register(crate::commands::timeseries::TsAlterCommand);
    registry.register(crate::commands::timeseries::TsAddCommand);
    registry.register(crate::commands::timeseries::TsMaddCommand);
    registry.register(crate::commands::timeseries::TsIncrbyCommand);
    registry.register(crate::commands::timeseries::TsDecrbyCommand);
    registry.register(crate::commands::timeseries::TsDelCommand);
    registry.register(crate::commands::timeseries::TsGetCommand);
    registry.register(crate::commands::timeseries::TsRangeCommand);
    registry.register(crate::commands::timeseries::TsRevrangeCommand);
    registry.register(crate::commands::timeseries::TsInfoCommand);

    // JSON commands
    registry.register(crate::commands::json::JsonSetCommand);
    registry.register(crate::commands::json::JsonGetCommand);
    registry.register(crate::commands::json::JsonDelCommand);
    registry.register(crate::commands::json::JsonMgetCommand);
    registry.register(crate::commands::json::JsonTypeCommand);
    registry.register(crate::commands::json::JsonNumIncrByCommand);
    registry.register(crate::commands::json::JsonNumMultByCommand);
    registry.register(crate::commands::json::JsonStrAppendCommand);
    registry.register(crate::commands::json::JsonStrLenCommand);
    registry.register(crate::commands::json::JsonArrAppendCommand);
    registry.register(crate::commands::json::JsonArrIndexCommand);
    registry.register(crate::commands::json::JsonArrInsertCommand);
    registry.register(crate::commands::json::JsonArrLenCommand);
    registry.register(crate::commands::json::JsonArrPopCommand);
    registry.register(crate::commands::json::JsonArrTrimCommand);
    registry.register(crate::commands::json::JsonObjKeysCommand);
    registry.register(crate::commands::json::JsonObjLenCommand);
    registry.register(crate::commands::json::JsonClearCommand);
    registry.register(crate::commands::json::JsonToggleCommand);
    registry.register(crate::commands::json::JsonMergeCommand);
}
