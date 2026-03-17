//! Data-structure command implementations for FrogDB.
//!
//! This crate contains the Redis-compatible command implementations for
//! data-structure operations (strings, hashes, lists, sets, sorted sets,
//! streams, JSON, geo, bloom filters, bitmaps, timeseries, HyperLogLog).
//!
//! Server-specific commands (cluster, replication, config, client, ACL,
//! scripting, transactions, migration, persistence) remain in `frogdb-server`.

pub mod basic;
pub mod bitmap;
pub mod blocking;
pub mod bloom;
pub mod event_sourcing;
pub mod expiry;
pub mod generic;
pub mod geo;
pub mod hash;
pub mod hyperloglog;
pub mod json;
pub mod list;
pub mod scan;
pub mod set;
pub mod sort;
pub mod sorted_set;
pub mod stream;
pub mod string;
pub mod timeseries;
pub mod utils;

/// Register all data-structure commands with the given registry.
pub fn register_all(registry: &mut frogdb_core::CommandRegistry) {
    // Connection commands (basic)
    registry.register(basic::PingCommand);
    registry.register(basic::EchoCommand);
    registry.register(basic::QuitCommand);
    registry.register(basic::CommandCommand);

    // String commands (basic)
    registry.register(basic::GetCommand);
    registry.register(basic::SetCommand);
    registry.register(basic::DelCommand);
    registry.register(basic::ExistsCommand);

    // String commands (extended)
    registry.register(string::SetnxCommand);
    registry.register(string::SetexCommand);
    registry.register(string::PsetexCommand);
    registry.register(string::AppendCommand);
    registry.register(string::StrlenCommand);
    registry.register(string::GetrangeCommand);
    registry.register(string::SetrangeCommand);
    registry.register(string::GetdelCommand);
    registry.register(string::GetexCommand);

    // Deprecated string commands
    registry.register(string::GetsetCommand);
    registry.register(string::SubstrCommand);

    // Numeric commands
    registry.register(string::IncrCommand);
    registry.register(string::DecrCommand);
    registry.register(string::IncrbyCommand);
    registry.register(string::DecrbyCommand);
    registry.register(string::IncrbyfloatCommand);

    // Multi-key string commands
    registry.register(string::MgetCommand);
    registry.register(string::MsetCommand);
    registry.register(string::MsetnxCommand);

    // TTL/Expiry commands
    registry.register(expiry::ExpireCommand);
    registry.register(expiry::PexpireCommand);
    registry.register(expiry::ExpireatCommand);
    registry.register(expiry::PexpireatCommand);
    registry.register(expiry::TtlCommand);
    registry.register(expiry::PttlCommand);
    registry.register(expiry::PersistCommand);
    registry.register(expiry::ExpiretimeCommand);
    registry.register(expiry::PexpiretimeCommand);

    // Generic commands
    registry.register(generic::TypeCommand);
    registry.register(generic::RenameCommand);
    registry.register(generic::RenamenxCommand);
    registry.register(generic::TouchCommand);
    registry.register(generic::UnlinkCommand);
    registry.register(generic::ObjectCommand);
    registry.register(generic::DebugCommand);
    registry.register(generic::CopyCommand);
    registry.register(generic::RandomkeyCommand);

    // Sorted set commands - basic
    registry.register(sorted_set::ZaddCommand);
    registry.register(sorted_set::ZremCommand);
    registry.register(sorted_set::ZscoreCommand);
    registry.register(sorted_set::ZmscoreCommand);
    registry.register(sorted_set::ZcardCommand);
    registry.register(sorted_set::ZincrbyCommand);

    // Sorted set commands - ranking
    registry.register(sorted_set::ZrankCommand);
    registry.register(sorted_set::ZrevrankCommand);

    // Sorted set commands - range queries
    registry.register(sorted_set::ZrangeCommand);
    registry.register(sorted_set::ZrangebyscoreCommand);
    registry.register(sorted_set::ZrevrangeCommand);
    registry.register(sorted_set::ZrevrangebyscoreCommand);
    registry.register(sorted_set::ZrangebylexCommand);
    registry.register(sorted_set::ZrevrangebylexCommand);
    registry.register(sorted_set::ZcountCommand);
    registry.register(sorted_set::ZlexcountCommand);

    // Sorted set commands - pop & random
    registry.register(sorted_set::ZpopminCommand);
    registry.register(sorted_set::ZpopmaxCommand);
    registry.register(sorted_set::ZmpopCommand);
    registry.register(sorted_set::ZrandmemberCommand);

    // Sorted set commands - set operations
    registry.register(sorted_set::ZunionCommand);
    registry.register(sorted_set::ZunionstoreCommand);
    registry.register(sorted_set::ZinterCommand);
    registry.register(sorted_set::ZinterstoreCommand);
    registry.register(sorted_set::ZintercardCommand);
    registry.register(sorted_set::ZdiffCommand);
    registry.register(sorted_set::ZdiffstoreCommand);

    // Sorted set commands - other
    registry.register(sorted_set::ZscanCommand);
    registry.register(sorted_set::ZrangestoreCommand);
    registry.register(sorted_set::ZremrangebyrankCommand);
    registry.register(sorted_set::ZremrangebyscoreCommand);
    registry.register(sorted_set::ZremrangebylexCommand);

    // Hash commands
    registry.register(hash::HsetCommand);
    registry.register(hash::HsetnxCommand);
    registry.register(hash::HgetCommand);
    registry.register(hash::HdelCommand);
    registry.register(hash::HmsetCommand);
    registry.register(hash::HmgetCommand);
    registry.register(hash::HgetallCommand);
    registry.register(hash::HkeysCommand);
    registry.register(hash::HvalsCommand);
    registry.register(hash::HexistsCommand);
    registry.register(hash::HlenCommand);
    registry.register(hash::HincrbyCommand);
    registry.register(hash::HincrbyfloatCommand);
    registry.register(hash::HstrlenCommand);
    registry.register(hash::HscanCommand);
    registry.register(hash::HrandfieldCommand);

    // Set commands
    registry.register(set::SaddCommand);
    registry.register(set::SremCommand);
    registry.register(set::SmembersCommand);
    registry.register(set::SismemberCommand);
    registry.register(set::SmismemberCommand);
    registry.register(set::ScardCommand);
    registry.register(set::SunionCommand);
    registry.register(set::SinterCommand);
    registry.register(set::SdiffCommand);
    registry.register(set::SunionstoreCommand);
    registry.register(set::SinterstoreCommand);
    registry.register(set::SdiffstoreCommand);
    registry.register(set::SintercardCommand);
    registry.register(set::SrandmemberCommand);
    registry.register(set::SpopCommand);
    registry.register(set::SmoveCommand);
    registry.register(set::SscanCommand);

    // List commands
    registry.register(list::LpushCommand);
    registry.register(list::RpushCommand);
    registry.register(list::LpushxCommand);
    registry.register(list::RpushxCommand);
    registry.register(list::LpopCommand);
    registry.register(list::RpopCommand);
    registry.register(list::LlenCommand);
    registry.register(list::LrangeCommand);
    registry.register(list::LindexCommand);
    registry.register(list::LsetCommand);
    registry.register(list::LinsertCommand);
    registry.register(list::LremCommand);
    registry.register(list::LtrimCommand);
    registry.register(list::LposCommand);
    registry.register(list::LmoveCommand);
    registry.register(list::RpoplpushCommand);
    registry.register(list::LmpopCommand);

    // Blocking commands (list and sorted set)
    registry.register(blocking::BlpopCommand);
    registry.register(blocking::BrpopCommand);
    registry.register(blocking::BlmoveCommand);
    registry.register(blocking::BlmpopCommand);
    registry.register(blocking::BzpopminCommand);
    registry.register(blocking::BzpopmaxCommand);
    registry.register(blocking::BzmpopCommand);
    registry.register(blocking::BrpoplpushCommand);

    // Scan commands
    registry.register(scan::ScanCommand);
    registry.register(scan::KeysCommand);

    // Stream commands
    registry.register(stream::XaddCommand);
    registry.register(stream::XlenCommand);
    registry.register(stream::XrangeCommand);
    registry.register(stream::XrevrangeCommand);
    registry.register(stream::XdelCommand);
    registry.register(stream::XtrimCommand);
    registry.register(stream::XreadCommand);
    registry.register(stream::XgroupCommand);
    registry.register(stream::XreadgroupCommand);
    registry.register(stream::XackCommand);
    registry.register(stream::XpendingCommand);
    registry.register(stream::XclaimCommand);
    registry.register(stream::XautoclaimCommand);
    registry.register(stream::XinfoCommand);
    registry.register(stream::XsetidCommand);

    // Bitmap commands
    registry.register(bitmap::SetbitCommand);
    registry.register(bitmap::GetbitCommand);
    registry.register(bitmap::BitcountCommand);
    registry.register(bitmap::BitopCommand);
    registry.register(bitmap::BitposCommand);
    registry.register(bitmap::BitfieldCommand);
    registry.register(bitmap::BitfieldRoCommand);

    // Geo commands
    registry.register(geo::GeoaddCommand);
    registry.register(geo::GeodistCommand);
    registry.register(geo::GeohashCommand);
    registry.register(geo::GeoposCommand);
    registry.register(geo::GeosearchCommand);
    registry.register(geo::GeosearchstoreCommand);
    registry.register(geo::GeoradiusCommand);
    registry.register(geo::GeoradiusbymemberCommand);
    registry.register(geo::GeoradiusRoCommand);
    registry.register(geo::GeoradiusbymemberRoCommand);

    // Bloom filter commands
    registry.register(bloom::BfReserve);
    registry.register(bloom::BfAdd);
    registry.register(bloom::BfMadd);
    registry.register(bloom::BfExists);
    registry.register(bloom::BfMexists);
    registry.register(bloom::BfInsert);
    registry.register(bloom::BfInfo);
    registry.register(bloom::BfCard);
    registry.register(bloom::BfScandump);
    registry.register(bloom::BfLoadchunk);

    // HyperLogLog commands
    registry.register(hyperloglog::PfaddCommand);
    registry.register(hyperloglog::PfcountCommand);
    registry.register(hyperloglog::PfmergeCommand);
    registry.register(hyperloglog::PfdebugCommand);
    registry.register(hyperloglog::PfselftestCommand);

    // Sort commands
    registry.register(sort::SortCommand);
    registry.register(sort::SortRoCommand);

    // String (LCS)
    registry.register(string::LcsCommand);

    // TimeSeries commands
    registry.register(timeseries::TsCreateCommand);
    registry.register(timeseries::TsAlterCommand);
    registry.register(timeseries::TsAddCommand);
    registry.register(timeseries::TsMaddCommand);
    registry.register(timeseries::TsIncrbyCommand);
    registry.register(timeseries::TsDecrbyCommand);
    registry.register(timeseries::TsDelCommand);
    registry.register(timeseries::TsGetCommand);
    registry.register(timeseries::TsRangeCommand);
    registry.register(timeseries::TsRevrangeCommand);
    registry.register(timeseries::TsInfoCommand);
    registry.register(timeseries::TsQueryIndexCommand);
    registry.register(timeseries::TsMgetCommand);
    registry.register(timeseries::TsMrangeCommand);
    registry.register(timeseries::TsMrevrangeCommand);
    registry.register(timeseries::TsCreateRuleCommand);
    registry.register(timeseries::TsDeleteRuleCommand);

    // JSON commands
    registry.register(json::JsonSetCommand);
    registry.register(json::JsonGetCommand);
    registry.register(json::JsonDelCommand);
    registry.register(json::JsonMgetCommand);
    registry.register(json::JsonTypeCommand);
    registry.register(json::JsonNumIncrByCommand);
    registry.register(json::JsonNumMultByCommand);
    registry.register(json::JsonStrAppendCommand);
    registry.register(json::JsonStrLenCommand);
    registry.register(json::JsonArrAppendCommand);
    registry.register(json::JsonArrIndexCommand);
    registry.register(json::JsonArrInsertCommand);
    registry.register(json::JsonArrLenCommand);
    registry.register(json::JsonArrPopCommand);
    registry.register(json::JsonArrTrimCommand);
    registry.register(json::JsonObjKeysCommand);
    registry.register(json::JsonObjLenCommand);
    registry.register(json::JsonClearCommand);
    registry.register(json::JsonToggleCommand);
    registry.register(json::JsonMergeCommand);
    registry.register(json::JsonDebugCommand);

    // Event Sourcing commands (FrogDB extensions)
    registry.register(event_sourcing::EsAppendCommand);
    registry.register(event_sourcing::EsReadCommand);
    registry.register(event_sourcing::EsReplayCommand);
    registry.register(event_sourcing::EsInfoCommand);
    registry.register(event_sourcing::EsSnapshotCommand);
    registry.register(event_sourcing::EsAllCommand);
}
