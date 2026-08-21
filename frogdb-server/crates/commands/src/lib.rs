//! Data-structure command implementations for FrogDB.
//!
//! This crate contains the Redis-compatible command implementations for
//! data-structure operations (strings, hashes, lists, sets, sorted sets,
//! streams, JSON, geo, bloom filters, bitmaps, timeseries, HyperLogLog).
//!
//! Server-specific commands (cluster, replication, config, client, ACL,
//! scripting, transactions, migration, persistence) remain in `frogdb-server`.
//!
//! # Feature gating
//!
//! The families above split into two tiers. The **core profile** (`basic`,
//! `string`, `bitmap`, `list`, `hash`, `set`, `sorted_set`, `expiry`,
//! `blocking`, `scan`, `generic`, `sort`, `utils`) is always compiled. Every
//! other family sits behind a cargo feature of the same name so a targeted
//! core-area build can skip it; `full` turns them all back on and is what any
//! consumer that enumerates the registry must depend on.
//!
//! Gated families are leaves: none of them is referenced by another module in
//! this crate, and the only shared helper they reach for (`utils`) lives in the
//! core profile. `frogdb-core`/`frogdb-types` are untouched — the `Value`
//! variants for every family exist regardless of which commands are compiled.

pub mod basic;
pub mod bitmap;
pub mod blocking;
#[cfg(feature = "bloom")]
pub mod bloom;
#[cfg(feature = "cms")]
pub mod cms;
pub mod command_meta;
#[cfg(feature = "cuckoo")]
pub mod cuckoo;
#[cfg(feature = "event-sourcing")]
pub mod event_sourcing;
pub mod expiry;
pub mod generic;
#[cfg(feature = "geo")]
pub mod geo;
pub mod hash;
#[cfg(feature = "hyperloglog")]
pub mod hyperloglog;
#[cfg(feature = "json")]
pub mod json;
pub mod list;
pub mod scan;
pub mod set;
pub mod sort;
pub mod sorted_set;
#[cfg(feature = "stream")]
pub mod stream;
pub mod string;
#[cfg(feature = "tdigest")]
pub mod tdigest;
#[cfg(feature = "timeseries")]
pub mod timeseries;
#[cfg(feature = "topk")]
pub mod topk;
pub mod upstream;
pub mod utils;
#[cfg(feature = "vectorset")]
pub mod vectorset;

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
    registry.register(string::MsetexCommand);

    // Redis 8.4 string commands
    registry.register(string::DigestCommand);
    registry.register(string::DelexCommand);

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
    // DEBUG is registered by the server as a `CommandImpl::Connection` executor
    // (see `debug_conn_command::DEBUG_CONN_COMMAND`); `COMMAND GETKEYS` resolves
    // DEBUG OBJECT's key through that connection command's `dynamic_keys` via the
    // registry union, so no shard-local `DebugCommand` stub is registered here.
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

    // Hash field expiry commands
    registry.register(hash::HexpireCommand);
    registry.register(hash::HpexpireCommand);
    registry.register(hash::HexpireatCommand);
    registry.register(hash::HpexpireatCommand);
    registry.register(hash::HttlCommand);
    registry.register(hash::HpttlCommand);
    registry.register(hash::HexpiretimeCommand);
    registry.register(hash::HpexpiretimeCommand);
    registry.register(hash::HpersistCommand);

    // Redis 8.0 hash commands
    registry.register(hash::HgetdelCommand);
    registry.register(hash::HgetexCommand);
    registry.register(hash::HsetexCommand);

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

    #[cfg(feature = "stream")]
    {
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
        registry.register(stream::XdelexCommand);
        registry.register(stream::XackdelCommand);
    }

    // Bitmap commands
    registry.register(bitmap::SetbitCommand);
    registry.register(bitmap::GetbitCommand);
    registry.register(bitmap::BitcountCommand);
    registry.register(bitmap::BitopCommand);
    registry.register(bitmap::BitposCommand);
    registry.register(bitmap::BitfieldCommand);
    registry.register(bitmap::BitfieldRoCommand);

    #[cfg(feature = "geo")]
    {
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
    }

    #[cfg(feature = "bloom")]
    {
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
    }

    #[cfg(feature = "cuckoo")]
    {
        // Cuckoo filter commands
        registry.register(cuckoo::CfReserve);
        registry.register(cuckoo::CfAdd);
        registry.register(cuckoo::CfAddnx);
        registry.register(cuckoo::CfInsert);
        registry.register(cuckoo::CfInsertnx);
        registry.register(cuckoo::CfExists);
        registry.register(cuckoo::CfMexists);
        registry.register(cuckoo::CfDel);
        registry.register(cuckoo::CfCount);
        registry.register(cuckoo::CfInfo);
        registry.register(cuckoo::CfScandump);
        registry.register(cuckoo::CfLoadchunk);
    }

    #[cfg(feature = "tdigest")]
    {
        // T-Digest commands
        registry.register(tdigest::TdCreate);
        registry.register(tdigest::TdAdd);
        registry.register(tdigest::TdMerge);
        registry.register(tdigest::TdReset);
        registry.register(tdigest::TdQuantile);
        registry.register(tdigest::TdCdf);
        registry.register(tdigest::TdRank);
        registry.register(tdigest::TdRevrank);
        registry.register(tdigest::TdMin);
        registry.register(tdigest::TdMax);
        registry.register(tdigest::TdInfo);
        registry.register(tdigest::TdTrimmedMean);
    }

    #[cfg(feature = "hyperloglog")]
    {
        // HyperLogLog commands
        registry.register(hyperloglog::PfaddCommand);
        registry.register(hyperloglog::PfcountCommand);
        registry.register(hyperloglog::PfmergeCommand);
        registry.register(hyperloglog::PfdebugCommand);
        registry.register(hyperloglog::PfselftestCommand);
    }

    // Sort commands
    registry.register(sort::SortCommand);
    registry.register(sort::SortRoCommand);

    // String (LCS)
    registry.register(string::LcsCommand);

    #[cfg(feature = "timeseries")]
    {
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
    }

    #[cfg(feature = "json")]
    {
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
    }

    #[cfg(feature = "cms")]
    {
        // Count-Min Sketch commands
        registry.register(cms::CmsInitByDim);
        registry.register(cms::CmsInitByProb);
        registry.register(cms::CmsIncrBy);
        registry.register(cms::CmsQuery);
        registry.register(cms::CmsMerge);
        registry.register(cms::CmsInfo);
    }

    #[cfg(feature = "topk")]
    {
        // Top-K commands
        registry.register(topk::TopkReserve);
        registry.register(topk::TopkAdd);
        registry.register(topk::TopkIncrby);
        registry.register(topk::TopkQuery);
        registry.register(topk::TopkCount);
        registry.register(topk::TopkList);
        registry.register(topk::TopkInfo);
    }

    #[cfg(feature = "vectorset")]
    {
        // Vector set commands
        registry.register(vectorset::VaddCommand);
        registry.register(vectorset::VsimCommand);
        registry.register(vectorset::VcardCommand);
        registry.register(vectorset::VdimCommand);
        registry.register(vectorset::VembCommand);
        registry.register(vectorset::VremCommand);
        registry.register(vectorset::VgetattrCommand);
        registry.register(vectorset::VsetattrCommand);
        registry.register(vectorset::VinfoCommand);
        registry.register(vectorset::VlinksCommand);
        registry.register(vectorset::VrandmemberCommand);
        registry.register(vectorset::VrangeCommand);
    }

    #[cfg(feature = "event-sourcing")]
    {
        // Event Sourcing commands (FrogDB extensions)
        registry.register(event_sourcing::EsAppendCommand);
        registry.register(event_sourcing::EsReadCommand);
        registry.register(event_sourcing::EsReplayCommand);
        registry.register(event_sourcing::EsInfoCommand);
        registry.register(event_sourcing::EsSnapshotCommand);
        registry.register(event_sourcing::EsAllCommand);
    }
}

#[cfg(test)]
mod denyoom_tests {
    use super::register_all;
    use frogdb_core::{CommandFlags, CommandRegistry};

    /// `DENYOOM` is what the shard's `maxmemory` gate consults
    /// (`frogdb-core`, `shard/execution.rs`), so these declarations decide
    /// which commands an operator can still run once an instance under
    /// `noeviction` is over its limit.
    ///
    /// The expected values are Redis's own `CMD_DENYOOM` bits, read from
    /// redis/redis 8.6.0 `src/commands/*.json` (as compiled into
    /// `src/commands.def`) — the same upstream revision `website/src/data/
    /// redis-commands-8x.json` is vendored from.
    #[test]
    fn denyoom_matches_redis_for_known_commands() {
        let mut registry = CommandRegistry::new();
        register_all(&mut registry);

        // (command, expected DENYOOM). Only `core-profile` commands belong in
        // the always-checked table — the family features (stream, json, ...)
        // are off by default, and a missing entry would read as a failure.
        // `mut` is only exercised by the feature-gated extensions below.
        #[allow(unused_mut)]
        let mut expected: Vec<(&str, bool)> = vec![
            // Allocating writes: refused over the limit.
            ("SET", true),
            ("APPEND", true),
            ("SETRANGE", true),
            ("INCR", true),
            ("HSET", true),
            ("LPUSH", true),
            ("SADD", true),
            ("ZADD", true),
            // Freeing writes: still admitted over the limit, which is the
            // whole point of the flag — these are the recovery commands.
            ("DEL", false),
            ("UNLINK", false),
            ("LPOP", false),
            ("RPOP", false),
            ("SREM", false),
            ("ZREM", false),
            ("HDEL", false),
            ("LTRIM", false),
            ("EXPIRE", false),
            ("GETDEL", false),
            // Reads never carry it.
            ("GET", false),
            ("MGET", false),
            ("STRLEN", false),
            ("LRANGE", false),
        ];

        #[cfg(feature = "stream")]
        expected.extend([("XADD", true), ("XDEL", false)]);

        for (name, want) in expected {
            let entry = registry
                .get_entry(name)
                .unwrap_or_else(|| panic!("{name} should be registered"));
            let got = entry.flags().contains(CommandFlags::DENYOOM);
            assert_eq!(
                got, want,
                "{name}: DENYOOM should be {want} (Redis 8.6.0 `CMD_DENYOOM`), got {got}"
            );
        }
    }

    /// `DENYOOM` is only meaningful on a write: the gate never runs for a
    /// read, so a read carrying the flag would be a lie in `COMMAND INFO`.
    #[test]
    fn denyoom_is_never_declared_without_write() {
        let mut registry = CommandRegistry::new();
        register_all(&mut registry);

        let offenders: Vec<&str> = registry
            .iter()
            .filter(|(_, entry)| {
                entry.flags().contains(CommandFlags::DENYOOM)
                    && !entry.flags().contains(CommandFlags::WRITE)
            })
            .map(|(name, _)| name)
            .collect();

        assert!(
            offenders.is_empty(),
            "DENYOOM without WRITE is unreachable by the gate: {offenders:?}"
        );
    }
}
