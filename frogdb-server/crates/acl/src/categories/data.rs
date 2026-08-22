//! Static data tables for command categories.

use std::collections::HashMap;
use std::sync::LazyLock;

use super::CommandCategory;
/// Alias keeping [`ALL_CATEGORIES`] to one row per command.
use super::CommandCategory as C;

/// Primary category mapping for commands.
pub(super) static COMMAND_CATEGORIES: LazyLock<HashMap<&'static str, CommandCategory>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();

        // String commands
        for cmd in [
            "get",
            "set",
            "append",
            "getrange",
            "setrange",
            "strlen",
            "incr",
            "incrby",
            "incrbyfloat",
            "decr",
            "decrby",
            "mget",
            "mset",
            "msetnx",
            "setnx",
            "setex",
            "psetex",
            "getset",
            "getdel",
            "getex",
        ] {
            map.insert(cmd, CommandCategory::String);
        }

        // List commands
        for cmd in [
            "lpush", "rpush", "lpushx", "rpushx", "lpop", "rpop", "lrange", "lindex", "lset",
            "llen", "linsert", "lrem", "ltrim", "lpos", "lmove", "lmpop",
        ] {
            map.insert(cmd, CommandCategory::List);
        }

        // Blocking commands
        for cmd in ["blpop", "brpop", "blmove", "blmpop", "brpoplpush", "blmove"] {
            map.insert(cmd, CommandCategory::Blocking);
        }

        // Set commands
        for cmd in [
            "sadd",
            "srem",
            "smembers",
            "sismember",
            "smismember",
            "scard",
            "spop",
            "srandmember",
            "sdiff",
            "sdiffstore",
            "sinter",
            "sinterstore",
            "sintercard",
            "sunion",
            "sunionstore",
            "smove",
            "sscan",
        ] {
            map.insert(cmd, CommandCategory::Set);
        }

        // Hash commands
        for cmd in [
            "hset",
            "hget",
            "hmset",
            "hmget",
            "hdel",
            "hexists",
            "hgetall",
            "hincrby",
            "hincrbyfloat",
            "hkeys",
            "hvals",
            "hlen",
            "hsetnx",
            "hstrlen",
            "hscan",
            "hrandfield",
        ] {
            map.insert(cmd, CommandCategory::Hash);
        }

        // Sorted set commands
        for cmd in [
            "zadd",
            "zrem",
            "zscore",
            "zrank",
            "zrevrank",
            "zrange",
            "zrevrange",
            "zrangebyscore",
            "zrevrangebyscore",
            "zrangebylex",
            "zrevrangebylex",
            "zcount",
            "zlexcount",
            "zcard",
            "zincrby",
            "zinterstore",
            "zunionstore",
            "zdiff",
            "zdiffstore",
            "zinter",
            "zunion",
            "zrangestore",
            "zmpop",
            "bzmpop",
            "zpopmin",
            "zpopmax",
            "bzpopmin",
            "bzpopmax",
            "zrandmember",
            "zscan",
            "zmscore",
        ] {
            map.insert(cmd, CommandCategory::Sortedset);
        }

        // Stream commands
        for cmd in [
            "xadd",
            "xread",
            "xreadgroup",
            "xrange",
            "xrevrange",
            "xlen",
            "xtrim",
            "xdel",
            "xgroup",
            "xinfo",
            "xack",
            "xclaim",
            "xautoclaim",
            "xpending",
            "xsetid",
        ] {
            map.insert(cmd, CommandCategory::Stream);
        }

        // Pub/sub commands
        for cmd in [
            "subscribe",
            "unsubscribe",
            "psubscribe",
            "punsubscribe",
            "publish",
            "pubsub",
            "ssubscribe",
            "sunsubscribe",
            "spublish",
        ] {
            map.insert(cmd, CommandCategory::Pubsub);
        }

        // Scripting commands
        for cmd in [
            "eval",
            "evalsha",
            "evalsha_ro",
            "eval_ro",
            "script",
            "fcall",
            "fcall_ro",
            "function",
        ] {
            map.insert(cmd, CommandCategory::Scripting);
        }

        // Keyspace commands
        for cmd in [
            "del",
            "unlink",
            "exists",
            "expire",
            "expireat",
            "expiretime",
            "pexpire",
            "pexpireat",
            "pexpiretime",
            "ttl",
            "pttl",
            "persist",
            "type",
            "rename",
            "renamenx",
            "copy",
            "dump",
            "restore",
            "object",
            "touch",
            "scan",
            "keys",
            "randomkey",
            "wait",
            "waitaof",
            "sort",
            "sort_ro",
        ] {
            map.insert(cmd, CommandCategory::Keyspace);
        }

        // Connection commands
        for cmd in [
            "auth", "ping", "echo", "quit", "select", "client", "reset", "hello",
        ] {
            map.insert(cmd, CommandCategory::Connection);
        }

        // Transaction commands
        for cmd in ["multi", "exec", "discard", "watch", "unwatch"] {
            map.insert(cmd, CommandCategory::Transaction);
        }

        // Admin commands
        for cmd in [
            "acl",
            "bgrewriteaof",
            "bgsave",
            "command",
            "config",
            "dbsize",
            "debug",
            "flushall",
            "flushdb",
            "info",
            "lastsave",
            "memory",
            "module",
            "monitor",
            "replicaof",
            "slaveof",
            "role",
            "save",
            "shutdown",
            "slowlog",
            "swapdb",
            "time",
            "latency",
            "failover",
            "cluster",
        ] {
            map.insert(cmd, CommandCategory::Admin);
        }

        // Bitmap commands
        for cmd in [
            "setbit",
            "getbit",
            "bitcount",
            "bitop",
            "bitpos",
            "bitfield",
            "bitfield_ro",
        ] {
            map.insert(cmd, CommandCategory::Bitmap);
        }

        // Geo commands
        for cmd in [
            "geoadd",
            "geodist",
            "geohash",
            "geopos",
            "georadius",
            "georadiusbymember",
            "geosearch",
            "geosearchstore",
        ] {
            map.insert(cmd, CommandCategory::Geo);
        }

        // HyperLogLog commands
        for cmd in ["pfadd", "pfcount", "pfmerge", "pfdebug", "pfselftest"] {
            map.insert(cmd, CommandCategory::Hyperloglog);
        }

        map
    });

/// Every ACL category a command belongs to, keyed by its lowercase wire name.
///
/// This table is what FrogDB's ACL engine actually enforces: `+@read` /
/// `-@dangerous` resolve through [`CommandCategory::all_for_command`], and
/// `COMMAND INFO`'s category array is the same set sorted into Redis's
/// declaration order. A command missing from here is covered by **no**
/// `@category` rule at all, in either direction.
///
/// **Sourcing.** The values agree with `redis/redis` at `REDIS_COMPAT_TARGET`,
/// but they are not copied from it at build time — per ADR-0005 the vendored
/// snapshot is evidence, never the answer FrogDB gives. Upstream states only
/// half of each row explicitly (`acl_categories` in `src/commands/*.json`) and
/// derives the rest at registration time from the command's flags
/// (`setImplicitACLCategories`): `write` implies `@write`, `readonly` implies
/// `@read` unless the command is `@scripting`, `admin` implies `@admin` *and*
/// `@dangerous`, `pubsub` implies `@pubsub`, `blocking` implies `@blocking`,
/// `fast` implies `@fast`, and anything not `@fast` is `@slow`. The derived
/// half here follows **FrogDB's** flags, so a command FrogDB implements
/// differently (`PFDEBUG` is a pure read for us; `WAITAOF` never blocks)
/// carries the categories its real behavior earns.
///
/// The join is held to that shape permanently by
/// `upstream_metadata_tests::vendored_acl_categories_agree_with_our_table` in
/// the server crate, which re-derives the expectation from the vendored rows
/// and fails on any drift.
///
/// **Container commands.** FrogDB registers one `CommandSpec` per container
/// and its ACL engine gates the container as a whole, so a container's row is
/// the union over the subcommands it dispatches — `CLUSTER` is `@admin` because
/// `CLUSTER SETSLOT` is, even though `CLUSTER INFO` alone would not be. Where
/// FrogDB additionally splits the container's *admin port* surface
/// (`frogdb_core::split_admin_surface_commands`, e.g. `MEMORY PURGE`), `@admin`
/// and `@dangerous` are added on top of upstream's union so `-@admin` covers
/// the half FrogDB itself refuses on a plain client port. The cost of
/// container granularity is recorded with the parity gate: `-@admin` also
/// denies the public half (`CLIENT SETNAME`), which Redis, gating per
/// subcommand, would allow.
///
/// Rows are sorted by name; each row's categories are in the order
/// `COMMAND INFO` reports them.
const ALL_CATEGORIES: &[(&str, &[CommandCategory])] = &[
    ("acl", &[C::Admin, C::Slow, C::Dangerous]),
    ("append", &[C::Write, C::String, C::Fast]),
    ("asking", &[C::Fast, C::Connection]),
    ("auth", &[C::Fast, C::Connection]),
    ("bgrewriteaof", &[C::Admin, C::Slow, C::Dangerous]),
    ("bgsave", &[C::Admin, C::Slow, C::Dangerous]),
    ("bitcount", &[C::Read, C::Bitmap, C::Slow]),
    ("bitfield", &[C::Write, C::Bitmap, C::Slow]),
    ("bitfield_ro", &[C::Read, C::Bitmap, C::Fast]),
    ("bitop", &[C::Write, C::Bitmap, C::Slow]),
    ("bitpos", &[C::Read, C::Bitmap, C::Slow]),
    ("blmove", &[C::Write, C::List, C::Slow, C::Blocking]),
    ("blmpop", &[C::Write, C::List, C::Slow, C::Blocking]),
    ("blpop", &[C::Write, C::List, C::Slow, C::Blocking]),
    ("brpop", &[C::Write, C::List, C::Slow, C::Blocking]),
    ("brpoplpush", &[C::Write, C::List, C::Slow, C::Blocking]),
    ("bzmpop", &[C::Write, C::Sortedset, C::Slow, C::Blocking]),
    ("bzpopmax", &[C::Write, C::Sortedset, C::Fast, C::Blocking]),
    ("bzpopmin", &[C::Write, C::Sortedset, C::Fast, C::Blocking]),
    ("client", &[C::Admin, C::Slow, C::Dangerous, C::Connection]),
    ("cluster", &[C::Admin, C::Slow, C::Dangerous]),
    ("command", &[C::Slow, C::Connection]),
    ("config", &[C::Admin, C::Slow, C::Dangerous]),
    ("copy", &[C::Keyspace, C::Write, C::Slow]),
    ("dbsize", &[C::Keyspace, C::Read, C::Fast]),
    ("debug", &[C::Admin, C::Slow, C::Dangerous]),
    ("decr", &[C::Write, C::String, C::Fast]),
    ("decrby", &[C::Write, C::String, C::Fast]),
    ("del", &[C::Keyspace, C::Write, C::Slow]),
    ("delex", &[C::Write, C::String, C::Fast]),
    ("digest", &[C::Read, C::String, C::Fast]),
    ("discard", &[C::Fast, C::Transaction]),
    ("dump", &[C::Keyspace, C::Read, C::Slow]),
    ("echo", &[C::Fast, C::Connection]),
    ("eval", &[C::Slow, C::Scripting]),
    ("eval_ro", &[C::Slow, C::Scripting]),
    ("evalsha", &[C::Slow, C::Scripting]),
    ("evalsha_ro", &[C::Slow, C::Scripting]),
    ("exec", &[C::Slow, C::Transaction]),
    ("exists", &[C::Keyspace, C::Read, C::Fast]),
    ("expire", &[C::Keyspace, C::Write, C::Fast]),
    ("expireat", &[C::Keyspace, C::Write, C::Fast]),
    ("expiretime", &[C::Keyspace, C::Read, C::Fast]),
    ("fcall", &[C::Slow, C::Scripting]),
    ("fcall_ro", &[C::Slow, C::Scripting]),
    ("flushall", &[C::Keyspace, C::Write, C::Slow, C::Dangerous]),
    ("flushdb", &[C::Keyspace, C::Write, C::Slow, C::Dangerous]),
    // forces a shard-local finalization pass — an operator verb with no Redis counterpart, categorised like the DEBUG/FAILOVER class it sits with
    ("frogdb.finalize", &[C::Admin, C::Slow, C::Dangerous]),
    // discloses per-shard access concentration, the same disclosure surface HOTKEYS carries upstream
    ("frogdb.hotshards", &[C::Admin, C::Slow, C::Dangerous]),
    // a constant string read, categorised like LOLWUT
    ("frogdb.version", &[C::Read, C::Fast]),
    ("function", &[C::Write, C::Slow, C::Scripting]),
    ("geoadd", &[C::Write, C::Geo, C::Slow]),
    ("geodist", &[C::Read, C::Geo, C::Slow]),
    ("geohash", &[C::Read, C::Geo, C::Slow]),
    ("geopos", &[C::Read, C::Geo, C::Slow]),
    ("georadius", &[C::Write, C::Geo, C::Slow]),
    ("georadius_ro", &[C::Read, C::Geo, C::Slow]),
    ("georadiusbymember", &[C::Write, C::Geo, C::Slow]),
    ("georadiusbymember_ro", &[C::Read, C::Geo, C::Slow]),
    ("geosearch", &[C::Read, C::Geo, C::Slow]),
    ("geosearchstore", &[C::Write, C::Geo, C::Slow]),
    ("get", &[C::Read, C::String, C::Fast]),
    ("getbit", &[C::Read, C::Bitmap, C::Fast]),
    ("getdel", &[C::Write, C::String, C::Fast]),
    ("getex", &[C::Write, C::String, C::Fast]),
    ("getrange", &[C::Read, C::String, C::Slow]),
    ("getset", &[C::Write, C::String, C::Fast]),
    ("hdel", &[C::Write, C::Hash, C::Fast]),
    ("hello", &[C::Fast, C::Connection]),
    ("hexists", &[C::Read, C::Hash, C::Fast]),
    ("hexpire", &[C::Write, C::Hash, C::Fast]),
    ("hexpireat", &[C::Write, C::Hash, C::Fast]),
    ("hexpiretime", &[C::Read, C::Hash, C::Fast]),
    ("hget", &[C::Read, C::Hash, C::Fast]),
    ("hgetall", &[C::Read, C::Hash, C::Slow]),
    ("hgetdel", &[C::Write, C::Hash, C::Fast]),
    ("hgetex", &[C::Write, C::Hash, C::Fast]),
    ("hincrby", &[C::Write, C::Hash, C::Fast]),
    ("hincrbyfloat", &[C::Write, C::Hash, C::Fast]),
    ("hkeys", &[C::Read, C::Hash, C::Slow]),
    ("hlen", &[C::Read, C::Hash, C::Fast]),
    ("hmget", &[C::Read, C::Hash, C::Fast]),
    ("hmset", &[C::Write, C::Hash, C::Fast]),
    ("hotkeys", &[C::Admin, C::Slow, C::Dangerous]),
    ("hpersist", &[C::Write, C::Hash, C::Fast]),
    ("hpexpire", &[C::Write, C::Hash, C::Fast]),
    ("hpexpireat", &[C::Write, C::Hash, C::Fast]),
    ("hpexpiretime", &[C::Read, C::Hash, C::Fast]),
    ("hpttl", &[C::Read, C::Hash, C::Fast]),
    ("hrandfield", &[C::Read, C::Hash, C::Slow]),
    ("hscan", &[C::Read, C::Hash, C::Slow]),
    ("hset", &[C::Write, C::Hash, C::Fast]),
    ("hsetex", &[C::Write, C::Hash, C::Fast]),
    ("hsetnx", &[C::Write, C::Hash, C::Fast]),
    ("hstrlen", &[C::Read, C::Hash, C::Fast]),
    ("httl", &[C::Read, C::Hash, C::Fast]),
    ("hvals", &[C::Read, C::Hash, C::Slow]),
    ("incr", &[C::Write, C::String, C::Fast]),
    ("incrby", &[C::Write, C::String, C::Fast]),
    ("incrbyfloat", &[C::Write, C::String, C::Fast]),
    ("info", &[C::Slow, C::Dangerous]),
    ("keys", &[C::Keyspace, C::Read, C::Slow, C::Dangerous]),
    ("lastsave", &[C::Admin, C::Fast, C::Dangerous]),
    ("latency", &[C::Admin, C::Slow, C::Dangerous]),
    ("lcs", &[C::Read, C::String, C::Slow]),
    ("lindex", &[C::Read, C::List, C::Slow]),
    ("linsert", &[C::Write, C::List, C::Slow]),
    ("llen", &[C::Read, C::List, C::Fast]),
    ("lmove", &[C::Write, C::List, C::Slow]),
    ("lmpop", &[C::Write, C::List, C::Slow]),
    ("lolwut", &[C::Read, C::Fast]),
    ("lpop", &[C::Write, C::List, C::Fast]),
    ("lpos", &[C::Read, C::List, C::Slow]),
    ("lpush", &[C::Write, C::List, C::Fast]),
    ("lpushx", &[C::Write, C::List, C::Fast]),
    ("lrange", &[C::Read, C::List, C::Slow]),
    ("lrem", &[C::Write, C::List, C::Slow]),
    ("lset", &[C::Write, C::List, C::Slow]),
    ("ltrim", &[C::Write, C::List, C::Slow]),
    ("memory", &[C::Read, C::Admin, C::Slow, C::Dangerous]),
    ("mget", &[C::Read, C::String, C::Fast]),
    ("migrate", &[C::Keyspace, C::Write, C::Slow, C::Dangerous]),
    ("module", &[C::Admin, C::Slow, C::Dangerous]),
    ("monitor", &[C::Admin, C::Slow, C::Dangerous]),
    ("move", &[C::Keyspace, C::Write, C::Fast]),
    ("mset", &[C::Write, C::String, C::Slow]),
    ("msetex", &[C::Write, C::String, C::Slow]),
    ("msetnx", &[C::Write, C::String, C::Slow]),
    ("multi", &[C::Fast, C::Transaction]),
    ("object", &[C::Keyspace, C::Read, C::Slow]),
    ("persist", &[C::Keyspace, C::Write, C::Fast]),
    ("pexpire", &[C::Keyspace, C::Write, C::Fast]),
    ("pexpireat", &[C::Keyspace, C::Write, C::Fast]),
    ("pexpiretime", &[C::Keyspace, C::Read, C::Fast]),
    ("pfadd", &[C::Write, C::Hyperloglog, C::Fast]),
    ("pfcount", &[C::Read, C::Hyperloglog, C::Slow]),
    (
        "pfdebug",
        &[C::Read, C::Hyperloglog, C::Admin, C::Slow, C::Dangerous],
    ),
    ("pfmerge", &[C::Write, C::Hyperloglog, C::Slow]),
    (
        "pfselftest",
        &[C::Hyperloglog, C::Admin, C::Slow, C::Dangerous],
    ),
    ("ping", &[C::Fast, C::Connection]),
    ("psetex", &[C::Write, C::String, C::Slow]),
    ("psubscribe", &[C::Pubsub, C::Slow]),
    ("psync", &[C::Admin, C::Slow, C::Dangerous]),
    ("pttl", &[C::Keyspace, C::Read, C::Fast]),
    ("publish", &[C::Pubsub, C::Fast]),
    ("pubsub", &[C::Pubsub, C::Slow]),
    ("punsubscribe", &[C::Pubsub, C::Slow]),
    ("quit", &[C::Fast, C::Connection]),
    ("randomkey", &[C::Keyspace, C::Read, C::Slow]),
    ("readonly", &[C::Fast, C::Connection]),
    ("readwrite", &[C::Fast, C::Connection]),
    ("rename", &[C::Keyspace, C::Write, C::Slow]),
    ("renamenx", &[C::Keyspace, C::Write, C::Fast]),
    ("replconf", &[C::Admin, C::Slow, C::Dangerous]),
    ("replicaof", &[C::Admin, C::Slow, C::Dangerous]),
    ("reset", &[C::Fast, C::Connection]),
    ("restore", &[C::Keyspace, C::Write, C::Slow, C::Dangerous]),
    ("role", &[C::Admin, C::Fast, C::Dangerous]),
    ("rpop", &[C::Write, C::List, C::Fast]),
    ("rpoplpush", &[C::Write, C::List, C::Slow]),
    ("rpush", &[C::Write, C::List, C::Fast]),
    ("rpushx", &[C::Write, C::List, C::Fast]),
    ("sadd", &[C::Write, C::Set, C::Fast]),
    ("save", &[C::Admin, C::Slow, C::Dangerous]),
    ("scan", &[C::Keyspace, C::Read, C::Slow]),
    ("scard", &[C::Read, C::Set, C::Fast]),
    ("script", &[C::Slow, C::Scripting]),
    ("sdiff", &[C::Read, C::Set, C::Slow]),
    ("sdiffstore", &[C::Write, C::Set, C::Slow]),
    ("select", &[C::Fast, C::Connection]),
    ("set", &[C::Write, C::String, C::Slow]),
    ("setbit", &[C::Write, C::Bitmap, C::Slow]),
    ("setex", &[C::Write, C::String, C::Slow]),
    ("setnx", &[C::Write, C::String, C::Fast]),
    ("setrange", &[C::Write, C::String, C::Slow]),
    ("shutdown", &[C::Admin, C::Slow, C::Dangerous]),
    ("sinter", &[C::Read, C::Set, C::Slow]),
    ("sintercard", &[C::Read, C::Set, C::Slow]),
    ("sinterstore", &[C::Write, C::Set, C::Slow]),
    ("sismember", &[C::Read, C::Set, C::Fast]),
    ("slaveof", &[C::Admin, C::Slow, C::Dangerous]),
    ("slowlog", &[C::Admin, C::Slow, C::Dangerous]),
    ("smembers", &[C::Read, C::Set, C::Slow]),
    ("smismember", &[C::Read, C::Set, C::Fast]),
    ("smove", &[C::Write, C::Set, C::Fast]),
    (
        "sort",
        &[
            C::Write,
            C::Set,
            C::Sortedset,
            C::List,
            C::Slow,
            C::Dangerous,
        ],
    ),
    (
        "sort_ro",
        &[
            C::Read,
            C::Set,
            C::Sortedset,
            C::List,
            C::Slow,
            C::Dangerous,
        ],
    ),
    ("spop", &[C::Write, C::Set, C::Fast]),
    ("spublish", &[C::Pubsub, C::Fast]),
    ("srandmember", &[C::Read, C::Set, C::Slow]),
    ("srem", &[C::Write, C::Set, C::Fast]),
    ("sscan", &[C::Read, C::Set, C::Slow]),
    ("ssubscribe", &[C::Pubsub, C::Slow]),
    // an operator-facing health summary; INFO is its nearest sibling, minus @dangerous — STATUS discloses no client or keyspace detail
    ("status", &[C::Read, C::Slow]),
    ("strlen", &[C::Read, C::String, C::Fast]),
    ("subscribe", &[C::Pubsub, C::Slow]),
    ("substr", &[C::Read, C::String, C::Slow]),
    ("sunion", &[C::Read, C::Set, C::Slow]),
    ("sunionstore", &[C::Write, C::Set, C::Slow]),
    ("sunsubscribe", &[C::Pubsub, C::Slow]),
    ("swapdb", &[C::Keyspace, C::Write, C::Fast, C::Dangerous]),
    ("sync", &[C::Admin, C::Slow, C::Dangerous]),
    ("time", &[C::Fast]),
    ("touch", &[C::Keyspace, C::Read, C::Fast]),
    ("ttl", &[C::Keyspace, C::Read, C::Fast]),
    ("type", &[C::Keyspace, C::Read, C::Fast]),
    ("unlink", &[C::Keyspace, C::Write, C::Fast]),
    ("unsubscribe", &[C::Pubsub, C::Slow]),
    ("unwatch", &[C::Fast, C::Transaction]),
    ("wait", &[C::Slow, C::Blocking, C::Connection]),
    ("waitaof", &[C::Slow, C::Connection]),
    ("watch", &[C::Fast, C::Transaction]),
    ("xack", &[C::Write, C::Stream, C::Fast]),
    ("xackdel", &[C::Write, C::Stream, C::Fast]),
    ("xadd", &[C::Write, C::Stream, C::Fast]),
    ("xautoclaim", &[C::Write, C::Stream, C::Fast]),
    ("xclaim", &[C::Write, C::Stream, C::Fast]),
    ("xdel", &[C::Write, C::Stream, C::Fast]),
    ("xdelex", &[C::Write, C::Stream, C::Fast]),
    ("xgroup", &[C::Write, C::Stream, C::Slow]),
    ("xinfo", &[C::Read, C::Stream, C::Slow]),
    ("xlen", &[C::Read, C::Stream, C::Fast]),
    ("xpending", &[C::Read, C::Stream, C::Slow]),
    ("xrange", &[C::Read, C::Stream, C::Slow]),
    ("xread", &[C::Read, C::Stream, C::Slow, C::Blocking]),
    ("xreadgroup", &[C::Write, C::Stream, C::Slow, C::Blocking]),
    ("xrevrange", &[C::Read, C::Stream, C::Slow]),
    ("xsetid", &[C::Write, C::Stream, C::Fast]),
    ("xtrim", &[C::Write, C::Stream, C::Slow]),
    ("zadd", &[C::Write, C::Sortedset, C::Fast]),
    ("zcard", &[C::Read, C::Sortedset, C::Fast]),
    ("zcount", &[C::Read, C::Sortedset, C::Fast]),
    ("zdiff", &[C::Read, C::Sortedset, C::Slow]),
    ("zdiffstore", &[C::Write, C::Sortedset, C::Slow]),
    ("zincrby", &[C::Write, C::Sortedset, C::Fast]),
    ("zinter", &[C::Read, C::Sortedset, C::Slow]),
    ("zintercard", &[C::Read, C::Sortedset, C::Slow]),
    ("zinterstore", &[C::Write, C::Sortedset, C::Slow]),
    ("zlexcount", &[C::Read, C::Sortedset, C::Fast]),
    ("zmpop", &[C::Write, C::Sortedset, C::Slow]),
    ("zmscore", &[C::Read, C::Sortedset, C::Fast]),
    ("zpopmax", &[C::Write, C::Sortedset, C::Fast]),
    ("zpopmin", &[C::Write, C::Sortedset, C::Fast]),
    ("zrandmember", &[C::Read, C::Sortedset, C::Slow]),
    ("zrange", &[C::Read, C::Sortedset, C::Slow]),
    ("zrangebylex", &[C::Read, C::Sortedset, C::Slow]),
    ("zrangebyscore", &[C::Read, C::Sortedset, C::Slow]),
    ("zrangestore", &[C::Write, C::Sortedset, C::Slow]),
    ("zrank", &[C::Read, C::Sortedset, C::Fast]),
    ("zrem", &[C::Write, C::Sortedset, C::Fast]),
    ("zremrangebylex", &[C::Write, C::Sortedset, C::Slow]),
    ("zremrangebyrank", &[C::Write, C::Sortedset, C::Slow]),
    ("zremrangebyscore", &[C::Write, C::Sortedset, C::Slow]),
    ("zrevrange", &[C::Read, C::Sortedset, C::Slow]),
    ("zrevrangebylex", &[C::Read, C::Sortedset, C::Slow]),
    ("zrevrangebyscore", &[C::Read, C::Sortedset, C::Slow]),
    ("zrevrank", &[C::Read, C::Sortedset, C::Fast]),
    ("zscan", &[C::Read, C::Sortedset, C::Slow]),
    ("zscore", &[C::Read, C::Sortedset, C::Fast]),
    ("zunion", &[C::Read, C::Sortedset, C::Slow]),
    ("zunionstore", &[C::Write, C::Sortedset, C::Slow]),
];

/// All categories for each command.
pub(super) static COMMAND_ALL_CATEGORIES: LazyLock<HashMap<&'static str, Vec<CommandCategory>>> =
    LazyLock::new(|| {
        ALL_CATEGORIES
            .iter()
            .map(|(name, categories)| (*name, categories.to_vec()))
            .collect()
    });

/// Commands in each category.
pub(super) static CATEGORY_COMMANDS: LazyLock<HashMap<CommandCategory, Vec<&'static str>>> =
    LazyLock::new(|| {
        let mut map: HashMap<CommandCategory, Vec<&'static str>> = HashMap::new();

        for category in CommandCategory::all() {
            map.insert(*category, Vec::new());
        }

        for (cmd, categories) in COMMAND_ALL_CATEGORIES.iter() {
            for category in categories {
                if let Some(cmds) = map.get_mut(category) {
                    cmds.push(*cmd);
                }
            }
        }

        // Sort each category's commands
        for cmds in map.values_mut() {
            cmds.sort();
            cmds.dedup();
        }

        map
    });

#[cfg(test)]
mod tests {
    use super::*;

    /// The table is a flat slice folded into a `HashMap`, so a duplicated name
    /// would silently keep only the last row and a misfiled one would be hard
    /// to find by eye. Sorted-and-unique makes both impossible.
    #[test]
    fn all_categories_table_is_sorted_and_unique() {
        for pair in ALL_CATEGORIES.windows(2) {
            assert!(
                pair[0].0 < pair[1].0,
                "ALL_CATEGORIES must be sorted by name with no duplicates: \
                 {:?} precedes {:?}",
                pair[0].0,
                pair[1].0
            );
        }
        assert_eq!(COMMAND_ALL_CATEGORIES.len(), ALL_CATEGORIES.len());
    }

    /// Names are the lowercase wire spelling — `all_for_command` lowercases its
    /// argument and looks up directly, so an uppercase row would never match.
    #[test]
    fn all_categories_table_is_lowercase() {
        for (name, _) in ALL_CATEGORIES {
            assert!(
                !name.chars().any(|c| c.is_ascii_uppercase()),
                "{name} must be spelled lowercase"
            );
        }
    }

    /// Every row carries exactly one of `@fast` / `@slow`: Redis assigns
    /// `@slow` to whatever is not `@fast`, so neither "both" nor "neither" is a
    /// state any command can legitimately be in.
    #[test]
    fn every_command_is_fast_xor_slow() {
        for (name, cats) in ALL_CATEGORIES {
            let fast = cats.contains(&CommandCategory::Fast);
            let slow = cats.contains(&CommandCategory::Slow);
            assert!(fast ^ slow, "{name} must be exactly one of @fast / @slow");
        }
    }
}
