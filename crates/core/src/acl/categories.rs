//! Command categories for ACL.
//!
//! Redis command categories as defined in the ACL system.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::LazyLock;

/// Command categories for ACL permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandCategory {
    /// Administrative commands (SHUTDOWN, CONFIG, etc.)
    Admin,
    /// Bitmap commands (SETBIT, GETBIT, etc.)
    Bitmap,
    /// Blocking commands (BLPOP, BRPOP, etc.)
    Blocking,
    /// Connection commands (AUTH, PING, etc.)
    Connection,
    /// Dangerous commands (FLUSHALL, DEBUG, etc.)
    Dangerous,
    /// Fast O(1) commands
    Fast,
    /// Geo commands (GEOADD, GEODIST, etc.)
    Geo,
    /// Hash commands (HSET, HGET, etc.)
    Hash,
    /// HyperLogLog commands (PFADD, PFCOUNT, etc.)
    Hyperloglog,
    /// Keyspace commands (DEL, RENAME, etc.)
    Keyspace,
    /// List commands (LPUSH, RPOP, etc.)
    List,
    /// Pub/Sub commands
    Pubsub,
    /// Read commands
    Read,
    /// Scripting commands (EVAL, SCRIPT, etc.)
    Scripting,
    /// Set commands (SADD, SMEMBERS, etc.)
    Set,
    /// Slow commands
    Slow,
    /// Sorted set commands (ZADD, ZRANGE, etc.)
    Sortedset,
    /// Stream commands (XADD, XREAD, etc.)
    Stream,
    /// String commands (GET, SET, etc.)
    String,
    /// Transaction commands (MULTI, EXEC, etc.)
    Transaction,
    /// Write commands
    Write,
}

impl CommandCategory {
    /// Get all categories.
    pub fn all() -> &'static [CommandCategory] {
        &[
            CommandCategory::Admin,
            CommandCategory::Bitmap,
            CommandCategory::Blocking,
            CommandCategory::Connection,
            CommandCategory::Dangerous,
            CommandCategory::Fast,
            CommandCategory::Geo,
            CommandCategory::Hash,
            CommandCategory::Hyperloglog,
            CommandCategory::Keyspace,
            CommandCategory::List,
            CommandCategory::Pubsub,
            CommandCategory::Read,
            CommandCategory::Scripting,
            CommandCategory::Set,
            CommandCategory::Slow,
            CommandCategory::Sortedset,
            CommandCategory::Stream,
            CommandCategory::String,
            CommandCategory::Transaction,
            CommandCategory::Write,
        ]
    }

    /// Get the string name of this category.
    pub fn name(&self) -> &'static str {
        match self {
            CommandCategory::Admin => "admin",
            CommandCategory::Bitmap => "bitmap",
            CommandCategory::Blocking => "blocking",
            CommandCategory::Connection => "connection",
            CommandCategory::Dangerous => "dangerous",
            CommandCategory::Fast => "fast",
            CommandCategory::Geo => "geo",
            CommandCategory::Hash => "hash",
            CommandCategory::Hyperloglog => "hyperloglog",
            CommandCategory::Keyspace => "keyspace",
            CommandCategory::List => "list",
            CommandCategory::Pubsub => "pubsub",
            CommandCategory::Read => "read",
            CommandCategory::Scripting => "scripting",
            CommandCategory::Set => "set",
            CommandCategory::Slow => "slow",
            CommandCategory::Sortedset => "sortedset",
            CommandCategory::Stream => "stream",
            CommandCategory::String => "string",
            CommandCategory::Transaction => "transaction",
            CommandCategory::Write => "write",
        }
    }

    /// Parse a category from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(CommandCategory::Admin),
            "bitmap" => Some(CommandCategory::Bitmap),
            "blocking" => Some(CommandCategory::Blocking),
            "connection" => Some(CommandCategory::Connection),
            "dangerous" => Some(CommandCategory::Dangerous),
            "fast" => Some(CommandCategory::Fast),
            "geo" => Some(CommandCategory::Geo),
            "hash" => Some(CommandCategory::Hash),
            "hyperloglog" => Some(CommandCategory::Hyperloglog),
            "keyspace" => Some(CommandCategory::Keyspace),
            "list" => Some(CommandCategory::List),
            "pubsub" => Some(CommandCategory::Pubsub),
            "read" => Some(CommandCategory::Read),
            "scripting" => Some(CommandCategory::Scripting),
            "set" => Some(CommandCategory::Set),
            "slow" => Some(CommandCategory::Slow),
            "sortedset" => Some(CommandCategory::Sortedset),
            "stream" => Some(CommandCategory::Stream),
            "string" => Some(CommandCategory::String),
            "transaction" => Some(CommandCategory::Transaction),
            "write" => Some(CommandCategory::Write),
            _ => None,
        }
    }

    /// Get the primary category for a command.
    pub fn for_command(cmd: &str) -> Option<Self> {
        COMMAND_CATEGORIES.get(&cmd.to_lowercase().as_str()).copied()
    }

    /// Get all categories for a command.
    pub fn all_for_command(cmd: &str) -> Vec<Self> {
        COMMAND_ALL_CATEGORIES
            .get(&cmd.to_lowercase().as_str())
            .cloned()
            .unwrap_or_default()
    }

    /// Get all commands in this category.
    pub fn commands(&self) -> Vec<&'static str> {
        CATEGORY_COMMANDS
            .get(self)
            .cloned()
            .unwrap_or_default()
    }
}

impl FromStr for CommandCategory {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CommandCategory::parse(s).ok_or(())
    }
}

/// Primary category mapping for commands.
static COMMAND_CATEGORIES: LazyLock<HashMap<&'static str, CommandCategory>> = LazyLock::new(|| {
    let mut map = HashMap::new();

    // String commands
    for cmd in ["get", "set", "append", "getrange", "setrange", "strlen", "incr", "incrby",
                "incrbyfloat", "decr", "decrby", "mget", "mset", "msetnx", "setnx", "setex",
                "psetex", "getset", "getdel", "getex"] {
        map.insert(cmd, CommandCategory::String);
    }

    // List commands
    for cmd in ["lpush", "rpush", "lpushx", "rpushx", "lpop", "rpop", "lrange", "lindex",
                "lset", "llen", "linsert", "lrem", "ltrim", "lpos", "lmove", "lmpop"] {
        map.insert(cmd, CommandCategory::List);
    }

    // Blocking commands
    for cmd in ["blpop", "brpop", "blmove", "blmpop", "brpoplpush", "blmove"] {
        map.insert(cmd, CommandCategory::Blocking);
    }

    // Set commands
    for cmd in ["sadd", "srem", "smembers", "sismember", "smismember", "scard", "spop",
                "srandmember", "sdiff", "sdiffstore", "sinter", "sinterstore", "sintercard",
                "sunion", "sunionstore", "smove", "sscan"] {
        map.insert(cmd, CommandCategory::Set);
    }

    // Hash commands
    for cmd in ["hset", "hget", "hmset", "hmget", "hdel", "hexists", "hgetall", "hincrby",
                "hincrbyfloat", "hkeys", "hvals", "hlen", "hsetnx", "hstrlen", "hscan",
                "hrandfield"] {
        map.insert(cmd, CommandCategory::Hash);
    }

    // Sorted set commands
    for cmd in ["zadd", "zrem", "zscore", "zrank", "zrevrank", "zrange", "zrevrange",
                "zrangebyscore", "zrevrangebyscore", "zrangebylex", "zrevrangebylex",
                "zcount", "zlexcount", "zcard", "zincrby", "zinterstore", "zunionstore",
                "zdiff", "zdiffstore", "zinter", "zunion", "zrangestore", "zmpop",
                "bzmpop", "zpopmin", "zpopmax", "bzpopmin", "bzpopmax", "zrandmember",
                "zscan", "zmscore"] {
        map.insert(cmd, CommandCategory::Sortedset);
    }

    // Stream commands
    for cmd in ["xadd", "xread", "xreadgroup", "xrange", "xrevrange", "xlen", "xtrim",
                "xdel", "xgroup", "xinfo", "xack", "xclaim", "xautoclaim", "xpending",
                "xsetid"] {
        map.insert(cmd, CommandCategory::Stream);
    }

    // Pub/sub commands
    for cmd in ["subscribe", "unsubscribe", "psubscribe", "punsubscribe", "publish",
                "pubsub", "ssubscribe", "sunsubscribe", "spublish"] {
        map.insert(cmd, CommandCategory::Pubsub);
    }

    // Scripting commands
    for cmd in ["eval", "evalsha", "evalsha_ro", "eval_ro", "script", "fcall",
                "fcall_ro", "function"] {
        map.insert(cmd, CommandCategory::Scripting);
    }

    // Keyspace commands
    for cmd in ["del", "unlink", "exists", "expire", "expireat", "expiretime", "pexpire",
                "pexpireat", "pexpiretime", "ttl", "pttl", "persist", "type", "rename",
                "renamenx", "copy", "dump", "restore", "object", "touch", "scan", "keys",
                "randomkey", "wait", "waitaof", "sort", "sort_ro"] {
        map.insert(cmd, CommandCategory::Keyspace);
    }

    // Connection commands
    for cmd in ["auth", "ping", "echo", "quit", "select", "client", "reset", "hello"] {
        map.insert(cmd, CommandCategory::Connection);
    }

    // Transaction commands
    for cmd in ["multi", "exec", "discard", "watch", "unwatch"] {
        map.insert(cmd, CommandCategory::Transaction);
    }

    // Admin commands
    for cmd in ["acl", "bgrewriteaof", "bgsave", "command", "config", "dbsize", "debug",
                "flushall", "flushdb", "info", "lastsave", "memory", "module", "monitor",
                "replicaof", "slaveof", "role", "save", "shutdown", "slowlog", "swapdb",
                "time", "latency", "failover", "cluster"] {
        map.insert(cmd, CommandCategory::Admin);
    }

    // Bitmap commands
    for cmd in ["setbit", "getbit", "bitcount", "bitop", "bitpos", "bitfield",
                "bitfield_ro"] {
        map.insert(cmd, CommandCategory::Bitmap);
    }

    // Geo commands
    for cmd in ["geoadd", "geodist", "geohash", "geopos", "georadius", "georadiusbymember",
                "geosearch", "geosearchstore"] {
        map.insert(cmd, CommandCategory::Geo);
    }

    // HyperLogLog commands
    for cmd in ["pfadd", "pfcount", "pfmerge", "pfdebug", "pfselftest"] {
        map.insert(cmd, CommandCategory::Hyperloglog);
    }

    map
});

/// All categories for each command.
static COMMAND_ALL_CATEGORIES: LazyLock<HashMap<&'static str, Vec<CommandCategory>>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();

        // String commands - most are fast reads or writes
        map.insert("get", vec![CommandCategory::String, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("set", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("setnx", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("setex", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("psetex", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("append", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("incr", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("incrby", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("incrbyfloat", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("decr", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("decrby", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("mget", vec![CommandCategory::String, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("mset", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("msetnx", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("strlen", vec![CommandCategory::String, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("getrange", vec![CommandCategory::String, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("setrange", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("getset", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("getdel", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("getex", vec![CommandCategory::String, CommandCategory::Write, CommandCategory::Fast]);

        // List commands
        map.insert("lpush", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("rpush", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("lpushx", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("rpushx", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("lpop", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("rpop", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("lrange", vec![CommandCategory::List, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("lindex", vec![CommandCategory::List, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("lset", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("llen", vec![CommandCategory::List, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("linsert", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("lrem", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("ltrim", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("lpos", vec![CommandCategory::List, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("lmove", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("lmpop", vec![CommandCategory::List, CommandCategory::Write, CommandCategory::Slow]);

        // Blocking commands
        map.insert("blpop", vec![CommandCategory::List, CommandCategory::Blocking, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("brpop", vec![CommandCategory::List, CommandCategory::Blocking, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("blmove", vec![CommandCategory::List, CommandCategory::Blocking, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("blmpop", vec![CommandCategory::List, CommandCategory::Blocking, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("brpoplpush", vec![CommandCategory::List, CommandCategory::Blocking, CommandCategory::Write, CommandCategory::Slow]);

        // Set commands
        map.insert("sadd", vec![CommandCategory::Set, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("srem", vec![CommandCategory::Set, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("smembers", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("sismember", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("smismember", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("scard", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("spop", vec![CommandCategory::Set, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("srandmember", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("sdiff", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("sdiffstore", vec![CommandCategory::Set, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("sinter", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("sinterstore", vec![CommandCategory::Set, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("sintercard", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("sunion", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("sunionstore", vec![CommandCategory::Set, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("smove", vec![CommandCategory::Set, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("sscan", vec![CommandCategory::Set, CommandCategory::Read, CommandCategory::Slow]);

        // Hash commands
        map.insert("hset", vec![CommandCategory::Hash, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("hget", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("hmset", vec![CommandCategory::Hash, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("hmget", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("hdel", vec![CommandCategory::Hash, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("hexists", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("hgetall", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("hincrby", vec![CommandCategory::Hash, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("hincrbyfloat", vec![CommandCategory::Hash, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("hkeys", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("hvals", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("hlen", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("hsetnx", vec![CommandCategory::Hash, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("hstrlen", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("hscan", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("hrandfield", vec![CommandCategory::Hash, CommandCategory::Read, CommandCategory::Slow]);

        // Sorted set commands
        map.insert("zadd", vec![CommandCategory::Sortedset, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("zrem", vec![CommandCategory::Sortedset, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("zscore", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("zrank", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("zrevrank", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("zrange", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zrevrange", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zrangebyscore", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zrevrangebyscore", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zrangebylex", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zrevrangebylex", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zcount", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zlexcount", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zcard", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("zincrby", vec![CommandCategory::Sortedset, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("zinterstore", vec![CommandCategory::Sortedset, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("zunionstore", vec![CommandCategory::Sortedset, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("zpopmin", vec![CommandCategory::Sortedset, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("zpopmax", vec![CommandCategory::Sortedset, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("bzpopmin", vec![CommandCategory::Sortedset, CommandCategory::Blocking, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("bzpopmax", vec![CommandCategory::Sortedset, CommandCategory::Blocking, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("zrandmember", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zscan", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("zmscore", vec![CommandCategory::Sortedset, CommandCategory::Read, CommandCategory::Fast]);

        // Stream commands
        map.insert("xadd", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("xread", vec![CommandCategory::Stream, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("xreadgroup", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("xrange", vec![CommandCategory::Stream, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("xrevrange", vec![CommandCategory::Stream, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("xlen", vec![CommandCategory::Stream, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("xtrim", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("xdel", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("xgroup", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("xinfo", vec![CommandCategory::Stream, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("xack", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("xclaim", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("xautoclaim", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("xpending", vec![CommandCategory::Stream, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("xsetid", vec![CommandCategory::Stream, CommandCategory::Write, CommandCategory::Fast]);

        // Pub/sub commands
        map.insert("subscribe", vec![CommandCategory::Pubsub, CommandCategory::Slow]);
        map.insert("unsubscribe", vec![CommandCategory::Pubsub, CommandCategory::Slow]);
        map.insert("psubscribe", vec![CommandCategory::Pubsub, CommandCategory::Slow]);
        map.insert("punsubscribe", vec![CommandCategory::Pubsub, CommandCategory::Slow]);
        map.insert("publish", vec![CommandCategory::Pubsub, CommandCategory::Fast]);
        map.insert("pubsub", vec![CommandCategory::Pubsub, CommandCategory::Slow]);
        map.insert("ssubscribe", vec![CommandCategory::Pubsub, CommandCategory::Slow]);
        map.insert("sunsubscribe", vec![CommandCategory::Pubsub, CommandCategory::Slow]);
        map.insert("spublish", vec![CommandCategory::Pubsub, CommandCategory::Fast]);

        // Scripting commands
        map.insert("eval", vec![CommandCategory::Scripting, CommandCategory::Slow]);
        map.insert("evalsha", vec![CommandCategory::Scripting, CommandCategory::Slow]);
        map.insert("script", vec![CommandCategory::Scripting, CommandCategory::Slow]);

        // Keyspace commands
        map.insert("del", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("unlink", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("exists", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("expire", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("expireat", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("expiretime", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("pexpire", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("pexpireat", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("pexpiretime", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("ttl", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("pttl", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("persist", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("type", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("rename", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("renamenx", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("copy", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("dump", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("restore", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("object", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("touch", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("scan", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("keys", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("randomkey", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("sort", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("sort_ro", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Slow]);

        // Connection commands
        map.insert("auth", vec![CommandCategory::Connection, CommandCategory::Fast]);
        map.insert("ping", vec![CommandCategory::Connection, CommandCategory::Fast]);
        map.insert("echo", vec![CommandCategory::Connection, CommandCategory::Fast]);
        map.insert("quit", vec![CommandCategory::Connection, CommandCategory::Fast]);
        map.insert("select", vec![CommandCategory::Connection, CommandCategory::Fast]);
        map.insert("client", vec![CommandCategory::Connection, CommandCategory::Slow]);
        map.insert("reset", vec![CommandCategory::Connection, CommandCategory::Fast]);
        map.insert("hello", vec![CommandCategory::Connection, CommandCategory::Fast]);

        // Transaction commands
        map.insert("multi", vec![CommandCategory::Transaction, CommandCategory::Fast]);
        map.insert("exec", vec![CommandCategory::Transaction, CommandCategory::Slow]);
        map.insert("discard", vec![CommandCategory::Transaction, CommandCategory::Fast]);
        map.insert("watch", vec![CommandCategory::Transaction, CommandCategory::Fast]);
        map.insert("unwatch", vec![CommandCategory::Transaction, CommandCategory::Fast]);

        // Admin commands
        map.insert("acl", vec![CommandCategory::Admin, CommandCategory::Slow]);
        map.insert("bgrewriteaof", vec![CommandCategory::Admin, CommandCategory::Slow]);
        map.insert("bgsave", vec![CommandCategory::Admin, CommandCategory::Slow]);
        map.insert("command", vec![CommandCategory::Connection, CommandCategory::Slow]);
        map.insert("config", vec![CommandCategory::Admin, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("dbsize", vec![CommandCategory::Keyspace, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("debug", vec![CommandCategory::Admin, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("flushall", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("flushdb", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("info", vec![CommandCategory::Admin, CommandCategory::Slow]);
        map.insert("lastsave", vec![CommandCategory::Admin, CommandCategory::Fast]);
        map.insert("memory", vec![CommandCategory::Admin, CommandCategory::Slow]);
        map.insert("module", vec![CommandCategory::Admin, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("replicaof", vec![CommandCategory::Admin, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("slaveof", vec![CommandCategory::Admin, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("role", vec![CommandCategory::Admin, CommandCategory::Fast]);
        map.insert("save", vec![CommandCategory::Admin, CommandCategory::Slow]);
        map.insert("shutdown", vec![CommandCategory::Admin, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("slowlog", vec![CommandCategory::Admin, CommandCategory::Slow]);
        map.insert("swapdb", vec![CommandCategory::Keyspace, CommandCategory::Write, CommandCategory::Slow, CommandCategory::Dangerous]);
        map.insert("time", vec![CommandCategory::Admin, CommandCategory::Fast]);

        // Bitmap commands
        map.insert("setbit", vec![CommandCategory::Bitmap, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("getbit", vec![CommandCategory::Bitmap, CommandCategory::Read, CommandCategory::Fast]);
        map.insert("bitcount", vec![CommandCategory::Bitmap, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("bitop", vec![CommandCategory::Bitmap, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("bitpos", vec![CommandCategory::Bitmap, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("bitfield", vec![CommandCategory::Bitmap, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("bitfield_ro", vec![CommandCategory::Bitmap, CommandCategory::Read, CommandCategory::Slow]);

        // Geo commands
        map.insert("geoadd", vec![CommandCategory::Geo, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("geodist", vec![CommandCategory::Geo, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("geohash", vec![CommandCategory::Geo, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("geopos", vec![CommandCategory::Geo, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("georadius", vec![CommandCategory::Geo, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("georadiusbymember", vec![CommandCategory::Geo, CommandCategory::Write, CommandCategory::Slow]);
        map.insert("geosearch", vec![CommandCategory::Geo, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("geosearchstore", vec![CommandCategory::Geo, CommandCategory::Write, CommandCategory::Slow]);

        // HyperLogLog commands
        map.insert("pfadd", vec![CommandCategory::Hyperloglog, CommandCategory::Write, CommandCategory::Fast]);
        map.insert("pfcount", vec![CommandCategory::Hyperloglog, CommandCategory::Read, CommandCategory::Slow]);
        map.insert("pfmerge", vec![CommandCategory::Hyperloglog, CommandCategory::Write, CommandCategory::Slow]);

        map
    });

/// Commands in each category.
static CATEGORY_COMMANDS: LazyLock<HashMap<CommandCategory, Vec<&'static str>>> = LazyLock::new(|| {
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

    #[test]
    fn test_category_names() {
        assert_eq!(CommandCategory::Admin.name(), "admin");
        assert_eq!(CommandCategory::String.name(), "string");
        assert_eq!(CommandCategory::Write.name(), "write");
    }

    #[test]
    fn test_category_from_str() {
        assert_eq!(CommandCategory::parse("admin"), Some(CommandCategory::Admin));
        assert_eq!(CommandCategory::parse("WRITE"), Some(CommandCategory::Write));
        assert_eq!(CommandCategory::parse("invalid"), None);
        // Test FromStr trait
        assert_eq!("admin".parse::<CommandCategory>(), Ok(CommandCategory::Admin));
        assert!("invalid".parse::<CommandCategory>().is_err());
    }

    #[test]
    fn test_command_category() {
        assert_eq!(CommandCategory::for_command("GET"), Some(CommandCategory::String));
        assert_eq!(CommandCategory::for_command("lpush"), Some(CommandCategory::List));
        assert_eq!(CommandCategory::for_command("ZADD"), Some(CommandCategory::Sortedset));
        assert_eq!(CommandCategory::for_command("AUTH"), Some(CommandCategory::Connection));
    }

    #[test]
    fn test_all_categories_for_command() {
        let cats = CommandCategory::all_for_command("GET");
        assert!(cats.contains(&CommandCategory::String));
        assert!(cats.contains(&CommandCategory::Read));
        assert!(cats.contains(&CommandCategory::Fast));

        let cats = CommandCategory::all_for_command("SET");
        assert!(cats.contains(&CommandCategory::String));
        assert!(cats.contains(&CommandCategory::Write));
    }

    #[test]
    fn test_category_commands() {
        let cmds = CommandCategory::String.commands();
        assert!(cmds.contains(&"get"));
        assert!(cmds.contains(&"set"));

        let cmds = CommandCategory::List.commands();
        assert!(cmds.contains(&"lpush"));
        assert!(cmds.contains(&"rpop"));
    }

    #[test]
    fn test_all_categories() {
        let all = CommandCategory::all();
        assert!(all.contains(&CommandCategory::Admin));
        assert!(all.contains(&CommandCategory::String));
        assert!(all.contains(&CommandCategory::Write));
        assert!(all.contains(&CommandCategory::Read));
        assert_eq!(all.len(), 21);
    }
}
