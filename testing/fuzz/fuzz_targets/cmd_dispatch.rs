#![no_main]
use bytes::Bytes;
use frogdb_commands::register_all;
use frogdb_core::{CommandContext, CommandRegistry, HashMapStore, ShardMessage};
use frogdb_protocol::ProtocolVersion;
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Shard-local, synchronous commands suitable for in-process fuzzing.
const COMMANDS: &[&str] = &[
    "SET", "GET", "DEL", "APPEND", "INCR", "DECR", "STRLEN", "SETNX", "GETSET", "MSET", "MGET",
    "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE", "LINDEX",
    "SADD", "SREM", "SMEMBERS", "SCARD",
    "ZADD", "ZREM", "ZRANGE", "ZCARD",
    "HSET", "HGET", "HDEL",
    "EXPIRE", "TTL", "TYPE", "EXISTS", "RENAME", "PERSIST",
    "SETBIT", "GETBIT", "BITCOUNT",
];

#[derive(Arbitrary, Debug)]
struct CmdInput {
    ops: Vec<CmdOp>,
}

#[derive(Arbitrary, Debug)]
struct CmdOp {
    cmd_index: u8,
    args: Vec<Vec<u8>>,
}

fuzz_target!(|input: CmdInput| {
    // Cap operation count to keep execution time bounded.
    if input.ops.len() > 64 {
        return;
    }

    let mut store = HashMapStore::new();
    let mut registry = CommandRegistry::new();
    register_all(&mut registry);

    let shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>> = Arc::new(vec![]);

    for op in &input.ops {
        // Cap per-operation arg count and size.
        if op.args.len() > 8 {
            continue;
        }
        if op.args.iter().any(|a| a.len() > 256) {
            continue;
        }

        let cmd_name = COMMANDS[op.cmd_index as usize % COMMANDS.len()];

        let handler = match registry.get(cmd_name) {
            Some(h) => h,
            None => continue,
        };

        let args: Vec<Bytes> = op.args.iter().map(|a| Bytes::from(a.clone())).collect();

        // Arity check mirrors server behavior — skip invalid arg counts.
        if !handler.arity().check(args.len()) {
            continue;
        }

        let mut ctx = CommandContext::new(
            &mut store,
            &shard_senders,
            0, // shard_id
            1, // num_shards
            0, // conn_id
            ProtocolVersion::Resp2,
        );

        // We only care about panics — ignore returned Result.
        let _ = handler.execute(&mut ctx, &args);
    }
});
