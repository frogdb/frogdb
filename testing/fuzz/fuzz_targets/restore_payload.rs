#![no_main]
//! Fuzz the RESTORE command *path*, not just the low-level `deserialize` function.
//!
//! The existing `deserialize` target exercises memory-safety of the parser in
//! isolation. RESTORE does more: it deserializes a DUMP frame and, on success,
//! *materializes* the value into the keyspace (`store.set`) where subsequent
//! commands read it. The interesting residue (issue 25) is a corrupted payload that
//! still *parses* but yields a wrong-type / semantically-garbage value — that value
//! only causes trouble once it is stored and used.
//!
//! RESTORE's handler lives in the top-level server crate (not `frogdb-commands`), so
//! it cannot be dispatched through `register_all` here. This target instead mirrors
//! the handler's exact body — `deserialize(payload)` then, on `Ok`, `store.set` — and
//! then drives real read commands (via the command registry) against the freshly
//! materialized key, plus a re-`serialize` (the DUMP side). Any panic, in the parser
//! or in a command reading a parsed-but-corrupt value, is a bug.
//!
//! Two frame sources are fuzzed:
//!   1. a *seeded* frame: build a genuine value with a small write script, DUMP it via
//!      `serialize`, then bit-mutate the bytes — maximizing "valid-header, corrupt
//!      body that still parses" coverage;
//!   2. a *constructed* frame: a valid 24-byte header wrapped around arbitrary body
//!      bytes (as the `deserialize` target does) for broad header/marker coverage.

use bytes::Bytes;
use frogdb_commands::register_all;
use frogdb_core::{
    CommandContext, CommandRegistry, HashMapStore, KeyMetadata, ShardSender, Store, deserialize,
    serialize,
};
use frogdb_protocol::ProtocolVersion;
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

/// 24-byte serialization header size (`frogdb_persistence::HEADER_SIZE`).
const HEADER_SIZE: usize = 24;
/// Known type markers occupy bytes 0..=16.
const NUM_TYPES: u8 = 17;
/// Offset of the little-endian u64 payload-length field in the header.
const LEN_FIELD_OFFSET: usize = 16;

/// A write command used to seed a genuine value that we then DUMP and corrupt.
#[derive(Arbitrary, Debug)]
struct SeedOp {
    cmd_index: u8,
    args: Vec<Vec<u8>>,
}

/// Write commands that build a value under the seed key. Kept to shard-local,
/// synchronous commands present in `register_all`.
const SEED_CMDS: &[&str] = &[
    "SET", "APPEND", "RPUSH", "LPUSH", "SADD", "ZADD", "HSET", "SETBIT", "INCR",
];

/// Read commands exercised against the materialized (possibly corrupt) key.
const READ_CMDS: &[&str] = &[
    "TYPE", "GET", "STRLEN", "LLEN", "LRANGE", "LINDEX", "SCARD", "SMEMBERS", "ZCARD", "ZRANGE",
    "HLEN", "HGETALL", "EXISTS", "TTL",
];

#[derive(Arbitrary, Debug)]
struct RestoreInput {
    /// Ops used to seed a real value that is then DUMPed and corrupted.
    seed_ops: Vec<SeedOp>,
    /// Byte mutations applied to the frame: (offset, xor-mask) pairs.
    mutations: Vec<(u16, u8)>,
    /// If seeding produced no value, fall back to a constructed frame from these bytes.
    fallback_type: u8,
    fallback_body: Vec<u8>,
}

/// The seed key and the RESTORE target key.
const SEED_KEY: &[u8] = b"seed";
const RESTORE_KEY: &[u8] = b"k";

/// Build a `CommandContext` over `store` for shard-local execution.
fn ctx<'a>(
    store: &'a mut HashMapStore,
    shard_senders: &'a Arc<Vec<ShardSender>>,
) -> CommandContext<'a> {
    CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
}

/// Run one registry command by name, ignoring the result (panics are what matter).
fn run_cmd(
    registry: &CommandRegistry,
    store: &mut HashMapStore,
    shard_senders: &Arc<Vec<ShardSender>>,
    name: &str,
    args: &[Bytes],
) {
    let handler = match registry.get(name) {
        Some(h) => h,
        None => return,
    };
    if !handler.arity().check(args.len()) {
        return;
    }
    let mut c = ctx(store, shard_senders);
    let _ = handler.execute(&mut c, args);
}

/// Apply the (offset, xor) mutations to `frame` in place.
fn corrupt(frame: &mut [u8], mutations: &[(u16, u8)]) {
    let len = frame.len();
    if len == 0 {
        return;
    }
    for &(off, mask) in mutations {
        let idx = (off as usize) % len;
        frame[idx] ^= mask;
    }
}

/// Construct a frame with a valid header around arbitrary body bytes (broad-coverage
/// fallback identical in spirit to the `deserialize` target's construction).
fn constructed_frame(type_byte: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = vec![0u8; HEADER_SIZE + body.len()];
    frame[0] = type_byte % NUM_TYPES;
    frame[LEN_FIELD_OFFSET..LEN_FIELD_OFFSET + 8]
        .copy_from_slice(&(body.len() as u64).to_le_bytes());
    frame[HEADER_SIZE..].copy_from_slice(body);
    frame
}

fuzz_target!(|input: RestoreInput| {
    // Bound work to keep executions fast.
    if input.seed_ops.len() > 16 || input.mutations.len() > 64 {
        return;
    }
    if input.fallback_body.len() > 16 * 1024 {
        return;
    }

    let mut store = HashMapStore::new();
    let mut registry = CommandRegistry::new();
    register_all(&mut registry);
    let shard_senders: Arc<Vec<ShardSender>> = Arc::new(vec![]);

    // --- 1. Seed a genuine value under SEED_KEY. ---
    for op in &input.seed_ops {
        if op.args.len() > 6 || op.args.iter().any(|a| a.len() > 256) {
            continue;
        }
        let name = SEED_CMDS[op.cmd_index as usize % SEED_CMDS.len()];
        // Force the seed key as the first argument so a value lands under SEED_KEY.
        let mut args: Vec<Bytes> = Vec::with_capacity(op.args.len() + 1);
        args.push(Bytes::from_static(SEED_KEY));
        args.extend(op.args.iter().map(|a| Bytes::from(a.clone())));
        run_cmd(&registry, &mut store, &shard_senders, name, &args);
    }

    // --- 2. Produce a DUMP frame: real value if seeded, else a constructed frame. ---
    let mut frame = match store.get(SEED_KEY) {
        Some(value) => {
            let metadata = KeyMetadata::new(value.memory_size());
            serialize(&value, &metadata)
        }
        None => constructed_frame(input.fallback_type, &input.fallback_body),
    };

    // --- 3. Corrupt the frame. ---
    corrupt(&mut frame, &input.mutations);

    // --- 4. Mirror RESTORE's handler body: deserialize, and on success store.set. ---
    //     This is the exact contract the server-crate RestoreCommand implements.
    if let Ok((value, _metadata)) = deserialize(&frame) {
        // Re-serialize (the DUMP side) — round-trip must not panic on a parsed value.
        let meta = KeyMetadata::new(value.memory_size());
        let _ = serialize(&value, &meta);

        // Materialize into the keyspace exactly as RESTORE does.
        store.set(Bytes::from_static(RESTORE_KEY), value);

        // --- 5. Exercise the materialized (possibly wrong-type/garbage) value through
        //         the real command path. A handler must never panic on it. ---
        for &name in READ_CMDS {
            // Minimal, arity-valid argument shapes per command.
            let args: Vec<Bytes> = match name {
                "LRANGE" | "ZRANGE" => vec![
                    Bytes::from_static(RESTORE_KEY),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"-1"),
                ],
                "LINDEX" => vec![Bytes::from_static(RESTORE_KEY), Bytes::from_static(b"0")],
                _ => vec![Bytes::from_static(RESTORE_KEY)],
            };
            run_cmd(&registry, &mut store, &shard_senders, name, &args);
        }
    }
});
