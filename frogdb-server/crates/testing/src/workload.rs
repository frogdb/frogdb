//! Seeded, pure, transport-agnostic RESP-level workload generator.
//!
//! A [`Workload`] is a set of per-client [`ClientScript`]s, each a sequence of
//! [`ScriptedOp`]s with sim-time think hints. The op vocabulary is **exactly**
//! the Phase-1 model vocabulary (KV/tx, lists, hashes, zsets, streams), and
//! every encoding constraint the Phase-1 reviews established is enforced here
//! (no `|` in keys/values, only `*`/full `ms-seq` stream ids, blocking ops
//! staggered per key, single-type keys so a per-key sub-history has one model).
//!
//! Determinism: `generate(seed, profile, num_clients, ops_per_client)` is a
//! pure function of its inputs — same inputs ⇒ byte-identical [`Workload`].

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Workload shape — controls the op-mix weighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Profile {
    /// Weighted toward WATCH/EXEC transactions plus plain KV.
    TxHeavy,
    /// Weighted toward blocking pops paired with producers on the same keys.
    BlockingHeavy,
    /// Even spread across all five type families.
    Mixed,
}

/// One RESP command with sim-time think hints. `command`/`args` are exactly
/// what goes on the wire; the recorder logs the same into `History`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScriptedOp {
    /// Lowercase command name, from the Phase-1 vocabulary.
    pub command: String,
    /// Command arguments (never contain the reserved `|` delimiter).
    #[serde(with = "crate::history::bytes_vec_serde_pub")]
    pub args: Vec<Bytes>,
    /// Sim-time to sleep BEFORE issuing this op (ms). Blocking ops on the same
    /// key get distinct offsets so registrations never overlap (FIFO guard).
    pub think_ms: u64,
}

/// One client's ordered op sequence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClientScript {
    pub client_id: u64,
    pub ops: Vec<ScriptedOp>,
}

/// A complete seeded workload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Workload {
    pub seed: u64,
    pub profile: Profile,
    pub clients: Vec<ClientScript>,
}

/// Internal type family. Each key belongs to exactly one family so a per-key
/// sub-history is single-model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    Kv,
    List,
    Hash,
    ZSet,
    Stream,
}

const FAMILIES: [Family; 5] = [
    Family::Kv,
    Family::List,
    Family::Hash,
    Family::ZSet,
    Family::Stream,
];

/// Per-key blocking-stagger step (ms). Distinct offsets keep concurrent
/// blocking registrations on one key from landing in the same tick window.
const STAGGER_STEP: u64 = 7;

impl Workload {
    /// Deterministic: same `(seed, profile, num_clients, ops_per_client)` ⇒
    /// byte-identical `Workload`.
    pub fn generate(
        seed: u64,
        profile: Profile,
        num_clients: usize,
        ops_per_client: usize,
    ) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let families = build_key_families(seed);

        let mut clients = Vec::with_capacity(num_clients);
        for client_id in 0..num_clients as u64 {
            // Per-client, per-key blocking think offset (strictly increasing so
            // two blocking ops on the same key never share a think_ms).
            let mut block_offset: std::collections::HashMap<Vec<u8>, u64> =
                std::collections::HashMap::new();
            let mut ops = Vec::with_capacity(ops_per_client);
            for _ in 0..ops_per_client {
                let fam = pick_family(profile, &mut rng);
                gen_op(
                    fam,
                    profile,
                    &families,
                    &mut rng,
                    &mut block_offset,
                    &mut ops,
                );
            }
            clients.push(ClientScript { client_id, ops });
        }

        Workload {
            seed,
            profile,
            clients,
        }
    }

    /// The 8–16 hash-tagged keys this workload draws from (pins shard
    /// placement). Order is deterministic in `seed`.
    pub fn key_space(seed: u64) -> Vec<Bytes> {
        let families = build_key_families(seed);
        let mut out = Vec::new();
        for fam in FAMILIES {
            out.extend(families.get(fam).iter().cloned());
        }
        out
    }
}

/// Deterministic per-family key buckets. 8–16 keys total, each hash-tagged
/// `{tagN}kind` so `hash_slot` pins them; never contains `|`.
fn build_key_families(seed: u64) -> KeyFamilies {
    // 2 or 3 keys per family → 10–15 total; deterministic in `seed`.
    let per = 2 + (seed % 2) as usize; // 2 or 3
    let mut kv = Vec::new();
    let mut list = Vec::new();
    let mut hash = Vec::new();
    let mut zset = Vec::new();
    let mut stream = Vec::new();
    for (fam_idx, (bucket, kind)) in [
        (&mut kv, "kv"),
        (&mut list, "ls"),
        (&mut hash, "hs"),
        (&mut zset, "zs"),
        (&mut stream, "st"),
    ]
    .into_iter()
    .enumerate()
    {
        for i in 0..per {
            let tag = fam_idx * 4 + i;
            bucket.push(Bytes::from(format!("{{t{tag}}}{kind}{i}")));
        }
    }
    KeyFamilies {
        kv,
        list,
        hash,
        zset,
        stream,
    }
}

struct KeyFamilies {
    kv: Vec<Bytes>,
    list: Vec<Bytes>,
    hash: Vec<Bytes>,
    zset: Vec<Bytes>,
    stream: Vec<Bytes>,
}

impl KeyFamilies {
    fn get(&self, fam: Family) -> &[Bytes] {
        match fam {
            Family::Kv => &self.kv,
            Family::List => &self.list,
            Family::Hash => &self.hash,
            Family::ZSet => &self.zset,
            Family::Stream => &self.stream,
        }
    }
}

/// Pick a family according to the profile weighting.
fn pick_family(profile: Profile, rng: &mut StdRng) -> Family {
    match profile {
        // KV-heavy (transactions live in the Kv family).
        Profile::TxHeavy => {
            let r = rng.random_range(0..100);
            if r < 70 {
                Family::Kv
            } else if r < 80 {
                Family::List
            } else if r < 88 {
                Family::Hash
            } else if r < 96 {
                Family::ZSet
            } else {
                Family::Stream
            }
        }
        // List + ZSet heavy (that is where blocking ops live).
        Profile::BlockingHeavy => {
            let r = rng.random_range(0..100);
            if r < 50 {
                Family::List
            } else if r < 85 {
                Family::ZSet
            } else if r < 92 {
                Family::Kv
            } else if r < 96 {
                Family::Hash
            } else {
                Family::Stream
            }
        }
        // Even spread.
        Profile::Mixed => FAMILIES[rng.random_range(0..FAMILIES.len())],
    }
}

/// A short numeric decimal value, so KV/hash `incr`-family ops interoperate
/// with `set`/`hset` without WRONGTYPE/parse errors.
fn num_value(rng: &mut StdRng) -> Bytes {
    Bytes::from(rng.random_range(0..1000u32).to_string())
}

/// A short `[a-z0-9]` element value; never contains `|`.
fn alnum_value(rng: &mut StdRng) -> Bytes {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let len = rng.random_range(1..5);
    let mut v = Vec::with_capacity(len);
    for _ in 0..len {
        v.push(ALPHABET[rng.random_range(0..ALPHABET.len())]);
    }
    Bytes::from(v)
}

fn pick<'a>(items: &'a [Bytes], rng: &mut StdRng) -> &'a Bytes {
    &items[rng.random_range(0..items.len())]
}

/// A small finite blocking timeout (fractional seconds, as the wire expects).
/// Finite AND short so a client whose blocking ops all time out still finishes
/// its script well within the bounded turmoil sim window; the FIFO/exactly-once
/// checkers cover served vs timed out. Values: "0.1" or "0.2".
fn block_timeout(rng: &mut StdRng) -> Bytes {
    Bytes::from(if rng.random_range(0..2) == 0 {
        "0.1"
    } else {
        "0.2"
    })
}

/// Next distinct per-key blocking think offset for this client.
fn next_block_offset(
    block_offset: &mut std::collections::HashMap<Vec<u8>, u64>,
    key: &Bytes,
) -> u64 {
    let slot = block_offset.entry(key.to_vec()).or_insert(3);
    let v = *slot;
    *slot += STAGGER_STEP;
    v
}

/// Small non-blocking think delay (ms).
fn think(rng: &mut StdRng) -> u64 {
    rng.random_range(0..5)
}

/// Emit one (or, for blocking, a producer + a consumer) op into `ops`.
fn gen_op(
    fam: Family,
    profile: Profile,
    families: &KeyFamilies,
    rng: &mut StdRng,
    block_offset: &mut std::collections::HashMap<Vec<u8>, u64>,
    ops: &mut Vec<ScriptedOp>,
) {
    match fam {
        Family::Kv => gen_kv(profile, families, rng, ops),
        Family::List => gen_list(profile, families, rng, block_offset, ops),
        Family::Hash => gen_hash(families, rng, ops),
        Family::ZSet => gen_zset(profile, families, rng, block_offset, ops),
        Family::Stream => gen_stream(families, rng, ops),
    }
}

fn push_op(ops: &mut Vec<ScriptedOp>, command: &str, args: Vec<Bytes>, think_ms: u64) {
    ops.push(ScriptedOp {
        command: command.to_string(),
        args,
        think_ms,
    });
}

fn gen_kv(profile: Profile, families: &KeyFamilies, rng: &mut StdRng, ops: &mut Vec<ScriptedOp>) {
    let keys = &families.kv;
    // In TxHeavy, bias toward transactions.
    let tx_bias = matches!(profile, Profile::TxHeavy);
    let r = rng.random_range(0..100);
    let t = think(rng);
    if tx_bias && r < 35 {
        gen_exec(keys, rng, ops, t);
        return;
    }
    if tx_bias && r < 45 {
        // WATCH one or two keys (all args are keys per default_keys_of).
        let k = pick(keys, rng).clone();
        push_op(ops, "watch", vec![k], t);
        return;
    }
    if tx_bias && r < 50 {
        push_op(ops, "discard", vec![], t);
        return;
    }
    let k = pick(keys, rng).clone();
    match rng.random_range(0..4) {
        0 => push_op(ops, "set", vec![k, num_value(rng)], t),
        1 => push_op(ops, "get", vec![k], t),
        2 => push_op(ops, "incr", vec![k], t),
        _ => push_op(ops, "del", vec![k], t),
    }
}

/// Emit an EXEC wrapping 1–3 single-key `set/get/incr/del` sub-commands, in the
/// `[num_cmds, name, num_args, args...]` encoding `parse_exec_commands` expects.
fn gen_exec(keys: &[Bytes], rng: &mut StdRng, ops: &mut Vec<ScriptedOp>, think_ms: u64) {
    let n = rng.random_range(1..4);
    let mut args: Vec<Bytes> = vec![Bytes::from(n.to_string())];
    for _ in 0..n {
        let k = pick(keys, rng).clone();
        match rng.random_range(0..4) {
            0 => {
                args.push(Bytes::from("set"));
                args.push(Bytes::from("2"));
                args.push(k);
                args.push(num_value(rng));
            }
            1 => {
                args.push(Bytes::from("get"));
                args.push(Bytes::from("1"));
                args.push(k);
            }
            2 => {
                args.push(Bytes::from("incr"));
                args.push(Bytes::from("1"));
                args.push(k);
            }
            _ => {
                args.push(Bytes::from("del"));
                args.push(Bytes::from("1"));
                args.push(k);
            }
        }
    }
    push_op(ops, "exec", args, think_ms);
}

fn gen_list(
    profile: Profile,
    families: &KeyFamilies,
    rng: &mut StdRng,
    block_offset: &mut std::collections::HashMap<Vec<u8>, u64>,
    ops: &mut Vec<ScriptedOp>,
) {
    let keys = &families.list;
    let blocking_bias = matches!(profile, Profile::BlockingHeavy);
    let r = rng.random_range(0..100);
    let t = think(rng);
    if blocking_bias && r < 40 {
        // Producer + blocking consumer on the same key.
        let k = pick(keys, rng).clone();
        push_op(ops, "rpush", vec![k.clone(), alnum_value(rng)], t);
        let off = next_block_offset(block_offset, &k);
        let cmd = if rng.random_range(0..2) == 0 {
            "blpop"
        } else {
            "brpop"
        };
        push_op(ops, cmd, vec![k, block_timeout(rng)], off);
        return;
    }
    if blocking_bias && r < 55 && keys.len() >= 2 {
        // Cross-key BLMOVE (dropped from per-key linearizability; conservation
        // covers it): src dst LEFT RIGHT timeout.
        let src = pick(keys, rng).clone();
        let dst = pick(keys, rng).clone();
        push_op(ops, "lpush", vec![src.clone(), alnum_value(rng)], t);
        let off = next_block_offset(block_offset, &src);
        push_op(
            ops,
            "blmove",
            vec![
                src,
                dst,
                Bytes::from("LEFT"),
                Bytes::from("RIGHT"),
                block_timeout(rng),
            ],
            off,
        );
        return;
    }
    let k = pick(keys, rng).clone();
    match rng.random_range(0..7) {
        0 => push_op(ops, "lpush", vec![k, alnum_value(rng)], t),
        1 => push_op(ops, "rpush", vec![k, alnum_value(rng)], t),
        2 => push_op(ops, "lpop", vec![k], t),
        3 => push_op(ops, "rpop", vec![k], t),
        4 => push_op(ops, "llen", vec![k], t),
        5 => push_op(
            ops,
            "lrange",
            vec![k, Bytes::from("0"), Bytes::from("-1")],
            t,
        ),
        _ => {
            // Same-key LMOVE (kept by the partitioner): src == dst.
            push_op(
                ops,
                "lmove",
                vec![k.clone(), k, Bytes::from("LEFT"), Bytes::from("RIGHT")],
                t,
            );
        }
    }
}

fn gen_hash(families: &KeyFamilies, rng: &mut StdRng, ops: &mut Vec<ScriptedOp>) {
    let keys = &families.hash;
    let k = pick(keys, rng).clone();
    let t = think(rng);
    // Small field space keeps HGETALL replies (and their canonicalization)
    // small; numeric values so HINCRBY interoperates with HSET.
    let field = Bytes::from(format!("f{}", rng.random_range(0..4)));
    match rng.random_range(0..6) {
        0 => push_op(ops, "hset", vec![k, field, num_value(rng)], t),
        1 => push_op(ops, "hdel", vec![k, field], t),
        2 => push_op(ops, "hget", vec![k, field], t),
        3 => push_op(
            ops,
            "hincrby",
            vec![k, field, Bytes::from(rng.random_range(1..5).to_string())],
            t,
        ),
        4 => push_op(ops, "hgetall", vec![k], t),
        _ => push_op(ops, "hlen", vec![k], t),
    }
}

fn gen_zset(
    profile: Profile,
    families: &KeyFamilies,
    rng: &mut StdRng,
    block_offset: &mut std::collections::HashMap<Vec<u8>, u64>,
    ops: &mut Vec<ScriptedOp>,
) {
    let keys = &families.zset;
    let blocking_bias = matches!(profile, Profile::BlockingHeavy);
    let r = rng.random_range(0..100);
    let t = think(rng);
    if blocking_bias && r < 45 {
        // Producer + blocking consumer on the same key.
        let k = pick(keys, rng).clone();
        let score = Bytes::from(rng.random_range(0..100).to_string());
        let m = Bytes::from(format!("m{}", rng.random_range(0..5)));
        push_op(ops, "zadd", vec![k.clone(), score, m], t);
        let off = next_block_offset(block_offset, &k);
        let cmd = if rng.random_range(0..2) == 0 {
            "bzpopmin"
        } else {
            "bzpopmax"
        };
        push_op(ops, cmd, vec![k, block_timeout(rng)], off);
        return;
    }
    let k = pick(keys, rng).clone();
    let m = Bytes::from(format!("m{}", rng.random_range(0..5)));
    match rng.random_range(0..4) {
        0 => {
            let score = Bytes::from(rng.random_range(0..100).to_string());
            push_op(ops, "zadd", vec![k, score, m], t)
        }
        1 => push_op(ops, "zrem", vec![k, m], t),
        2 => push_op(ops, "zscore", vec![k, m], t),
        _ => push_op(ops, "zcard", vec![k], t),
    }
}

fn gen_stream(families: &KeyFamilies, rng: &mut StdRng, ops: &mut Vec<ScriptedOp>) {
    let keys = &families.stream;
    let k = pick(keys, rng).clone();
    let t = think(rng);
    match rng.random_range(0..3) {
        // XADD key * field value — auto id avoids cross-client non-monotonic
        // rejections; the model accepts `*`.
        0 => push_op(
            ops,
            "xadd",
            vec![
                k,
                Bytes::from("*"),
                Bytes::from(format!("f{}", rng.random_range(0..3))),
                alnum_value(rng),
            ],
            t,
        ),
        1 => push_op(ops, "xlen", vec![k], t),
        // XREAD key 0 — read from the start (after id "0").
        _ => push_op(ops, "xread", vec![k, Bytes::from("0")], t),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_ops(w: &Workload) -> impl Iterator<Item = &ScriptedOp> {
        w.clients.iter().flat_map(|c| c.ops.iter())
    }

    #[test]
    fn generate_is_deterministic() {
        let a = Workload::generate(7, Profile::Mixed, 4, 20);
        let b = Workload::generate(7, Profile::Mixed, 4, 20);
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
        let c = Workload::generate(8, Profile::Mixed, 4, 20);
        assert_ne!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&c).unwrap()
        );
    }

    #[test]
    fn no_pipe_in_keys_or_values() {
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::Mixed, 4, 30);
            for op in all_ops(&w) {
                for a in &op.args {
                    assert!(!a.contains(&b'|'), "arg {:?} contains reserved '|'", a);
                }
            }
        }
    }

    #[test]
    fn only_phase1_vocabulary_emitted() {
        const ALLOWED: &[&str] = &[
            "set", "get", "del", "incr", "mset", "mget", "watch", "exec", "discard", "lpush",
            "rpush", "lpop", "rpop", "lmove", "llen", "lrange", "blpop", "brpop", "blmove", "hset",
            "hdel", "hget", "hincrby", "hgetall", "hlen", "zadd", "zrem", "zscore", "zcard",
            "bzpopmin", "bzpopmax", "xadd", "xlen", "xread",
        ];
        for seed in 0..40 {
            for profile in [Profile::TxHeavy, Profile::BlockingHeavy, Profile::Mixed] {
                let w = Workload::generate(seed, profile, 4, 30);
                for op in all_ops(&w) {
                    assert!(
                        ALLOWED.contains(&op.command.as_str()),
                        "forbidden op {}",
                        op.command
                    );
                }
            }
        }
    }

    #[test]
    fn no_forbidden_stream_ids() {
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::Mixed, 4, 40);
            for op in all_ops(&w).filter(|o| o.command == "xadd") {
                let id = String::from_utf8_lossy(&op.args[1]);
                assert!(
                    id == "*"
                        || id.split_once('-').is_some_and(
                            |(m, s)| m.parse::<u64>().is_ok() && s.parse::<u64>().is_ok()
                        ),
                    "xadd id {id} must be '*' or full ms-seq (never ms-* or $)"
                );
                assert_ne!(id, "$");
            }
        }
    }

    #[test]
    fn blocking_ops_staggered_per_key() {
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::BlockingHeavy, 4, 40);
            for c in &w.clients {
                let mut seen: std::collections::HashMap<&[u8], u64> = Default::default();
                for op in c.ops.iter().filter(|o| {
                    matches!(
                        o.command.as_str(),
                        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" | "blmove"
                    )
                }) {
                    let key = op.args[0].as_ref();
                    if let Some(prev) = seen.insert(key, op.think_ms) {
                        assert_ne!(prev, op.think_ms, "overlapping blocking regs on same key");
                    }
                }
            }
        }
    }

    #[test]
    fn generated_vocabulary_is_model_routable() {
        for seed in 0..200u64 {
            let w = Workload::generate(seed, Profile::Mixed, 3, 25);
            for op in w.clients.iter().flat_map(|c| &c.ops) {
                let keys = crate::partition::default_keys_of(&op.command, &op.args);
                assert!(
                    !keys.is_empty() || op.command == "discard",
                    "op {} not routable by default_keys_of",
                    op.command
                );
            }
        }
    }
}
