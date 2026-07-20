//! Seeded, pure, transport-agnostic RESP-level workload generator.
//!
//! A [`Workload`] is a set of per-client [`ClientScript`]s, each a sequence of
//! [`ScriptedOp`]s with sim-time think hints. The op vocabulary is **exactly**
//! the Phase-1/4a model vocabulary (KV/tx, lists, hashes, zsets, streams,
//! stream consumer groups), and every encoding constraint the Phase-1 reviews
//! established is enforced here
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
    /// Each key's *blocking* consumer is pinned to one owner client, so per-key
    /// registrations are unambiguously ordered for the invoke-time FIFO proxy.
    BlockingHeavy,
    /// Concurrent multi-waiter blocking races: **every** client may block on a
    /// shared key (the [`blocking_owner`] single-consumer restriction is
    /// dropped) with a *long* timeout, so multiple waiters park at once and a
    /// delayed producer serves them in registration order. Sound only because
    /// the exact FIFO checker consumes the mid-run `DEBUG WAITQUEUE`
    /// registration ordinals (not the invoke-time proxy), and each client
    /// blocks at most once per key across its script (see the per-client
    /// `multi_waited` guard in [`Workload::generate`]).
    MultiWaiter,
    /// Even spread across all six type families.
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
    StreamGroup,
    Script,
}

const FAMILIES: [Family; 7] = [
    Family::Kv,
    Family::List,
    Family::Hash,
    Family::ZSet,
    Family::Stream,
    Family::StreamGroup,
    Family::Script,
];

/// The fixed Lua script pool: (pseudo-op name, single-key Lua source). Shared
/// with the turmoil runner, which SCRIPT LOADs / EVALs / EVALSHAs these.
pub const SCRIPT_POOL: &[(&str, &str)] = &[
    (
        "script_getset",
        "local o=redis.call('GET',KEYS[1]); redis.call('SET',KEYS[1],ARGV[1]); return o",
    ),
    (
        "script_cincr",
        "if redis.call('EXISTS',KEYS[1])==1 then return redis.call('INCR',KEYS[1]) else return -1 end",
    ),
    (
        "script_setnx_get",
        "if redis.call('EXISTS',KEYS[1])==0 then redis.call('SET',KEYS[1],ARGV[1]) end; return redis.call('GET',KEYS[1])",
    ),
    (
        "script_lpush_llen",
        "redis.call('LPUSH',KEYS[1],ARGV[1]); return redis.call('LLEN',KEYS[1])",
    ),
    (
        "script_rpush_llen",
        "redis.call('RPUSH',KEYS[1],ARGV[1]); return redis.call('LLEN',KEYS[1])",
    ),
];

/// Per-key blocking-stagger step (ms). Distinct offsets keep concurrent
/// blocking registrations on one key from landing in the same tick window.
const STAGGER_STEP: u64 = 7;

/// The single client allowed to issue *blocking* pops on `key`.
///
/// Blocking waiters must be staggered per key so the FIFO wake-order checker
/// (which uses invoke order as a proxy for the server's registration order)
/// never sees two waiters whose registration order is ambiguous. Within one
/// client, blocking ops are already staggered by [`next_block_offset`] *and*
/// serialized on the wire (a client sends its next op only after the prior
/// reply), so a single client's blocking pops on a key have an unambiguous
/// registration order. Across clients there is no such guarantee — two
/// clients' blocking pops on the same key can overlap arbitrarily under
/// simulated network latency, landing squarely in the checker's documented
/// false-positive window. Pinning each key's blocking consumer to one
/// deterministic owner removes that ambiguity. Producers (pushes) still come
/// from every client, so push/pop concurrency is unchanged; only *concurrent
/// multi-waiter* races on one key are excluded (their sound verification needs
/// phase-2 `DEBUG WAITQUEUE` registration dumps, not an invoke-time proxy).
fn blocking_owner(key: &[u8], num_clients: usize) -> u64 {
    // FNV-1a over the key bytes; deterministic and stable across runs.
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h % num_clients.max(1) as u64
}

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
            // Per-client set of stream-group keys this client has already
            // issued the group-creating `XGROUP CREATE` for (see gen_stream_group).
            let mut created: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            // Per-client set of keys this client has already issued a
            // multi-waiter blocking pop on. Binding soundness guard: the exact
            // FIFO checker joins on `(key, client_id)` and keeps a client's
            // *first* ordinal per key, so a client that parked twice on one key
            // would have its second wait judged against its first registration —
            // a false FIFO verdict. Cap it at one blocking pop per key per
            // client (see `Profile::MultiWaiter`).
            let mut multi_waited: std::collections::HashSet<Vec<u8>> =
                std::collections::HashSet::new();
            let mut ops = Vec::with_capacity(ops_per_client);
            for _ in 0..ops_per_client {
                let fam = pick_family(profile, &mut rng);
                gen_op(
                    fam,
                    profile,
                    &families,
                    &mut rng,
                    &mut block_offset,
                    &mut created,
                    &mut multi_waited,
                    client_id,
                    num_clients,
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

    /// The 12–18 hash-tagged keys this workload draws from (6 families ×
    /// 2–3 keys each; pins shard placement). Order is deterministic in `seed`.
    pub fn key_space(seed: u64) -> Vec<Bytes> {
        let families = build_key_families(seed);
        let mut out = Vec::new();
        for fam in FAMILIES {
            out.extend(families.get(fam).iter().cloned());
        }
        out
    }
}

/// Deterministic per-family key buckets. 12–18 keys total, each hash-tagged
/// `{tagN}kind` so `hash_slot` pins them; never contains `|`.
fn build_key_families(seed: u64) -> KeyFamilies {
    // 6 families × (2 or 3 keys each) → 12–18 total; deterministic in `seed`.
    let per = 2 + (seed % 2) as usize; // 2 or 3
    let mut kv = Vec::new();
    let mut list = Vec::new();
    let mut hash = Vec::new();
    let mut zset = Vec::new();
    let mut stream = Vec::new();
    let mut stream_group = Vec::new();
    for (fam_idx, (bucket, kind)) in [
        (&mut kv, "kv"),
        (&mut list, "ls"),
        (&mut hash, "hs"),
        (&mut zset, "zs"),
        (&mut stream, "st"),
        (&mut stream_group, "sg"),
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
        stream_group,
    }
}

struct KeyFamilies {
    kv: Vec<Bytes>,
    list: Vec<Bytes>,
    hash: Vec<Bytes>,
    zset: Vec<Bytes>,
    stream: Vec<Bytes>,
    stream_group: Vec<Bytes>,
}

impl KeyFamilies {
    fn get(&self, fam: Family) -> &[Bytes] {
        match fam {
            Family::Kv => &self.kv,
            Family::List => &self.list,
            Family::Hash => &self.hash,
            Family::ZSet => &self.zset,
            Family::Stream => &self.stream,
            Family::StreamGroup => &self.stream_group,
            // Script pseudo-ops reuse the KV and List key buckets (per
            // `gen_script`); they own no dedicated key space, so `key_space`
            // contributes nothing extra for this family.
            Family::Script => &[],
        }
    }
}

/// Pick a family according to the profile weighting.
fn pick_family(profile: Profile, rng: &mut StdRng) -> Family {
    match profile {
        // KV-heavy (transactions live in the Kv family).
        Profile::TxHeavy => {
            let r = rng.random_range(0..100);
            if r < 68 {
                Family::Kv
            } else if r < 78 {
                Family::List
            } else if r < 86 {
                Family::Hash
            } else if r < 94 {
                Family::ZSet
            } else if r < 96 {
                Family::Stream
            } else if r < 98 {
                Family::StreamGroup
            } else {
                Family::Script
            }
        }
        // Almost entirely List + ZSet (the two blocking families), so many
        // clients converge on the small shared list/zset key space and park
        // concurrently on the same keys.
        Profile::MultiWaiter => {
            let r = rng.random_range(0..100);
            if r < 55 { Family::List } else { Family::ZSet }
        }
        // List + ZSet heavy (that is where blocking ops live).
        Profile::BlockingHeavy => {
            let r = rng.random_range(0..100);
            if r < 48 {
                Family::List
            } else if r < 82 {
                Family::ZSet
            } else if r < 90 {
                Family::Kv
            } else if r < 94 {
                Family::Hash
            } else if r < 96 {
                Family::Stream
            } else if r < 98 {
                Family::StreamGroup
            } else {
                Family::Script
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

/// A same-slot transaction confines all keys to a single group sharing one
/// hash tag. With the current per-key distinct-tag key space, the guaranteed
/// same-slot group is a single key reused across sub-commands; document that
/// widening the key space to multi-key same-tag groups is a follow-up.
fn same_slot_group<'a>(keys: &'a [Bytes], rng: &mut StdRng) -> &'a Bytes {
    pick(keys, rng)
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

/// Long blocking timeout (seconds) for the multi-waiter path. Chosen so the
/// parked window (until a delayed producer serves the waiter) comfortably
/// exceeds the ~50 sim-ms `DEBUG WAITQUEUE` prober cadence, guaranteeing the
/// prober catches every concurrent waiter's registration ordinal.
const MULTI_WAITER_TIMEOUT: &str = "5";

/// Small early think (ms) before a multi-waiter blocking pop, so waiters
/// register near the start of the sim and overlap in the queue.
fn multi_waiter_think(rng: &mut StdRng) -> u64 {
    rng.random_range(0..25)
}

/// Larger delayed think (ms) before a multi-waiter *producer* push, so the
/// push fires after concurrent waiters have parked (letting the prober observe
/// them first) and then serves them in registration order.
fn producer_think(rng: &mut StdRng) -> u64 {
    rng.random_range(120..400)
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
#[allow(clippy::too_many_arguments)]
fn gen_op(
    fam: Family,
    profile: Profile,
    families: &KeyFamilies,
    rng: &mut StdRng,
    block_offset: &mut std::collections::HashMap<Vec<u8>, u64>,
    created: &mut std::collections::HashSet<Vec<u8>>,
    multi_waited: &mut std::collections::HashSet<Vec<u8>>,
    client_id: u64,
    num_clients: usize,
    ops: &mut Vec<ScriptedOp>,
) {
    match fam {
        Family::Kv => gen_kv(profile, families, rng, ops),
        Family::List => gen_list(
            profile,
            families,
            rng,
            block_offset,
            multi_waited,
            client_id,
            num_clients,
            ops,
        ),
        Family::Hash => gen_hash(families, rng, ops),
        Family::ZSet => gen_zset(
            profile,
            families,
            rng,
            block_offset,
            multi_waited,
            client_id,
            num_clients,
            ops,
        ),
        Family::Stream => gen_stream(families, rng, ops),
        Family::StreamGroup => gen_stream_group(families, rng, created, ops),
        Family::Script => gen_script(families, rng, ops),
    }
}

/// Emit a single script pseudo-op: pick a pool script and a target key from the
/// matching family (`list` for the `push_llen` scripts, `kv` otherwise). The
/// arg shape mirrors what the models expect: `[key]` for `script_cincr` (no
/// ARGV), `[key, value]` for the rest (ARGV[1] = a list element or a numeric).
fn gen_script(families: &KeyFamilies, rng: &mut StdRng, ops: &mut Vec<ScriptedOp>) {
    let (name, _) = SCRIPT_POOL[rng.random_range(0..SCRIPT_POOL.len())];
    let list_effect = name.contains("push_llen");
    let key = if list_effect {
        pick(&families.list, rng).clone()
    } else {
        pick(&families.kv, rng).clone()
    };
    let t = think(rng);
    // KV-effect scripts that write take an ARGV[1]; cincr takes none.
    let args = if name == "script_cincr" {
        vec![key]
    } else {
        vec![
            key,
            if list_effect {
                alnum_value(rng)
            } else {
                num_value(rng)
            },
        ]
    };
    push_op(ops, name, args, t);
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
        // ~85% of transactions are single-slot (commit on the fast path).
        let same_slot = rng.random_range(0..100) < 85;
        gen_exec(keys, rng, ops, t, same_slot);
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
///
/// `same_slot` confines every sub-command key to one reused group key (so the
/// EXEC hashes to a single slot and commits on the fast path); otherwise each
/// sub-command's key is drawn independently, which — given the per-key
/// distinct-tag key space — usually spans slots.
fn gen_exec(
    keys: &[Bytes],
    rng: &mut StdRng,
    ops: &mut Vec<ScriptedOp>,
    think_ms: u64,
    same_slot: bool,
) {
    let n = rng.random_range(1..4);
    let group = same_slot_group(keys, rng).clone();
    let mut args: Vec<Bytes> = vec![Bytes::from(n.to_string())];
    for _ in 0..n {
        let k = if same_slot {
            group.clone()
        } else {
            pick(keys, rng).clone()
        };
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

#[allow(clippy::too_many_arguments)]
fn gen_list(
    profile: Profile,
    families: &KeyFamilies,
    rng: &mut StdRng,
    block_offset: &mut std::collections::HashMap<Vec<u8>, u64>,
    multi_waited: &mut std::collections::HashSet<Vec<u8>>,
    client_id: u64,
    num_clients: usize,
    ops: &mut Vec<ScriptedOp>,
) {
    let keys = &families.list;
    if matches!(profile, Profile::MultiWaiter) {
        gen_multi_waiter(keys, MultiWaiterFamily::List, rng, multi_waited, ops);
        return;
    }
    let blocking_bias = matches!(profile, Profile::BlockingHeavy);
    let r = rng.random_range(0..100);
    let t = think(rng);
    if blocking_bias && r < 40 {
        // Producer + consumer on the same key. Only the key's blocking owner
        // issues the *blocking* pop (keeps per-key blocking registrations
        // single-client and thus unambiguously ordered for the FIFO checker);
        // other clients issue an equivalent non-blocking pop instead.
        let k = pick(keys, rng).clone();
        push_op(ops, "rpush", vec![k.clone(), alnum_value(rng)], t);
        if blocking_owner(&k, num_clients) == client_id {
            let off = next_block_offset(block_offset, &k);
            let cmd = if rng.random_range(0..2) == 0 {
                "blpop"
            } else {
                "brpop"
            };
            push_op(ops, cmd, vec![k, block_timeout(rng)], off);
        } else {
            let cmd = if rng.random_range(0..2) == 0 {
                "lpop"
            } else {
                "rpop"
            };
            push_op(ops, cmd, vec![k], think(rng));
        }
        return;
    }
    if blocking_bias && r < 55 && keys.len() >= 2 {
        // BLMOVE registers a blocking waiter on `src`, so only src's blocking
        // owner may issue it; other clients do the equivalent non-blocking
        // cross-key LMOVE. Cross-key element conservation covers both, and the
        // partitioner splits the move into per-key pop/push halves.
        let src = pick(keys, rng).clone();
        let dst = pick(keys, rng).clone();
        push_op(ops, "lpush", vec![src.clone(), alnum_value(rng)], t);
        if blocking_owner(&src, num_clients) == client_id {
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
        } else {
            push_op(
                ops,
                "lmove",
                vec![src, dst, Bytes::from("LEFT"), Bytes::from("RIGHT")],
                think(rng),
            );
        }
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

/// Which blocking family a multi-waiter op targets — selects the producer push
/// and the blocking-pop command pair.
#[derive(Clone, Copy)]
enum MultiWaiterFamily {
    List,
    ZSet,
}

/// Emit one op on the multi-waiter path: either a *long-timeout* blocking pop
/// (this client becomes one of possibly many concurrent waiters on `k`) or a
/// *delayed* producer that serves parked waiters in registration order.
///
/// The single-owner [`blocking_owner`] restriction is intentionally dropped
/// here — every client may block on any key — because the exact FIFO checker
/// judges served order against the mid-run `DEBUG WAITQUEUE` registration
/// ordinals, not the invoke-time proxy. The one binding constraint is
/// **at most one blocking pop per key per client** (`multi_waited` guard); a
/// client that would block a second time on the same key instead issues a
/// producer push, so its `(key, client_id)` join stays single-valued.
fn gen_multi_waiter(
    keys: &[Bytes],
    family: MultiWaiterFamily,
    rng: &mut StdRng,
    multi_waited: &mut std::collections::HashSet<Vec<u8>>,
    ops: &mut Vec<ScriptedOp>,
) {
    let k = pick(keys, rng).clone();
    let roll = rng.random_range(0..100);
    // Waiter path: ~55% of ops, but only if this client has not already parked
    // on `k` (the soundness guard). Otherwise fall through to the producer.
    if roll < 55 && !multi_waited.contains(k.as_ref()) {
        multi_waited.insert(k.to_vec());
        let timeout = Bytes::from(MULTI_WAITER_TIMEOUT);
        let think = multi_waiter_think(rng);
        match family {
            MultiWaiterFamily::List => {
                let cmd = if rng.random_range(0..2) == 0 {
                    "blpop"
                } else {
                    "brpop"
                };
                push_op(ops, cmd, vec![k, timeout], think);
            }
            MultiWaiterFamily::ZSet => {
                let cmd = if rng.random_range(0..2) == 0 {
                    "bzpopmin"
                } else {
                    "bzpopmax"
                };
                push_op(ops, cmd, vec![k, timeout], think);
            }
        }
    } else {
        // Producer path: a delayed push/add that serves the oldest parked
        // waiter FIFO. The delay lets concurrent waiters register (and the
        // prober observe them) before the first element arrives.
        let delay = producer_think(rng);
        match family {
            MultiWaiterFamily::List => {
                push_op(ops, "rpush", vec![k, alnum_value(rng)], delay);
            }
            MultiWaiterFamily::ZSet => {
                let score = Bytes::from(rng.random_range(0..100).to_string());
                let m = Bytes::from(format!("m{}", rng.random_range(0..5)));
                push_op(ops, "zadd", vec![k, score, m], delay);
            }
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

#[allow(clippy::too_many_arguments)]
fn gen_zset(
    profile: Profile,
    families: &KeyFamilies,
    rng: &mut StdRng,
    block_offset: &mut std::collections::HashMap<Vec<u8>, u64>,
    multi_waited: &mut std::collections::HashSet<Vec<u8>>,
    client_id: u64,
    num_clients: usize,
    ops: &mut Vec<ScriptedOp>,
) {
    let keys = &families.zset;
    if matches!(profile, Profile::MultiWaiter) {
        gen_multi_waiter(keys, MultiWaiterFamily::ZSet, rng, multi_waited, ops);
        return;
    }
    let blocking_bias = matches!(profile, Profile::BlockingHeavy);
    let r = rng.random_range(0..100);
    let t = think(rng);
    if blocking_bias && r < 45 {
        // Producer + consumer on the same key. As with lists, only the key's
        // blocking owner issues the blocking pop; others issue a non-blocking
        // ZREM of the same member so per-key blocking waiters stay single-client.
        let k = pick(keys, rng).clone();
        let score = Bytes::from(rng.random_range(0..100).to_string());
        let m = Bytes::from(format!("m{}", rng.random_range(0..5)));
        push_op(ops, "zadd", vec![k.clone(), score, m.clone()], t);
        if blocking_owner(&k, num_clients) == client_id {
            let off = next_block_offset(block_offset, &k);
            let cmd = if rng.random_range(0..2) == 0 {
                "bzpopmin"
            } else {
                "bzpopmax"
            };
            push_op(ops, cmd, vec![k, block_timeout(rng)], off);
        } else {
            push_op(ops, "zrem", vec![k, m], think(rng));
        }
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

/// Consumer-group ops on a stream: XGROUP CREATE (once per client per key,
/// MKSTREAM so the group creates the stream too), then a bounded interleave
/// of XADD/XREADGROUP/XPENDING/XAUTOCLAIM/XCLAIM/XACK. Single group `g0` per
/// key keeps the PEL checker's per-(stream, group) scoping simple (matches
/// the model's bounded consumer-group vocabulary: min-idle always 0, no
/// NOACK, no blocking, XAUTOCLAIM/XCLAIM JUSTID-only forms).
fn gen_stream_group(
    families: &KeyFamilies,
    rng: &mut StdRng,
    created: &mut std::collections::HashSet<Vec<u8>>,
    ops: &mut Vec<ScriptedOp>,
) {
    let keys = &families.stream_group;
    let k = pick(keys, rng).clone();
    let group = Bytes::from("g0");
    let t = think(rng);
    // Ensure the group exists (MKSTREAM makes the stream too). Idempotent
    // BUSYGROUP re-creates are accepted by the model as no-ops.
    if created.insert(k.to_vec()) {
        push_op(
            ops,
            "xgroup",
            vec![
                Bytes::from("CREATE"),
                k.clone(),
                group.clone(),
                Bytes::from("0"),
                Bytes::from("MKSTREAM"),
            ],
            t,
        );
        return;
    }
    let consumer = Bytes::from(format!("c{}", rng.random_range(0..3)));
    match rng.random_range(0..7) {
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
        1 => push_op(
            ops,
            "xreadgroup",
            vec![
                Bytes::from("GROUP"),
                group,
                consumer,
                Bytes::from("COUNT"),
                Bytes::from("10"),
                Bytes::from("STREAMS"),
                k,
                Bytes::from(">"),
            ],
            t,
        ),
        2 => push_op(
            ops,
            "xreadgroup",
            vec![
                Bytes::from("GROUP"),
                group,
                consumer,
                Bytes::from("STREAMS"),
                k,
                Bytes::from("0"),
            ],
            t,
        ),
        3 => push_op(ops, "xpending", vec![k, group], t),
        4 => {
            // XAUTOCLAIM JUSTID from 0 (claims all eligible to `consumer`).
            // COUNT 1000 (not the usual 10): the model expects a single call
            // to return cursor 0-0 (full-range claim in one shot). A COUNT
            // smaller than the achievable PEL size would make the real
            // server legally return a non-zero cursor and the model would
            // spuriously reject that correct reply; 1000 exceeds any PEL
            // size these bounded workloads can build.
            push_op(
                ops,
                "xautoclaim",
                vec![
                    k,
                    group,
                    consumer,
                    Bytes::from("0"),
                    Bytes::from("0"),
                    Bytes::from("COUNT"),
                    Bytes::from("1000"),
                    Bytes::from("JUSTID"),
                ],
                t,
            );
        }
        5 => {
            // XCLAIM wire no-op (id 0-0 is never in a PEL): exercises the
            // command/recorder/model path; live-id claims happen via XAUTOCLAIM.
            push_op(
                ops,
                "xclaim",
                vec![k, group, consumer, Bytes::from("0"), Bytes::from("0-0")],
                t,
            );
        }
        _ => {
            // XACK the group PEL's low id space is unknown to the generator, so
            // ack a plausible recent id space via a wildcard-free explicit id is
            // not possible; instead ack nothing-harmful by acking id "0-0"
            // (count 0). Real acks happen via the model observing prior reads.
            push_op(ops, "xack", vec![k, group, Bytes::from("0-0")], t);
        }
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
            "set",
            "get",
            "del",
            "incr",
            "mset",
            "mget",
            "watch",
            "exec",
            "discard",
            "lpush",
            "rpush",
            "lpop",
            "rpop",
            "lmove",
            "llen",
            "lrange",
            "blpop",
            "brpop",
            "blmove",
            "hset",
            "hdel",
            "hget",
            "hincrby",
            "hgetall",
            "hlen",
            "zadd",
            "zrem",
            "zscore",
            "zcard",
            "bzpopmin",
            "bzpopmax",
            "xadd",
            "xlen",
            "xread",
            "xgroup",
            "xreadgroup",
            "xack",
            "xpending",
            "xclaim",
            "xautoclaim",
            "script_getset",
            "script_cincr",
            "script_setnx_get",
            "script_lpush_llen",
            "script_rpush_llen",
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
    fn txheavy_mostly_same_slot_exec() {
        // In TxHeavy, the strong majority of EXECs must be single-slot: every
        // sub-command key hashes to the same slot as the first (so the server
        // commits on the fast path rather than rejecting/needing VLL). A
        // deliberate minority may still be cross-slot.
        use crate::partition::default_keys_of;

        fn slot(k: &[u8]) -> u16 {
            // CRC16-XMODEM over the hash tag, mirroring the server's hash_slot.
            let tagged = {
                let s = k.iter().position(|&b| b == b'{');
                match s {
                    Some(si) => {
                        let rest = &k[si + 1..];
                        match rest.iter().position(|&b| b == b'}') {
                            Some(ei) if ei > 0 => &rest[..ei],
                            _ => k,
                        }
                    }
                    None => k,
                }
            };
            let mut crc: u16 = 0;
            for &byte in tagged {
                crc ^= (byte as u16) << 8;
                for _ in 0..8 {
                    crc = if crc & 0x8000 != 0 {
                        (crc << 1) ^ 0x1021
                    } else {
                        crc << 1
                    };
                }
            }
            crc % 16384
        }

        let mut same = 0u32;
        let mut cross = 0u32;
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::TxHeavy, 4, 40);
            for op in w
                .clients
                .iter()
                .flat_map(|c| &c.ops)
                .filter(|o| o.command == "exec")
            {
                // Classify by SUB-COMMAND count, not key count: default_keys_of
                // DEDUPS (exec_keys, partition.rs), so a same-slot EXEC that
                // reuses one key yields keys.len() == 1 and must count as same.
                let num_cmds = op
                    .args
                    .first()
                    .and_then(|n| String::from_utf8_lossy(n).parse::<usize>().ok())
                    .unwrap_or(0);
                if num_cmds < 2 {
                    continue;
                }
                let keys = default_keys_of("exec", &op.args);
                let s0 = slot(&keys[0]);
                if keys.len() == 1 || keys.iter().all(|k| slot(k) == s0) {
                    same += 1;
                } else {
                    cross += 1;
                }
            }
        }
        assert!(
            same > cross * 3,
            "expected same-slot EXECs to dominate: same={same} cross={cross}"
        );
        assert!(
            cross > 0,
            "keep a deliberate minority of cross-slot EXECs: cross={cross}"
        );
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
    fn blocking_ops_single_owner_per_key() {
        // Cross-client guard: every blocking pop on a given key must come from
        // exactly one client, so per-key blocking registrations are never
        // ambiguously ordered for the FIFO wake-order checker.
        const BLOCKING: &[&str] = &["blpop", "brpop", "bzpopmin", "bzpopmax", "blmove"];
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::BlockingHeavy, 4, 40);
            // key -> the single client id allowed to block on it.
            let mut owner: std::collections::HashMap<Vec<u8>, u64> = Default::default();
            for c in &w.clients {
                for op in c
                    .ops
                    .iter()
                    .filter(|o| BLOCKING.contains(&o.command.as_str()))
                {
                    // For blmove the blocking waiter registers on the src (arg 0).
                    let key = op.args[0].to_vec();
                    match owner.get(&key) {
                        Some(&cid) => assert_eq!(
                            cid,
                            c.client_id,
                            "key {:?} has blocking ops from >1 client",
                            String::from_utf8_lossy(&key)
                        ),
                        None => {
                            owner.insert(key, c.client_id);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn multi_waiter_at_most_one_blocking_pop_per_key_per_client() {
        // Binding soundness guard: on the MultiWaiter path each client may park
        // on a key at most once across its whole script (the exact FIFO join
        // keeps a client's first ordinal per key, so a second wait would be
        // judged against a stale registration).
        const BLOCKING: &[&str] = &["blpop", "brpop", "bzpopmin", "bzpopmax"];
        for seed in 0..60 {
            let w = Workload::generate(seed, Profile::MultiWaiter, 4, 40);
            for c in &w.clients {
                let mut seen: std::collections::HashSet<Vec<u8>> = Default::default();
                for op in c
                    .ops
                    .iter()
                    .filter(|o| BLOCKING.contains(&o.command.as_str()))
                {
                    let key = op.args[0].to_vec();
                    assert!(
                        seen.insert(key.clone()),
                        "seed {seed} client {} blocked twice on key {:?} (multi-waiter guard breached)",
                        c.client_id,
                        String::from_utf8_lossy(&key)
                    );
                }
            }
        }
    }

    #[test]
    fn multi_waiter_allows_more_than_one_client_per_key() {
        // The inverse of `blocking_ops_single_owner_per_key`, scoped to the
        // MultiWaiter path: at least one key must receive blocking pops from
        // more than one client (otherwise no concurrent multi-waiter race is
        // exercised). Every blocking pop still carries the long timeout, and
        // producers still appear so the parked waiters can be served.
        const BLOCKING: &[&str] = &["blpop", "brpop", "bzpopmin", "bzpopmax"];
        let mut saw_multi_client_key = false;
        let mut saw_producer = false;
        for seed in 0..60 {
            let w = Workload::generate(seed, Profile::MultiWaiter, 4, 40);
            // key -> set of client ids that block on it.
            let mut clients_per_key: std::collections::HashMap<
                Vec<u8>,
                std::collections::HashSet<u64>,
            > = Default::default();
            for c in &w.clients {
                for op in &c.ops {
                    if BLOCKING.contains(&op.command.as_str()) {
                        assert_eq!(
                            String::from_utf8_lossy(&op.args[1]),
                            MULTI_WAITER_TIMEOUT,
                            "multi-waiter blocking pop must carry the long timeout"
                        );
                        clients_per_key
                            .entry(op.args[0].to_vec())
                            .or_default()
                            .insert(c.client_id);
                    }
                    if matches!(op.command.as_str(), "rpush" | "zadd") {
                        saw_producer = true;
                    }
                }
            }
            if clients_per_key.values().any(|clients| clients.len() > 1) {
                saw_multi_client_key = true;
            }
        }
        assert!(
            saw_multi_client_key,
            "MultiWaiter must produce at least one key with blocking pops from >1 client"
        );
        assert!(
            saw_producer,
            "MultiWaiter must emit producers to serve parked waiters"
        );
    }

    #[test]
    fn script_vocabulary_emitted_and_routable() {
        use crate::partition::default_keys_of;
        let names: Vec<&str> = SCRIPT_POOL.iter().map(|(n, _)| *n).collect();
        let mut seen = false;
        for seed in 0..60 {
            let w = Workload::generate(seed, Profile::Mixed, 4, 40);
            for op in w.clients.iter().flat_map(|c| &c.ops) {
                if names.contains(&op.command.as_str()) {
                    seen = true;
                    assert!(!default_keys_of(&op.command, &op.args).is_empty());
                }
            }
        }
        assert!(seen, "generator must emit script pseudo-ops");
    }

    #[test]
    fn stream_group_vocabulary_emitted_and_routable() {
        use crate::partition::default_keys_of;
        const GROUP_OPS: &[&str] = &[
            "xgroup",
            "xreadgroup",
            "xack",
            "xpending",
            "xclaim",
            "xautoclaim",
        ];
        let mut seen: std::collections::HashSet<&str> = Default::default();
        for seed in 0..60 {
            let w = Workload::generate(seed, Profile::Mixed, 4, 40);
            for op in w.clients.iter().flat_map(|c| &c.ops) {
                if GROUP_OPS.contains(&op.command.as_str()) {
                    seen.insert(GROUP_OPS.iter().find(|g| **g == op.command).unwrap());
                    // Every group op must route to a non-empty key set.
                    assert!(
                        !default_keys_of(&op.command, &op.args).is_empty(),
                        "group op {} not routable",
                        op.command
                    );
                }
            }
        }
        assert!(
            seen.contains("xreadgroup") && seen.contains("xack"),
            "generator must emit consumer-group ops; saw {seen:?}"
        );
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
