//! Per-key history partitioning for scalable linearizability checking.
//!
//! # Cross-key op projection semantics
//!
//! Some ops touch multiple keys *atomically* (`mset`, `mget`, committed `exec`)
//! while others touch multiple keys *non-atomically from a per-key point of
//! view* — a blocking pop watches several keys and is served by whichever one
//! has data first (`blpop k1 k2 timeout`), and `lmove`/`blmove` atomically move
//! an element from a source list to a (possibly different) destination list.
//!
//! For atomic multi-key ops we explode the op into one atomic sub-op per key
//! (see the `mset`/`mget`/`exec` arms of [`project_for_key`]) — this is sound
//! because the whole op really did apply to every touched key at once.
//!
//! For the non-atomic multi-key ops, naively replicating the *whole* op into
//! every touched key's sub-history is unsound: `lmove src dst left right ->
//! "x"` landing whole in `dst`'s partition claims `dst` popped `"x"` *from
//! itself*, which the per-key `ListModel` correctly rejects — a spurious
//! non-linearizable verdict caused by the projection, not a real bug in the
//! system under test. Likewise `blpop k1 k2 timeout -> "k1|x"` landing in
//! `k2`'s partition claims `k2` served a value under `k1`'s name, which no
//! per-key model can make sense of.
//!
//! So these ops are instead attributed only to the single key that can
//! soundly claim them, or dropped from every per-key partition when no single
//! key can claim them soundly. In the drop case, cross-key conservation
//! checkers (not per-key linearizability) are responsible for catching bugs
//! involving the op — see the `blpop`/`brpop`/`bzpopmin`/`bzpopmax`/`lmove`/
//! `blmove` arms of [`project_for_key`] for the exact rules.

use crate::history::{History, OpKind};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

/// Parse the `exec` argument encoding into `(command_name, command_args)` pairs.
///
/// Encoding: `[num_cmds, name1, num_args1, args1..., name2, ...]`.
pub(crate) fn parse_exec_commands(args: &[Bytes]) -> Option<Vec<(String, Vec<Bytes>)>> {
    let num: usize = String::from_utf8_lossy(args.first()?).parse().ok()?;
    let mut idx = 1;
    let mut cmds = Vec::with_capacity(num);
    for _ in 0..num {
        let name = String::from_utf8_lossy(args.get(idx)?).to_lowercase();
        idx += 1;
        let na: usize = String::from_utf8_lossy(args.get(idx)?).parse().ok()?;
        idx += 1;
        let mut cargs = Vec::with_capacity(na);
        for _ in 0..na {
            cargs.push(args.get(idx)?.clone());
            idx += 1;
        }
        cmds.push((name, cargs));
    }
    Some(cmds)
}

/// Distinct keys (first-seen order) touched by an `exec` op's sub-commands.
fn exec_keys(args: &[Bytes]) -> Vec<Bytes> {
    let mut keys = Vec::new();
    if let Some(cmds) = parse_exec_commands(args) {
        for (_name, cargs) in cmds {
            // Assumes the phase-1 KVModel exec vocabulary is single-key
            // (set/get/incr/del): the sub-command's key is always its first
            // argument. Revisit if exec ever wraps a multi-key command.
            if let Some(k) = cargs.first()
                && !keys.contains(k)
            {
                keys.push(k.clone());
            }
        }
    }
    keys
}

/// Default key-extraction covering the phase-1 op vocabulary.
pub fn default_keys_of(function: &str, args: &[Bytes]) -> Vec<Bytes> {
    match function {
        "mget" => args.to_vec(),
        "mset" => args.iter().step_by(2).cloned().collect(),
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => {
            if args.len() >= 2 {
                args[..args.len() - 1].to_vec()
            } else {
                Vec::new()
            }
        }
        "lmove" | "blmove" => args.iter().take(2).cloned().collect(),
        "exec" => exec_keys(args),
        "get" | "set" | "read" | "write" | "cas" | "del" | "delete" | "incr" | "lpush"
        | "rpush" | "lpop" | "rpop" | "llen" | "lrange" | "hset" | "hdel" | "hget" | "hincrby"
        | "hgetall" | "hlen" | "zadd" | "zrem" | "zscore" | "zcard" | "xadd" | "xlen" | "xread" => {
            args.first().cloned().into_iter().collect()
        }
        // `watch key1 key2 ...` can watch any number of keys; all of them are keys.
        "watch" => args.to_vec(),
        _ => Vec::new(),
    }
}

/// Project an op onto a single key, returning the per-key sub-op
/// `(function, args, result)` or `None` if the op has no effect for that key.
fn project_for_key(
    function: &str,
    args: &[Bytes],
    result: Option<&Bytes>,
    key: &Bytes,
) -> Option<(String, Vec<Bytes>, Option<Bytes>)> {
    match function {
        "mset" => {
            // Redis `mset` is last-wins when a key appears more than once
            // (e.g. `MSET a 1 a 2` leaves `a` = 2), so scan the whole
            // argument list and keep the last matching pair rather than
            // returning on the first match.
            let mut i = 0;
            let mut last_match: Option<(Bytes, Bytes)> = None;
            while i + 1 < args.len() {
                if args[i] == *key {
                    last_match = Some((args[i].clone(), args[i + 1].clone()));
                }
                i += 2;
            }
            last_match.map(|(k, v)| {
                (
                    "set".to_string(),
                    vec![k, v],
                    Some(Bytes::from_static(b"OK")),
                )
            })
        }
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => {
            // Blocking pops watch N keys but are non-atomic across them: only
            // the single key that actually served the result (or, for a
            // single-watched-key op, the lone key itself) can soundly claim
            // the op. See the module doc comment for why.
            if args.len() < 2 {
                return None;
            }
            let timeout = args.last().expect("checked len >= 2").clone();
            match result {
                None => {
                    // Timed out: single-key ops keep their exact (already
                    // sound) semantics; multi-key timeouts are dropped from
                    // every per-key partition because no single key can
                    // individually claim "I timed out" when it was racing
                    // against sibling keys. Cross-key conservation checkers
                    // cover this case instead.
                    if args.len() == 2 {
                        Some((function.to_string(), args.to_vec(), None))
                    } else {
                        None
                    }
                }
                Some(r) => {
                    // Hit: result is encoded "served_key|elem" (or
                    // "served_key|member" for the sorted-set variants).
                    let s = String::from_utf8_lossy(r);
                    let served_key = s.split('|').next().unwrap_or("");
                    if served_key.as_bytes() == key.as_ref() {
                        Some((
                            function.to_string(),
                            vec![key.clone(), timeout],
                            Some(r.clone()),
                        ))
                    } else {
                        None
                    }
                }
            }
        }
        "lmove" | "blmove" => {
            // `lmove src dst FROM TO ... -> elem` atomically removes from src
            // and pushes to dst. When src == dst it is effectively a single-key
            // op and is kept as-is.
            //
            // When src != dst, replicating the *whole* op into either partition
            // is unsound (see the module doc). Instead we split it into its two
            // sound halves, each sharing the parent op's window:
            //   - src's partition sees the pop half: an `lpop`/`rpop` (per the
            //     FROM side) that returned `elem` — exactly what the source list
            //     observably did.
            //   - dst's partition sees the push half: the projection-only
            //     `lmove_push` synthetic op (per the TO side; see the
            //     `lmove_push` arm of `ListModel::step`), which deposits `elem`
            //     so later pops of it in dst are explicable rather than phantom.
            // A nil result (timeout, nothing moved) has no state effect and is
            // dropped from both partitions; cross-key element conservation still
            // covers the move as a whole.
            let src = args.first()?;
            let dst = args.get(1)?;
            if src == dst {
                return Some((function.to_string(), args.to_vec(), result.cloned()));
            }
            let elem = result?; // nil/timeout: no effect, drop from both.
            let from = args.get(2)?;
            let to = args.get(3)?;
            let is_left = |s: &Bytes| String::from_utf8_lossy(s).eq_ignore_ascii_case("left");
            if key == src {
                let pop = if is_left(from) { "lpop" } else { "rpop" };
                Some((pop.to_string(), vec![src.clone()], Some(elem.clone())))
            } else if key == dst {
                let side = if is_left(to) { "L" } else { "R" };
                Some((
                    "lmove_push".to_string(),
                    vec![
                        dst.clone(),
                        elem.clone(),
                        Bytes::from_static(side.as_bytes()),
                    ],
                    None,
                ))
            } else {
                None
            }
        }
        "mget" => {
            let idx = args.iter().position(|a| a == key)?;
            let joined = result
                .map(|r| String::from_utf8_lossy(r).to_string())
                .unwrap_or_default();
            let fields: Vec<&str> = if joined.is_empty() {
                Vec::new()
            } else {
                joined.split('|').collect()
            };
            let val = fields.get(idx).copied().unwrap_or("nil");
            let sub_result = if val == "nil" {
                None
            } else {
                Some(Bytes::from(val.to_string()))
            };
            Some(("get".to_string(), vec![key.clone()], sub_result))
        }
        "exec" => explode_exec_for_key(args, result, key),
        _ => Some((function.to_string(), args.to_vec(), result.cloned())),
    }
}

/// Re-encode a committed exec restricted to the commands touching `key`.
fn explode_exec_for_key(
    args: &[Bytes],
    result: Option<&Bytes>,
    key: &Bytes,
) -> Option<(String, Vec<Bytes>, Option<Bytes>)> {
    let cmds = parse_exec_commands(args)?;
    // A rejected transaction has no committed per-key effect: an aborted exec
    // (nil → `None`) and an errored exec (CROSSSLOT/EXECABORT → `Some("ERR:…")`)
    // are both no-ops. They must be dropped from every per-key partition *before*
    // the split-by-`|` explode below — otherwise the `"ERR:…"` marker would be
    // re-encoded as a sub-result and, worse, only the sub-command at index 0
    // would inherit it while every other touched key would get an empty result,
    // which `KVModel::exec` misreads as a committed-but-wrong EXEC (manufacturing
    // false non-linearizability). `KVModel::exec` also treats these as no-ops, but
    // that guard never sees them once they are exploded, so we must gate here too.
    let results: Vec<String> = match result {
        None => return None,
        Some(r) if is_errored_exec_result(r) => return None,
        Some(r) => {
            let s = String::from_utf8_lossy(r);
            if s.is_empty() {
                Vec::new()
            } else {
                s.split('|').map(str::to_string).collect()
            }
        }
    };
    let mut sub_cmds: Vec<(String, Vec<Bytes>)> = Vec::new();
    let mut sub_results: Vec<String> = Vec::new();
    for (i, (name, cargs)) in cmds.iter().enumerate() {
        // Assumes the phase-1 KVModel exec vocabulary is single-key
        // (set/get/incr/del): the sub-command's key is always its first
        // argument. Revisit if exec ever wraps a multi-key command.
        if cargs.first() == Some(key) {
            sub_cmds.push((name.clone(), cargs.clone()));
            if let Some(res) = results.get(i) {
                sub_results.push(res.clone());
            }
        }
    }
    if sub_cmds.is_empty() {
        return None;
    }
    let mut new_args = vec![Bytes::from(sub_cmds.len().to_string())];
    for (name, cargs) in &sub_cmds {
        new_args.push(Bytes::from(name.clone()));
        new_args.push(Bytes::from(cargs.len().to_string()));
        new_args.extend(cargs.clone());
    }
    Some((
        "exec".to_string(),
        new_args,
        Some(Bytes::from(sub_results.join("|"))),
    ))
}

/// Split `history` into per-key sub-histories. Multi-key atomic ops are
/// exploded into per-key atomic sub-ops that share the parent op's window.
pub fn partition_by_key(
    history: &History,
    keys_of: impl Fn(&str, &[Bytes]) -> Vec<Bytes>,
) -> HashMap<Bytes, History> {
    // Result-by-id for completed ops only (checker needs complete histories).
    let mut results: HashMap<u64, Option<Bytes>> = HashMap::new();
    for c in history.completed_operations() {
        results.insert(c.id, c.result.clone());
    }

    let mut subs: HashMap<Bytes, History> = HashMap::new();
    let mut sub_ids: HashMap<(u64, Bytes), u64> = HashMap::new();

    // Replay raw records in timestamp order so sub-op windows preserve overlap.
    for op in history.operations() {
        let Some(result) = results.get(&op.id) else {
            continue;
        };
        // Dedup first-seen order: a keys_of impl may report the same key
        // more than once for one op (e.g. `MGET a a`, `MSET a 1 a 2`, or
        // `lmove a a left right` before the src==dst collapse). Without
        // this, the per-key loop below would invoke/respond twice against
        // the same sub_ids slot, and the second `sub.respond` would panic
        // (History::respond's `.expect` fires because the first respond
        // already removed the pending entry).
        let mut keys = keys_of(&op.function, &op.args);
        let mut seen_keys: HashSet<Bytes> = HashSet::new();
        keys.retain(|k| seen_keys.insert(k.clone()));
        for key in keys {
            let Some((f, a, r)) = project_for_key(&op.function, &op.args, result.as_ref(), &key)
            else {
                continue;
            };
            match op.kind {
                OpKind::Invoke => {
                    let sub = subs.entry(key.clone()).or_default();
                    let sid = sub.invoke(op.client_id, f, a);
                    sub_ids.insert((op.id, key.clone()), sid);
                }
                OpKind::Return => {
                    if let Some(sid) = sub_ids.get(&(op.id, key.clone()))
                        && let Some(sub) = subs.get_mut(&key)
                    {
                        sub.respond(*sid, r);
                    }
                }
            }
        }
    }
    subs
}

/// True iff a recorded EXEC/transaction result denotes an aborted or rejected
/// transaction rather than a committed one. The turmoil recorder encodes a
/// server error reply (CROSSSLOT / EXECABORT / …) as the pipe-safe marker
/// `"ERR:<message>"` (see `OperationHistory::encode_array_result`); a
/// WATCH-abort is instead recorded as `None`. Callers treat both as no-ops.
///
/// Known edge: a *committed* EXEC whose first sub-command itself errored
/// (e.g. `INCR` on a non-integer) would also start with `"ERR:…"` and be
/// misread as rejected here — so `explode_exec_for_key` drops the whole
/// committed EXEC from every per-key partition as a no-op (its `None`-on-error
/// guard fires on `is_errored_exec_result`). The phase-1 generator's exec
/// vocabulary (set/get/incr/del over numeric values) cannot produce that
/// sub-command type error, so this is currently unreachable. If the vocabulary
/// ever grows error-capable sub-commands, the net is the cross-key conservation
/// checkers: the dropped write is invisible to per-key WGL, but conservation
/// (which reconciles effects across sibling keys) still catches the missing
/// mutation. Revisit both this gate and the explode drop before adding such a
/// sub-command.
pub fn is_errored_exec_result(result: &Bytes) -> bool {
    result.starts_with(b"ERR:")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use crate::models::{KVModel, ListModel};
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn default_keys_of_single_and_multi() {
        assert_eq!(default_keys_of("set", &[b("k"), b("v")]), vec![b("k")]);
        assert_eq!(
            default_keys_of("mget", &[b("a"), b("c")]),
            vec![b("a"), b("c")]
        );
        assert_eq!(
            default_keys_of("mset", &[b("a"), b("1"), b("c"), b("2")]),
            vec![b("a"), b("c")]
        );
        assert_eq!(
            default_keys_of("blpop", &[b("k1"), b("k2"), b("0")]),
            vec![b("k1"), b("k2")]
        );
        assert_eq!(
            default_keys_of("lmove", &[b("s"), b("d"), b("left"), b("right")]),
            vec![b("s"), b("d")]
        );
    }

    #[test]
    fn partition_splits_independent_keys() {
        let mut h = History::new();
        let s1 = h.invoke(1, "set", vec![b("a"), b("1")]);
        h.respond(s1, Some(b("OK")));
        let s2 = h.invoke(1, "set", vec![b("c"), b("2")]);
        h.respond(s2, Some(b("OK")));
        let g = h.invoke(2, "get", vec![b("a")]);
        h.respond(g, Some(b("1")));

        let parts = partition_by_key(&h, default_keys_of);
        assert_eq!(parts.len(), 2);
        for sub in parts.values() {
            assert!(check_linearizability::<KVModel>(sub).is_linearizable);
        }
        // key "a" got 2 ops (set + get), key "c" got 1 (set).
        assert_eq!(parts[&b("a")].completed_operations().len(), 2);
        assert_eq!(parts[&b("c")].completed_operations().len(), 1);
    }

    #[test]
    fn partition_explodes_mset() {
        let mut h = History::new();
        let m = h.invoke(1, "mset", vec![b("a"), b("1"), b("c"), b("2")]);
        h.respond(m, Some(b("OK")));

        let parts = partition_by_key(&h, default_keys_of);
        // Each key sees an atomic `set key val` sub-op returning OK.
        let a = &parts[&b("a")];
        let ops = a.completed_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].function, "set");
        assert_eq!(ops[0].args, vec![b("a"), b("1")]);
        assert!(check_linearizability::<KVModel>(a).is_linearizable);
    }

    #[test]
    fn partition_explodes_mget() {
        let mut h = History::new();
        let s = h.invoke(1, "set", vec![b("a"), b("1")]);
        h.respond(s, Some(b("OK")));
        let m = h.invoke(2, "mget", vec![b("a"), b("c")]);
        h.respond(m, Some(b("1|nil")));

        let parts = partition_by_key(&h, default_keys_of);
        assert!(check_linearizability::<KVModel>(&parts[&b("a")]).is_linearizable);
        // key "c" sub-op is `get c -> nil`.
        let c_ops = parts[&b("c")].completed_operations();
        assert_eq!(c_ops[0].function, "get");
        assert_eq!(c_ops[0].result, None);
    }

    #[test]
    fn partition_explodes_committed_exec_per_key() {
        // EXEC: SET a 1, SET c 2  -> "OK|OK"
        let mut h = History::new();
        let e = h.invoke(
            1,
            "exec",
            vec![
                b("2"),
                b("set"),
                b("2"),
                b("a"),
                b("1"),
                b("set"),
                b("2"),
                b("c"),
                b("2"),
            ],
        );
        h.respond(e, Some(b("OK|OK")));

        let parts = partition_by_key(&h, default_keys_of);
        let a_ops = parts[&b("a")].completed_operations();
        assert_eq!(a_ops[0].function, "exec");
        // Sub-exec re-encoded with only key "a"'s command, result "OK".
        assert_eq!(a_ops[0].result, Some(b("OK")));
        assert!(check_linearizability::<KVModel>(&parts[&b("a")]).is_linearizable);
    }

    #[test]
    fn partition_skips_aborted_exec() {
        let mut h = History::new();
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("a"), b("1")]);
        h.respond(e, None); // aborted (nil)
        let parts = partition_by_key(&h, default_keys_of);
        assert!(!parts.contains_key(&b("a")) || parts[&b("a")].completed_operations().is_empty());
    }

    #[test]
    fn partition_skips_errored_exec() {
        // A CROSSSLOT/EXECABORT-rejected EXEC is recorded as `Some("ERR:…")`.
        // It committed nothing, so every touched key must see a no-op — NOT an
        // exploded sub-op. Regression: the old explode split the "ERR:…" marker
        // by `|`, gave it only to the sub-command at index 0, and left every
        // other key with an empty result that `KVModel::exec` misread as a
        // committed-but-wrong EXEC (false non-linearizability). Here the erroring
        // key `b` sits at index 1 — the position that used to be poisoned.
        let mut h = History::new();
        // Establish a real value for `b` so a later read is only explicable if
        // the errored EXEC left `b` untouched.
        let s = h.invoke(1, "set", vec![b("b"), b("7")]);
        h.respond(s, Some(b("OK")));
        let e = h.invoke(
            2,
            "exec",
            vec![
                b("2"),
                b("set"),
                b("2"),
                b("a"),
                b("1"),
                b("set"),
                b("2"),
                b("b"),
                b("9"),
            ],
        );
        h.respond(
            e,
            Some(b(
                "ERR:CROSSSLOT Keys in request don't hash to the same slot",
            )),
        );
        let g = h.invoke(3, "get", vec![b("b")]);
        h.respond(g, Some(b("7"))); // still 7: the errored EXEC never wrote 9
        let parts = partition_by_key(&h, default_keys_of);
        // Neither key carries an exploded sub-op from the rejected EXEC.
        assert!(!parts.contains_key(&b("a")) || parts[&b("a")].completed_operations().is_empty());
        let b_ops = parts[&b("b")].completed_operations();
        assert!(
            b_ops.iter().all(|op| op.function != "exec"),
            "rejected EXEC must not explode into a per-key sub-op: {b_ops:?}"
        );
        // And `b`'s sub-history (set 7; get 7) stays linearizable.
        assert!(check_linearizability::<KVModel>(&parts[&b("b")]).is_linearizable);
    }

    #[test]
    fn mget_duplicate_keys_no_panic() {
        // MGET a a: the duplicate key must not double-invoke/respond a's
        // sub-history (previously panicked in History::respond).
        let mut h = History::new();
        let s = h.invoke(1, "set", vec![b("a"), b("1")]);
        h.respond(s, Some(b("OK")));
        let m = h.invoke(2, "mget", vec![b("a"), b("a")]);
        h.respond(m, Some(b("1|1")));

        let parts = partition_by_key(&h, default_keys_of);
        assert!(check_linearizability::<KVModel>(&parts[&b("a")]).is_linearizable);
    }

    #[test]
    fn mset_duplicate_key_last_wins() {
        // MSET a 1 a 2: Redis semantics are last-wins, so a's projected
        // sub-op must be `set a 2`, not `set a 1`.
        let mut h = History::new();
        let m = h.invoke(1, "mset", vec![b("a"), b("1"), b("a"), b("2")]);
        h.respond(m, Some(b("OK")));

        let parts = partition_by_key(&h, default_keys_of);
        let ops = parts[&b("a")].completed_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].function, "set");
        assert_eq!(ops[0].args, vec![b("a"), b("2")]);
    }

    #[test]
    fn multikey_blpop_hit_attributed_to_serving_key() {
        // BLPOP k1 k2 0 -> "k1|x": only k1 (the key that actually served the
        // value) may claim the op; k2 must see nothing from it.
        let mut h = History::new();
        let push = h.invoke(1, "rpush", vec![b("k1"), b("x")]);
        h.respond(push, Some(b("1")));
        let blk = h.invoke(2, "blpop", vec![b("k1"), b("k2"), b("0")]);
        h.respond(blk, Some(b("k1|x")));

        let parts = partition_by_key(&h, default_keys_of);
        assert!(check_linearizability::<ListModel>(&parts[&b("k1")]).is_linearizable);
        assert_eq!(
            parts
                .get(&b("k2"))
                .map(|p| p.completed_operations().len())
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn multikey_blpop_timeout_dropped() {
        // BLPOP k1 k2 0 -> nil: no single key can soundly claim a multi-key
        // timeout, so it must be dropped from every per-key partition.
        let mut h = History::new();
        let blk = h.invoke(1, "blpop", vec![b("k1"), b("k2"), b("0")]);
        h.respond(blk, None);

        let parts = partition_by_key(&h, default_keys_of);
        assert!(
            parts
                .get(&b("k1"))
                .map(|p| p.completed_operations().is_empty())
                .unwrap_or(true)
        );
        assert!(
            parts
                .get(&b("k2"))
                .map(|p| p.completed_operations().is_empty())
                .unwrap_or(true)
        );
    }

    #[test]
    fn cross_key_lmove_split_into_pop_and_push() {
        // LMOVE a b LEFT RIGHT -> x (a != b) splits into a pop half on `a`
        // (lpop -> x, since FROM=LEFT) and a push half on `b` (lmove_push
        // side R, since TO=RIGHT). Both partitions must linearize, and a
        // later pop of the moved element on `b` must be explicable rather
        // than a phantom.
        let mut h = History::new();
        let push = h.invoke(1, "rpush", vec![b("a"), b("x")]);
        h.respond(push, Some(b("1")));
        let mv = h.invoke(2, "lmove", vec![b("a"), b("b"), b("LEFT"), b("RIGHT")]);
        h.respond(mv, Some(b("x")));
        // `b` later pops the moved element: previously a false phantom.
        let pop = h.invoke(3, "lpop", vec![b("b")]);
        h.respond(pop, Some(b("x")));

        let parts = partition_by_key(&h, default_keys_of);

        // `a`: rpush x then lpop -> x (the move's pop half).
        let a_ops = parts[&b("a")].completed_operations();
        assert_eq!(a_ops.len(), 2);
        assert_eq!(a_ops[1].function, "lpop");
        assert_eq!(a_ops[1].result, Some(b("x")));
        assert!(check_linearizability::<ListModel>(&parts[&b("a")]).is_linearizable);

        // `b`: lmove_push (deposit) then lpop -> x. Must linearize.
        let b_ops = parts[&b("b")].completed_operations();
        assert_eq!(b_ops.len(), 2);
        assert_eq!(b_ops[0].function, "lmove_push");
        assert!(check_linearizability::<ListModel>(&parts[&b("b")]).is_linearizable);
    }

    #[test]
    fn cross_key_lmove_timeout_dropped() {
        // Nil result (BLMOVE timed out, nothing moved) has no state effect and
        // is dropped from both partitions.
        let mut h = History::new();
        let mv = h.invoke(
            1,
            "blmove",
            vec![b("a"), b("b"), b("LEFT"), b("RIGHT"), b("0")],
        );
        h.respond(mv, None);
        let parts = partition_by_key(&h, default_keys_of);
        assert!(
            parts
                .get(&b("a"))
                .map(|p| p.completed_operations().is_empty())
                .unwrap_or(true)
        );
        assert!(
            parts
                .get(&b("b"))
                .map(|p| p.completed_operations().is_empty())
                .unwrap_or(true)
        );
    }

    #[test]
    fn same_key_lmove_kept() {
        // LMOVE a a left right -> x is effectively single-key and must be
        // kept in a's partition.
        let mut h = History::new();
        let push = h.invoke(1, "rpush", vec![b("a"), b("x")]);
        h.respond(push, Some(b("1")));
        let mv = h.invoke(2, "lmove", vec![b("a"), b("a"), b("left"), b("right")]);
        h.respond(mv, Some(b("x")));

        let parts = partition_by_key(&h, default_keys_of);
        let ops = parts[&b("a")].completed_operations();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[1].function, "lmove");
        assert!(check_linearizability::<ListModel>(&parts[&b("a")]).is_linearizable);
    }

    #[test]
    fn watch_multi_key() {
        assert_eq!(
            default_keys_of("watch", &[b("k1"), b("k2")]),
            vec![b("k1"), b("k2")]
        );
    }
}
