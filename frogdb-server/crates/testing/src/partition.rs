//! Per-key history partitioning for scalable linearizability checking.

use crate::history::{History, OpKind};
use bytes::Bytes;
use std::collections::HashMap;

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
        | "hgetall" | "hlen" | "zadd" | "zrem" | "zscore" | "zcard" | "xadd" | "xlen" | "xread"
        | "watch" => args.first().cloned().into_iter().collect(),
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
            let mut i = 0;
            while i + 1 < args.len() {
                if args[i] == *key {
                    return Some((
                        "set".to_string(),
                        vec![args[i].clone(), args[i + 1].clone()],
                        Some(Bytes::from_static(b"OK")),
                    ));
                }
                i += 2;
            }
            None
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
    // Aborted exec (nil) has no committed per-key effect.
    let results: Vec<String> = match result {
        None => return None,
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
        for key in keys_of(&op.function, &op.args) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use crate::models::KVModel;
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
}
