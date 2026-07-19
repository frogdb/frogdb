//! Hash sequential model (HSET/HDEL/HGET/HINCRBY/HGETALL/HLEN).

use super::{Model, expect_int};
use bytes::Bytes;
use std::collections::HashMap;

/// Sequential model for Redis hashes.
#[derive(Debug, Clone, Default)]
pub struct HashModel;

/// State for the hash model.
#[derive(Debug, Clone, Default)]
pub struct HashState {
    /// key -> (field -> value).
    pub hashes: HashMap<Bytes, HashMap<Bytes, Bytes>>,
}

impl Model for HashModel {
    type State = HashState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "hset" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let h = new.hashes.entry(key.clone()).or_default();
                let mut added = 0i64;
                let mut i = 1;
                while i + 1 < args.len() {
                    if h.insert(args[i].clone(), args[i + 1].clone()).is_none() {
                        added += 1;
                    }
                    i += 2;
                }
                if expect_int(result, added) {
                    Some(new)
                } else {
                    None
                }
            }
            "hdel" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let mut removed = 0i64;
                if let Some(h) = new.hashes.get_mut(key) {
                    for f in &args[1..] {
                        if h.remove(f).is_some() {
                            removed += 1;
                        }
                    }
                    if h.is_empty() {
                        new.hashes.remove(key);
                    }
                }
                if expect_int(result, removed) {
                    Some(new)
                } else {
                    None
                }
            }
            "hget" => {
                if args.len() < 2 {
                    return None;
                }
                let val = state.hashes.get(&args[0]).and_then(|h| h.get(&args[1]));
                if result == val {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "hincrby" => {
                if args.len() < 3 {
                    return None;
                }
                let (key, field) = (&args[0], &args[1]);
                let delta: i64 = String::from_utf8_lossy(&args[2]).parse().ok()?;
                let cur = state
                    .hashes
                    .get(key)
                    .and_then(|h| h.get(field))
                    .and_then(|v| String::from_utf8_lossy(v).parse::<i64>().ok())
                    .unwrap_or(0);
                let nv = cur + delta;
                let mut new = state.clone();
                new.hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), Bytes::from(nv.to_string()));
                if expect_int(result, nv) {
                    Some(new)
                } else {
                    None
                }
            }
            "hgetall" => {
                if args.is_empty() {
                    return None;
                }
                let mut pairs: Vec<(String, String)> = state
                    .hashes
                    .get(&args[0])
                    .map(|h| {
                        h.iter()
                            .map(|(f, v)| {
                                (
                                    String::from_utf8_lossy(f).to_string(),
                                    String::from_utf8_lossy(v).to_string(),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                pairs.sort();
                let expected = pairs
                    .into_iter()
                    .flat_map(|(f, v)| [f, v])
                    .collect::<Vec<_>>()
                    .join("|");
                match result {
                    Some(r) if String::from_utf8_lossy(r) == expected => Some(state.clone()),
                    None if expected.is_empty() => Some(state.clone()),
                    _ => None,
                }
            }
            "hlen" => {
                if args.is_empty() {
                    return None;
                }
                let len = state.hashes.get(&args[0]).map_or(0, HashMap::len) as i64;
                if expect_int(result, len) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn hash_happy_path() {
        let s = HashState::default();
        // HSET h f1 v1 f2 v2 -> 2 new fields
        let s = HashModel::step(
            &s,
            "hset",
            &[b("h"), b("f1"), b("v1"), b("f2"), b("v2")],
            Some(&b("2")),
        )
        .unwrap();
        // HGET h f1 -> v1
        assert!(HashModel::step(&s, "hget", &[b("h"), b("f1")], Some(&b("v1"))).is_some());
        // HLEN -> 2
        assert!(HashModel::step(&s, "hlen", &[b("h")], Some(&b("2"))).is_some());
        // HGETALL sorted -> f1|v1|f2|v2
        assert!(HashModel::step(&s, "hgetall", &[b("h")], Some(&b("f1|v1|f2|v2"))).is_some());
        // HSET existing field -> 0 new
        let s = HashModel::step(&s, "hset", &[b("h"), b("f1"), b("v9")], Some(&b("0"))).unwrap();
        assert!(HashModel::step(&s, "hget", &[b("h"), b("f1")], Some(&b("v9"))).is_some());
        // HDEL h f1 -> 1 removed
        let s = HashModel::step(&s, "hdel", &[b("h"), b("f1")], Some(&b("1"))).unwrap();
        assert!(HashModel::step(&s, "hget", &[b("h"), b("f1")], None).is_some());
    }

    #[test]
    fn hash_hincrby() {
        let s = HashState::default();
        let s = HashModel::step(&s, "hincrby", &[b("h"), b("n"), b("5")], Some(&b("5"))).unwrap();
        assert!(
            HashModel::step(&s, "hincrby", &[b("h"), b("n"), b("-2")], Some(&b("3"))).is_some()
        );
    }

    #[test]
    fn hash_strict_rejects_unknown() {
        let s = HashState::default();
        assert!(HashModel::step(&s, "get", &[b("h")], Some(&b("v"))).is_none());
    }

    #[test]
    fn hash_wrong_result_rejected() {
        let s = HashState::default();
        assert!(HashModel::step(&s, "hset", &[b("h"), b("f"), b("v")], Some(&b("0"))).is_none());
    }

    #[test]
    fn hash_linearizable() {
        let mut h = History::new();
        let w = h.invoke(1, "hset", vec![b("h"), b("f"), b("v")]);
        h.respond(w, Some(b("1")));
        let r = h.invoke(2, "hget", vec![b("h"), b("f")]);
        h.respond(r, Some(b("v")));
        assert!(check_linearizability::<HashModel>(&h).is_linearizable);
    }
}
