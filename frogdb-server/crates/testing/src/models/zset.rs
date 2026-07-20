//! Sorted-set sequential model (ZADD/ZREM/ZSCORE/ZCARD + blocking BZPOP).

use super::{Model, expect_int, fmt_score};
use bytes::Bytes;
use std::collections::HashMap;

/// Sequential model for Redis sorted sets.
#[derive(Debug, Clone, Default)]
pub struct ZSetModel;

/// State for the sorted-set model.
#[derive(Debug, Clone, Default)]
pub struct ZSetState {
    /// key -> (member -> score).
    pub zsets: HashMap<Bytes, HashMap<Bytes, f64>>,
}

impl Model for ZSetModel {
    type State = ZSetState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "zadd" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let z = new.zsets.entry(key.clone()).or_default();
                let mut added = 0i64;
                let mut i = 1;
                while i + 1 < args.len() {
                    let score: f64 = String::from_utf8_lossy(&args[i])
                        .parse()
                        .ok()
                        .filter(|s: &f64| !s.is_nan())?;
                    let member = args[i + 1].clone();
                    if z.insert(member, score).is_none() {
                        added = added.checked_add(1)?;
                    }
                    i += 2;
                }
                if expect_int(result, added) {
                    Some(new)
                } else {
                    None
                }
            }
            "zrem" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let mut removed = 0i64;
                if let Some(z) = new.zsets.get_mut(key) {
                    for m in &args[1..] {
                        if z.remove(m).is_some() {
                            removed = removed.checked_add(1)?;
                        }
                    }
                    if z.is_empty() {
                        new.zsets.remove(key);
                    }
                }
                if expect_int(result, removed) {
                    Some(new)
                } else {
                    None
                }
            }
            "zscore" => {
                if args.len() < 2 {
                    return None;
                }
                let score = state.zsets.get(&args[0]).and_then(|z| z.get(&args[1]));
                match (score, result) {
                    (Some(s), Some(r)) if String::from_utf8_lossy(r) == fmt_score(*s) => {
                        Some(state.clone())
                    }
                    (None, None) => Some(state.clone()),
                    _ => None,
                }
            }
            "zcard" => {
                if args.is_empty() {
                    return None;
                }
                let card = state.zsets.get(&args[0]).map_or(0, HashMap::len) as i64;
                if expect_int(result, card) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "bzpopmin" | "bzpopmax" => {
                if args.len() < 2 {
                    return None;
                }
                let keys = &args[..args.len() - 1];
                let want_min = function == "bzpopmin";
                let first = keys
                    .iter()
                    .find(|k| state.zsets.get(*k).is_some_and(|z| !z.is_empty()));
                match (first, result) {
                    (None, None) => Some(state.clone()),
                    (None, Some(_)) => None,
                    (Some(_), None) => None,
                    (Some(k), Some(r)) => {
                        let z = state.zsets.get(k)?;
                        // DIVERGENCE: on score ties we pop the lexicographically
                        // smallest member for both MIN and MAX (Redis' exact tie
                        // rule is not modeled in phase 1; workloads avoid ties).
                        let (member, score) = z
                            .iter()
                            .min_by(|(am, asc), (bm, bsc)| {
                                let ord = asc.partial_cmp(bsc).unwrap_or(std::cmp::Ordering::Equal);
                                let ord = if want_min { ord } else { ord.reverse() };
                                ord.then_with(|| am.cmp(bm))
                            })
                            .map(|(m, s)| (m.clone(), *s))?;
                        let mut new = state.clone();
                        let zz = new.zsets.get_mut(k)?;
                        zz.remove(&member);
                        if zz.is_empty() {
                            new.zsets.remove(k);
                        }
                        let expected = format!(
                            "{}|{}|{}",
                            String::from_utf8_lossy(k),
                            String::from_utf8_lossy(&member),
                            fmt_score(score)
                        );
                        if String::from_utf8_lossy(r) == expected {
                            Some(new)
                        } else {
                            None
                        }
                    }
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
    fn zset_happy_path() {
        let s = ZSetState::default();
        // ZADD z 1 a 2 b -> 2 new
        let s = ZSetModel::step(
            &s,
            "zadd",
            &[b("z"), b("1"), b("a"), b("2"), b("b")],
            Some(&b("2")),
        )
        .unwrap();
        assert!(ZSetModel::step(&s, "zscore", &[b("z"), b("a")], Some(&b("1"))).is_some());
        assert!(ZSetModel::step(&s, "zcard", &[b("z")], Some(&b("2"))).is_some());
        // Re-add existing member with new score -> 0 new
        let s = ZSetModel::step(&s, "zadd", &[b("z"), b("5"), b("a")], Some(&b("0"))).unwrap();
        assert!(ZSetModel::step(&s, "zscore", &[b("z"), b("a")], Some(&b("5"))).is_some());
        // ZREM -> 1
        let s = ZSetModel::step(&s, "zrem", &[b("z"), b("a")], Some(&b("1"))).unwrap();
        assert!(ZSetModel::step(&s, "zscore", &[b("z"), b("a")], None).is_some());
    }

    #[test]
    fn bzpopmin_selects_lowest_score() {
        let s = ZSetState::default();
        let s = ZSetModel::step(
            &s,
            "zadd",
            &[b("z"), b("2"), b("b"), b("1"), b("a")],
            Some(&b("2")),
        )
        .unwrap();
        // BZPOPMIN z 0 -> z|a|1
        assert!(ZSetModel::step(&s, "bzpopmin", &[b("z"), b("0")], Some(&b("z|a|1"))).is_some());
        // Popping the wrong (non-min) member is illegal.
        assert!(ZSetModel::step(&s, "bzpopmin", &[b("z"), b("0")], Some(&b("z|b|2"))).is_none());
    }

    #[test]
    fn bzpopmax_timeout_only_when_empty() {
        let s = ZSetState::default();
        // Empty -> timeout legal.
        assert!(ZSetModel::step(&s, "bzpopmax", &[b("z"), b("0")], None).is_some());
        let s = ZSetModel::step(&s, "zadd", &[b("z"), b("1"), b("a")], Some(&b("1"))).unwrap();
        // Non-empty -> cannot time out.
        assert!(ZSetModel::step(&s, "bzpopmax", &[b("z"), b("0")], None).is_none());
        assert!(ZSetModel::step(&s, "bzpopmax", &[b("z"), b("0")], Some(&b("z|a|1"))).is_some());
    }

    #[test]
    fn zset_strict_rejects_unknown() {
        let s = ZSetState::default();
        assert!(ZSetModel::step(&s, "sadd", &[b("z"), b("a")], Some(&b("1"))).is_none());
    }

    #[test]
    fn zadd_nan_score_rejected() {
        let s = ZSetState::default();
        assert!(ZSetModel::step(&s, "zadd", &[b("z"), b("nan"), b("a")], Some(&b("1"))).is_none());
    }

    #[test]
    fn bzpop_tie_break_lex_smallest() {
        let s = ZSetState::default();
        let s = ZSetModel::step(
            &s,
            "zadd",
            &[b("z"), b("1"), b("a"), b("1"), b("b")],
            Some(&b("2")),
        )
        .unwrap();
        // BZPOPMIN: equal scores -> lexicographically smallest member (a) wins.
        assert!(ZSetModel::step(&s, "bzpopmin", &[b("z"), b("0")], Some(&b("z|a|1"))).is_some());
        assert!(ZSetModel::step(&s, "bzpopmin", &[b("z"), b("0")], Some(&b("z|b|1"))).is_none());
        // BZPOPMAX: per documented divergence, ties also resolve to the
        // lexicographically smallest member (a), not the largest.
        assert!(ZSetModel::step(&s, "bzpopmax", &[b("z"), b("0")], Some(&b("z|a|1"))).is_some());
        assert!(ZSetModel::step(&s, "bzpopmax", &[b("z"), b("0")], Some(&b("z|b|1"))).is_none());
    }

    #[test]
    fn zscore_fractional() {
        let s = ZSetState::default();
        let s = ZSetModel::step(&s, "zadd", &[b("z"), b("1.5"), b("a")], Some(&b("1"))).unwrap();
        assert!(ZSetModel::step(&s, "zscore", &[b("z"), b("a")], Some(&b("1.5"))).is_some());
    }

    #[test]
    fn bzpopmin_window_linearizable() {
        let mut h = History::new();
        let blk = h.invoke(2, "bzpopmin", vec![b("z"), b("0")]);
        let add = h.invoke(1, "zadd", vec![b("z"), b("1"), b("a")]);
        h.respond(add, Some(b("1")));
        h.respond(blk, Some(b("z|a|1")));
        assert!(check_linearizability::<ZSetModel>(&h).is_linearizable);
    }
}
