//! List sequential model (LPUSH/RPUSH/LPOP/RPOP/LMOVE/LLEN/LRANGE + blocking).

use super::{Model, expect_int};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};

/// Sequential model for Redis lists.
#[derive(Debug, Clone, Default)]
pub struct ListModel;

/// State for the list model.
#[derive(Debug, Clone, Default)]
pub struct ListState {
    /// key -> ordered elements (front = head).
    pub lists: HashMap<Bytes, VecDeque<Bytes>>,
}

impl ListState {
    fn len(&self, key: &Bytes) -> usize {
        self.lists.get(key).map_or(0, VecDeque::len)
    }

    fn is_empty_list(&self, key: &Bytes) -> bool {
        self.len(key) == 0
    }

    /// Drop the key if its list became empty (keeps state canonical).
    fn prune(&mut self, key: &Bytes) {
        if self.lists.get(key).is_some_and(VecDeque::is_empty) {
            self.lists.remove(key);
        }
    }
}

fn side(arg: &Bytes) -> Option<bool> {
    // true = left/head, false = right/tail.
    match String::from_utf8_lossy(arg).to_lowercase().as_str() {
        "left" => Some(true),
        "right" => Some(false),
        _ => None,
    }
}

impl Model for ListModel {
    type State = ListState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "lpush" | "rpush" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let list = new.lists.entry(key.clone()).or_default();
                for v in &args[1..] {
                    if function == "lpush" {
                        list.push_front(v.clone());
                    } else {
                        list.push_back(v.clone());
                    }
                }
                let len = list.len() as i64;
                if expect_int(result, len) {
                    Some(new)
                } else {
                    None
                }
            }
            "lpop" | "rpop" => {
                if args.is_empty() {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let popped = new.lists.get_mut(key).and_then(|l| {
                    if function == "lpop" {
                        l.pop_front()
                    } else {
                        l.pop_back()
                    }
                });
                match popped {
                    Some(elem) => {
                        new.prune(key);
                        if result == Some(&elem) {
                            Some(new)
                        } else {
                            None
                        }
                    }
                    None => {
                        if result.is_none() {
                            Some(state.clone())
                        } else {
                            None
                        }
                    }
                }
            }
            "llen" => {
                if args.is_empty() {
                    return None;
                }
                if expect_int(result, state.len(&args[0]) as i64) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "lrange" => {
                if args.len() < 3 {
                    return None;
                }
                let key = &args[0];
                let start: i64 = String::from_utf8_lossy(&args[1]).parse().ok()?;
                let stop: i64 = String::from_utf8_lossy(&args[2]).parse().ok()?;
                let empty = VecDeque::new();
                let list = state.lists.get(key).unwrap_or(&empty);
                let len = list.len() as i64;
                let s = if start < 0 {
                    (len + start).max(0)
                } else {
                    start
                };
                let e_raw = if stop < 0 { len + stop } else { stop };
                let e = e_raw.min(len - 1);
                let mut elems: Vec<String> = Vec::new();
                if s <= e && s < len {
                    for idx in s..=e {
                        elems.push(String::from_utf8_lossy(&list[idx as usize]).to_string());
                    }
                }
                let expected = elems.join("|");
                match result {
                    Some(r) if String::from_utf8_lossy(r) == expected => Some(state.clone()),
                    None if expected.is_empty() => Some(state.clone()),
                    _ => None,
                }
            }
            "lmove" => {
                if args.len() < 4 {
                    return None;
                }
                let (src, dst) = (&args[0], &args[1]);
                let from = side(&args[2])?;
                let to = side(&args[3])?;
                let mut new = state.clone();
                let elem = new
                    .lists
                    .get_mut(src)
                    .and_then(|l| if from { l.pop_front() } else { l.pop_back() });
                match elem {
                    Some(e) => {
                        new.prune(src);
                        let d = new.lists.entry(dst.clone()).or_default();
                        if to {
                            d.push_front(e.clone());
                        } else {
                            d.push_back(e.clone());
                        }
                        if result == Some(&e) { Some(new) } else { None }
                    }
                    None => {
                        if result.is_none() {
                            Some(state.clone())
                        } else {
                            None
                        }
                    }
                }
            }
            "blpop" | "brpop" => {
                if args.len() < 2 {
                    return None;
                }
                let keys = &args[..args.len() - 1];
                let first = keys.iter().find(|k| !state.is_empty_list(k));
                match (first, result) {
                    (None, None) => Some(state.clone()),
                    (None, Some(_)) => None,
                    (Some(_), None) => None,
                    (Some(k), Some(r)) => {
                        let mut new = state.clone();
                        let elem = new.lists.get_mut(k).and_then(|l| {
                            if function == "blpop" {
                                l.pop_front()
                            } else {
                                l.pop_back()
                            }
                        })?;
                        new.prune(k);
                        let expected = format!(
                            "{}|{}",
                            String::from_utf8_lossy(k),
                            String::from_utf8_lossy(&elem)
                        );
                        if String::from_utf8_lossy(r) == expected {
                            Some(new)
                        } else {
                            None
                        }
                    }
                }
            }
            // Projection-only synthetic op (never on the wire): the
            // destination-side push half of a cross-key `lmove`/`blmove`, split
            // out by `partition::project_for_key`. Args are
            // `[dst_key, element, side]` where side is "L" (push to head) or "R"
            // (push to tail). It has no observable reply, so the recorded
            // `result` is ignored and the push always applies — its whole job is
            // to make the element the move deposited visible to later pops in the
            // destination key's sub-history. Without it, a subsequent pop of that
            // element would look like a phantom (element never pushed here) and
            // spuriously fail linearizability. See the `lmove`/`blmove` arm of
            // `partition::project_for_key` for the source-side (pop) half.
            "lmove_push" => {
                if args.len() < 3 {
                    return None;
                }
                let key = &args[0];
                let elem = args[1].clone();
                let to_left = String::from_utf8_lossy(&args[2]) == "L";
                let mut new = state.clone();
                let list = new.lists.entry(key.clone()).or_default();
                if to_left {
                    list.push_front(elem);
                } else {
                    list.push_back(elem);
                }
                Some(new)
            }
            "blmove" => {
                if args.len() < 5 {
                    return None;
                }
                let (src, dst) = (&args[0], &args[1]);
                let from = side(&args[2])?;
                let to = side(&args[3])?;
                if state.is_empty_list(src) {
                    return if result.is_none() {
                        Some(state.clone())
                    } else {
                        None
                    };
                }
                let mut new = state.clone();
                let elem = new
                    .lists
                    .get_mut(src)
                    .and_then(|l| if from { l.pop_front() } else { l.pop_back() })?;
                new.prune(src);
                let d = new.lists.entry(dst.clone()).or_default();
                if to {
                    d.push_front(elem.clone());
                } else {
                    d.push_back(elem.clone());
                }
                if result == Some(&elem) {
                    Some(new)
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
    fn list_happy_path() {
        let s = ListState::default();
        // RPUSH k a  -> len 1
        let s = ListModel::step(&s, "rpush", &[b("k"), b("a")], Some(&b("1"))).unwrap();
        // RPUSH k b  -> len 2
        let s = ListModel::step(&s, "rpush", &[b("k"), b("b")], Some(&b("2"))).unwrap();
        // LLEN k -> 2
        assert!(ListModel::step(&s, "llen", &[b("k")], Some(&b("2"))).is_some());
        // LRANGE k 0 -1 -> a|b
        assert!(
            ListModel::step(&s, "lrange", &[b("k"), b("0"), b("-1")], Some(&b("a|b"))).is_some()
        );
        // LPOP k -> a
        let s = ListModel::step(&s, "lpop", &[b("k")], Some(&b("a"))).unwrap();
        // RPOP k -> b
        let s = ListModel::step(&s, "rpop", &[b("k")], Some(&b("b"))).unwrap();
        // LPOP empty -> nil
        assert!(ListModel::step(&s, "lpop", &[b("k")], None).is_some());
    }

    #[test]
    fn list_wrong_pop_rejected() {
        let s = ListState::default();
        let s = ListModel::step(&s, "rpush", &[b("k"), b("a")], Some(&b("1"))).unwrap();
        // Popping a value that isn't the head is illegal.
        assert!(ListModel::step(&s, "lpop", &[b("k")], Some(&b("z"))).is_none());
    }

    #[test]
    fn list_lmove() {
        let s = ListState::default();
        let s = ListModel::step(&s, "rpush", &[b("src"), b("x")], Some(&b("1"))).unwrap();
        // LMOVE src dst left right -> x
        let s = ListModel::step(
            &s,
            "lmove",
            &[b("src"), b("dst"), b("left"), b("right")],
            Some(&b("x")),
        )
        .unwrap();
        assert!(ListModel::step(&s, "llen", &[b("src")], Some(&b("0"))).is_some());
        assert!(ListModel::step(&s, "lpop", &[b("dst")], Some(&b("x"))).is_some());
    }

    #[test]
    fn list_strict_rejects_unknown() {
        let s = ListState::default();
        assert!(ListModel::step(&s, "sadd", &[b("k"), b("v")], Some(&b("1"))).is_none());
    }

    #[test]
    fn blpop_timeout_legal_when_empty() {
        let s = ListState::default();
        // Empty list -> BLPOP may time out (nil).
        assert!(ListModel::step(&s, "blpop", &[b("k"), b("0")], None).is_some());
        // Empty list -> BLPOP cannot return an element.
        assert!(ListModel::step(&s, "blpop", &[b("k"), b("0")], Some(&b("k|a"))).is_none());
    }

    #[test]
    fn blpop_hit_legal_when_head_matches() {
        let s = ListState::default();
        let s = ListModel::step(&s, "rpush", &[b("k"), b("a")], Some(&b("1"))).unwrap();
        // Non-empty list -> BLPOP must serve the head, and cannot time out here.
        assert!(ListModel::step(&s, "blpop", &[b("k"), b("0")], Some(&b("k|a"))).is_some());
        assert!(ListModel::step(&s, "blpop", &[b("k"), b("0")], None).is_none());
    }

    #[test]
    fn blpop_window_linearizable() {
        // A BLPOP whose window overlaps the RPUSH that supplies its element
        // is linearizable (linearization point after the push).
        let mut h = History::new();
        let blk = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        let push = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(push, Some(b("1")));
        h.respond(blk, Some(b("k|a")));
        let r = check_linearizability::<ListModel>(&h);
        assert!(r.is_linearizable);
    }

    #[test]
    fn blpop_timeout_with_element_present_not_linearizable() {
        // Element pushed and never removed, yet a fully-concurrent-after BLPOP
        // times out: no linearization point leaves the list empty for it.
        let mut h = History::new();
        let push = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(push, Some(b("1")));
        let blk = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(blk, None); // timeout despite a present element
        let r = check_linearizability::<ListModel>(&h);
        assert!(!r.is_linearizable);
    }

    #[test]
    fn lmove_push_tail_then_pop_fifo() {
        // Synthetic destination-push (side "R" = tail) makes the deposited
        // element visible: RPUSH x, lmove_push y (tail), then LPOP twice yields
        // x, y in FIFO order.
        let s = ListState::default();
        let s = ListModel::step(&s, "rpush", &[b("k"), b("x")], Some(&b("1"))).unwrap();
        let s = ListModel::step(&s, "lmove_push", &[b("k"), b("y"), b("R")], None).unwrap();
        let s = ListModel::step(&s, "lpop", &[b("k")], Some(&b("x"))).unwrap();
        assert!(ListModel::step(&s, "lpop", &[b("k")], Some(&b("y"))).is_some());
    }

    #[test]
    fn lmove_push_head_places_at_front() {
        // Side "L" = head: the element becomes the new head.
        let s = ListState::default();
        let s = ListModel::step(&s, "rpush", &[b("k"), b("x")], Some(&b("1"))).unwrap();
        let s = ListModel::step(&s, "lmove_push", &[b("k"), b("y"), b("L")], None).unwrap();
        assert!(ListModel::step(&s, "lpop", &[b("k")], Some(&b("y"))).is_some());
    }

    use proptest::prelude::*;

    proptest! {
        /// RPUSH-ing then LPOP-ing every element in FIFO order always stays
        /// consistent with a VecDeque reference implementation.
        #[test]
        fn rpush_then_lpop_matches_reference(vals in proptest::collection::vec("[a-z]{1,3}", 0..12)) {
            let mut state = ListState::default();
            let mut reference: std::collections::VecDeque<Bytes> = std::collections::VecDeque::new();
            for (i, v) in vals.iter().enumerate() {
                let vb = Bytes::from(v.clone());
                reference.push_back(vb.clone());
                let len = (i + 1) as i64;
                state = ListModel::step(&state, "rpush", &[b("k"), vb], Some(&Bytes::from(len.to_string())))
                    .expect("rpush must apply");
            }
            while let Some(expected) = reference.pop_front() {
                state = ListModel::step(&state, "lpop", &[b("k")], Some(&expected))
                    .expect("lpop must match FIFO head");
            }
            prop_assert!(ListModel::step(&state, "lpop", &[b("k")], None).is_some());
        }
    }
}
