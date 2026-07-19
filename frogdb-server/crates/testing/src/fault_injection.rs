//! History-corruption helpers used by checker self-tests.
//!
//! A correct checker must reject each corruption and accept the original.

use crate::history::{History, OpKind, Operation};
use std::collections::HashMap;

fn is_pop(function: &str) -> bool {
    matches!(function, "lpop" | "rpop" | "blpop" | "brpop")
}

/// Rebuild a history from raw records, replaying invoke/return in list order so
/// timestamps follow the (possibly reordered) record sequence.
fn rebuild(records: &[Operation]) -> History {
    let mut h = History::new();
    let mut idmap: HashMap<u64, u64> = HashMap::new();
    for r in records {
        match r.kind {
            OpKind::Invoke => {
                let nid = h.invoke(r.client_id, r.function.clone(), r.args.clone());
                idmap.insert(r.id, nid);
            }
            OpKind::Return => {
                if let Some(nid) = idmap.get(&r.id) {
                    h.respond(*nid, r.result.clone());
                }
            }
        }
    }
    h
}

/// Corruption: drop the first successful pop entirely (an element that was
/// delivered now vanishes). Trips `check_exactly_once_delivery` (LostElement).
pub fn drop_delivery(history: &History) -> History {
    let target = history
        .completed_operations()
        .into_iter()
        .find(|op| is_pop(&op.function) && op.result.is_some())
        .map(|op| op.id);
    let mut recs: Vec<Operation> = history.operations().to_vec();
    if let Some(id) = target {
        recs.retain(|o| o.id != id);
    }
    rebuild(&recs)
}

/// Corruption: duplicate the first successful pop (double delivery). Trips
/// `check_exactly_once_delivery` (MultipleDelivery / PhantomDelivery).
pub fn duplicate_delivery(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    let inv = recs
        .iter()
        .find(|o| matches!(o.kind, OpKind::Invoke) && is_pop(&o.function))
        .cloned();
    if let Some(inv) = inv {
        let ret = recs
            .iter()
            .find(|o| o.id == inv.id && matches!(o.kind, OpKind::Return) && o.result.is_some())
            .cloned();
        if let Some(ret) = ret {
            let mut dinv = inv.clone();
            dinv.id = u64::MAX;
            let mut dret = ret.clone();
            dret.id = u64::MAX;
            recs.push(dinv);
            recs.push(dret);
        }
    }
    rebuild(&recs)
}

/// Corruption: remove the first push entirely (a lost write). The element it
/// introduced either never existed for its later delivery (PhantomDelivery) or
/// unbalances the counts. Trips `check_exactly_once_delivery`.
pub fn lose_element(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    let target = recs
        .iter()
        .find(|o| {
            matches!(o.kind, OpKind::Invoke) && matches!(o.function.as_str(), "lpush" | "rpush")
        })
        .map(|o| o.id);
    if let Some(id) = target {
        recs.retain(|o| o.id != id);
    }
    rebuild(&recs)
}

/// Corruption: swap the serve order of the two earliest blocking-pop hits so a
/// later waiter returns first. Trips `check_fifo_wake_order` (FifoViolation).
pub fn reorder_completions(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    let idxs: Vec<usize> = recs
        .iter()
        .enumerate()
        .filter(|(_, o)| {
            matches!(o.kind, OpKind::Return)
                && matches!(o.function.as_str(), "blpop" | "brpop")
                && o.result.is_some()
        })
        .map(|(i, _)| i)
        .take(2)
        .collect();
    if idxs.len() == 2 {
        recs.swap(idxs[0], idxs[1]);
    }
    rebuild(&recs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conservation::{check_exactly_once_delivery, check_fifo_wake_order};
    use crate::history::History;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn valid_delivery_history() -> History {
        let mut h = History::new();
        let p1 = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(p1, Some(b("1")));
        let p2 = h.invoke(1, "rpush", vec![b("k"), b("b")]);
        h.respond(p2, Some(b("2")));
        let q1 = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q1, Some(b("a")));
        let q2 = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q2, Some(b("b")));
        h
    }

    fn valid_fifo_history() -> History {
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(w1, Some(b("k|a")));
        h.respond(w2, Some(b("k|b")));
        h
    }

    #[test]
    fn original_delivery_history_passes() {
        assert!(check_exactly_once_delivery(&valid_delivery_history(), &HashMap::new()).is_ok());
    }

    #[test]
    fn drop_delivery_is_caught() {
        let corrupt = drop_delivery(&valid_delivery_history());
        assert!(check_exactly_once_delivery(&corrupt, &HashMap::new()).is_err());
    }

    #[test]
    fn duplicate_delivery_is_caught() {
        let corrupt = duplicate_delivery(&valid_delivery_history());
        assert!(check_exactly_once_delivery(&corrupt, &HashMap::new()).is_err());
    }

    #[test]
    fn lose_element_is_caught() {
        let corrupt = lose_element(&valid_delivery_history());
        assert!(check_exactly_once_delivery(&corrupt, &HashMap::new()).is_err());
    }

    #[test]
    fn original_fifo_history_passes() {
        assert!(check_fifo_wake_order(&valid_fifo_history()).is_ok());
    }

    #[test]
    fn reorder_completions_is_caught() {
        let corrupt = reorder_completions(&valid_fifo_history());
        assert!(check_fifo_wake_order(&corrupt).is_err());
    }
}
