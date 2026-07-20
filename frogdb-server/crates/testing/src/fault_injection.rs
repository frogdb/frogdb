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

/// Corruption: remove the first push entirely (a lost write). The pop that
/// later delivers the removed element's value now has no matching push, so
/// it registers as a phantom delivery. Trips `check_exactly_once_delivery`
/// (PhantomDelivery).
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

/// Corruption: rewrite an XPENDING summary so its degenerate single-id range
/// (`min == max`) reports a total greater than 1 (that one id owned by two
/// consumers). Trips `check_pel_conservation` (PelDoubleOwned).
pub fn double_own_pel(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    for o in recs.iter_mut() {
        if o.kind == OpKind::Return
            && o.function == "xpending"
            && let Some(r) = &o.result
        {
            let s = String::from_utf8_lossy(r).to_string();
            if s != "0" {
                let f: Vec<&str> = s.split('|').collect();
                if f.len() >= 4 {
                    // total=2 but the range is still a single id ("min"),
                    // owned by c1:1 AND c2:1.
                    let joined = format!("2|{}|{}|c1:1,c2:1", f[1], f[2]);
                    o.result = Some(bytes::Bytes::from(joined));
                    break;
                }
            }
        }
    }
    rebuild(&recs)
}

/// Corruption: remove the FIRST delivered entry from the first `>` xreadgroup
/// result that delivered two or more entries. The dropped id is then never
/// delivered/PEL'd/acked while the same read shows a higher id — trips
/// `check_pel_conservation` (StreamEntryLost).
pub fn lose_stream_entry(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    for o in recs.iter_mut() {
        if o.kind == OpKind::Return
            && o.function == "xreadgroup"
            && let Some(r) = &o.result
        {
            let s = String::from_utf8_lossy(r).to_string();
            if let Some((_, rest)) = s.split_once('|') {
                o.result = Some(bytes::Bytes::from(rest.to_string()));
                break;
            }
        }
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
    use crate::conservation::{
        ConservationViolation, check_exactly_once_delivery, check_fifo_wake_order,
    };
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
        assert!(matches!(
            check_exactly_once_delivery(&corrupt, &HashMap::new()),
            Err(ConservationViolation::LostElement { .. })
        ));
    }

    #[test]
    fn duplicate_delivery_is_caught() {
        let corrupt = duplicate_delivery(&valid_delivery_history());
        assert!(matches!(
            check_exactly_once_delivery(&corrupt, &HashMap::new()),
            Err(ConservationViolation::MultipleDelivery { .. })
        ));
    }

    #[test]
    fn lose_element_is_caught() {
        let corrupt = lose_element(&valid_delivery_history());
        assert!(matches!(
            check_exactly_once_delivery(&corrupt, &HashMap::new()),
            Err(ConservationViolation::PhantomDelivery { .. })
        ));
    }

    #[test]
    fn original_fifo_history_passes() {
        assert!(check_fifo_wake_order(&valid_fifo_history()).is_ok());
    }

    #[test]
    fn reorder_completions_is_caught() {
        let corrupt = reorder_completions(&valid_fifo_history());
        assert!(matches!(
            check_fifo_wake_order(&corrupt),
            Err(ConservationViolation::FifoViolation { .. })
        ));
    }

    #[test]
    fn reorder_completions_is_caught_by_exact_checker() {
        use crate::conservation::{WaiterRegistrationOrder, check_fifo_wake_order_exact};
        // Fixed registration order matching the original (legal) invoke/serve
        // order: client 1 registered before client 2.
        let mut order = WaiterRegistrationOrder::default();
        order.insert(b("k"), 1, 1);
        order.insert(b("k"), 2, 2);

        let original = valid_fifo_history();
        assert!(check_fifo_wake_order_exact(&original, &order).is_ok());

        // Swap the serve order (client 2 served before client 1) without
        // changing the registration order fixture: client 2 (registered
        // second) is now served before client 1 (registered first) -> a FIFO
        // violation. This particular swap would also be caught by the
        // invoke-proxy checker; the point here is to exercise the exact
        // checker's ordinal-comparison path end to end. The discriminating
        // case where registration order and invoke order *disagree* — and only
        // the exact checker can see it — lives in
        // `conservation::tests::exact_fifo_uses_registration_order_not_invoke_order`.
        let corrupt = reorder_completions(&original);
        assert!(matches!(
            check_fifo_wake_order_exact(&corrupt, &order),
            Err(ConservationViolation::FifoViolation { .. })
        ));
    }

    fn valid_group_history() -> History {
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a, Some(b("1-1")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        let r = h.invoke(
            2,
            "xreadgroup",
            vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b(">")],
        );
        h.respond(r, Some(b("1-1,f,v")));
        let ack = h.invoke(2, "xack", vec![b("st"), b("g"), b("1-1")]);
        h.respond(ack, Some(b("1")));
        h
    }

    #[test]
    fn original_group_history_passes() {
        use crate::conservation::check_pel_conservation;
        assert!(check_pel_conservation(&valid_group_history()).is_ok());
    }

    #[test]
    fn double_own_pel_is_caught() {
        use crate::conservation::{ConservationViolation, check_pel_conservation};
        // Add an xpending observation to corrupt.
        let mut h = valid_group_history();
        let p = h.invoke(2, "xpending", vec![b("st"), b("g")]);
        h.respond(p, Some(b("1|1-1|1-1|c1:1")));
        let corrupt = double_own_pel(&h);
        assert!(matches!(
            check_pel_conservation(&corrupt),
            Err(ConservationViolation::PelDoubleOwned { .. })
        ));
    }

    #[test]
    fn lose_stream_entry_is_caught() {
        use crate::conservation::{ConservationViolation, check_pel_conservation};
        // Two entries delivered in one '>' read; dropping the first from the
        // reply makes it a skipped (lost) entry.
        let mut h = History::new();
        let a1 = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a1, Some(b("1-1")));
        let a2 = h.invoke(1, "xadd", vec![b("st"), b("2-1"), b("f"), b("w")]);
        h.respond(a2, Some(b("2-1")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        let r = h.invoke(
            2,
            "xreadgroup",
            vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b(">")],
        );
        h.respond(r, Some(b("1-1,f,v|2-1,f,w")));
        assert!(check_pel_conservation(&h).is_ok());
        let corrupt = lose_stream_entry(&h);
        assert!(matches!(
            check_pel_conservation(&corrupt),
            Err(ConservationViolation::StreamEntryLost { .. })
        ));
    }
}
