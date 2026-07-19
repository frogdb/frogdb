//! Whole-history conservation checkers (pure scans, not WGL-based).

use crate::history::History;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

/// A conservation-invariant violation. Carries the offending op id(s) and a
/// human-readable description (via the `Display`/`Error` message).
#[derive(Debug, thiserror::Error)]
pub enum ConservationViolation {
    /// An element was delivered (or left in final state) more times than pushed.
    #[error(
        "element {element:?} (pushed by op {pushed_by}) delivered {times} times, over-consumed"
    )]
    MultipleDelivery {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of a push that introduced it.
        pushed_by: u64,
        /// Observed delivery count.
        times: usize,
    },
    /// An element was pushed but neither delivered nor present at quiesce.
    #[error(
        "element {element:?} (pushed by op {pushed_by}) was neither delivered nor in final state"
    )]
    LostElement {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of the push that introduced it.
        pushed_by: u64,
    },
    /// An element was delivered that was never pushed.
    #[error("element {element:?} delivered by op {delivered_by} was never pushed")]
    PhantomDelivery {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of the delivering pop.
        delivered_by: u64,
    },
    /// A transfer workload failed to conserve the sum over the tracked keys.
    #[error("transaction sum not conserved on {keys:?}: expected {expected}, computed {computed}")]
    SumMismatch {
        /// The tracked keys.
        keys: Vec<String>,
        /// The invariant target sum.
        expected: i64,
        /// The sum implied by the recorded history.
        computed: i64,
    },
    /// A committed EXEC ignored a concurrent write to a watched key.
    #[error(
        "watch false-negative: exec op {exec_op} committed though op {writer_op} wrote watched key {key:?} after watch op {watch_op}"
    )]
    WatchFalseNegative {
        /// The committed exec op id.
        exec_op: u64,
        /// The watch op id.
        watch_op: u64,
        /// The interfering writer op id.
        writer_op: u64,
        /// The watched key.
        key: Vec<u8>,
    },
    /// Blocked poppers on a key were not served in registration (invoke) order.
    #[error(
        "FIFO wake order violated on key {key:?}: op {served} (later waiter) served before op {waiter}"
    )]
    FifoViolation {
        /// The key.
        key: Vec<u8>,
        /// The op served out of order.
        served: u64,
        /// The earlier waiter it jumped ahead of.
        waiter: u64,
    },
}

/// Every pushed element is delivered to exactly one popper XOR present at
/// quiesce; no element delivered twice or lost. `final_elements` maps each key
/// to the elements remaining in its list after the workload drains.
pub fn check_exactly_once_delivery(
    history: &History,
    final_elements: &HashMap<Bytes, Vec<Bytes>>,
) -> Result<(), ConservationViolation> {
    fn record_push(
        value: Bytes,
        op: u64,
        pushed: &mut HashMap<Bytes, i64>,
        push_op: &mut HashMap<Bytes, u64>,
    ) {
        *pushed.entry(value.clone()).or_default() += 1;
        push_op.entry(value).or_insert(op);
    }

    let mut pushed: HashMap<Bytes, i64> = HashMap::new();
    let mut push_op: HashMap<Bytes, u64> = HashMap::new();
    let mut delivered: HashMap<Bytes, (i64, u64)> = HashMap::new(); // count, last op id

    for op in history.completed_operations() {
        match op.function.as_str() {
            "lpush" | "rpush" => {
                for v in op.args.iter().skip(1) {
                    record_push(v.clone(), op.id, &mut pushed, &mut push_op);
                }
            }
            "lpop" | "rpop" => {
                if let Some(r) = &op.result {
                    let e = delivered.entry(r.clone()).or_insert((0, op.id));
                    e.0 += 1;
                    e.1 = op.id;
                }
            }
            "blpop" | "brpop" => {
                if let Some(r) = &op.result
                    && let Some((_, elem)) = String::from_utf8_lossy(r).split_once('|')
                {
                    let key = Bytes::from(elem.to_string());
                    let e = delivered.entry(key).or_insert((0, op.id));
                    e.0 += 1;
                    e.1 = op.id;
                }
            }
            "lmove" | "blmove" => {
                if let Some(r) = &op.result {
                    // Counts as both a delivery (from src) and a push (to dst).
                    let e = delivered.entry(r.clone()).or_insert((0, op.id));
                    e.0 += 1;
                    e.1 = op.id;
                    record_push(r.clone(), op.id, &mut pushed, &mut push_op);
                }
            }
            _ => {}
        }
    }

    let mut final_counts: HashMap<Bytes, i64> = HashMap::new();
    for elems in final_elements.values() {
        for e in elems {
            *final_counts.entry(e.clone()).or_default() += 1;
        }
    }

    let mut values: HashSet<Bytes> = HashSet::new();
    values.extend(pushed.keys().cloned());
    values.extend(delivered.keys().cloned());
    values.extend(final_counts.keys().cloned());

    for v in values {
        let p = pushed.get(&v).copied().unwrap_or(0);
        let (d, last_op) = delivered.get(&v).copied().unwrap_or((0, 0));
        let f = final_counts.get(&v).copied().unwrap_or(0);
        if p == 0 && d > 0 {
            return Err(ConservationViolation::PhantomDelivery {
                element: v.to_vec(),
                delivered_by: last_op,
            });
        }
        if d + f > p {
            return Err(ConservationViolation::MultipleDelivery {
                element: v.to_vec(),
                pushed_by: push_op.get(&v).copied().unwrap_or(0),
                times: d as usize,
            });
        }
        if d + f < p {
            return Err(ConservationViolation::LostElement {
                element: v.to_vec(),
                pushed_by: push_op.get(&v).copied().unwrap_or(0),
            });
        }
    }
    Ok(())
}

/// Blocked poppers (BLPOP/BRPOP hits) on a key are served in invoke order.
pub fn check_fifo_wake_order(history: &History) -> Result<(), ConservationViolation> {
    // key -> [(invoke_time, return_time, op_id)] for served blocking pops.
    let mut by_key: HashMap<Bytes, Vec<(u64, u64, u64)>> = HashMap::new();
    for op in history.completed_operations() {
        if matches!(op.function.as_str(), "blpop" | "brpop")
            && op.result.is_some()
            && let Some(key) = op.args.first()
        {
            by_key
                .entry(key.clone())
                .or_default()
                .push((op.invoke_time, op.return_time, op.id));
        }
    }
    for (key, mut served) in by_key {
        served.sort_by_key(|x| x.1); // by serve (return) order
        for w in served.windows(2) {
            if w[0].0 > w[1].0 {
                // Served earlier but invoked later -> jumped an earlier waiter.
                return Err(ConservationViolation::FifoViolation {
                    key: key.to_vec(),
                    served: w[0].2,
                    waiter: w[1].2,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::History;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn push_pop_history() -> History {
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

    #[test]
    fn delivery_ok_when_all_consumed() {
        let h = push_pop_history();
        assert!(check_exactly_once_delivery(&h, &HashMap::new()).is_ok());
    }

    #[test]
    fn delivery_ok_with_leftover_in_final_state() {
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![b("k"), b("x")]);
        h.respond(p, Some(b("1")));
        let mut final_state = HashMap::new();
        final_state.insert(b("k"), vec![b("x")]);
        assert!(check_exactly_once_delivery(&h, &final_state).is_ok());
    }

    #[test]
    fn delivery_detects_double_pop() {
        let mut h = push_pop_history();
        // A second, illegal delivery of "a".
        let q = h.invoke(3, "lpop", vec![b("k")]);
        h.respond(q, Some(b("a")));
        assert!(matches!(
            check_exactly_once_delivery(&h, &HashMap::new()),
            Err(ConservationViolation::MultipleDelivery { .. })
        ));
    }

    #[test]
    fn delivery_detects_lost_element() {
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(p, Some(b("1")));
        // Never delivered, not in final state -> lost.
        assert!(matches!(
            check_exactly_once_delivery(&h, &HashMap::new()),
            Err(ConservationViolation::LostElement { .. })
        ));
    }

    #[test]
    fn fifo_ok_when_served_in_invoke_order() {
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(w1, Some(b("k|a")));
        h.respond(w2, Some(b("k|b")));
        assert!(check_fifo_wake_order(&h).is_ok());
    }

    #[test]
    fn fifo_detects_out_of_order_wake() {
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        // w2 (later waiter) served before w1 (earlier waiter).
        h.respond(w2, Some(b("k|b")));
        h.respond(w1, Some(b("k|a")));
        assert!(matches!(
            check_fifo_wake_order(&h),
            Err(ConservationViolation::FifoViolation { .. })
        ));
    }
}
