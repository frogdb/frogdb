//! The invariant pipeline: runs the Phase-1 checkers over a recorded
//! `History`, applying the WGL scaling guard (bounded state search) and the
//! inconclusive-downgrade (a key WGL bails on becomes conservation-only, never
//! a silent pass), then the conservation checkers, then an optional quiescence
//! stage that is compiled but skipped while the Phase-2 DEBUG probes are absent.

#![allow(dead_code)]

use std::collections::HashMap;

use bytes::Bytes;
use frogdb_testing::{
    HashModel, History, KVModel, ListModel, StreamModel, ZSetModel, check_exactly_once_delivery,
    check_fifo_wake_order, check_linearizability_bounded, check_watch_no_false_negative,
    default_keys_of, partition_by_key,
};

/// Per-key op cap before WGL is skipped (conservation still covers the key).
pub const MAX_OPS_PER_KEY: usize = 200;
/// State-search bound for the bounded WGL checker.
pub const MAX_WGL_STATES: u64 = 200_000;

/// Structured verdict from the pipeline. `passed()` iff `violations` is empty.
#[derive(Debug, Default)]
pub struct InvariantReport {
    /// Human-readable violations; empty means pass.
    pub violations: Vec<String>,
    /// Keys WGL bailed on (inconclusive or over the op cap) → conservation-only.
    pub downgraded_keys: Vec<String>,
    /// False while the Phase-2 DEBUG probes are absent.
    pub quiescence_checked: bool,
}

impl InvariantReport {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }
}

/// The Phase-1 model family a per-key sub-history belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    Kv,
    List,
    Hash,
    ZSet,
    Stream,
}

/// Map a (projected) command name to its model family.
fn family_of(function: &str) -> Option<Family> {
    match function {
        "set" | "get" | "del" | "incr" | "mset" | "mget" | "watch" | "exec" | "cas" | "read"
        | "write" => Some(Family::Kv),
        "lpush" | "rpush" | "lpop" | "rpop" | "lmove" | "llen" | "lrange" | "blpop" | "brpop"
        | "blmove" => Some(Family::List),
        "hset" | "hdel" | "hget" | "hincrby" | "hgetall" | "hlen" => Some(Family::Hash),
        "zadd" | "zrem" | "zscore" | "zcard" | "bzpopmin" | "bzpopmax" => Some(Family::ZSet),
        "xadd" | "xlen" | "xread" => Some(Family::Stream),
        _ => None,
    }
}

/// Decide a sub-history's family from its first completed op (keys are
/// single-type under the generator).
fn model_for(sub: &History) -> Option<Family> {
    sub.completed_operations()
        .first()
        .and_then(|op| family_of(&op.function))
}

/// Run the full invariant pipeline with default bounds.
pub fn check_all(
    history: &History,
    final_elements: &HashMap<Bytes, Vec<Bytes>>,
) -> InvariantReport {
    check_all_with(history, final_elements, MAX_OPS_PER_KEY, MAX_WGL_STATES)
}

/// Pipeline with explicit bounds (tests use a tiny `max_states` to force the
/// inconclusive-downgrade path).
pub fn check_all_with(
    history: &History,
    final_elements: &HashMap<Bytes, Vec<Bytes>>,
    max_ops_per_key: usize,
    max_states: u64,
) -> InvariantReport {
    let mut report = InvariantReport::default();

    // Stage 1: response legality (cheap, always).
    check_response_legality(history, &mut report);

    // Stage 2: per-key partition + bounded WGL with inconclusive-downgrade.
    let partitions = partition_by_key(history, default_keys_of);
    for (key, sub) in &partitions {
        let key_str = String::from_utf8_lossy(key).to_string();
        let Some(family) = model_for(sub) else {
            continue; // no routable/completed op for this key
        };
        if sub.completed_operations().len() > max_ops_per_key {
            // Over the scaling cap: skip WGL, conservation still covers it.
            eprintln!(
                "WGL cap: key {key_str} has >{max_ops_per_key} ops; \
                 downgraded to conservation-only"
            );
            report.downgraded_keys.push(key_str);
            continue;
        }
        let result = run_bounded(family, sub, max_states);
        if result.inconclusive {
            eprintln!(
                "WGL inconclusive for key {key_str} (state bound {max_states} hit); \
                 downgraded to conservation-only"
            );
            report.downgraded_keys.push(key_str);
        } else if !result.is_linearizable {
            report.violations.push(format!(
                "key {key_str} ({family:?}) not linearizable (problematic ops {:?})",
                result.problematic_ops
            ));
        }
    }

    // Stage 3: conservation (whole history).
    if let Err(e) = check_exactly_once_delivery(history, final_elements) {
        report
            .violations
            .push(format!("exactly-once delivery: {e}"));
    }
    if let Err(e) = check_fifo_wake_order(history) {
        report.violations.push(format!("FIFO wake order: {e}"));
    }
    if let Err(e) = check_watch_no_false_negative(history) {
        report.violations.push(format!("WATCH false-negative: {e}"));
    }

    // Stage 4: quiescence (optional; skipped while Phase-2 probes are absent).
    if quiescence_available() {
        report.quiescence_checked = true;
        // Phase-2 DEBUG LOCKTABLE/WAITQUEUE/MEMORY-CHECK/EXPIRY-INDEX-CHECK go
        // here once they land; asserting empty/consistent state at quiesce.
    } else {
        report.quiescence_checked = false;
        eprintln!("quiescence skipped (phase 2 DEBUG probes absent)");
    }

    report
}

/// Whether the Phase-2 DEBUG quiescence probes have landed. Flip to `true`
/// (with the probe calls in stage 4) once they exist — no restructuring needed.
fn quiescence_available() -> bool {
    false
}

/// Dispatch the bounded linearizability check to the family's model.
fn run_bounded(
    family: Family,
    sub: &History,
    max_states: u64,
) -> frogdb_testing::LinearizabilityResult {
    match family {
        Family::Kv => check_linearizability_bounded::<KVModel>(sub, max_states),
        Family::List => check_linearizability_bounded::<ListModel>(sub, max_states),
        Family::Hash => check_linearizability_bounded::<HashModel>(sub, max_states),
        Family::ZSet => check_linearizability_bounded::<ZSetModel>(sub, max_states),
        Family::Stream => check_linearizability_bounded::<StreamModel>(sub, max_states),
    }
}

/// Commands whose reply, when present, must be a base-10 integer.
const INT_REPLY: &[&str] = &[
    "incr", "del", "llen", "lpush", "rpush", "hlen", "hdel", "hset", "hincrby", "zadd", "zrem",
    "zcard", "xlen",
];

/// Stage 1: scan completed ops for reply shapes that are illegal for their
/// command family (an integer-reply command returning a non-integer).
fn check_response_legality(history: &History, report: &mut InvariantReport) {
    for op in history.completed_operations() {
        if INT_REPLY.contains(&op.function.as_str())
            && let Some(r) = &op.result
            && std::str::from_utf8(r)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .is_none()
        {
            report.violations.push(format!(
                "op {} ({}) returned non-integer reply {:?}",
                op.id,
                op.function,
                String::from_utf8_lossy(r)
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_history_passes_and_dirty_history_flags() {
        // clean: SET x 1 ; GET x -> 1
        let mut h = History::new();
        let s = h.invoke(1, "set", vec![Bytes::from("{t}x"), Bytes::from("1")]);
        h.respond(s, Some(Bytes::from("OK")));
        let g = h.invoke(2, "get", vec![Bytes::from("{t}x")]);
        h.respond(g, Some(Bytes::from("1")));
        assert!(check_all(&h, &Default::default()).passed());

        // dirty: GET x -> 2 with no writer of 2 -> non-linearizable
        let mut d = History::new();
        let s = d.invoke(1, "set", vec![Bytes::from("{t}x"), Bytes::from("1")]);
        d.respond(s, Some(Bytes::from("OK")));
        let g = d.invoke(2, "get", vec![Bytes::from("{t}x")]);
        d.respond(g, Some(Bytes::from("2")));
        assert!(!check_all(&d, &Default::default()).passed());
    }

    #[test]
    fn inconclusive_key_downgrades_not_fails() {
        // Many writes on one key + a tiny state bound forces the checker to
        // bail (inconclusive). The key must land in downgraded_keys and NOT
        // produce a violation.
        let mut h = History::new();
        // Fully overlapping writes (all invoked before any responds) make the
        // search space large; a state bound of 1 forces the checker to bail.
        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(h.invoke(
                (i % 3) as u64,
                "set",
                vec![Bytes::from("{t}k"), Bytes::from(i.to_string())],
            ));
        }
        for id in ids {
            h.respond(id, Some(Bytes::from("OK")));
        }
        let report = check_all_with(&h, &Default::default(), MAX_OPS_PER_KEY, 1);
        assert!(
            report.passed(),
            "inconclusive must not fail: {:?}",
            report.violations
        );
        assert!(
            report.downgraded_keys.iter().any(|k| k == "{t}k"),
            "expected {{t}}k in downgraded_keys, got {:?}",
            report.downgraded_keys
        );
    }

    #[test]
    fn quiescence_is_skipped_while_probes_absent() {
        let h = History::new();
        let report = check_all(&h, &Default::default());
        assert!(!report.quiescence_checked);
    }

    // ---- Harness self-tests (silent-green guard) ----
    //
    // A deliberately broken shim must be caught by the pipeline, proving the
    // checkers are not silently passing everything.

    #[test]
    fn lost_element_is_flagged() {
        // Push a unique element, never deliver it, and leave final_elements
        // empty: exactly-once must report a LostElement violation.
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![Bytes::from("{t}L"), Bytes::from("zzz")]);
        h.respond(p, Some(Bytes::from("1")));
        let report = check_all(&h, &Default::default());
        assert!(!report.passed(), "a lost element must fail the report");
        assert!(
            report
                .violations
                .iter()
                .any(|v| v.contains("exactly-once") || v.to_lowercase().contains("lost")),
            "expected a lost-element violation, got {:?}",
            report.violations
        );
    }

    #[test]
    fn never_written_get_is_nonlinearizable() {
        // GET returns a value nobody wrote -> the WGL checker must flag it.
        let mut h = History::new();
        let g = h.invoke(1, "get", vec![Bytes::from("{t}g")]);
        h.respond(g, Some(Bytes::from("phantom")));
        let report = check_all(&h, &Default::default());
        assert!(
            !report.passed(),
            "a phantom read must fail the report: {:?}",
            report.violations
        );
    }

    #[test]
    fn assert_write_order_catches_out_of_order() {
        use frogdb_core::persistence::{FakeWalLog, RecordedWalEffect, WalEffectKind};
        let log = FakeWalLog::default();
        {
            let mut guard = log.0.lock().unwrap();
            guard.push(RecordedWalEffect {
                order: 5,
                kind: WalEffectKind::Set,
                key: Some(b"a".to_vec()),
                seq: 1,
            });
            guard.push(RecordedWalEffect {
                order: 2, // decreasing -> out of order
                kind: WalEffectKind::Set,
                key: Some(b"b".to_vec()),
                seq: 2,
            });
        }
        assert!(
            log.assert_write_order().is_err(),
            "an out-of-order WAL log must be rejected"
        );
    }
}
