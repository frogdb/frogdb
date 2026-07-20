//! Tier-4 quiescence adapter: turn the RESP replies of the four Phase-2 DEBUG
//! introspection commands into `frogdb_testing` snapshot structs, and run the
//! quiescence checkers over them.
//!
//! The sim clients speak RESP2 (they never `HELLO 3`), so every `DEBUG`
//! `Response::Map` arrives flattened to a `*`-array of alternating `key, value`
//! elements (see `protocol::response::to_resp2_frame`). The workload runner's
//! recursive RESP parser decodes that into an [`OperationResult`] tree; these
//! pure functions walk that tree. LOCKTABLE / WAITQUEUE / EXPIRY-INDEX-CHECK
//! reply with a sentinel bulk string (`# … is empty/consistent`) when every
//! shard is clean — that parses to zero snapshots, which the checkers accept.
//! MEMORY-CHECK always replies with a per-shard map.

#![allow(dead_code)]

use frogdb_testing::{
    ExpiryIndexSnapshot, LockTableSnapshot, MemoryCheckSnapshot, QuiescenceViolation,
    WaitQueueSnapshot, check_expiry_index_consistent, check_locktable_empty,
    check_memory_accounting, check_waitqueue_empty,
};

use super::sim_harness::OperationResult;

/// The four parsed DEBUG-reply snapshot sets gathered once the server quiesces.
#[derive(Debug, Default, Clone)]
pub struct QuiescenceSnapshots {
    pub lock_table: Vec<LockTableSnapshot>,
    pub wait_queue: Vec<WaitQueueSnapshot>,
    pub memory: Vec<MemoryCheckSnapshot>,
    pub expiry_index: Vec<ExpiryIndexSnapshot>,
}

impl QuiescenceSnapshots {
    /// Build the snapshot bundle from the four raw DEBUG replies (in the fixed
    /// LOCKTABLE / WAITQUEUE / MEMORY-CHECK / EXPIRY-INDEX-CHECK order).
    pub fn from_replies(
        locktable: &OperationResult,
        waitqueue: &OperationResult,
        memory: &OperationResult,
        expiry: &OperationResult,
    ) -> Self {
        Self {
            lock_table: parse_locktable(locktable),
            wait_queue: parse_waitqueue(waitqueue),
            memory: parse_memory_check(memory),
            expiry_index: parse_expiry_index(expiry),
        }
    }
}

/// Run the four tier-4 quiescence checkers over the gathered snapshots,
/// returning one human-readable string per violated invariant (empty = clean).
pub fn check_quiescence(snap: &QuiescenceSnapshots) -> Vec<String> {
    let mut violations = Vec::new();
    let mut record = |r: Result<(), QuiescenceViolation>| {
        if let Err(e) = r {
            violations.push(format!("quiescence: {e}"));
        }
    };
    record(check_locktable_empty(&snap.lock_table));
    record(check_waitqueue_empty(&snap.wait_queue));
    record(check_memory_accounting(&snap.memory));
    record(check_expiry_index_consistent(&snap.expiry_index));
    violations
}

/// Parse `shard:<id>` → `<id>`.
fn shard_id_of(r: &OperationResult) -> Option<usize> {
    match r {
        OperationResult::String(b) => std::str::from_utf8(b)
            .ok()?
            .strip_prefix("shard:")?
            .parse()
            .ok(),
        _ => None,
    }
}

/// Look up a value by field name in a RESP2-flattened map (`[k, v, k, v, …]`).
fn field<'a>(map: &'a OperationResult, name: &str) -> Option<&'a OperationResult> {
    let OperationResult::Array(items) = map else {
        return None;
    };
    items.chunks(2).find_map(|c| match c {
        [OperationResult::String(k), v] if k.as_ref() == name.as_bytes() => Some(v),
        _ => None,
    })
}

fn as_int(r: &OperationResult) -> Option<i64> {
    match r {
        OperationResult::Integer(n) => Some(*n),
        _ => None,
    }
}

fn as_array(r: &OperationResult) -> Option<&[OperationResult]> {
    match r {
        OperationResult::Array(a) => Some(a.as_slice()),
        _ => None,
    }
}

/// Iterate a RESP2-flattened top-level `shard:<id> → detail` map, yielding
/// `(shard_id, detail)`. A sentinel bulk string (`# …`) yields nothing.
fn shard_entries(reply: &OperationResult) -> Vec<(usize, &OperationResult)> {
    let OperationResult::Array(items) = reply else {
        return Vec::new();
    };
    items
        .chunks(2)
        .filter_map(|c| match c {
            [k, v] => shard_id_of(k).map(|id| (id, v)),
            _ => None,
        })
        .collect()
}

/// Parse a `DEBUG LOCKTABLE` reply. `intents` is an array of per-intent maps, so
/// its length is the intent-key count; `continuation_lock` is nil when unheld.
pub fn parse_locktable(reply: &OperationResult) -> Vec<LockTableSnapshot> {
    shard_entries(reply)
        .into_iter()
        .map(|(shard_id, detail)| LockTableSnapshot {
            shard_id,
            intent_key_count: field(detail, "intents")
                .and_then(as_array)
                .map_or(0, <[_]>::len),
            continuation_lock_held: matches!(
                field(detail, "continuation_lock"),
                Some(v) if !matches!(v, OperationResult::Nil)
            ),
        })
        .collect()
}

/// Parse a `DEBUG WAITQUEUE` reply, reading each shard's `total_waiters` count.
pub fn parse_waitqueue(reply: &OperationResult) -> Vec<WaitQueueSnapshot> {
    shard_entries(reply)
        .into_iter()
        .map(|(shard_id, detail)| WaitQueueSnapshot {
            shard_id,
            total_waiters: field(detail, "total_waiters")
                .and_then(as_int)
                .unwrap_or(0)
                .max(0) as usize,
        })
        .collect()
}

/// Parse a `DEBUG MEMORY-CHECK` reply (always a per-shard map, no sentinel).
pub fn parse_memory_check(reply: &OperationResult) -> Vec<MemoryCheckSnapshot> {
    shard_entries(reply)
        .into_iter()
        .map(|(shard_id, detail)| MemoryCheckSnapshot {
            shard_id,
            tracked_bytes: field(detail, "tracked_bytes")
                .and_then(as_int)
                .unwrap_or(0)
                .max(0) as u64,
            recomputed_bytes: field(detail, "recomputed_bytes")
                .and_then(as_int)
                .unwrap_or(0)
                .max(0) as u64,
        })
        .collect()
}

/// Parse a `DEBUG EXPIRY-INDEX-CHECK` reply. `anomalies` is an array of per-key
/// anomaly maps, so its length is the anomaly count.
pub fn parse_expiry_index(reply: &OperationResult) -> Vec<ExpiryIndexSnapshot> {
    shard_entries(reply)
        .into_iter()
        .map(|(shard_id, detail)| ExpiryIndexSnapshot {
            shard_id,
            anomaly_count: field(detail, "anomalies")
                .and_then(as_array)
                .map_or(0, <[_]>::len),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn s(v: &str) -> OperationResult {
        OperationResult::String(Bytes::from(v.to_string()))
    }
    fn arr(v: Vec<OperationResult>) -> OperationResult {
        OperationResult::Array(v)
    }

    #[test]
    fn locktable_sentinel_parses_to_no_snapshots() {
        // A quiesced server replies with the empty sentinel bulk string.
        let reply = s("# lock table is empty");
        assert!(parse_locktable(&reply).is_empty());
        // …and the checker accepts an empty snapshot set.
        assert!(check_locktable_empty(&parse_locktable(&reply)).is_ok());
    }

    #[test]
    fn locktable_map_parses_intents_and_continuation_lock() {
        // shard:0 -> { continuation_lock: nil, intents: [] }  (clean)
        // shard:1 -> { continuation_lock: "txid:7 …", intents: [ {…}, {…} ] }
        let reply = arr(vec![
            s("shard:0"),
            arr(vec![
                s("continuation_lock"),
                OperationResult::Nil,
                s("intents"),
                arr(vec![]),
            ]),
            s("shard:1"),
            arr(vec![
                s("continuation_lock"),
                s("txid:7 conn_id:3 age_ms:1"),
                s("intents"),
                arr(vec![
                    arr(vec![s("key"), s("k1")]),
                    arr(vec![s("key"), s("k2")]),
                ]),
            ]),
        ]);
        let snaps = parse_locktable(&reply);
        assert_eq!(snaps.len(), 2);
        assert_eq!(snaps[0].shard_id, 0);
        assert_eq!(snaps[0].intent_key_count, 0);
        assert!(!snaps[0].continuation_lock_held);
        assert_eq!(snaps[1].shard_id, 1);
        assert_eq!(snaps[1].intent_key_count, 2);
        assert!(snaps[1].continuation_lock_held);
        // A held continuation lock + intents must be flagged by the checker.
        assert!(check_locktable_empty(&snaps).is_err());
    }

    #[test]
    fn waitqueue_reads_total_waiters() {
        assert!(parse_waitqueue(&s("# wait queue is empty")).is_empty());
        let reply = arr(vec![
            s("shard:2"),
            arr(vec![
                s("total_waiters"),
                OperationResult::Integer(3),
                s("keys"),
                arr(vec![]),
            ]),
        ]);
        let snaps = parse_waitqueue(&reply);
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].shard_id, 2);
        assert_eq!(snaps[0].total_waiters, 3);
        assert!(check_waitqueue_empty(&snaps).is_err());
    }

    #[test]
    fn memory_check_reads_tracked_and_recomputed() {
        let reply = arr(vec![
            s("shard:0"),
            arr(vec![
                s("tracked_bytes"),
                OperationResult::Integer(4096),
                s("recomputed_bytes"),
                OperationResult::Integer(4096),
                s("diff"),
                OperationResult::Integer(0),
                s("consistent"),
                OperationResult::Integer(1),
            ]),
            s("shard:1"),
            arr(vec![
                s("tracked_bytes"),
                OperationResult::Integer(4096),
                s("recomputed_bytes"),
                OperationResult::Integer(5000),
                s("diff"),
                OperationResult::Integer(904),
                s("consistent"),
                OperationResult::Integer(0),
            ]),
        ]);
        let snaps = parse_memory_check(&reply);
        assert_eq!(snaps.len(), 2);
        assert_eq!(snaps[0].tracked_bytes, 4096);
        assert_eq!(snaps[0].recomputed_bytes, 4096);
        assert_eq!(snaps[1].tracked_bytes, 4096);
        assert_eq!(snaps[1].recomputed_bytes, 5000);
        // shard:1 drifts -> checker must reject.
        assert!(check_memory_accounting(&snaps).is_err());
        // Only shard:0 (consistent) -> checker passes.
        assert!(check_memory_accounting(&snaps[..1]).is_ok());
    }

    #[test]
    fn expiry_index_counts_anomalies() {
        assert!(parse_expiry_index(&s("# expiry index is consistent")).is_empty());
        let reply = arr(vec![
            s("shard:4"),
            arr(vec![
                s("total_entries"),
                OperationResult::Integer(10),
                s("anomalies"),
                arr(vec![
                    arr(vec![s("key"), s("a"), s("kind"), s("Orphaned")]),
                    arr(vec![s("key"), s("b"), s("kind"), s("Stale")]),
                ]),
            ]),
        ]);
        let snaps = parse_expiry_index(&reply);
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].shard_id, 4);
        assert_eq!(snaps[0].anomaly_count, 2);
        assert!(check_expiry_index_consistent(&snaps).is_err());
    }

    #[test]
    fn clean_bundle_reports_no_violations() {
        // The quiesced-server case: three sentinels + a consistent memory map.
        let snap = QuiescenceSnapshots::from_replies(
            &s("# lock table is empty"),
            &s("# wait queue is empty"),
            &arr(vec![
                s("shard:0"),
                arr(vec![
                    s("tracked_bytes"),
                    OperationResult::Integer(128),
                    s("recomputed_bytes"),
                    OperationResult::Integer(128),
                    s("diff"),
                    OperationResult::Integer(0),
                    s("consistent"),
                    OperationResult::Integer(1),
                ]),
            ]),
            &s("# expiry index is consistent"),
        );
        assert!(check_quiescence(&snap).is_empty());
    }

    #[test]
    fn dirty_bundle_surfaces_each_violation() {
        let snap = QuiescenceSnapshots {
            lock_table: vec![LockTableSnapshot {
                shard_id: 0,
                intent_key_count: 1,
                continuation_lock_held: false,
            }],
            wait_queue: vec![WaitQueueSnapshot {
                shard_id: 0,
                total_waiters: 2,
            }],
            memory: vec![MemoryCheckSnapshot {
                shard_id: 0,
                tracked_bytes: 1,
                recomputed_bytes: 2,
            }],
            expiry_index: vec![ExpiryIndexSnapshot {
                shard_id: 0,
                anomaly_count: 1,
            }],
        };
        let violations = check_quiescence(&snap);
        assert_eq!(
            violations.len(),
            4,
            "each checker must contribute: {violations:?}"
        );
        assert!(violations.iter().all(|v| v.starts_with("quiescence: ")));
    }
}
