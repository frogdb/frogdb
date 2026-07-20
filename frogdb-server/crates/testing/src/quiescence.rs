//! Tier-4 quiescence checkers (concurrency-invariant-testing design).
//!
//! These consume **parsed DEBUG-reply snapshots** — transport-agnostic structs,
//! not RESP types and not `frogdb-core` info structs — so the same checkers
//! ingest snapshots from turmoil, real-network, and later Jepsen/replication
//! runs. Run after a workload drains and expiry settles: every check must hold.

/// Parsed `DEBUG LOCKTABLE` snapshot for one shard.
#[derive(Debug, Clone)]
pub struct LockTableSnapshot {
    pub shard_id: usize,
    pub intent_key_count: usize,
    pub continuation_lock_held: bool,
}

/// Parsed `DEBUG WAITQUEUE` snapshot for one shard.
#[derive(Debug, Clone)]
pub struct WaitQueueSnapshot {
    pub shard_id: usize,
    pub total_waiters: usize,
    pub waiters: Vec<WaiterOrdinal>,
}

/// One parked waiter's registration ordinal, from a mid-run DEBUG WAITQUEUE
/// observation. `registration_seq` is the queue-wide monotonic ordinal (smaller
/// = registered earlier), enabling exact FIFO wake-order checking.
#[derive(Debug, Clone)]
pub struct WaiterOrdinal {
    pub key: Vec<u8>,
    pub conn_id: u64,
    pub registration_seq: u64,
}

/// Parsed `DEBUG MEMORY-CHECK` snapshot for one shard.
#[derive(Debug, Clone)]
pub struct MemoryCheckSnapshot {
    pub shard_id: usize,
    pub tracked_bytes: u64,
    pub recomputed_bytes: u64,
}

/// Parsed `DEBUG EXPIRY-INDEX-CHECK` snapshot for one shard.
#[derive(Debug, Clone)]
pub struct ExpiryIndexSnapshot {
    pub shard_id: usize,
    pub anomaly_count: usize,
}

/// A quiescence-invariant violation. Parallel to
/// [`crate::conservation::ConservationViolation`]: disjoint input (parsed DEBUG
/// replies vs a `History`), so it is its own enum rather than a shared one.
#[derive(Debug, thiserror::Error)]
pub enum QuiescenceViolation {
    /// The VLL lock table still holds intents or a continuation lock at quiesce.
    #[error(
        "shard {shard_id}: lock table not empty at quiesce ({intent_keys} intent key(s), continuation_lock={continuation_lock})"
    )]
    LockTableNotEmpty {
        shard_id: usize,
        intent_keys: usize,
        continuation_lock: bool,
    },
    /// The wait queue still holds blocked waiters at quiesce.
    #[error("shard {shard_id}: wait queue not empty at quiesce ({waiters} waiter(s))")]
    WaitQueueNotEmpty { shard_id: usize, waiters: usize },
    /// The tracked memory counter disagrees with the recomputed live size.
    #[error(
        "shard {shard_id}: memory accounting drift (tracked {tracked}, recomputed {recomputed})"
    )]
    MemoryAccountingDrift {
        shard_id: usize,
        tracked: u64,
        recomputed: u64,
    },
    /// The expiry index disagrees with entry deadlines.
    #[error("shard {shard_id}: expiry index inconsistent ({anomalies} anomaly/anomalies)")]
    ExpiryIndexInconsistent { shard_id: usize, anomalies: usize },
}

/// VLL lock table empty on every shard (no leaked intents or continuation locks).
pub fn check_locktable_empty(snapshots: &[LockTableSnapshot]) -> Result<(), QuiescenceViolation> {
    for s in snapshots {
        if s.intent_key_count > 0 || s.continuation_lock_held {
            return Err(QuiescenceViolation::LockTableNotEmpty {
                shard_id: s.shard_id,
                intent_keys: s.intent_key_count,
                continuation_lock: s.continuation_lock_held,
            });
        }
    }
    Ok(())
}

/// Wait queue empty on every shard (no orphaned waiters after disconnects/timeouts).
pub fn check_waitqueue_empty(snapshots: &[WaitQueueSnapshot]) -> Result<(), QuiescenceViolation> {
    for s in snapshots {
        if s.total_waiters > 0 {
            return Err(QuiescenceViolation::WaitQueueNotEmpty {
                shard_id: s.shard_id,
                waiters: s.total_waiters,
            });
        }
    }
    Ok(())
}

/// Tracked `memory_used` matches the recomputed live size on every shard.
pub fn check_memory_accounting(
    snapshots: &[MemoryCheckSnapshot],
) -> Result<(), QuiescenceViolation> {
    for s in snapshots {
        if s.tracked_bytes != s.recomputed_bytes {
            return Err(QuiescenceViolation::MemoryAccountingDrift {
                shard_id: s.shard_id,
                tracked: s.tracked_bytes,
                recomputed: s.recomputed_bytes,
            });
        }
    }
    Ok(())
}

/// Expiry index has no entry pointing at a persistent or deleted key on any shard.
pub fn check_expiry_index_consistent(
    snapshots: &[ExpiryIndexSnapshot],
) -> Result<(), QuiescenceViolation> {
    for s in snapshots {
        if s.anomaly_count > 0 {
            return Err(QuiescenceViolation::ExpiryIndexInconsistent {
                shard_id: s.shard_id,
                anomalies: s.anomaly_count,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locktable_empty_passes_and_fails() {
        let clean = [
            LockTableSnapshot {
                shard_id: 0,
                intent_key_count: 0,
                continuation_lock_held: false,
            },
            LockTableSnapshot {
                shard_id: 1,
                intent_key_count: 0,
                continuation_lock_held: false,
            },
        ];
        assert!(check_locktable_empty(&clean).is_ok());

        let leaked_intent = [LockTableSnapshot {
            shard_id: 3,
            intent_key_count: 2,
            continuation_lock_held: false,
        }];
        assert!(matches!(
            check_locktable_empty(&leaked_intent),
            Err(QuiescenceViolation::LockTableNotEmpty {
                shard_id: 3,
                intent_keys: 2,
                ..
            })
        ));

        let leaked_lock = [LockTableSnapshot {
            shard_id: 1,
            intent_key_count: 0,
            continuation_lock_held: true,
        }];
        assert!(matches!(
            check_locktable_empty(&leaked_lock),
            Err(QuiescenceViolation::LockTableNotEmpty {
                continuation_lock: true,
                ..
            })
        ));
    }

    #[test]
    fn waitqueue_empty_passes_and_fails() {
        assert!(
            check_waitqueue_empty(&[WaitQueueSnapshot {
                shard_id: 0,
                total_waiters: 0,
                waiters: Vec::new(),
            }])
            .is_ok()
        );
        assert!(matches!(
            check_waitqueue_empty(&[WaitQueueSnapshot {
                shard_id: 2,
                total_waiters: 1,
                waiters: Vec::new(),
            }]),
            Err(QuiescenceViolation::WaitQueueNotEmpty {
                shard_id: 2,
                waiters: 1
            })
        ));
    }

    #[test]
    fn waitqueue_snapshot_carries_ordinals() {
        let s = WaitQueueSnapshot {
            shard_id: 0,
            total_waiters: 2,
            waiters: vec![
                WaiterOrdinal {
                    key: b"k".to_vec(),
                    conn_id: 7,
                    registration_seq: 3,
                },
                WaiterOrdinal {
                    key: b"k".to_vec(),
                    conn_id: 9,
                    registration_seq: 5,
                },
            ],
        };
        assert_eq!(s.waiters.len(), 2);
        assert!(s.waiters[0].registration_seq < s.waiters[1].registration_seq);
        // Non-empty still trips the emptiness checker.
        assert!(check_waitqueue_empty(&[s]).is_err());
    }

    #[test]
    fn memory_accounting_passes_and_fails() {
        let ok = [MemoryCheckSnapshot {
            shard_id: 0,
            tracked_bytes: 4096,
            recomputed_bytes: 4096,
        }];
        assert!(check_memory_accounting(&ok).is_ok());

        let drift = [MemoryCheckSnapshot {
            shard_id: 1,
            tracked_bytes: 4096,
            recomputed_bytes: 5000,
        }];
        assert!(matches!(
            check_memory_accounting(&drift),
            Err(QuiescenceViolation::MemoryAccountingDrift {
                shard_id: 1,
                tracked: 4096,
                recomputed: 5000,
            })
        ));
    }

    #[test]
    fn expiry_index_consistent_passes_and_fails() {
        assert!(
            check_expiry_index_consistent(&[ExpiryIndexSnapshot {
                shard_id: 0,
                anomaly_count: 0
            }])
            .is_ok()
        );
        assert!(matches!(
            check_expiry_index_consistent(&[ExpiryIndexSnapshot {
                shard_id: 4,
                anomaly_count: 3
            }]),
            Err(QuiescenceViolation::ExpiryIndexInconsistent {
                shard_id: 4,
                anomalies: 3
            })
        ));
    }
}
