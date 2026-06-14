//! Active-expiry coordinator.
//!
//! Owns the *decision + deletion* half of active expiry: read the due key/field
//! sets from the store and delete them under a time budget. Everything that
//! touches shard-only state (client tracking, search indexes, keyspace
//! notifications, USDT probes, metrics, the version counter) stays shard-side
//! and is applied *past the seam* from the returned [`ExpiryResult`].
//!
//! Because the coordinator names only the [`Store`] trait and a clock, it is
//! unit-testable against a bare `HashMapStore` — no `ShardWorker`, no channels,
//! no `tokio` runtime. See the tests at the bottom of this file.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::store::Store;

/// Default per-cycle wall-clock budget for active expiry.
///
/// Matches the historical inline budget in the event loop. Deletions stop once
/// the cycle has spent this long, leaving the rest for the next tick.
const DEFAULT_BUDGET: Duration = Duration::from_millis(25);

/// Default cap on entries pulled from the store per scan batch.
///
/// The store's expired-set scan clones every due entry it collects up front, so
/// an unbounded scan under a TTL avalanche could stall the shard long before the
/// time budget is consulted. Collecting in capped batches bounds that up-front
/// allocation; the budget is re-checked between and within batches, so the cycle
/// still stops on time while making forward progress across batches (deletions
/// remove entries from the index, so the next batch sees the next slice).
const DEFAULT_BATCH_SIZE: usize = 1024;

/// What one active-expiry cycle deleted.
///
/// This is the seam between the coordinator (decides + deletes) and the shard
/// (applies tracking / search / notify / metrics effects). Nothing in here
/// references shard-only state, so the coordinator is testable against a bare
/// [`Store`].
#[derive(Debug, Default, Clone)]
pub struct ExpiryResult {
    /// Keys removed because their own TTL elapsed. The shard fires the full
    /// effect set for each (tracking, search, `expired` notification, probe).
    pub deleted_keys: Vec<Bytes>,
    /// Keys removed because their last live hash field expired (hash emptied).
    /// The shard invalidates tracking + search and emits a `del` notification
    /// + probe for these.
    pub emptied_keys: Vec<Bytes>,
    /// Total hash fields purged across all keys (drives `frogdb_fields_expired_total`).
    pub fields_expired: u64,
    /// True if the time budget stopped the cycle before draining all due keys.
    pub budget_exhausted: bool,
}

impl ExpiryResult {
    /// Number of keys removed in this cycle (key-level TTL **and**
    /// field-emptied). Drives `add_expired_keys` + `frogdb_keys_expired_total`.
    ///
    /// A field-emptied key is a genuine key expiration and is counted here
    /// exactly once; the fields that triggered it are counted separately in
    /// [`ExpiryResult::fields_expired`], so the key and field counters never
    /// double-count each other.
    pub fn keys_expired(&self) -> u64 {
        (self.deleted_keys.len() + self.emptied_keys.len()) as u64
    }

    /// Whether anything happened this cycle (gate for `increment_version`).
    pub fn is_empty(&self) -> bool {
        self.deleted_keys.is_empty() && self.emptied_keys.is_empty() && self.fields_expired == 0
    }
}

/// Owns the active-expiry decision + deletion. Holds the cycle's tunables so the
/// budget/sampling policy can evolve without touching the event loop.
pub struct ActiveExpiryCoordinator {
    /// Per-cycle wall-clock budget.
    budget: Duration,
    /// Max entries collected from the store per scan batch (bounds the up-front
    /// clone cost of the expired-set scan).
    batch_size: usize,
}

impl Default for ActiveExpiryCoordinator {
    fn default() -> Self {
        Self {
            budget: DEFAULT_BUDGET,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl ActiveExpiryCoordinator {
    /// Construct a coordinator with an explicit budget (used by tests to make
    /// budget exhaustion deterministic).
    #[cfg(test)]
    fn with_budget(budget: Duration) -> Self {
        Self {
            budget,
            ..Self::default()
        }
    }

    /// Construct a coordinator with an explicit budget and batch size (used by
    /// tests to exercise the multi-batch scan path deterministically).
    #[cfg(test)]
    fn with_config(budget: Duration, batch_size: usize) -> Self {
        Self { budget, batch_size }
    }

    /// Run one active-expiry cycle: delete TTL-expired keys and purge expired
    /// hash fields, under the time budget. Returns what was deleted so the
    /// caller can apply side effects past the seam. Touches only the store.
    ///
    /// The store is scanned in bounded batches (`batch_size`) so the up-front
    /// clone of the due set cannot grow without bound under a TTL avalanche.
    /// Deleting a key removes it from the store's expiry index, so each batch
    /// advances to the next slice; the time budget is checked between and within
    /// batches, so the scan is covered by the budget too.
    pub fn run_cycle(&mut self, store: &mut dyn Store, now: Instant) -> ExpiryResult {
        let mut result = ExpiryResult::default();
        let start = Instant::now();

        // --- Key-level expiry ---
        loop {
            if start.elapsed() > self.budget {
                result.budget_exhausted = true;
                return result;
            }
            let batch = store.get_expired_keys_limited(now, self.batch_size);
            let batch_len = batch.len();
            if batch_len == 0 {
                break;
            }
            let mut progressed = false;
            for key in batch {
                if start.elapsed() > self.budget {
                    result.budget_exhausted = true;
                    return result;
                }
                if store.delete(&key) {
                    result.deleted_keys.push(key);
                    progressed = true;
                }
            }
            // Index drained, or stuck on entries `delete` won't remove
            // (defensive: avoids spinning the whole budget on dead entries).
            if batch_len < self.batch_size || !progressed {
                break;
            }
        }

        // --- Field-level (hash field TTL) expiry ---
        // Dedup keys with multiple expired fields across the whole cycle: each
        // key is purged once (purging removes all its expired fields).
        let mut seen: HashSet<Bytes> = HashSet::new();
        loop {
            if start.elapsed() > self.budget {
                result.budget_exhausted = true;
                return result;
            }
            let batch = store.get_expired_fields_limited(now, self.batch_size);
            let batch_len = batch.len();
            if batch_len == 0 {
                break;
            }
            let keys: Vec<Bytes> = batch
                .into_iter()
                .filter_map(|(key, _field)| seen.insert(key.clone()).then_some(key))
                .collect();
            if keys.is_empty() {
                // Whole batch was keys already purged this cycle; their fields
                // are gone, so there is nothing left to make progress on.
                break;
            }
            let mut purged_any = false;
            for key in keys {
                if start.elapsed() > self.budget {
                    result.budget_exhausted = true;
                    return result;
                }
                let existed_before = store.get(&key).is_some();
                let purged = store.purge_expired_hash_fields(&key) as u64;
                if purged > 0 {
                    result.fields_expired += purged;
                    purged_any = true;
                }
                if existed_before && store.get(&key).is_none() {
                    result.emptied_keys.push(key);
                }
            }
            // Index drained, or nothing could be purged (defensive).
            if batch_len < self.batch_size || !purged_any {
                break;
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::store::HashMapStore;
    use crate::types::{HashValue, ListpackThresholds, Value};

    /// A key whose own TTL is already in the past.
    fn set_expired_key(store: &mut HashMapStore, key: &str) {
        let past = Instant::now() - Duration::from_secs(60);
        store.set(Bytes::from(key.to_string()), Value::string("v"));
        assert!(store.set_expiry(key.as_bytes(), past));
    }

    /// A hash with `fields`, each given an already-elapsed field TTL (both on the
    /// value and in the store's field-expiry index, matching HEXPIRE).
    fn set_hash_with_expired_fields(store: &mut HashMapStore, key: &str, fields: &[&str]) {
        let past = Instant::now() - Duration::from_secs(60);
        let mut hash = HashValue::new();
        for f in fields {
            hash.set(
                Bytes::from(f.to_string()),
                Bytes::from("v"),
                ListpackThresholds::DEFAULT_HASH,
            );
            hash.set_field_expiry(f.as_bytes(), past);
        }
        store.set(Bytes::from(key.to_string()), Value::Hash(hash));
        for f in fields {
            store.set_field_expiry(key.as_bytes(), f.as_bytes(), past);
        }
    }

    #[test]
    fn deletes_all_due_keys_within_budget() {
        let mut store = HashMapStore::new();
        for i in 0..10 {
            set_expired_key(&mut store, &format!("k{i}"));
        }
        // A live key must survive.
        store.set(Bytes::from("live"), Value::string("v"));

        let mut coord = ActiveExpiryCoordinator::default();
        let result = coord.run_cycle(&mut store, Instant::now());

        assert_eq!(result.deleted_keys.len(), 10);
        assert_eq!(result.keys_expired(), 10);
        assert!(!result.budget_exhausted);
        assert!(result.emptied_keys.is_empty());
        assert_eq!(result.fields_expired, 0);
        assert!(store.contains(b"live"));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn budget_zero_stops_early_without_deleting_everything() {
        let mut store = HashMapStore::new();
        for i in 0..100 {
            set_expired_key(&mut store, &format!("k{i}"));
        }

        // A zero budget is exhausted on the first elapsed() check.
        let mut coord = ActiveExpiryCoordinator::with_budget(Duration::ZERO);
        let result = coord.run_cycle(&mut store, Instant::now());

        assert!(result.budget_exhausted);
        assert!(result.deleted_keys.len() < 100);
        // Whatever was not deleted is still resident.
        assert_eq!(store.len(), 100 - result.deleted_keys.len());
    }

    #[test]
    fn does_not_count_already_gone_keys() {
        // Nothing is due -> nothing deleted, result empty, no double counting.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("live"), Value::string("v"));

        let mut coord = ActiveExpiryCoordinator::default();
        let result = coord.run_cycle(&mut store, Instant::now());

        assert_eq!(result.keys_expired(), 0);
        assert!(result.is_empty());
        assert!(store.contains(b"live"));
    }

    #[test]
    fn field_sweep_counts_fields_not_emptied_when_hash_survives() {
        let mut store = HashMapStore::new();
        // Two fields, only one of which expires.
        let past = Instant::now() - Duration::from_secs(60);
        let future = Instant::now() + Duration::from_secs(60);
        let mut hash = HashValue::new();
        hash.set(
            Bytes::from("f1"),
            Bytes::from("v1"),
            ListpackThresholds::DEFAULT_HASH,
        );
        hash.set(
            Bytes::from("f2"),
            Bytes::from("v2"),
            ListpackThresholds::DEFAULT_HASH,
        );
        hash.set_field_expiry(b"f1", past);
        hash.set_field_expiry(b"f2", future);
        store.set(Bytes::from("h"), Value::Hash(hash));
        store.set_field_expiry(b"h", b"f1", past);
        store.set_field_expiry(b"h", b"f2", future);

        let mut coord = ActiveExpiryCoordinator::default();
        let result = coord.run_cycle(&mut store, Instant::now());

        assert_eq!(result.fields_expired, 1);
        assert!(result.emptied_keys.is_empty());
        assert!(result.deleted_keys.is_empty());
        assert!(store.contains(b"h"));
        // keys_expired counts only removed keys, not surviving field purges.
        assert_eq!(result.keys_expired(), 0);
    }

    #[test]
    fn field_sweep_reports_emptied_key_when_last_field_expires() {
        let mut store = HashMapStore::new();
        set_hash_with_expired_fields(&mut store, "h", &["only"]);

        let mut coord = ActiveExpiryCoordinator::default();
        let result = coord.run_cycle(&mut store, Instant::now());

        assert_eq!(result.fields_expired, 1);
        assert_eq!(result.emptied_keys, vec![Bytes::from("h")]);
        assert!(result.deleted_keys.is_empty());
        // The key is gone from the store.
        assert!(!store.contains(b"h"));
        // The emptied key counts as one expired key.
        assert_eq!(result.keys_expired(), 1);
    }

    #[test]
    fn dedups_multiple_expired_fields_on_one_key() {
        let mut store = HashMapStore::new();
        // Three fields all expire; the hash empties and the key is removed once.
        set_hash_with_expired_fields(&mut store, "h", &["a", "b", "c"]);

        let mut coord = ActiveExpiryCoordinator::default();
        let result = coord.run_cycle(&mut store, Instant::now());

        assert_eq!(result.fields_expired, 3);
        // Exactly one emptied key despite three expired fields.
        assert_eq!(result.emptied_keys, vec![Bytes::from("h")]);
        assert_eq!(result.keys_expired(), 1);
        assert!(!store.contains(b"h"));
    }

    #[test]
    fn keys_expired_combines_both_deletion_paths() {
        let mut store = HashMapStore::new();
        set_expired_key(&mut store, "plain");
        set_hash_with_expired_fields(&mut store, "h", &["only"]);

        let mut coord = ActiveExpiryCoordinator::default();
        let result = coord.run_cycle(&mut store, Instant::now());

        assert_eq!(result.deleted_keys, vec![Bytes::from("plain")]);
        assert_eq!(result.emptied_keys, vec![Bytes::from("h")]);
        assert_eq!(result.fields_expired, 1);
        // One key-level + one field-emptied = two expired keys, no double count.
        assert_eq!(result.keys_expired(), 2);
    }

    #[test]
    fn drains_keys_across_multiple_batches() {
        // More due keys than fit in one batch: the bounded scan must still drain
        // them all by advancing batch by batch (deletes remove index entries).
        let mut store = HashMapStore::new();
        for i in 0..20 {
            set_expired_key(&mut store, &format!("k{i}"));
        }

        let mut coord = ActiveExpiryCoordinator::with_config(Duration::from_secs(5), 4);
        let result = coord.run_cycle(&mut store, Instant::now());

        assert_eq!(result.deleted_keys.len(), 20);
        assert!(!result.budget_exhausted);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn drains_emptied_keys_across_multiple_batches() {
        // More field-emptied keys than one field batch holds.
        let mut store = HashMapStore::new();
        for i in 0..10 {
            set_hash_with_expired_fields(&mut store, &format!("h{i}"), &["only"]);
        }

        let mut coord = ActiveExpiryCoordinator::with_config(Duration::from_secs(5), 3);
        let result = coord.run_cycle(&mut store, Instant::now());

        assert_eq!(result.emptied_keys.len(), 10);
        assert_eq!(result.fields_expired, 10);
        assert_eq!(result.keys_expired(), 10);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn store_delete_does_not_bump_expired_counter_but_add_expired_keys_does() {
        // Pins the invariant the shard relies on: `delete` is the single
        // removal primitive and does NOT touch the expired-keys stat, so the
        // shard's `add_expired_keys(keys_expired())` is the sole source.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string("v"));

        assert_eq!(store.expired_keys(), 0);
        assert!(store.delete(b"k"));
        assert_eq!(store.expired_keys(), 0, "delete must not bump expired_keys");

        store.add_expired_keys(3);
        assert_eq!(store.expired_keys(), 3);
    }
}
