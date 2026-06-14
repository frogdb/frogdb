//! Column-family layout reconciliation for [`RocksStore`](super::RocksStore).
//!
//! A `RocksStore` keeps one set of RocksDB column families per shard
//! (`shard_<n>`, `tiered_warm_<n>`, `search_meta_<n>`, plus the implicit
//! `default`). *Which* of those families must be opened is the store's most
//! load-bearing recovery invariant: open too few and RocksDB refuses the whole
//! database; open the wrong ones and keys are misrouted. That decision must be
//! derived from what is actually persisted, reconciled against config — never
//! from a single live flag.
//!
//! [`ColumnFamilyManifest`] owns that one question. `reconcile` is a pure
//! function over `(existing CFs, num_shards, warm_enabled)` with no RocksDB, no
//! tempdir, and no I/O, so every reconcile outcome is a cheap in-memory
//! assertion. It folds two independent invariants — shard count *and* the warm
//! toggle — behind one call, plus the prefix classification that both rely on.
use super::config::RocksError;
use tracing::error;

/// The set of column families a [`RocksStore`](super::RocksStore) must open,
/// derived from persisted state reconciled against configuration — never from a
/// single live flag.
///
/// This is the one place that knows the CF layout and the invariants that bind
/// it to what is on disk. [`open_with_warm`](super::RocksStore::open_with_warm)
/// consumes a [`required`](Self::required) list; it no longer decides anything.
#[derive(Debug)]
pub(crate) struct ColumnFamilyManifest {
    /// `shard_<n>` — the persisted count is authoritative.
    shards: Vec<String>,
    /// `tiered_warm_<n>` — present iff the warm tier is active.
    warm: Vec<String>,
    /// `search_meta_<n>` — always present.
    search_meta: Vec<String>,
    /// `default` — present on every non-fresh database.
    has_default: bool,
}

impl ColumnFamilyManifest {
    /// Reconcile the configured layout against what is actually persisted.
    ///
    /// `existing` is the live CF list from `DB::list_cf`. An empty slice means a
    /// fresh database: config wins and there is nothing to reconcile. For an
    /// existing database every persisted invariant is checked against config,
    /// and any drift that would orphan or misroute data is a hard error — the
    /// store must open *all* persisted CFs or refuse to open at all.
    pub(crate) fn reconcile(
        path_str: &str,
        existing: &[String],
        num_shards: usize,
        warm_enabled: bool,
    ) -> Result<Self, RocksError> {
        let shard = |i| format!("shard_{i}");
        let warm = |i| format!("tiered_warm_{i}");
        let meta = |i| format!("search_meta_{i}");

        // Fresh DB: stamp the layout from config; nothing on disk to reconcile.
        if existing.is_empty() {
            return Ok(Self {
                shards: (0..num_shards).map(shard).collect(),
                warm: if warm_enabled {
                    (0..num_shards).map(warm).collect()
                } else {
                    Vec::new()
                },
                search_meta: (0..num_shards).map(meta).collect(),
                has_default: false,
            });
        }

        // Invariant 1 — shard count. The persisted shard layout is recorded
        // implicitly by the `shard_<n>` families. A mismatch would otherwise
        // either fail deep in RocksDB with a cryptic "column families not
        // opened" error (when shrinking) or silently misroute every key under
        // the new hash space (when growing). Fail loudly with both counts.
        let persisted_shards = count_persisted_shards(existing);
        if persisted_shards != 0 && persisted_shards != num_shards {
            error!(
                path = %path_str,
                persisted = persisted_shards,
                configured = num_shards,
                "RocksDB shard count mismatch; aborting recovery to avoid data loss"
            );
            return Err(RocksError::ShardCountMismatch {
                path: path_str.to_owned(),
                persisted: persisted_shards,
                configured: num_shards,
            });
        }

        // Invariant 2 — warm tier. A directory that persisted `tiered_warm_*`
        // CFs cannot reopen with the warm tier disabled: those CFs would be
        // left unopened and RocksDB refuses the whole DB. off -> on is *not* an
        // error: it is a legitimate first-enable that creates the warm CFs.
        let persisted_warm = existing.iter().any(|cf| is_warm_cf(cf));
        if persisted_warm && !warm_enabled {
            error!(
                path = %path_str,
                "RocksDB warm-tier toggle mismatch; aborting recovery to avoid orphaning tiered_warm_* data"
            );
            return Err(RocksError::WarmTierMismatch {
                path: path_str.to_owned(),
            });
        }

        Ok(Self {
            shards: (0..num_shards).map(shard).collect(),
            // Open the warm CFs if config asks for them OR they already exist.
            warm: if warm_enabled || persisted_warm {
                (0..num_shards).map(warm).collect()
            } else {
                Vec::new()
            },
            search_meta: (0..num_shards).map(meta).collect(),
            has_default: existing.iter().any(|c| c == "default"),
        })
    }

    /// Every CF that must be passed to `open_cf_descriptors`, in open order.
    pub(crate) fn required(&self) -> impl Iterator<Item = &str> {
        self.has_default
            .then_some("default")
            .into_iter()
            .chain(self.shards.iter().map(String::as_str))
            .chain(self.warm.iter().map(String::as_str))
            .chain(self.search_meta.iter().map(String::as_str))
    }

    /// Names this store later resolves at runtime via `cf_handle`.
    pub(crate) fn shard_names(&self) -> &[String] {
        &self.shards
    }
    /// Names this store later resolves at runtime via `warm_cf_handle`.
    pub(crate) fn warm_names(&self) -> &[String] {
        &self.warm
    }
    /// Names this store later resolves at runtime via `search_meta_cf_handle`.
    pub(crate) fn search_meta_names(&self) -> &[String] {
        &self.search_meta
    }
}

/// Count the persisted data shards by inspecting the existing column-family
/// names. Data shards live in `shard_<n>` families; `tiered_warm_<n>` and
/// `search_meta_<n>` families are deliberately ignored so the count reflects
/// exactly the logical shard count the data was written with.
pub(crate) fn count_persisted_shards(existing_cfs: &[String]) -> usize {
    existing_cfs
        .iter()
        .filter(|name| {
            name.strip_prefix("shard_").is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
            })
        })
        .count()
}

/// Whether `name` is a warm-tier column family (`tiered_warm_<n>` with a
/// non-empty, all-digit suffix).
fn is_warm_cf(name: &str) -> bool {
    name.strip_prefix("tiered_warm_")
        .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh (empty) directory takes its whole layout from config: shards and
    /// search-meta for every shard, warm only when the flag is on, and no
    /// `default` (RocksDB creates it implicitly on first open).
    #[test]
    fn fresh_dir_warm_off_stamps_config_layout() {
        let m = ColumnFamilyManifest::reconcile("/db", &[], 2, false).unwrap();
        assert_eq!(m.shard_names(), ["shard_0", "shard_1"]);
        assert!(m.warm_names().is_empty());
        assert_eq!(m.search_meta_names(), ["search_meta_0", "search_meta_1"]);
        assert_eq!(
            m.required().collect::<Vec<_>>(),
            ["shard_0", "shard_1", "search_meta_0", "search_meta_1"]
        );
    }

    /// Fresh + warm-on stamps the warm CFs into the layout too.
    #[test]
    fn fresh_dir_warm_on_includes_warm() {
        let m = ColumnFamilyManifest::reconcile("/db", &[], 2, true).unwrap();
        assert_eq!(m.warm_names(), ["tiered_warm_0", "tiered_warm_1"]);
        assert_eq!(
            m.required().collect::<Vec<_>>(),
            [
                "shard_0",
                "shard_1",
                "tiered_warm_0",
                "tiered_warm_1",
                "search_meta_0",
                "search_meta_1",
            ]
        );
    }

    /// A matching existing layout reconciles to the same required set, with the
    /// persisted `default` surfaced first in open order.
    #[test]
    fn existing_matching_shards_includes_default_first() {
        let existing = svec(&[
            "default",
            "shard_0",
            "shard_1",
            "search_meta_0",
            "search_meta_1",
        ]);
        let m = ColumnFamilyManifest::reconcile("/db", &existing, 2, false).unwrap();
        assert_eq!(
            m.required().collect::<Vec<_>>(),
            [
                "default",
                "shard_0",
                "shard_1",
                "search_meta_0",
                "search_meta_1"
            ]
        );
    }

    /// Invariant 1: a persisted shard count that disagrees with config is a
    /// hard `ShardCountMismatch`, carrying both counts and the path.
    #[test]
    fn mismatched_shard_count_is_hard_error() {
        let existing = svec(&["default", "shard_0", "shard_1", "shard_2", "shard_3"]);
        match ColumnFamilyManifest::reconcile("/db", &existing, 2, false) {
            Err(RocksError::ShardCountMismatch {
                path,
                persisted,
                configured,
            }) => {
                assert_eq!(path, "/db");
                assert_eq!(persisted, 4);
                assert_eq!(configured, 2);
            }
            other => panic!("expected ShardCountMismatch, got {other:?}"),
        }
    }

    /// Invariant 2: persisted warm CFs + warm configured off is a hard
    /// `WarmTierMismatch` — reopening warm-off would orphan the warm data.
    #[test]
    fn persisted_warm_with_warm_off_is_hard_error() {
        let existing = svec(&[
            "default",
            "shard_0",
            "shard_1",
            "tiered_warm_0",
            "tiered_warm_1",
        ]);
        match ColumnFamilyManifest::reconcile("/db", &existing, 2, false) {
            Err(RocksError::WarmTierMismatch { path }) => assert_eq!(path, "/db"),
            other => panic!("expected WarmTierMismatch, got {other:?}"),
        }
    }

    /// Persisted warm CFs + warm still on reconciles cleanly, keeping the warm
    /// CFs in the required set.
    #[test]
    fn persisted_warm_with_warm_on_keeps_warm() {
        let existing = svec(&[
            "default",
            "shard_0",
            "shard_1",
            "tiered_warm_0",
            "tiered_warm_1",
        ]);
        let m = ColumnFamilyManifest::reconcile("/db", &existing, 2, true).unwrap();
        assert_eq!(m.warm_names(), ["tiered_warm_0", "tiered_warm_1"]);
    }

    /// First-enable (off -> on): no persisted warm CFs but warm now configured
    /// on. Not an error — the warm CFs are added to the required set so the open
    /// path creates them fresh.
    #[test]
    fn no_persisted_warm_with_warm_on_creates_warm() {
        let existing = svec(&[
            "default",
            "shard_0",
            "shard_1",
            "search_meta_0",
            "search_meta_1",
        ]);
        let m = ColumnFamilyManifest::reconcile("/db", &existing, 2, true).unwrap();
        assert_eq!(m.warm_names(), ["tiered_warm_0", "tiered_warm_1"]);
        assert!(m.required().any(|cf| cf == "tiered_warm_0"));
    }

    /// `count_persisted_shards` counts only `shard_<n>` families with a
    /// non-empty all-digit suffix; warm, search-meta, `default`, and a
    /// non-numeric `shard_meta` are all ignored.
    #[test]
    fn count_persisted_shards_ignores_other_cfs() {
        let cfs = svec(&[
            "default",
            "shard_0",
            "shard_1",
            "shard_2",
            "tiered_warm_0",
            "tiered_warm_1",
            "search_meta_0",
            "search_meta_1",
            "shard_meta", // non-numeric suffix, must be ignored
        ]);
        assert_eq!(count_persisted_shards(&cfs), 3);
        assert_eq!(count_persisted_shards(&[]), 0);
    }

    fn svec(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }
}
