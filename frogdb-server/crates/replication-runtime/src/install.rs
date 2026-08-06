//! Installing a received full-resync dataset into the **live** keyspace.
//!
//! The replication crate receives a full sync but owns no store, so it delegates
//! the install through the injected [`SnapshotInstaller`] seam. This module is
//! the server-side implementation: it turns whichever [`FullSyncPayload`] landed
//! into per-shard entries and replaces each shard's live keyspace with them,
//! before the replica adopts the snapshot's offset and resumes streaming.
//!
//! Two payload shapes, one destination:
//!
//! - [`FullSyncPayload::StagedCheckpoint`] — the primary had RocksDB; the staged
//!   DB is opened and scanned shard by shard.
//! - [`FullSyncPayload::LiveDataset`] — the primary ran with
//!   `persistence.enabled = false` and serialized its keyspace out of RAM
//!   (issue 67); the blobs are decoded and each key routed to *this* node's
//!   shard for it, so the two nodes' shard counts need not agree.
//!
//! Without it a runtime `REPLICAOF <new-master>` left the demoted node serving
//! its own (possibly forked) keyspace until the next reboot — issue 61.
//!
//! **Why not swap the RocksDB handle.** The live keyspace is the per-shard
//! in-memory store owned by each shard task; RocksDB is the durability tier
//! behind it. Swapping the handle would leave every shard's in-memory map
//! untouched, which is precisely the bug. So the snapshot is read out of the
//! staged DB and pushed into the shards, which clear + restore and then persist
//! the restored keys through their own WAL — the same direction of travel as
//! boot recovery, just at runtime.
//!
//! **The staged dir survives the install** (the boot-time installer's contract
//! is untouched): a crash mid-install leaves the node re-installing the same
//! snapshot on the next boot, which is idempotent.
//!
//! **The checkpoint path requires the two nodes to agree on shard count and on
//! the warm tier**, where the live-dataset path does not. A staged checkpoint is
//! opened as a RocksDB with *this* node's `cluster.shard_count` and
//! `tiered_storage.enabled`, and `ColumnFamilyManifest::reconcile` refuses a DB
//! whose persisted column families disagree. That refusal is returned as
//! [`InstallError::Incompatible`], which is terminal: no later attempt can get
//! past a disagreement about the on-disk layout, so the replica stops asking
//! instead of making the primary cut and ship a fresh checkpoint on a timer
//! (issue 23). Every other failure stays [`InstallError::Transient`] and is
//! retried as before.

use frogdb_core::clock;
use std::io;
use std::path::Path;

use bytes::Bytes;
use frogdb_config::Config;
use frogdb_core::persistence::rocks::RocksError;
use frogdb_core::persistence::{RocksConfig, RocksStore, deserialize, recover_shard_into};
use frogdb_core::shard::shard_for_key;
use frogdb_core::sync::Arc;
use frogdb_core::{
    KeyMetadata, KeyType, ReplicationMsg, ShardSender, SnapshotEntry, Value,
    persistence::RestoreSink,
};
use frogdb_replication::replica::{FullSyncPayload, InstallError, SnapshotInstaller};
use std::collections::HashSet;
use std::time::Instant;
use tokio::sync::oneshot;

/// Reads a received full-resync dataset and installs it into the live shards.
pub struct LiveSnapshotInstaller {
    shard_senders: Arc<Vec<ShardSender>>,
    rocks_config: RocksConfig,
    warm_enabled: bool,
}

impl LiveSnapshotInstaller {
    pub fn new(
        shard_senders: Arc<Vec<ShardSender>>,
        rocks_config: RocksConfig,
        warm_enabled: bool,
    ) -> Self {
        Self {
            shard_senders,
            rocks_config,
            warm_enabled,
        }
    }

    /// The installer this node's config implies, erased into the seam the
    /// replica handler holds. Single construction site for both wiring points
    /// (boot-configured replica and runtime `REPLICAOF` demotion) so the two can
    /// not drift apart.
    pub fn for_config(config: &Config, shard_senders: Arc<Vec<ShardSender>>) -> SnapshotInstaller {
        Self::new(
            shard_senders,
            RocksConfig::from_persistence(&config.persistence),
            config.tiered_storage.enabled,
        )
        .into_installer()
    }

    /// Erase this into the injectable seam the replica handler holds.
    pub fn into_installer(self) -> SnapshotInstaller {
        let installer = Arc::new(self);
        Arc::new(move |payload: FullSyncPayload| {
            let installer = installer.clone();
            Box::pin(async move {
                match payload {
                    FullSyncPayload::StagedCheckpoint(dir) => installer.install(&dir).await,
                    FullSyncPayload::LiveDataset(blobs) => installer.install_dataset(blobs).await,
                }
            })
        })
    }

    /// Decode a live dataset and replace every shard's live keyspace with it.
    ///
    /// The blobs are the *primary's* shards, which say nothing about this node's
    /// partitioning: each decoded key is routed through [`shard_for_key`] for
    /// this node's shard count. Every shard is then installed — including the
    /// ones the dataset has no keys for, because an install is a replace and a
    /// skipped shard would keep its forked keys (the very bug this path exists
    /// to close).
    ///
    /// The install ordering and its per-shard (not cross-shard) atomicity are
    /// identical to [`Self::install`]; see that method's note.
    async fn install_dataset(&self, blobs: Vec<Vec<u8>>) -> Result<(), InstallError> {
        let start = clock::now();
        let num_shards = self.shard_senders.len();
        if num_shards == 0 {
            return Err(InstallError::Transient(io::Error::other(
                "no shards wired: cannot install a live-dataset full resync",
            )));
        }

        // Decoding a whole keyspace is CPU-bound; keep it off the runtime for
        // the same reason the checkpoint path spawns its RocksDB scan.
        let blob_count = blobs.len();
        let per_shard = tokio::task::spawn_blocking(move || route_dataset(blobs, num_shards))
            .await
            .map_err(|e| io::Error::other(format!("dataset decode task failed: {e}")))??;

        let total: usize = per_shard.iter().map(Vec::len).sum();
        self.install_per_shard(per_shard).await?;

        tracing::info!(
            keys = total,
            blobs = blob_count,
            shards = num_shards,
            duration_ms = start.elapsed().as_millis() as u64,
            "Installed full-resync live dataset into the live keyspace"
        );
        Ok(())
    }

    /// Hand each shard its entries and wait for the ack, shard by shard.
    async fn install_per_shard(&self, per_shard: Vec<Vec<SnapshotEntry>>) -> io::Result<()> {
        for (shard_id, entries) in per_shard.into_iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            self.shard_senders[shard_id]
                .send(ReplicationMsg::InstallSnapshot {
                    entries,
                    response_tx,
                })
                .await
                .map_err(|_| io::Error::other(format!("shard {shard_id} is gone")))?;
            response_rx.await.map_err(|_| {
                io::Error::other(format!("shard {shard_id} dropped the install ack"))
            })?;
        }
        Ok(())
    }

    /// Read `staged_dir` and replace every shard's live keyspace with it.
    ///
    /// **Consistency window: per shard, not across shards.** Each shard applies
    /// its clear + restore atomically inside its own task (see
    /// `ShardWorker::install_snapshot`), but the shards are walked in order, so
    /// a client reading during the install can see an already-installed shard
    /// and a not-yet-installed one in the same moment. That is the granularity
    /// every other cross-shard write already has, and it is chosen over
    /// quiescing all clients for the duration of a whole-dataset load: reads
    /// keep being served, from the pre-install snapshot, until their own shard
    /// swaps. The install completes before the replica adopts the snapshot's
    /// offset, so no *replicated* write can observe the half-installed state.
    async fn install(&self, staged_dir: &Path) -> Result<(), InstallError> {
        let start = clock::now();
        let num_shards = self.shard_senders.len();
        let dir = staged_dir.to_path_buf();
        let rocks_config = self.rocks_config.clone();
        let warm_enabled = self.warm_enabled;

        // RocksDB open + full scan is blocking work; keep it off the runtime.
        let per_shard = tokio::task::spawn_blocking(move || {
            read_snapshot(&dir, num_shards, &rocks_config, warm_enabled)
        })
        .await
        .map_err(|e| io::Error::other(format!("snapshot read task failed: {e}")))??;

        let total: usize = per_shard.iter().map(Vec::len).sum();
        self.install_per_shard(per_shard).await?;

        tracing::info!(
            keys = total,
            shards = num_shards,
            duration_ms = start.elapsed().as_millis() as u64,
            "Installed full-resync checkpoint into the live keyspace"
        );
        Ok(())
    }
}

/// Decode the primary's dataset blobs and bucket every key by *this* node's
/// partitioning.
///
/// Blocking: pure CPU over the whole keyspace. A blob that does not decode fails
/// the whole install — the dataset is installed as a complete replacement, so a
/// partially decoded one would silently drop keys the replica then claims to
/// hold. Expired keys are dropped at the source (the exporting shard), so
/// nothing here has to second-guess a TTL against the local clock.
fn route_dataset(blobs: Vec<Vec<u8>>, num_shards: usize) -> io::Result<Vec<Vec<SnapshotEntry>>> {
    let mut per_shard: Vec<Vec<SnapshotEntry>> = vec![Vec::new(); num_shards];
    for (index, blob) in blobs.iter().enumerate() {
        let entries = frogdb_core::persistence::read_entries(blob)
            .map_err(|e| io::Error::other(format!("dataset blob {index} did not decode: {e}")))?;
        for entry in entries {
            let shard = shard_for_key(&entry.key, num_shards);
            per_shard[shard].push(SnapshotEntry {
                key: entry.key,
                value: entry.value,
                metadata: entry.metadata,
            });
        }
    }
    Ok(per_shard)
}

/// Read every shard's entries out of the staged checkpoint DB.
///
/// Blocking: opens the staged RocksDB and scans it. The staged directory is a
/// complete database (the writer's commit point), so it is opened directly
/// rather than being renamed onto the live DB — the live DB converges through
/// the WAL writes each shard performs as it restores.
fn read_snapshot(
    staged_dir: &Path,
    num_shards: usize,
    rocks_config: &RocksConfig,
    warm_enabled: bool,
) -> Result<Vec<Vec<SnapshotEntry>>, InstallError> {
    let rocks = RocksStore::open_with_warm(staged_dir, num_shards, rocks_config, warm_enabled)
        .map_err(|e| classify_open_failure(e, num_shards, warm_enabled))?;

    let mut per_shard = Vec::with_capacity(num_shards);
    for shard_id in 0..num_shards {
        let mut sink = SnapshotSink::default();
        recover_shard_into(&rocks, shard_id, &mut sink)
            .map_err(|e| io::Error::other(format!("failed to read shard {shard_id}: {e}")))?;
        if warm_enabled {
            sink.absorb_warm(&rocks, shard_id)?;
        }
        per_shard.push(sink.entries);
    }
    Ok(per_shard)
}

/// Decide whether a failure to open the staged checkpoint is worth another
/// full resync.
///
/// Two of RocksDB's open failures are *structural*: the checkpoint's column
/// families say it was written by a node with a different shard count, or with
/// the warm tier in the other position. Neither is a property of this attempt —
/// the primary would cut and ship an identical checkpoint next time, and this
/// node would refuse it identically — so they are the terminal case (issue 23).
/// Everything else (a busy LOCK file, a torn transfer, an I/O error) can differ
/// on the next attempt and stays transient, which is also the safe default: a
/// transient failure mistaken for a terminal one strands a replica that would
/// have synced.
///
/// The `detail` names **both** sides, because the failure is a disagreement
/// between two nodes and neither one's config alone shows it. It is written for
/// the operator: it reaches `INFO replication`'s `master_sync_error` verbatim.
fn classify_open_failure(err: RocksError, num_shards: usize, warm_enabled: bool) -> InstallError {
    match err {
        RocksError::ShardCountMismatch { persisted, .. } => InstallError::Incompatible {
            detail: format!(
                "shard-count mismatch: the primary's checkpoint was written with {persisted} \
                 shard(s), this node is configured for {num_shards}. Replication between nodes \
                 with different shard counts requires them to agree; reconfigure this node to \
                 cluster.shard-count = {persisted} and restart, or replicate from a primary with \
                 {num_shards}."
            ),
        },
        RocksError::WarmTierMismatch { .. } => InstallError::Incompatible {
            detail: format!(
                "warm-tier mismatch: the primary's checkpoint carries tiered-storage column \
                 families, this node has tiered-storage.enabled = {warm_enabled}. Set \
                 tiered-storage.enabled = true on this node and restart, or replicate from a \
                 primary that runs without the warm tier."
            ),
        },
        other => InstallError::Transient(io::Error::other(format!(
            "failed to open staged checkpoint: {other}"
        ))),
    }
}

/// Collects a shard's snapshot entries in iteration order.
#[derive(Default)]
struct SnapshotSink {
    entries: Vec<SnapshotEntry>,
    keys: HashSet<Bytes>,
}

impl SnapshotSink {
    /// Fold the shard's warm-tier keys in as ordinary (hot) entries.
    ///
    /// A warm entry's value lives in the *staged* database, which is thrown away
    /// after the install, so it cannot stay warm on the receiving side — it is
    /// materialized instead, exactly like any other key of the snapshot. The
    /// receiving node re-demotes it under its own tiering policy. Hot wins over
    /// warm for the same key, matching boot recovery.
    ///
    /// Unlike boot recovery this never prunes stale warm entries from the source
    /// DB: the staged checkpoint is read-only input, not this node's database.
    fn absorb_warm(&mut self, rocks: &RocksStore, shard_id: usize) -> io::Result<()> {
        let now = clock::now();
        let iter = rocks
            .iter_warm_cf(shard_id)
            .map_err(|e| io::Error::other(format!("failed to read warm shard {shard_id}: {e}")))?;
        for (key, value) in iter {
            let Ok((val, metadata)) = deserialize(&value) else {
                tracing::warn!(
                    shard_id,
                    key = ?String::from_utf8_lossy(&key),
                    "Skipping undeserializable warm key in received checkpoint"
                );
                continue;
            };
            if metadata.expires_at.is_some_and(|at| at <= now) {
                continue;
            }
            let key = Bytes::copy_from_slice(&key);
            if self.contains(&key) {
                continue;
            }
            self.restore_entry(key, val, metadata);
        }
        Ok(())
    }
}

impl RestoreSink for SnapshotSink {
    fn restore_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata) {
        self.keys.insert(key.clone());
        self.entries.push(SnapshotEntry {
            key,
            value,
            metadata,
        });
    }

    /// Never called: warm entries are materialized as hot by
    /// [`SnapshotSink::absorb_warm`], which reads the value the warm recovery
    /// protocol does not hand to the sink.
    fn restore_warm_entry(&mut self, _key: Bytes, _metadata: KeyMetadata, _key_type: KeyType) {}

    fn contains(&self, key: &[u8]) -> bool {
        self.keys.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_shards::{
        drop_install_ack, fake_shards, keys as entry_keys, serve_install, text,
    };
    use frogdb_core::persistence::{CfTier, append_entry, serialize};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    /// A scratch directory that removes itself. The crate has no `tempfile`
    /// dependency and adding one would touch the workspace lockfile, which this
    /// test module has no business doing for four directories.
    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(tag: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "frogdb-replication-runtime-{}-{}-{}",
                tag,
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).expect("scratch dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// A key whose *hash* routes to `shard`, so a test can tell a key placed by
    /// the checkpoint's own partitioning apart from one re-hashed here.
    fn key_hashing_to(prefix: &str, shard: usize, num_shards: usize) -> String {
        (0..10_000)
            .map(|i| format!("{prefix}{i}"))
            .find(|k| shard_for_key(k.as_bytes(), num_shards) == shard)
            .expect("some suffix hashes to every shard")
    }

    /// A string entry, ready to be stored or framed.
    fn entry(value: &str, ttl: Option<Duration>) -> (Value, KeyMetadata) {
        let value = Value::string(value.to_string());
        let mut metadata = KeyMetadata::new(value.memory_size());
        metadata.expires_at = ttl.map(|d| Instant::now() + d);
        (value, metadata)
    }

    /// One primary shard's dataset blob.
    fn blob(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (key, value) in entries {
            let (value, metadata) = entry(value, None);
            append_entry(&mut out, key.as_bytes(), &value, &metadata);
        }
        out
    }

    fn new_installer(senders: Arc<Vec<ShardSender>>, warm: bool) -> LiveSnapshotInstaller {
        LiveSnapshotInstaller::new(senders, RocksConfig::default(), warm)
    }

    // ---- the live-dataset payload -------------------------------------------------

    // FM-REPLICATION-052
    /// The blobs are the *primary's* shards and say nothing about this node's
    /// partitioning, so every decoded key is re-routed through `shard_for_key`
    /// for this node's shard count — and none is dropped on the way.
    #[test]
    fn a_received_dataset_is_repartitioned_onto_this_nodes_shards() {
        let names: Vec<String> = (0..24).map(|i| format!("key:{i}")).collect();
        // Two primary shards' worth of blobs, split arbitrarily — the split is
        // the primary's business and must not survive into this node.
        let first: Vec<(&str, &str)> = names[..12].iter().map(|k| (k.as_str(), "v")).collect();
        let second: Vec<(&str, &str)> = names[12..].iter().map(|k| (k.as_str(), "v")).collect();

        let per_shard =
            route_dataset(vec![blob(&first), blob(&second)], 3).expect("well-formed blobs decode");

        assert_eq!(per_shard.len(), 3, "every shard of this node gets a slice");
        let landed: usize = per_shard.iter().map(Vec::len).sum();
        assert_eq!(landed, names.len(), "a re-partition must lose no key");
        for (shard, entries) in per_shard.iter().enumerate() {
            for e in entries {
                assert_eq!(
                    shard_for_key(&e.key, 3),
                    shard,
                    "{} landed on the wrong shard",
                    String::from_utf8_lossy(&e.key)
                );
                assert_eq!(text(&e.value), "v", "the value travels with the key");
            }
        }
        assert!(
            per_shard.iter().filter(|s| !s.is_empty()).count() > 1,
            "the fixture must actually span shards for this to mean anything"
        );
    }

    // FM-REPLICATION-052
    /// Every shard is installed, **including the ones the dataset has no keys
    /// for**: an install is a replace, so a skipped shard would keep its forked
    /// keys — the very bug this path exists to close.
    #[tokio::test]
    async fn every_shard_is_installed_including_the_ones_with_no_keys() {
        let mut shards = fake_shards(3);
        let installer = new_installer(shards.senders(), false);

        let on_zero = key_hashing_to("alpha", 0, 3);
        let on_two = key_hashing_to("gamma", 2, 3);
        let payload = vec![blob(&[(on_zero.as_str(), "one"), (on_two.as_str(), "two")])];

        let (installed, slices) = tokio::join!(installer.install_dataset(payload), async {
            let mut slices = Vec::new();
            for shard in 0..3 {
                slices.push(serve_install(shards.shard(shard)).await);
            }
            slices
        });

        installed.expect("every shard acked, so the install succeeds");
        assert_eq!(entry_keys(&slices[0]), vec![on_zero.clone()]);
        assert!(
            slices[1].is_empty(),
            "shard 1 holds none of the dataset's keys, and is still told so"
        );
        assert_eq!(entry_keys(&slices[2]), vec![on_two.clone()]);
        assert_eq!(text(&slices[0][0].value), "one");
        assert_eq!(text(&slices[2][0].value), "two");
    }

    // FM-REPLICATION-052
    /// A node with no shards wired cannot install a dataset, and says so rather
    /// than reporting a sync that installed nothing.
    #[tokio::test]
    async fn a_dataset_install_with_no_shards_wired_is_refused() {
        let installer = new_installer(Arc::new(Vec::new()), false);
        let err = installer
            .install_dataset(vec![blob(&[("k", "v")])])
            .await
            .expect_err("no shards means no install");
        assert!(
            err.to_string().contains("no shards wired"),
            "the refusal must be attributable, got {err}"
        );
    }

    // FM-REPLICATION-052
    /// A blob that does not decode fails the whole install: the dataset is
    /// installed as a complete replacement, so a partially decoded one would
    /// silently drop keys the replica then claims to hold.
    #[tokio::test]
    async fn a_blob_that_does_not_decode_fails_the_whole_install() {
        let mut shards = fake_shards(2);
        let installer = new_installer(shards.senders(), false);

        let err = installer
            .install_dataset(vec![blob(&[("k", "v")]), b"not a dataset".to_vec()])
            .await
            .expect_err("a corrupt blob must fail the install");
        assert!(
            err.to_string().contains("dataset blob 1 did not decode"),
            "the failing blob must be named, got {err}"
        );
        assert!(
            shards.untouched(0),
            "no shard may be replaced from a dataset that did not decode whole"
        );
        assert!(shards.untouched(1));
    }

    // FM-REPLICATION-052
    /// A shard that is gone, or that dies without acking, fails the install —
    /// the replica must not adopt the snapshot's offset over a keyspace only
    /// part of which was replaced.
    #[tokio::test]
    async fn a_shard_that_never_acks_the_install_fails_it() {
        let mut shards = fake_shards(3);
        shards.disconnect(1);
        let installer = new_installer(shards.senders(), false);
        let (installed, ()) = tokio::join!(
            installer.install_dataset(vec![blob(&[("k", "v")])]),
            async {
                serve_install(shards.shard(0)).await;
            }
        );
        let err = installed.expect_err("a gone shard must fail the install");
        assert!(err.to_string().contains("shard 1 is gone"), "got {err}");
        assert!(
            shards.untouched(2),
            "the install stops at the shard that failed"
        );

        let mut shards = fake_shards(2);
        let installer = new_installer(shards.senders(), false);
        let (installed, ()) = tokio::join!(
            installer.install_dataset(vec![blob(&[("k", "v")])]),
            async {
                serve_install(shards.shard(0)).await;
                drop_install_ack(shards.shard(1)).await;
            }
        );
        let err = installed.expect_err("a dropped ack must fail the install");
        assert!(
            err.to_string().contains("shard 1 dropped the install ack"),
            "got {err}"
        );
    }

    // ---- the staged-checkpoint payload --------------------------------------------

    // FM-REPLICATION-053
    /// A runtime checkpoint install reads **every** shard of the staged DB and
    /// hands each one to the shard of the same id: the staged database is the
    /// primary's partitioning already, so unlike the live-dataset path nothing
    /// is re-hashed here.
    #[tokio::test]
    async fn a_staged_checkpoint_is_read_shard_by_shard_and_installed_into_each() {
        let scratch = Scratch::new("checkpoint");
        // Placed in the checkpoint's shard 1 while its *hash* says shard 0, so
        // a re-route would be visible.
        let misplaced = key_hashing_to("beta", 0, 2);
        {
            let rocks =
                RocksStore::open_with_warm(scratch.path(), 2, &RocksConfig::default(), false)
                    .expect("staged checkpoint opens");
            let (value, metadata) = entry("one", None);
            rocks
                .put_tier(CfTier::Main, 0, b"alpha", &serialize(&value, &metadata))
                .unwrap();
            let (value, metadata) = entry("two", Some(Duration::from_secs(3600)));
            rocks
                .put_tier(
                    CfTier::Main,
                    1,
                    misplaced.as_bytes(),
                    &serialize(&value, &metadata),
                )
                .unwrap();
        }

        let mut shards = fake_shards(2);
        let install = new_installer(shards.senders(), false).into_installer();
        let payload = FullSyncPayload::StagedCheckpoint(scratch.path().to_path_buf());

        let (installed, slices) = tokio::join!(install(payload), async {
            let mut slices = Vec::new();
            for shard in 0..2 {
                slices.push(serve_install(shards.shard(shard)).await);
            }
            slices
        });

        installed.expect("a readable checkpoint installs");
        assert_eq!(entry_keys(&slices[0]), vec!["alpha".to_string()]);
        assert_eq!(text(&slices[0][0].value), "one");
        assert_eq!(
            entry_keys(&slices[1]),
            vec![misplaced.clone()],
            "the checkpoint's own partitioning is what a checkpoint install preserves"
        );
        assert!(
            slices[1][0].metadata.expires_at.is_some(),
            "a restored key keeps the TTL the checkpoint recorded for it"
        );
    }

    // FM-REPLICATION-053
    /// The staged DB's warm tier is materialized as ordinary hot entries — its
    /// values live in a database that is thrown away after the install, so a
    /// key left warm would be a key with nowhere to read its value from. A hot
    /// copy always wins, and is never duplicated by its warm shadow.
    #[tokio::test]
    async fn warm_tier_keys_are_materialized_and_a_hot_copy_wins() {
        let scratch = Scratch::new("warm");
        {
            let rocks =
                RocksStore::open_with_warm(scratch.path(), 1, &RocksConfig::default(), true)
                    .expect("staged checkpoint opens");
            let (value, metadata) = entry("hot-wins", None);
            rocks
                .put_tier(CfTier::Main, 0, b"dup", &serialize(&value, &metadata))
                .unwrap();
            let (value, metadata) = entry("warm-loses", None);
            rocks
                .put_warm(0, b"dup", &serialize(&value, &metadata))
                .unwrap();
            let (value, metadata) = entry("only-warm", None);
            rocks
                .put_warm(0, b"warm_only", &serialize(&value, &metadata))
                .unwrap();
        }

        let mut shards = fake_shards(1);
        let install = new_installer(shards.senders(), true).into_installer();
        let payload = FullSyncPayload::StagedCheckpoint(scratch.path().to_path_buf());
        let (installed, slice) = tokio::join!(install(payload), serve_install(shards.shard(0)));
        installed.expect("a readable checkpoint installs");

        let mut found = entry_keys(&slice);
        found.sort();
        assert_eq!(
            found,
            vec!["dup".to_string(), "warm_only".to_string()],
            "a warm-only key must arrive materialized, and a shadowed one must \
             not arrive twice"
        );
        let dup = slice.iter().find(|e| e.key == "dup").unwrap();
        assert_eq!(
            text(&dup.value),
            "hot-wins",
            "the hot copy is the live one; the warm shadow is stale"
        );
    }

    // FM-REPLICATION-053
    /// An expired warm key is not resurrected by the install. It is logically
    /// dead on the primary, so shipping it would make the replica serve a key
    /// the primary does not have.
    #[tokio::test]
    async fn an_expired_warm_key_is_not_resurrected() {
        let scratch = Scratch::new("warm-expiry");
        {
            let rocks =
                RocksStore::open_with_warm(scratch.path(), 1, &RocksConfig::default(), true)
                    .expect("staged checkpoint opens");
            // Expiring shortly *after* it is written and before it is read: the
            // expiry is anchored to a real deadline rather than to an `Instant`
            // subtraction, which is not guaranteed to be representable.
            let (value, metadata) = entry("already-dead", Some(Duration::from_millis(50)));
            rocks
                .put_warm(0, b"expired", &serialize(&value, &metadata))
                .unwrap();
            let (value, metadata) = entry("still-alive", Some(Duration::from_secs(3600)));
            rocks
                .put_warm(0, b"unexpired", &serialize(&value, &metadata))
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(120));

        let mut shards = fake_shards(1);
        let install = new_installer(shards.senders(), true).into_installer();
        let payload = FullSyncPayload::StagedCheckpoint(scratch.path().to_path_buf());
        let (installed, slice) = tokio::join!(install(payload), serve_install(shards.shard(0)));
        installed.expect("a readable checkpoint installs");

        assert_eq!(
            entry_keys(&slice),
            vec!["unexpired".to_string()],
            "a warm key past its deadline is dropped; one with time left is kept"
        );
    }

    // FM-REPLICATION-053
    // FM-REPLICATION-061
    /// A checkpoint this node cannot read is refused **loudly**, and no shard is
    /// touched. Two shapes reach here in production — a staged directory that is
    /// not a database at all, and one whose layout this node cannot adopt (a
    /// different shard count, or a warm tier this node has disabled). Every one
    /// of them must fail the sync rather than install what could be read: an
    /// install is a replace, so a partial read is `FLUSHALL` plus a subset.
    ///
    /// The two are refused differently, which is the whole of issue 23: an
    /// unopenable directory might open on the next attempt, a layout
    /// disagreement never will. The classification is asserted here, at the seam
    /// that makes it, and the reconnect loop's half of the contract is asserted
    /// in `frogdb-replication` (`a_geometry_mismatch_is_refused_once_and_not_retried`).
    #[tokio::test]
    async fn a_checkpoint_this_node_cannot_read_is_refused_and_touches_no_shard() {
        // Not a database.
        let scratch = Scratch::new("not-a-db");
        let not_a_dir = scratch.path().join("regular-file");
        std::fs::write(&not_a_dir, b"garbage").unwrap();
        let mut shards = fake_shards(1);
        let install = new_installer(shards.senders(), false).into_installer();
        let err = install(FullSyncPayload::StagedCheckpoint(not_a_dir))
            .await
            .expect_err("an unopenable checkpoint must fail the install");
        assert!(
            matches!(err, InstallError::Transient(_)),
            "a directory that is not a database says nothing about the two nodes' \
             configuration, so the next attempt may well succeed: got {err}"
        );
        assert!(
            err.to_string().contains("failed to open staged checkpoint"),
            "got {err}"
        );
        assert!(
            shards.untouched(0),
            "a checkpoint that could not be read must not clear the keyspace"
        );

        // A checkpoint cut by a primary with a different shard count. The
        // live-dataset path re-partitions (FM-REPLICATION-052); this one cannot,
        // and refusing is the only safe direction — opening it anyway would
        // misroute every key under the new hash space.
        let scratch = Scratch::new("wrong-shards");
        {
            let rocks =
                RocksStore::open_with_warm(scratch.path(), 2, &RocksConfig::default(), false)
                    .expect("staged checkpoint opens");
            let (value, metadata) = entry("one", None);
            rocks
                .put_tier(CfTier::Main, 0, b"alpha", &serialize(&value, &metadata))
                .unwrap();
        }
        let mut shards = fake_shards(1);
        let install = new_installer(shards.senders(), false).into_installer();
        let err = install(FullSyncPayload::StagedCheckpoint(
            scratch.path().to_path_buf(),
        ))
        .await
        .expect_err("a checkpoint with another shard count must fail the install");
        let InstallError::Incompatible { detail } = &err else {
            panic!("a shard-count disagreement can never be fixed by retrying: got {err}")
        };
        assert!(
            detail.contains("written with 2 shard(s)") && detail.contains("configured for 1"),
            "the operator has to see both sides — neither node's config alone shows the \
             disagreement: {detail}"
        );
        assert!(shards.untouched(0));

        // A checkpoint carrying a warm tier, landing on a node with tiering
        // disabled. Loud, and known: the two nodes must agree on the toggle for
        // the checkpoint path (see the module note on this limitation).
        let scratch = Scratch::new("warm-mismatch");
        {
            let rocks =
                RocksStore::open_with_warm(scratch.path(), 1, &RocksConfig::default(), true)
                    .expect("staged checkpoint opens");
            let (value, metadata) = entry("warm", None);
            rocks
                .put_warm(0, b"warm", &serialize(&value, &metadata))
                .unwrap();
        }
        let mut shards = fake_shards(1);
        let install = new_installer(shards.senders(), false).into_installer();
        let err = install(FullSyncPayload::StagedCheckpoint(
            scratch.path().to_path_buf(),
        ))
        .await
        .expect_err("a warm-tier checkpoint must not be half-installed on a hot-only node");
        let InstallError::Incompatible { detail } = &err else {
            panic!("a warm-tier disagreement can never be fixed by retrying: got {err}")
        };
        assert!(
            detail.contains("tiered-storage.enabled = false"),
            "the detail must name this node's side of the toggle: {detail}"
        );
        assert!(shards.untouched(0));
    }
}
