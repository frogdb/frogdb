# Proposal: Snapshot Creation Stager

Status: proposed
Date: 2026-06-17

## Problem

Snapshot creation is the durability-critical write half of FrogDB's checkpoint machinery: it cuts a
RocksDB checkpoint at a sequence number, copies the search-index sidecar, writes `metadata.json`,
and atomically promotes a staged directory (`.snapshot_NNNNN.tmp` → `snapshot_NNNNN`) into the live
snapshot set, finally repointing the `latest` symlink. The **install/recovery** half of this same
machinery — taking a staged checkpoint and swapping it in for the live database on boot — is already
a deep module: it validates the staged directory *before* touching the live data dir, refuses an
incomplete checkpoint rather than moving the live db aside, and has **8 crash-window tests** pinning
each intermediate on-disk state (`persistence/src/rocks/checkpoint.rs:26-73`,
`persistence/src/rocks/tests.rs:259-465`; recovery orchestrated per proposal 06). The install side
treats "a half-written database on disk" as a first-class hazard and is engineered around it.

The **creation** side is the mirror image and gets none of that care. It is a single ~22-line
nested closure (`persistence/src/snapshot/rocks_coordinator.rs:210-231`) — the body of a
`tokio::spawn` → `spawn_blocking` — written as run-on one-statement-per-line code with single-letter
bindings (`rs/sd/ip/sc/ec/lst/lm/mt/ms/ns/psh/dd/ce/td/fd/cp/se/ss/md/sb/mj/mtp`). It is the most
filesystem-destructive write path in the persistence crate and it has **zero tests** of its own
(`persistence/src/snapshot/tests.rs` covers only the no-op coordinator, metadata structs, and
`cleanup_old_snapshots` — never the create pipeline).

Because the pipeline has no seam, two correctness bugs hide in plain sight inside that closure:

1. **A search-index copy failure is `tracing::warn!`-and-continue.** If `copy_search_indexes` fails
   partway, the snapshot still proceeds: metadata is marked complete, the directory is promoted, and
   `latest` is repointed to a snapshot that is **missing its search indexes**. On restore this is a
   silent data gap — the keyspace comes back, the indexes do not, with no signal that the snapshot
   was incomplete.
2. **A metadata-write or final-rename failure leaks the temp directory forever.** Only the
   `create_checkpoint` failure path removes the `.snapshot_NNNNN.tmp` staging dir. Every later
   failure (`serde` serialize, `metadata.json` write, the two `std::fs::rename`s) propagates with
   `?` *without* removing the temp dir — which by then holds a full checkpoint copy (potentially
   gigabytes). `cleanup_old_snapshots` matches only the `snapshot_` prefix, never `.snapshot_…tmp`,
   so nothing ever reclaims these. Disk usage grows by one orphaned checkpoint per failed snapshot.

Both bugs share one root cause: cleanup is a single hand-placed `remove_dir_all` sitting on exactly
one of five failure paths, and the run-on closure makes that asymmetry invisible to a reader. The
contract "a snapshot is all-or-nothing — either a complete directory is promoted or nothing is left
behind" is asserted nowhere; it is supposed to emerge from the reader correctly tracking five `?`s
and one `if let Err` across a wall of single-letter code.

**Deletion test.** The staging/cleanup/retry/metrics/`pre_snapshot_hook` logic is *real* — it earns
its keep; deleting it would scatter staging, atomic promotion, scheduled re-runs, and metrics back
across the coordinator. The problem is not that the behaviour is unnecessary; it is that the
behaviour has no module. It lives in the *wrong shape* — one opaque closure rather than a named
pipeline whose interface states the all-or-nothing invariant the install side already enjoys.

This proposal owns the **snapshot creation contract**: a `SnapshotStager` staged pipeline whose
cleanup invariant is enforced by an RAII guard (so it cannot be placed on only one path), whose
search-copy failure aborts instead of silently shipping an incomplete snapshot, and whose stages are
individually failure-injectable — giving the creation side the crash-window test depth the install
side already has.

## Current state

### The creation pipeline — one closure, single-letter bindings (`rocks_coordinator.rs:210-231`)

The async driver spawns a loop; each iteration runs the pre-snapshot hook, then does the actual work
inside `spawn_blocking`. Quoted verbatim — the run-on, one-statement-per-line shape is itself the
evidence:

```rust
tokio::spawn(async move { let mut ce = ie; let mut start = Instant::now(); loop {
    { let h = psh.read().unwrap().clone(); if let Some(h) = h { h().await; } }
    let rs2 = rs.clone(); let sd2 = sd.clone(); let dd2 = dd.clone(); let se = ce;
    let result = tokio::task::spawn_blocking(move || {
        let sn = format!("snapshot_{:05}", se); let td = sd2.join(format!(".snapshot_{:05}.tmp", se)); let fd = sd2.join(&sn); let cp = td.join("checkpoint");
        if let Err(e) = std::fs::create_dir_all(&cp) { return Err(SnapshotError::Io(e)); }
        let seq = rs2.latest_sequence_number();
        if let Err(e) = rs2.create_checkpoint(&cp) { let _ = std::fs::remove_dir_all(&td); return Err(SnapshotError::Internal(format!("Failed to create checkpoint: {}", e))); }
        let ss = dd2.join("search"); if ss.exists() && let Err(e) = Self::copy_search_indexes(&ss, &td.join("search")) { tracing::warn!(error = %e, "Failed to copy search indexes to snapshot"); }
        let mut md = SnapshotMetadataFile::new(se, seq, ns); let mut sb = Self::calculate_dir_size(&cp).unwrap_or(0); let ssp = td.join("search"); if ssp.exists() { sb += Self::calculate_dir_size(&ssp).unwrap_or(0); } md.mark_complete(0, sb);
        let mj = serde_json::to_string_pretty(&md).map_err(|e| SnapshotError::Internal(format!("Failed to serialize metadata: {}", e)))?;
        let mtp = td.join("metadata.json.tmp"); std::fs::write(&mtp, &mj)?; std::fs::rename(&mtp, td.join("metadata.json"))?; std::fs::rename(&td, &fd)?;
        if let Err(e) = Self::update_latest_symlink(&sd2, &sn) { tracing::warn!(error = %e, "Failed to update latest symlink"); }
        if let Err(e) = Self::cleanup_old_snapshots(&sd2, ms) { tracing::warn!(error = %e, "Failed to cleanup old snapshots"); }
        Ok((md, seq, fd))
    }).await;
    // ...match result { metrics + tracing }... ip.store(false); if !sc.swap(false) { break; } ...
} }.instrument(tracing::info_span!("snapshot_create")));
```

Reading the five exit paths of the blocking closure as a table makes the asymmetry — invisible above
— explicit:

| Stage | Line | On failure | Removes `.snapshot_NNNNN.tmp`? |
|---|---|---|---|
| `create_dir_all(&cp)` | 215 | `return Err(Io)` | no (tmp barely exists yet) |
| `create_checkpoint(&cp)` | 217 | `remove_dir_all(&td)` then `return Err` | **yes** (the only path that cleans up) |
| `copy_search_indexes` | 218 | `tracing::warn!`, **continue** | n/a — proceeds to ship an incomplete snapshot |
| `serde_json::to_string_pretty` | 220 | `?` → return Err | **no — leak** |
| `write` + 2× `rename` (metadata, promote) | 221 | `?` → return Err | **no — leak** |

The checkpoint copy is the largest artifact on the path, so the leaking paths leak the most.

### The pieces the closure orchestrates (real, and worth keeping)

These helpers are correct and carry real behaviour — the staging machinery the deletion test says
earns its keep; they are just invoked from inside the opaque closure rather than from a named
pipeline:

```rust
// rocks_coordinator.rs:135-167 — retention: keep the newest `ms` snapshot_NNNNN dirs, delete the rest
pub(crate) fn cleanup_old_snapshots(sd: &std::path::Path, ms: usize) -> Result<(), SnapshotError> { ... }

// rocks_coordinator.rs:168-181 — atomic latest pointer via .latest.tmp → rename (unix) / write (non-unix)
fn update_latest_symlink(sd: &std::path::Path, sn: &str) -> Result<(), SnapshotError> { ... }

// rocks_coordinator.rs:101-134 — copy the search sidecar, hard-linking data files where possible
fn copy_search_indexes(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> { ... }
```

The async driver (`rocks_coordinator.rs:226-231`) also owns the **scheduled-snapshot retry loop**
(re-run if `scheduled` was set during the in-flight snapshot), the `in_progress`/`scheduled` atomic
handshake, and the metrics emission (`frogdb_snapshot_duration_seconds`,
`frogdb_snapshot_size_bytes`, `frogdb_snapshot_last_timestamp`, `frogdb_snapshot_in_progress`,
`frogdb_snapshot_epoch`, and `frogdb_persistence_errors_total{type=snapshot}` on failure/panic).
All of this is real and stays — but the *blocking pipeline* it wraps is what needs a seam.

### The install side it should mirror (`rocks/checkpoint.rs:36-73`)

The complement — installing a staged checkpoint on boot — already treats partial on-disk state as
the central hazard and validates before mutating the live dir:

```rust
// Refuse to install a staged directory that is not a complete RocksDB database. ...
// Validate *before* touching the live dir so the original data is left untouched on refusal.
if !crd.join("CURRENT").exists() {
    tracing::error!(checkpoint_dir = %crd.display(), "Staged checkpoint is missing its CURRENT manifest; refusing to install");
    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, /* ... */));
}
```

This path has 8 crash-window tests (`rocks/tests.rs:298-465`): absent marker, no parent, happy-path
install + backup, first-sync with no existing db, incomplete-dir refusal preserves data, crash after
backup recovers, idempotent after success, pre-existing backup left intact. The creation side
deserves the same depth, and its temp-dir staging makes the same crash-window testing possible.

## Proposed design

Introduce `SnapshotStager`: one private module that owns the blocking creation pipeline as named,
ordered stages, with all-or-nothing cleanup enforced by an RAII temp-dir guard. The async
spawn/loop, the `pre_snapshot_hook` await, the scheduled-retry handshake, and metrics stay in the
coordinator; the stager is the pure(ish) blocking core that `spawn_blocking` runs. Callers (the
coordinator) state *create a snapshot for this epoch*; "what counts as complete" and "clean up on
failure" live behind the seam, not in five scattered `?`s.

### The seam

```rust
/// Owns the staged creation of one snapshot. All-or-nothing: every stage builds
/// under `tmp`; only a fully-formed snapshot is atomically promoted to `final_dir`.
/// Any early return removes `tmp` (via the guard's Drop), so a failed snapshot
/// never leaks disk and an *incomplete* snapshot is never installed.
struct SnapshotStager {
    snapshot_dir: PathBuf,      // <snapshot_dir>
    tmp: PathBuf,               // <snapshot_dir>/.snapshot_NNNNN.tmp
    final_dir: PathBuf,         // <snapshot_dir>/snapshot_NNNNN
    name: String,              // "snapshot_NNNNN" (for the latest symlink)
    data_dir: PathBuf,          // source of the `search/` sidecar
    epoch: u64,
    num_shards: usize,
    max_snapshots: usize,
}

/// RAII cleanup. Removes `path` on Drop unless `commit()` ran. This is the single
/// owner of "remove the temp dir on failure" — it replaces the lone hand-placed
/// `remove_dir_all` that today fires on exactly one of five failure paths, so the
/// invariant can no longer be partially applied.
struct TmpDirGuard { path: PathBuf, committed: bool }
impl TmpDirGuard {
    fn new(path: &Path) -> Self { Self { path: path.to_path_buf(), committed: false } }
    fn commit(mut self) { self.committed = true; }
}
impl Drop for TmpDirGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

impl SnapshotStager {
    /// The whole blocking pipeline. Each `?` aborts the snapshot AND removes the
    /// temp dir via the guard. Returns the completed metadata on success.
    fn run(self, rocks: &RocksStore) -> Result<SnapshotMetadata, SnapshotError> {
        // Reclaim any stale tmp from a crashed run at this epoch, then claim a clean one.
        let _ = std::fs::remove_dir_all(&self.tmp);
        let guard = TmpDirGuard::new(&self.tmp);

        let seq = self.stage_checkpoint(rocks)?;   // create_checkpoint into tmp/checkpoint
        self.copy_indexes()?;                       // FATAL now — no index-less snapshot ships
        let md = self.finalize_metadata(seq)?;      // size accounting + tmp/metadata.json (via .tmp+rename)
        self.install()?;                            // atomic rename tmp -> final_dir
        guard.commit();                             // success: keep the promoted dir

        // Post-install, best-effort: the snapshot is already durable in `final_dir`.
        // These touch only the *pointer* and *retention*, not the snapshot's contents.
        if let Err(e) = self.update_latest_symlink() {
            tracing::warn!(error = %e, "Failed to update latest symlink after snapshot install");
        }
        if let Err(e) = self.cleanup_old_snapshots() {
            tracing::warn!(error = %e, "Failed to clean up old snapshots after install");
        }
        Ok(md.to_metadata())
    }

    /// Create the RocksDB checkpoint under tmp/checkpoint at the current sequence.
    fn stage_checkpoint(&self, rocks: &RocksStore) -> Result<u64, SnapshotError> {
        let cp = self.tmp.join("checkpoint");
        std::fs::create_dir_all(&cp)?;
        let seq = rocks.latest_sequence_number();
        rocks.create_checkpoint(&cp)
            .map_err(|e| SnapshotError::Internal(format!("Failed to create checkpoint: {e}")))?;
        Ok(seq)
    }

    /// Copy the search-index sidecar into the snapshot. Failure aborts the snapshot.
    fn copy_indexes(&self) -> Result<(), SnapshotError> {
        let src = self.data_dir.join("search");
        if src.exists() {
            Self::copy_search_indexes(&src, &self.tmp.join("search"))?; // was warn-and-continue
        }
        Ok(())
    }

    /// Compute the size, write metadata.json atomically (.tmp + rename).
    fn finalize_metadata(&self, seq: u64) -> Result<SnapshotMetadataFile, SnapshotError> { /* ... */ }

    /// Atomic promotion: rename tmp -> final_dir.
    fn install(&self) -> Result<(), SnapshotError> {
        std::fs::rename(&self.tmp, &self.final_dir)?;
        Ok(())
    }

    fn update_latest_symlink(&self) -> Result<(), SnapshotError> { /* ...existing... */ }
    fn cleanup_old_snapshots(&self) -> Result<(), SnapshotError> { /* ...existing... */ }
}
```

### Before / after: the coordinator's blocking body

Before — one closure, single-letter bindings, cleanup on one of five paths
(`rocks_coordinator.rs:213-225`):

```rust
let result = tokio::task::spawn_blocking(move || {
    let sn = format!("snapshot_{:05}", se); let td = sd2.join(format!(".snapshot_{:05}.tmp", se)); /* ...20 more single-letter lines... */
    Ok((md, seq, fd))
}).await;
```

After — the coordinator hands a value object to a named pipeline; staging, atomicity, and cleanup
are owned behind `run()`:

```rust
let result = tokio::task::spawn_blocking(move || {
    SnapshotStager {
        tmp: sd2.join(format!(".snapshot_{:05}.tmp", se)),
        final_dir: sd2.join(format!("snapshot_{:05}", se)),
        name: format!("snapshot_{:05}", se),
        snapshot_dir: sd2,
        data_dir: dd2,
        epoch: se,
        num_shards: ns,
        max_snapshots: ms,
    }
    .run(&rs2)
}).await;
```

The async loop (`rocks_coordinator.rs:226-231`) is unchanged: it still matches `result`, records the
duration/size/timestamp/error metrics, flips `in_progress`/`scheduled`, and re-runs for a scheduled
snapshot. Only the *contents* of `spawn_blocking` move behind the seam.

### Decisions baked into the seam

- **Search-copy failure is fatal.** An incomplete snapshot is worse than none: on abort the guard
  removes the temp dir, the *previous* good `snapshot_NNNNN` and its `latest` symlink are untouched
  (the stager never repoints `latest` before `install()`), so recovery still has a complete, if
  older, snapshot. Shipping an index-less snapshot, by contrast, replaces the last good one and
  produces a silent restore gap. Aborting preserves the invariant "every installed snapshot is
  complete," which is exactly what the install side already enforces from the read direction.
- **Symlink and old-snapshot-cleanup failures stay non-fatal.** They run *after* `install()`, when
  the snapshot is already a complete, durably-renamed directory. A failed `latest` update means the
  pointer lags by one snapshot (the new dir is still a valid recovery target on disk); a failed
  retention sweep means one extra old dir lingers. Neither corrupts or loses data, so neither should
  fail the snapshot — matching today's warn-and-continue for these two steps.
- **`TmpDirGuard`, not a hand-placed `remove_dir_all`.** The bug is structural: cleanup is a
  statement a reader must remember to place on every path. An RAII guard makes "no path leaks" the
  default and "keep it" the single explicit `commit()` after success — the same shape the install
  side uses for its before-mutation validation: make the safe behaviour the one you get for free.
- **Stale-tmp reclaim.** `run()` removes any pre-existing `.snapshot_NNNNN.tmp` at the same epoch
  before staging, so a tmp dir orphaned by a pre-fix crash (or a panic between guard construction
  paths) is reclaimed on the next attempt rather than wedging the epoch.

### Why this is the right depth

- **Locality.** The all-or-nothing invariant is one Drop impl plus one `commit()`, not five `?`s a
  reader must audit. "What does a complete snapshot require?" is the body of `run()`, top to bottom,
  in five named stages — versus a 22-line single-letter closure. Changing what staging means (e.g.
  adding a WAL-tail copy or fsync barrier) is a new stage method and one line in `run()`.
- **Leverage.** The seam deletes the entire class of "which failure path forgot to clean up?" bug
  (of which the temp-dir leak is the live instance) and the "did this path check completeness?" bug
  (the search-copy gap). It also unlocks the creation-side crash-window tests that are impossible
  today: each stage is a method that can be made to fail in isolation.
- **Not a new layer.** This deepens an existing seam rather than wrapping one. `cleanup_old_snapshots`,
  `update_latest_symlink`, and `copy_search_indexes` already exist as methods; the stager pulls the
  staging order, the atomic promotion, and the cleanup invariant under one roof and forbids the
  coordinator from open-coding them. The async/loop/metrics half — which is genuinely about
  scheduling and observability, not staging — stays in the coordinator.
- **Deletion test.** Removing `SnapshotStager` scatters staging, atomic promotion, completeness, and
  cleanup back into the coordinator closure — exactly today's shape. That the module *can't* be
  deleted without regressing the structure is the signal the seam is in the right place.

### Minor fold-in: the dead `OnWriteHook` trait

`OnWriteHook` (`persistence/src/snapshot/handle.rs:44-51`) is exported from the snapshot module and
re-exported through both `persistence/src/lib.rs:19` and `core/src/lib.rs:95`, but it is dead: the
only implementor is `NoopOnWriteHook` (marked `#[allow(dead_code)]`), there is **no construction
site** for `NoopOnWriteHook` anywhere in the tree, and there are **no `.on_write(` call sites**
(verified by grep). It advertises a snapshot-time copy-on-write hook that was never wired up. Since
this proposal touches the snapshot module's seams, fold in its removal (delete the trait, the noop
impl, and the two re-exports) — or, if a CoW snapshot hook is actually planned, this is the moment to
state that and give it a real instantiation. Low priority; noted for the same broom-stroke.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Phases 0 and 3 are behaviour-
preserving; Phases 1 and 2 are the two correctness fixes and change behaviour deliberately.

1. **Phase 0 — extract the closure into `SnapshotStager`.** Add `snapshot/stager.rs` with the
   `SnapshotStager` value object and the named stage methods (`stage_checkpoint`, `copy_indexes`,
   `finalize_metadata`, `install`, `update_latest_symlink`, `cleanup_old_snapshots`). Move
   `cleanup_old_snapshots`/`update_latest_symlink`/`copy_search_indexes`/`calculate_dir_size` onto
   it (keep `pub(crate) cleanup_old_snapshots` so its existing tests still call it). The coordinator
   builds the struct and calls `run()`; the async spawn/loop/hook/metrics are unchanged. Reproduce
   today's behaviour exactly for now (search copy still warn-and-continue, no guard yet).
   `just check frogdb-server`.
2. **Phase 1 — add `TmpDirGuard`; fix the temp-dir leak (flag 2).** Introduce the guard in `run()`
   so every early return removes `.snapshot_NNNNN.tmp`; drop the lone hand-placed `remove_dir_all`
   in `stage_checkpoint` (the guard now covers it). Add the stale-tmp reclaim at the top of `run()`.
   Behaviour change: a failed snapshot no longer leaks a checkpoint-sized temp dir.
3. **Phase 2 — make `copy_indexes` fatal; fix the incomplete-snapshot gap (flag 1).** Replace the
   `tracing::warn!`-and-continue with `?` so a search-copy failure aborts the snapshot (the guard
   removes tmp, the prior good snapshot remains). Behaviour change: incomplete snapshots are never
   installed.
4. **Phase 3 — tests.** Add the creation-side crash-window suite (below), mirroring the install
   side's 8. Each stage is now failure-injectable.
5. **Phase 4 — fold-in cleanup.** Delete the dead `OnWriteHook`/`NoopOnWriteHook` and the two
   re-exports (or wire a real hook). Optionally add a startup sweep that reclaims orphaned
   `.snapshot_*.tmp` dirs left by crashes from before Phase 1.

## Testing impact

Snapshot *creation* becomes testable without a running coordinator or a real workload — synthesize a
small `RocksStore` + data dir, run a `SnapshotStager`, and assert on the on-disk result. The install
side has 8 such tests; the creation side has zero. New tests, mirroring that depth:

- **Happy path.** Run the stager → `snapshot_NNNNN/{checkpoint,search,metadata.json}` exists,
  `metadata.json` is complete (`is_complete()`), `.snapshot_NNNNN.tmp` is gone, `latest` points at
  the new dir, `size_bytes` reflects checkpoint + search.
- **Checkpoint failure aborts cleanly.** Inject a `create_checkpoint` failure → `run()` errors, no
  `.snapshot_NNNNN.tmp` and no `snapshot_NNNNN` left behind, `latest` (if any) unchanged.
- **Search-copy failure aborts (flag 1 regression).** Make `copy_search_indexes` fail (e.g.
  unreadable source entry) → `run()` errors, temp dir removed, the *previous* good snapshot and its
  `latest` symlink untouched. Pins "no index-less snapshot is ever installed."
- **Metadata-write / final-rename failure leaves no leak (flag 2 regression).** Force the metadata
  write or the promote rename to fail → `run()` errors AND `.snapshot_NNNNN.tmp` is removed by the
  guard. This is the test the leak slips past today.
- **Crash-window: tmp present from a prior failed run.** Pre-create `.snapshot_NNNNN.tmp` with junk
  → `run()` reclaims it and produces a clean snapshot (no `Directory not empty` wedge).
- **Post-install non-fatal.** Make `update_latest_symlink` / `cleanup_old_snapshots` fail → `run()`
  still returns `Ok` with the snapshot durably installed; only a warning is logged.
- **Retention unchanged.** The existing `cleanup_old_snapshots` tests (`snapshot/tests.rs:104-136`)
  keep passing — retention behaviour is preserved, just relocated onto the stager.

## Risks / open questions

- **`latest` lag after a non-fatal symlink failure.** If `update_latest_symlink` fails post-install,
  the new (complete) snapshot exists on disk but `latest` still points at the previous one. Recovery
  reads `latest` (`rocks_coordinator.rs:62-85`), so it would pick the older snapshot — correct but
  stale. Acceptable (no data loss; the data is durable), but worth a note: a future recovery could
  fall back to the highest-numbered complete `snapshot_NNNNN` when `latest` is missing or older.
- **Hard-link sidecar across the promote rename.** `copy_search_indexes` hard-links search data files
  from the live `data_dir/search` into the temp dir where possible (`rocks_coordinator.rs:124-129`).
  The promote is a `rename` within `snapshot_dir`, so the links survive; but if `snapshot_dir` and
  `data_dir` are on different filesystems, hard-link fails and it falls back to `copy` (already
  handled). The stager must keep that fallback; no change, but the cross-FS case deserves a test.
- **`num_keys` is always recorded as `0`.** `mark_complete(0, sb)` (`rocks_coordinator.rs:219`)
  hardcodes the key count — `SnapshotMetadataFile.num_keys` is never populated on the create path.
  Out of scope for the staging fix, but the stager's `finalize_metadata` is the natural home to
  compute it (e.g. summing per-shard counts) if/when that field is made meaningful.
- **fsync / durability barrier.** Neither the current closure nor this proposal fsyncs the temp dir
  or its parent before/after the promote rename, so a power loss between the rename and the OS
  flushing the directory entry could lose a "completed" snapshot. This is pre-existing behaviour;
  the stager makes the promote a single named `install()` method, which is the right place to add a
  directory fsync later. Flagged, not fixed here.
- **Concurrency is already serialized.** `start_snapshot` gates on the `in_progress` atomic
  (`rocks_coordinator.rs:185-191`) and the epoch is fetch-add-incremented, so two stagers never
  target the same `snapshot_NNNNN`. The stager assumes this single-writer discipline; it does not
  add its own lock. If snapshotting ever becomes concurrent, the per-epoch tmp naming already
  isolates the staging dirs, but `latest`/cleanup would need a policy.

## Correctness flags

1. **Incomplete snapshot installed on search-copy failure — CONFIRMED.** At
   `rocks_coordinator.rs:218` a `copy_search_indexes` failure is `tracing::warn!`-and-continue; the
   pipeline proceeds to `mark_complete`, writes `metadata.json`, promotes the dir, and repoints
   `latest`. The result is a snapshot marked complete (`is_complete()` true) but missing some or all
   of its search-index sidecar — a silent data gap surfaced only on restore. Worse, `size_bytes`
   then reflects a partially-copied `search/` (`rocks_coordinator.rs:219` sums whatever made it),
   so the metadata under-reports without flagging incompleteness. Fix: make the search copy fatal
   (`copy_indexes()?`), so the snapshot aborts and the previous complete snapshot remains the
   recovery source.

2. **Temp-dir disk leak on metadata/rename failure — CONFIRMED.** Only the `create_checkpoint`
   failure path removes `.snapshot_NNNNN.tmp` (`rocks_coordinator.rs:217`). The `serde_json`
   serialize (`:220`), the `metadata.json` write, and the two `std::fs::rename`s (`:221`) all
   propagate with `?` and **never** remove the temp dir — which by then contains a full RocksDB
   checkpoint copy. `cleanup_old_snapshots` matches only the `snapshot_` prefix
   (`rocks_coordinator.rs:147`), never `.snapshot_…tmp`, so these orphans are never reclaimed. Disk
   usage grows by one checkpoint per failed snapshot until manual intervention. Fix: route every
   early return through `TmpDirGuard` so cleanup is the default; reclaim stale tmp dirs at the start
   of `run()`.

3. **The run-on single-line closure hides both bugs — NOTE (readability, root cause).** The pipeline
   is one ~22-line closure with single-letter bindings
   (`rs/sd/ip/sc/ec/lst/lm/mt/ms/ns/psh/dd/ce/td/fd/cp/se/ss/md/sb/mj/mtp`) and one statement per
   line (`rocks_coordinator.rs:210-231`). The cleanup asymmetry (flag 2) and the warn-and-continue
   (flag 1) are invisible at this density — a reader cannot see that four of five failure paths
   leak, or that one stage continues on error while the rest abort. This is not itself a runtime bug,
   but it is why flags 1 and 2 survived review. Fix: the named-stage `SnapshotStager` makes the
   exit-path behaviour readable and the cleanup invariant structural.

4. **Dead `OnWriteHook` trait — CONFIRMED (code-smell, not a runtime bug).** `OnWriteHook`
   (`handle.rs:44-51`) is exported and re-exported (`persistence/src/lib.rs:19`,
   `core/src/lib.rs:95`) but has only a `#[allow(dead_code)] NoopOnWriteHook` impl, no construction
   site, and no `.on_write(` callers (all verified by grep). It advertises a snapshot-time
   copy-on-write hook that was never wired up. Fix: delete the trait + noop impl + re-exports, or
   wire a real implementation if a CoW snapshot hook is intended.
</content>
</invoke>
