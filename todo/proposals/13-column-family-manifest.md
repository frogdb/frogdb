# Proposal: Column-Family Manifest

Status: proposed
Date: 2026-06-13

## Problem

`RocksStore::open_with_warm` decides *which* RocksDB column families to open by reading the
**current** `warm_enabled` flag, not by reading what the persisted database actually contains. The
set of CFs that "must exist" is the most load-bearing recovery invariant the store has — open too
few and RocksDB refuses the database; open the wrong ones and keys are misrouted — yet that set is
recomputed from a boolean argument on every boot. The seam is shallow: a four-argument function
hides a decision (reconcile config against on-disk reality) that it never actually makes.

The warm-tier toggle is the same `must-open-all-CFs` invariant that the shard-count guard already
hard-errors on (`RocksStore::open`, fixed in `95da0256`), but for warm it is **unguarded**. The
flag-derived CF list and the persisted reality diverge the moment an operator flips
`tiered-storage.enabled`:

| Direction | What happens today | File:line |
|-----------|--------------------|-----------|
| warm **on → off** | `warm_cf_names` is rebuilt empty, so the persisted `tiered_warm_*` CFs are excluded from `all_cf_names` and never added to the open descriptor set. RocksDB requires *every* existing CF to be opened, so the open fails with the cryptic "Column families not opened" error — startup-blocking, no FrogDB-level diagnosis. **Sticky**: once warm has ever been enabled, the data dir can never reopen with warm off. | `mod.rs:74-80`, `:84-88`, `:118-125`, `:126-134` |
| warm **off → on** | The warm CFs are absent from `existing_cfs`, so they fall through to the create loop and are created fresh and empty. This currently **succeeds** — legitimate first-enable. | `mod.rs:135-143` |

This corrects the proposal brief's hypothesis that the inverse direction produces "duplicate CFs":
it does not. `db.create_cf` is guarded by `if !existing_cfs.contains(cf_name)` (`mod.rs:136`), and
the fresh-DB branch builds descriptors from a duplicate-free `all_cf_names`, so no duplicate-CF path
exists. The real asymmetry is one-directional: **on → off is a cryptic hard failure; off → on is a
benign create.** Both, however, share the root defect — the open decision is driven by a flag
instead of being reconciled against persisted state.

The trigger is operator-reachable, not theoretical. Production wires `warm_enabled` straight from
config: `recovery/mod.rs:71` sets `warm_enabled: config.tiered_storage.enabled`, and
`recovery/shards.rs:40-45` passes it into `open_with_warm`. `tiered-storage.enabled` is a plain
`bool` config field (`config/src/tiered.rs:14-16`) an operator can flip in `frogdb.toml` between
restarts. Flip it off after warm data exists and the next boot dies on RocksDB's internal error
message.

**Deletion test (the diagnostic, not the fix):** the warm-toggle path has **zero** test coverage.
`tests.rs:110-125` (`test_warm_cf_reopen`) reopens warm → warm only; `tests.rs:214-232`
(`test_shard_count_validation_ignores_warm_cfs`) varies shard count, never the warm flag. There is
no test that reopens a warm-written dir with warm disabled — the failing case ships untested.

## Current state

### CF-name construction is flag-derived (`mod.rs:73-88`)

```rust
let cf_names: Vec<String> = (0..num_shards).map(|i| format!("shard_{}", i)).collect();
let warm_cf_names: Vec<String> = if warm_enabled {
    (0..num_shards)
        .map(|i| format!("tiered_warm_{}", i))
        .collect()
} else {
    Vec::new()
};
let search_meta_cf_names: Vec<String> = (0..num_shards)
    .map(|i| format!("search_meta_{}", i))
    .collect();
let all_cf_names: Vec<&String> = cf_names
    .iter()
    .chain(warm_cf_names.iter())
    .chain(search_meta_cf_names.iter())
    .collect();
```

`all_cf_names` — the set the open path treats as ground truth — is whatever the `warm_enabled`
argument says it should be. The persisted `tiered_warm_*` CFs are invisible to it when the flag is
off.

### The open / skip loop trusts that flag-derived set (`mod.rs:89-143`)

```rust
let db_exists = path.exists() && path.join("CURRENT").exists();
let db = if db_exists {
    let existing_cfs = DB::list_cf(&db_opts, path).unwrap_or_default();

    // ... shard-count guard here (see below) ...

    let mut cf_descriptors: Vec<ColumnFamilyDescriptor> = Vec::new();
    if existing_cfs.contains(&"default".to_string()) {
        cf_descriptors.push(ColumnFamilyDescriptor::new("default", Options::default()));
    }
    for cf_name in &all_cf_names {
        if existing_cfs.contains(cf_name) {
            cf_descriptors.push(ColumnFamilyDescriptor::new(
                cf_name.as_str(),
                cf_opts.clone(),
            ));
        }
    }
    let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
        &db_opts,
        path,
        cf_descriptors,
    )
    .map_err(|e| { /* ... */ })?;
    for cf_name in &all_cf_names {
        if !existing_cfs.contains(cf_name) {
            debug!(cf_name = %cf_name, "Creating column family");
            db.create_cf(cf_name.as_str(), &cf_opts).map_err(|e| { /* ... */ })?;
        }
    }
    db
} else { /* fresh-DB branch: descriptors from all_cf_names */ };
```

The descriptor list is the **intersection** of `existing_cfs` and `all_cf_names`. A persisted
`tiered_warm_*` that is *not* in `all_cf_names` (because warm is configured off) is in neither the
open list nor the create list — it is dropped on the floor, and RocksDB then rejects the open
because that CF exists on disk but was not opened.

### The precedent to mirror: the shard-count hard-error guard (`mod.rs:99-112`)

```rust
let persisted_shards = count_persisted_shards(&existing_cfs);
if persisted_shards != 0 && persisted_shards != num_shards {
    error!(
        path = %path_str,
        persisted = persisted_shards,
        configured = num_shards,
        "RocksDB shard count mismatch; aborting recovery to avoid data loss"
    );
    return Err(RocksError::ShardCountMismatch {
        path: path_str.clone(),
        persisted: persisted_shards,
        configured: num_shards,
    });
}
```

This is exactly the shape the warm-tier toggle needs: enumerate the persisted layout, compare it
against config, and `return Err(...)` *before* opening when they disagree. `count_persisted_shards`
(`mod.rs:317-326`) already derives the persisted shard count from `shard_*` CF names and
deliberately ignores `tiered_warm_*` and `search_meta_*` prefixes — the persisted state is already
the source of truth for shards. Warm is the one persisted invariant that still trusts the flag.

## Proposed design

Introduce a `ColumnFamilyManifest` module that owns one question — *"which column families must this
store open?"* — and answers it by **reconciling persisted state against config**, returning the
required CF set or a hard error. `open_with_warm` stops deriving CFs from a flag and instead asks
the manifest for a single, already-reconciled decision.

### New module: `frogdb-server/crates/persistence/src/rocks/manifest.rs`

```rust
/// The set of column families a `RocksStore` must open, derived from persisted
/// state reconciled against configuration — never from a single live flag.
///
/// This is the one place that knows the CF layout (`shard_<n>`,
/// `tiered_warm_<n>`, `search_meta_<n>`, `default`) and the invariants that bind
/// it to what is on disk. `open_with_warm` consumes a `required()` list; it no
/// longer decides anything.
pub(crate) struct ColumnFamilyManifest {
    shards: Vec<String>,       // shard_<n>        — persisted count is authoritative
    warm: Vec<String>,         // tiered_warm_<n>  — present iff warm is active
    search_meta: Vec<String>,  // search_meta_<n>  — always present
    has_default: bool,         // default          — present on every non-fresh DB
}

impl ColumnFamilyManifest {
    /// Reconcile the configured layout against what is actually persisted.
    ///
    /// `existing` is the live CF list from `DB::list_cf`. An empty slice means a
    /// fresh database: config wins, nothing to reconcile. For an existing
    /// database every persisted invariant is checked against config, and any
    /// drift that would orphan or misroute data is a hard error — the store must
    /// open *all* persisted CFs or refuse to open at all.
    pub(crate) fn reconcile(
        path_str: &str,
        existing: &[String],
        num_shards: usize,
        warm_enabled: bool,
    ) -> Result<Self, RocksError> {
        let shard = |i| format!("shard_{i}");
        let warm = |i| format!("tiered_warm_{i}");
        let meta = |i| format!("search_meta_{i}");

        // Fresh DB: stamp the layout from config.
        if existing.is_empty() {
            return Ok(Self {
                shards: (0..num_shards).map(shard).collect(),
                warm: if warm_enabled { (0..num_shards).map(warm).collect() } else { Vec::new() },
                search_meta: (0..num_shards).map(meta).collect(),
                has_default: false,
            });
        }

        // Invariant 1 — shard count (today's guard, now owned here).
        let persisted_shards = count_persisted_shards(existing);
        if persisted_shards != 0 && persisted_shards != num_shards {
            return Err(RocksError::ShardCountMismatch {
                path: path_str.to_owned(),
                persisted: persisted_shards,
                configured: num_shards,
            });
        }

        // Invariant 2 — warm tier (the previously-missing guard). A dir that
        // persisted tiered_warm_* CFs cannot reopen with warm disabled: those
        // CFs would be left unopened and RocksDB refuses the whole DB.
        // off -> on is *not* an error: it is a legitimate first-enable that
        // creates the warm CFs.
        let persisted_warm = existing.iter().any(|cf| is_warm_cf(cf));
        if persisted_warm && !warm_enabled {
            return Err(RocksError::WarmTierMismatch { path: path_str.to_owned() });
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

    /// Names this store later resolves at runtime (`cf_handle`, `warm_cf_handle`).
    pub(crate) fn shard_names(&self) -> &[String] { &self.shards }
    pub(crate) fn warm_names(&self) -> &[String] { &self.warm }
    pub(crate) fn search_meta_names(&self) -> &[String] { &self.search_meta }
}

fn is_warm_cf(name: &str) -> bool {
    name.strip_prefix("tiered_warm_")
        .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
}
```

`count_persisted_shards` moves into this module beside `is_warm_cf`: prefix-aware CF classification
becomes one module's job, so a future `search_meta` or new family invariant is added here, not
re-derived at the open site.

### New error variant (`config.rs`, beside `ShardCountMismatch`)

```rust
#[error(
    "warm-tier mismatch: data directory {path} was written with the warm tier (tiered \
     storage) enabled, but the server is configured with it disabled; refusing to start \
     because the persisted tiered_warm_* column families would be left unopened (re-enable \
     tiered-storage.enabled, or migrate the warm tier out before disabling it)"
)]
WarmTierMismatch { path: String },
```

### Before / after: `open_with_warm`

Before — the flag-derived construction (`mod.rs:73-88`) plus the open/skip loop (`mod.rs:114-143`),
two places that each re-derive "which CFs", with the warm invariant enforced nowhere.

After — one reconciled decision, then a single open:

```rust
let existing_cfs = if db_exists {
    // A failed enumeration cannot be silently treated as "no CFs": that bypasses
    // every reconcile invariant. See Correctness flags (additional bug).
    DB::list_cf(&db_opts, path).map_err(RocksError::from)?
} else {
    Vec::new()
};

let manifest =
    ColumnFamilyManifest::reconcile(&path_str, &existing_cfs, num_shards, warm_enabled)?;

let db = if db_exists {
    let cf_descriptors: Vec<ColumnFamilyDescriptor> = manifest
        .required()
        .filter(|cf| existing_cfs.iter().any(|e| e == cf))
        .map(|cf| {
            let opts = if cf == "default" { Options::default() } else { cf_opts.clone() };
            ColumnFamilyDescriptor::new(cf, opts)
        })
        .collect();
    let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, path, cf_descriptors)
        .map_err(|e| { error!(path = %path_str, error = %e, "Failed to open RocksDB"); RocksError::from(e) })?;
    for cf in manifest.required() {
        if cf != "default" && !existing_cfs.iter().any(|e| e == cf) {
            db.create_cf(cf, &cf_opts).map_err(RocksError::from)?;
        }
    }
    db
} else {
    let cf_descriptors: Vec<ColumnFamilyDescriptor> = manifest
        .required()
        .map(|cf| ColumnFamilyDescriptor::new(cf, cf_opts.clone()))
        .collect();
    DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, path, cf_descriptors)
        .map_err(|e| { error!(path = %path_str, error = %e, "Failed to open RocksDB"); RocksError::from(e) })?
};

Ok(Self {
    db,
    num_shards,
    cf_names: manifest.shard_names().to_vec(),
    warm_enabled,
    warm_cf_names: manifest.warm_names().to_vec(),
    search_meta_cf_names: manifest.search_meta_names().to_vec(),
})
```

`open_with_warm` no longer constructs CF names, no longer chains three vecs, no longer decides skip
vs. create from a flag. It asks the manifest, gets a reconciled answer (or an error), and opens.

### Why this is the right depth

- **Locality.** Every CF-layout invariant — shard count, warm toggle, prefix classification, and any
  future family — lives in `manifest.rs`. The open path becomes a transcription of `required()`. A
  change to "what CFs must exist" is a one-module edit, not a surgery threaded through name
  construction (`mod.rs:73-88`), the open loop (`:114-134`), and the create loop (`:135-143`).
- **Leverage.** The manifest is a pure function over `(existing CFs, num_shards, warm_enabled)`. It
  is exhaustively unit-testable with no RocksDB, no tempdir, no I/O — every reconcile outcome is a
  cheap table-driven assertion, where today each case costs a full open against a real on-disk
  database.
- **Two adapters prove the seam is real.** A module that hides exactly one invariant is a
  hypothetical seam — you cannot tell whether the boundary generalizes. The manifest folds **two**
  independent invariants (shard count *and* warm toggle) behind one `reconcile` call. Two adapters
  through the same interface is the evidence that the seam is real and not a wrapper drawn around a
  single special case.
- **Deletion test.** Standing up the manifest deletes real artifacts: the flag-derived `cf_names` /
  `warm_cf_names` / `search_meta_cf_names` / `all_cf_names` construction (`mod.rs:73-88`) and the
  inline shard-count guard (`mod.rs:99-112`) both collapse into `reconcile`. `count_persisted_shards`
  moves rather than duplicates. If the migration could not remove the inline construction and guard,
  the manifest would be the wrong shape — a layer on top instead of the owner.

## Migration plan

Both phases compile and pass tests independently; Phase 1 is shippable on its own as the correctness
fix.

**Phase 1 — the immediate `WarmTierMismatch` guard (correctness fix).** No new module. Add the
`WarmTierMismatch { path }` variant to `RocksError` (`config.rs`), and insert a guard immediately
after the shard-count guard (`mod.rs:112`), mirroring it exactly:

```rust
let persisted_warm = existing_cfs.iter().any(|cf| {
    cf.strip_prefix("tiered_warm_")
        .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
});
if persisted_warm && !warm_enabled {
    error!(
        path = %path_str,
        "RocksDB warm-tier toggle mismatch; aborting recovery to avoid orphaning tiered_warm_* data"
    );
    return Err(RocksError::WarmTierMismatch { path: path_str.clone() });
}
```

Also harden the enumeration: change `DB::list_cf(&db_opts, path).unwrap_or_default()` (`mod.rs:91`)
to propagate the error (see Correctness flags — the `unwrap_or_default` silently disables *both*
guards). Add the warm-toggle tests (below). `just check frogdb-persistence && just test
frogdb-persistence`.

**Phase 2 — extract `ColumnFamilyManifest`.** Move `count_persisted_shards` + `is_warm_cf` into
`manifest.rs`, add `reconcile`/`required`, and rewrite `open_with_warm` to call it (before/after
above). The two inline guards and the name-construction block are deleted, now expressed as the two
invariants inside `reconcile`. Phase 1's tests become regression coverage; add the manifest unit
tests. `just check frogdb-persistence && just test frogdb-persistence && just lint
frogdb-persistence`.

## Testing impact

Today: **zero** tests touch the warm toggle, and the only CF-layout validation runs through full
RocksDB opens.

Phase 1 (RocksStore-level, mirroring `tests.rs:153-232`):

- **`test_warm_toggle_on_then_off_fails`** — open warm-on, write warm data, drop; reopen warm-off →
  `Err(RocksError::WarmTierMismatch { .. })` with the path. The failing case that ships untested.
- **`test_warm_toggle_off_then_on_succeeds`** — open warm-off, write hot data, drop; reopen warm-on
  → `Ok`, warm ops work, hot data intact. Pins that the guard does **not** over-rotate and reject
  legitimate first-enable.
- **`test_warm_toggle_on_then_on_intact`** — already covered by `test_warm_cf_reopen`
  (`tests.rs:110-125`); keep as the warm-preserving baseline.

Phase 2 (manifest unit tests, no RocksDB — the leverage payoff):

- Pure `reconcile(existing, num_shards, warm_enabled)` table: fresh dir → config layout; matching
  shard count → required set; mismatched shard count → `ShardCountMismatch`; persisted-warm +
  warm-off → `WarmTierMismatch`; persisted-warm + warm-on → warm in required set; no-warm + warm-on
  → warm in required set (created); `default` present ⇒ in `required()`. Each is an in-memory
  assertion, where the equivalent RocksStore test costs a tempdir and a real open.

The existing shard-count suite (`tests.rs:153-232`) is unchanged: Phase 2 routes those assertions
through the manifest without altering behavior, so they double as the migration's safety net.

## Correctness flags

**Warm-tier toggle breaks reopen (the assigned correctness fix).** `open_with_warm` derives
`warm_cf_names` from the current `warm_enabled` argument (`mod.rs:74-80`) and excludes the persisted
`tiered_warm_*` CFs from `all_cf_names` (`mod.rs:84-88`) and from the open descriptor set
(`mod.rs:118-125`) when warm is configured off. A data directory created with tiered storage enabled
(`recovery/mod.rs:71` → `recovery/shards.rs:40-45`, driven by `tiered-storage.enabled`,
`config/src/tiered.rs:14-16`) then reopened with it disabled fails the open (`mod.rs:126-134`) with
RocksDB's cryptic "Column families not opened" error — the same `must-open-all-CFs` invariant the
shard-count guard (`mod.rs:99-112`, fixed `95da0256`) protects, still unguarded for warm. The failure
is sticky (any warm-enabled era locks the dir into warm-on) and untested (`tests.rs` covers warm→warm
and shard-count, never the toggle). Fix: hard-error with `WarmTierMismatch` on the on→off direction;
allow off→on as a first-enable.

**Additional bug — `DB::list_cf(...).unwrap_or_default()` silently disables the reconcile guards
(`mod.rs:91`).** When `DB::list_cf` fails (transient I/O, permissions, a damaged `MANIFEST`),
`unwrap_or_default()` yields an empty `existing_cfs` even though `db_exists` was already true
(`mod.rs:89`). Two downstream effects, both bad:
1. The shard-count guard is bypassed — `count_persisted_shards(&[]) == 0`, and the guard's
   `persisted_shards != 0` check (`mod.rs:100`) is false, so a real shard-count mismatch sails
   through unvalidated.
2. The open path then builds an empty descriptor set (no `default`, no shards — `mod.rs:115-125`)
   and asks RocksDB to open a non-empty database with nothing, surfacing as a confusing open failure
   instead of an actionable "could not enumerate column families".
A transient enumeration failure should be a hard, named error, not silently coerced into "fresh,
empty database". This defect was inherited by the shard-count guard and now also gates the warm
guard, since both depend on a trustworthy `existing_cfs`. Phase 1 fixes it by propagating the
`list_cf` error.

**Minor — implicit, per-branch `default`-CF handling (`mod.rs:115-117` vs `:146-149`).** The
existing-DB branch opens `default` only `if existing_cfs.contains("default")`; the fresh-DB branch
never lists `default` and relies on RocksDB's implicit default-CF creation. This works today but is
the kind of unstated special-case the manifest centralizes (`has_default` + `required()`), removing a
latent trap if the open path is ever refactored to treat the two branches uniformly. Not a runtime
bug; folded into Phase 2 rather than flagged for an independent fix.

## Risks / open questions

- **Is config toggling ever meant to be supported?** Phase 1 makes on→off a hard error, treating the
  flag as write-once-per-dir (like shard count). If operators are expected to disable tiered storage
  on an existing dir, that needs an explicit migration: drain/promote all `tiered_warm_*` data back
  to hot, then drop the warm CFs (`drop_cf`) before flipping the flag — an export/reimport path the
  manifest could own as a one-shot `migrate_disable_warm`. Decide whether that is a supported
  operation or a permanent "warm is sticky" stance before documenting the error's remediation text.
- **`default`-CF handling.** Should the fresh-DB branch open `default` explicitly rather than leaning
  on RocksDB's implicit creation, so `required()` is the single source of truth in both branches?
  Leaning toward yes (uniform branches), but it is a behavior-adjacent change worth its own
  verification that a fresh open still succeeds.
- **Forward-compat for new CF families.** A future family (e.g. a second warm tier, vector indexes)
  adds a new prefix. The manifest must classify it; an unrecognized persisted prefix should probably
  be a hard error ("this dir was written by a newer FrogDB") rather than silently ignored — but that
  is a stricter stance than today's "ignore unknown prefixes" (`count_persisted_shards` filters to
  `shard_*`). Pick the policy deliberately.
- **Derive from a persisted manifest file vs. enumerate live CFs.** This proposal reconciles against
  the live CF list (`DB::list_cf`), matching the shard-count guard's existing approach — no new
  on-disk metadata. A written `manifest.json` (CF set + feature flags + format version) would make
  intent explicit and survive RocksDB-internal CF churn, but adds a file to keep consistent with the
  actual CFs (a second source of truth, the thing this proposal otherwise avoids). Enumeration is the
  lower-risk default; revisit only if reconciliation needs to record intent the CF names cannot carry
  (e.g. "warm was deliberately drained, not never-enabled").
- **Snapshot/checkpoint interaction.** A checkpoint/full-sync copies the live CF set, so a warm-on
  primary stages warm CFs to a replica; if that replica boots warm-off, the new guard fires on the
  installed checkpoint. Confirm the replica derives `warm_enabled` from the same config invariant as
  the primary (it does today via `tiered-storage.enabled`), so a checkpoint never lands a warm dir on
  a warm-off node.
