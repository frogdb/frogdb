# Proposal: Search Index Lifecycle Manager

Status: proposed
Date: 2026-06-15

## Problem

The lifecycle of a search index — FT.CREATE, FT.DROP, FT.ALTER, FT.INFO, and startup recovery —
has no owning module. It is an *emergent* behavior: five files in three crates each re-type the same
conventions (the on-disk directory layout, the reserved-key namespace, the create-then-persist
ordering, the JSON metadata schema) and the feature works only because they happen to agree. The
interface lives nowhere. There is no `IndexLifecycleManager` type a caller can point at and say "this
owns search indexes." That is the definition of a shallow seam: the contract is spread thin across
every call site instead of concentrated behind one.

The lifecycle **fails the deletion test** twice over:

- *No single site to delete.* There is nothing you could remove to take out "search index
  lifecycle" — it is woven through a connection handler, two core shard modules, the per-concern
  persist helpers, and the inline recovery loop in `shards.rs`.
- *Removing any one site silently breaks the feature across commands.* Delete the persist block in
  `create.rs` and FT.CREATE still returns OK, but every created index vanishes on restart. Change
  the search-dir path layout in `create.rs` but not in the recovery loop and FT.CREATE keeps
  working while recovery silently opens a *fresh empty* index at the wrong path
  (`ShardSearchIndex::open` is `open_or_create`, `search/src/index.rs:213-218`). The create path and
  the recover path are split across crates with no shared contract, so they drift independently.

This also subsumes the documented DEFERRED item from proposal 06: "Search-index recovery deferred
(non-`Send` per-shard handles) — documented at the call site"
(`todo/proposals/INDEX.md:46-47`). The recovery orchestrator left this one site inline precisely
because per-shard index handles are not `Send`; this proposal gives that site a real home.

Verified evidence — one lifecycle concern, smeared across files:

| Lifecycle concern | Crate / file | file:line |
|---|---|---|
| Parse schema + broadcast FT.CREATE to all shards | server `connection/handlers/search/create.rs` | `:12-52` |
| Open tantivy dir + initial scan + commit + persist meta (create) | core `shard/search/create.rs` | `:34-115` |
| Drop: alias cleanup + delete meta + destroy files | core `shard/search/index_mgmt.rs` | `:8-44` |
| Info: assemble RediSearch-compatible introspection view | core `shard/search/index_mgmt.rs` | `:46-111` |
| Alter: reopen schema + re-index + commit + persist meta | core `shard/search/index_mgmt.rs` | `:123-227` |
| Per-concern persist helpers (alias / dict / config) | core `shard/search/mod.rs` | `:23-57` |
| Synonym persist (writes whole def) | core `shard/search/synonyms.rs` | `:32-40` |
| Startup recovery: iterate meta, open, restore alias/dict/config | server `server/shards.rs` | `:221-314` |
| Restore setter (four loose maps) | core `shard/worker.rs` | `:434-445` |

The same **on-disk path layout** is re-typed in three places —
`data_dir().join("search").join(name).join(format!("shard_{}", shard_id))` — at
`create.rs:34-40`, `index_mgmt.rs:28-32` (drop), and `shards.rs:243-248` (recovery). The **reserved
key namespace** (`__aliases__`, `__config__`, `__dict__:<name>`, and the `__`-prefix skip rule) is
split between the writers (`mod.rs:23-57`) and the recovery reader (`shards.rs:238`, `:285-307`),
agreeing only by convention with no shared constant. Each concern re-implements schema validation,
directory management, RocksDB I/O, and state assembly from scratch.

## Current state

### Create: persist is best-effort, OK is unconditional (`core/shard/search/create.rs:51-65`)

```rust
        // Persist to RocksDB search_meta CF
        if let Some(ref rocks) = self.persistence.rocks_store
            && let Ok(json) = serde_json::to_vec(&def)
            && let Err(e) =
                rocks.put_search_meta(self.identity.shard_id, index_name.as_bytes(), &json)
        {
            tracing::error!(error = %e, "Failed to persist search index metadata");
        }

        self.search.indexes.insert(index_name.clone(), idx);

        // Index existing keys matching prefix (unless SKIPINITIALSCAN)
        if def.skip_initial_scan {
            return vec![(Bytes::from_static(b"__ft_create__"), Response::ok())];
        }
```

The `put_search_meta` error is logged and execution continues; the index is inserted into the live
map and OK is returned (`:115`, also `:64`). A serialization failure short-circuits the `&&`-chain
and is not even logged. The initial-scan commit at `:110-112` is the same pattern — `if let Err(e) =
idx.commit() { tracing::error!(...) }` then OK. Persistence is not part of the success contract.

### Alter: schema applied in-memory first, durability best-effort (`core/shard/search/index_mgmt.rs:214-227`)

```rust
        if let Err(e) = idx.commit() {
            tracing::error!(error = %e, "Failed to commit after FT.ALTER re-index");
        }

        // Persist updated definition to RocksDB
        if let Some(ref rocks) = self.persistence.rocks_store
            && let Ok(json) = serde_json::to_vec(&new_def)
            && let Err(e) = rocks.put_search_meta(self.identity.shard_id, name.as_bytes(), &json)
        {
            tracing::error!(error = %e, "Failed to persist altered search index metadata");
        }

        vec![(Bytes::from_static(b"__ft_alter__"), Response::ok())]
```

The new schema was already applied in memory via `reopen_with_def` at `:168-169`. If the persist
fails, the live index serves the new field while RocksDB still holds the *old* definition.

### Recovery: inline, per-index errors swallowed (`server/server/shards.rs:241-282`)

```rust
                match serde_json::from_slice::<frogdb_search::SearchIndexDef>(&value_bytes) {
                    Ok(def) => {
                        let search_dir = config
                            .persistence
                            .data_dir
                            .join("search")
                            .join(&index_name)
                            .join(format!("shard_{}", shard_id));
                        match frogdb_search::ShardSearchIndex::open(def, &search_dir) {
                            Ok(idx) => {
                                info!(shard_id, index = %index_name, "Recovered search index");
                                indexes.insert(index_name, idx);
                            }
                            Err(e) => {
                                warn!(shard_id, index = %index_name, error = %e,
                                    "Failed to recover search index");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(shard_id, index = %index_name, error = %e,
                            "Failed to deserialize search index definition");
                    }
                }
```

A failed `open` or a corrupt `SearchIndexDef` produces a `warn!` and is dropped; the metadata stays
in RocksDB, so the divergence (meta says the index exists, the live map says it does not) is
permanent and unsignaled. The recovered maps are then handed to the worker through a four-argument
setter, `restore_search_state(indexes, aliases, dictionaries, config)`
(`core/shard/worker.rs:434-445`) — the only "contract" the worker exposes for its entire search
state is four bare `HashMap`s.

## Proposed design

One per-shard `IndexLifecycleManager` that owns every index's lifecycle behind a single seam. It
absorbs the loose `SearchState` maps, the path-layout convention, the reserved-key constants, and
the scattered persist helpers, and it enforces one load-bearing invariant:

> **Index creation persists before returning OK.** FT.CREATE / FT.ALTER report success only after
> the definition is durable in the `search_meta` CF. A persistence failure rolls back the in-memory
> and on-disk state and returns an error — never OK.

### The seam

```rust
// frogdb-server/crates/core/src/shard/search/lifecycle.rs

/// Owns the lifecycle of every search index on one shard: create, drop, alter,
/// info, and crash recovery. The single seam between command execution / startup
/// and (tantivy + usearch) on disk + the RocksDB `search_meta` column family.
///
/// Per-shard by construction: it holds non-`Send` tantivy `IndexWriter` and
/// `usearch::Index` handles (`search/src/index.rs:177-208`), so it is built and
/// lives inside the shard worker and is never shipped through the coordinator.
pub struct IndexLifecycleManager {
    shard_id: usize,
    data_dir: PathBuf,
    rocks: Option<Arc<RocksStore>>,
    indexes: HashMap<String, ShardSearchIndex>,
    aliases: HashMap<String, String>,
    dictionaries: HashMap<String, HashSet<String>>,
    config: HashMap<String, String>,
}

/// Errors carry enough to map to a RediSearch reply *and* to distinguish a
/// rolled-back operation from a durable one.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("Index already exists: {0}")]
    AlreadyExists(String),
    #[error("Unknown index name: {0}")]
    NotFound(String),
    #[error("Duplicate field in schema: {0}")]
    DuplicateField(String),
    #[error("schema/open error: {0}")]
    Schema(#[from] SearchError),
    /// Definition could not be made durable; the operation was rolled back.
    #[error("failed to persist index definition: {0}")]
    Persist(#[source] RocksError),
}

impl IndexLifecycleManager {
    /// FT.CREATE. Validates, opens the tantivy dir, runs the initial scan, and
    /// **persists the definition before reporting success**. On any persist
    /// failure the half-built index is rolled back (live map + on-disk dir) and
    /// `Err(Persist)` is returned. The `scan` closure indexes existing keys.
    pub fn create(
        &mut self,
        def: SearchIndexDef,
        scan: impl FnOnce(&mut ShardSearchIndex),
    ) -> Result<(), LifecycleError>;

    /// FT.DROPINDEX. Removes aliases, deletes metadata, and destroys files as
    /// one unit. The live-map entry is removed only once both disk artifacts are
    /// gone; partial failure is reported, not swallowed.
    pub fn drop_index(&mut self, name: &str) -> Result<(), LifecycleError>;

    /// FT.ALTER. Expands the schema, re-indexes, and persists the new definition
    /// before returning OK — same persist-before-OK invariant as `create`.
    pub fn alter(
        &mut self,
        name: &str,
        new_fields: Vec<FieldDef>,
        scan: impl FnOnce(&mut ShardSearchIndex),
    ) -> Result<(), LifecycleError>;

    /// FT.INFO. Assembles the introspection view from the live index.
    pub fn info(&self, name: &str) -> Result<&ShardSearchIndex, LifecycleError>;

    /// Startup recovery. Rebuilds all indexes + aliases/dictionaries/config from
    /// the `search_meta` CF. Idempotent; reports a per-index outcome instead of
    /// silently dropping corrupt entries.
    pub fn recover(
        rocks: Arc<RocksStore>,
        data_dir: PathBuf,
        shard_id: usize,
    ) -> RecoveryResult;
}

/// What recovery produces: the assembled manager plus an outcome per index so
/// the caller can surface corruption rather than lose it to a log line.
pub struct RecoveryResult {
    pub manager: IndexLifecycleManager,
    pub outcomes: Vec<(String, RecoveryOutcome)>,
}

pub enum RecoveryOutcome {
    Recovered { num_docs: u64 },
    /// Metadata present, tantivy directory failed to open. Quarantined: kept in
    /// the `search_meta` CF, absent from the live map, surfaced to the caller.
    Corrupt(SearchError),
    /// Metadata bytes did not deserialize into a `SearchIndexDef`.
    Undeserializable(String),
}
```

The single on-disk path convention now lives in one private method
(`fn index_dir(&self, name: &str) -> PathBuf`) and the reserved keys become module constants
(`ALIASES_KEY`, `CONFIG_KEY`, `DICT_PREFIX`, `RESERVED_PREFIX`), consumed by both the writers and
`recover`. The three copies of the path layout and the split namespace collapse to one source of
truth.

### How the non-`Send` constraint is handled

Proposal 06 left search recovery inline because shipping open per-shard index handles through the
coordinator's `RecoveredState` runs into `Send` (tantivy `IndexWriter`, `usearch::Index`). The
manager resolves this without changing that boundary: `recover` is an *associated constructor* that
runs at worker-spawn time — exactly where `shards.rs` already opens these handles — and the manager
is then installed into the worker it was built for. Handles never cross a thread boundary; the
coordinator still calls a per-shard function, but that function now returns a typed, testable result
instead of mutating four loose maps. The seam is a real type where there was only a convention.

### Before / after: the recovery loop in `shards.rs`

Before: ~90 lines of inline iteration, open, deserialize, and per-concern restore
(`server/server/shards.rs:221-314`), with errors swallowed into `warn!`.

After: the worker-spawn site calls the seam and surfaces outcomes:

```rust
        if ctx.config.persistence.enabled
            && let Some(rocks) = ctx.rocks_store.clone()
        {
            let data_dir = ctx.config.persistence.data_dir.clone();
            let result = IndexLifecycleManager::recover(rocks, data_dir, shard_id);
            for (name, outcome) in &result.outcomes {
                match outcome {
                    RecoveryOutcome::Recovered { .. } => {}
                    RecoveryOutcome::Corrupt(e) => warn!(
                        shard_id, index = %name, error = %e,
                        "search index quarantined (metadata kept, index unavailable)"),
                    RecoveryOutcome::Undeserializable(e) => warn!(
                        shard_id, index = %name, error = %e,
                        "search index metadata undeserializable"),
                }
            }
            worker.install_search_manager(result.manager);
        }
```

The free function `recover_search_indexes` (`shards.rs:221-314`) and the four-map
`restore_search_state` setter (`worker.rs:434-445`) are deleted; the worker holds an
`IndexLifecycleManager` and the `execute_ft_*` methods become thin adapters that call it.

### Why this is the right depth

- **Locality.** The path layout, the reserved-key namespace, the persist-before-OK invariant, and
  the recovery error policy live in one module. "Where do I add FT.ALTER DROP, or index rebuild, or
  a new persisted concern?" has one answer instead of "edit five files and hope they stay in sync."
- **Leverage.** One ~250-line module owns what is today spread across nine sites in three crates,
  and it fixes a class of silent-data-loss bugs (below) at the seam rather than per call site. Every
  future lifecycle operation inherits the durability invariant for free.
- **Deletion test.** Delete `lifecycle.rs` and you lose exactly the search index lifecycle — nothing
  else. Today there is no such artifact to delete, which is the diagnosis. After the change, the
  three path-layout copies, the split namespace, and the four-map setter are genuinely removed, not
  relocated — the marker of a correctly shaped seam.

## Migration plan

Behavior-preserving extraction, one phase at a time; each lands green on `just check && just test`.

1. **Introduce the type around the existing maps.** Add `core/shard/search/lifecycle.rs` with
   `IndexLifecycleManager` holding the four maps + `rocks` + `data_dir`. Move the path-layout method
   and reserved-key constants in. The worker holds the manager instead of the loose `SearchState`;
   `execute_ft_*` still contain their current bodies but reach through the manager's fields. No
   behavior change.
2. **Move create/drop/alter/info bodies into manager methods.** `execute_ft_*`
   (`create.rs`, `index_mgmt.rs`) become thin adapters that translate args, call the manager, and
   map `LifecycleError` to `Response`. This phase lands the **persist-before-OK** invariant: create
   and alter now return an error on persist/commit failure and roll back. This is the only
   intentional behavior change (the bug fixes); the Redis-compat suite covers the happy paths.
3. **Move recovery into `IndexLifecycleManager::recover`.** Replace the `shards.rs:221-314` free
   function with the seam call + outcome logging above; add `RecoveryResult`/`RecoveryOutcome` so
   corrupt indexes are surfaced rather than dropped. Delete `restore_search_state`
   (`worker.rs:434-445`), replaced by `install_search_manager`.
4. **Fold in the per-concern persist helpers.** `persist_aliases` / `persist_dict` /
   `persist_search_config` (`mod.rs:23-57`) and the synonym persist (`synonyms.rs:32-40`) become
   manager methods sharing the reserved-key constants; the duplicated `if let Some(rocks) && let
   Ok(json) && let Err(e)` blocks collapse to one helper.

## Testing impact

- **Lifecycle unit-testable without a shard.** The manager takes `Option<Arc<RocksStore>>` + a
  `data_dir`; tests open a real `RocksStore` over a `tempfile::tempdir()` (no worker, no scatter
  gather, no boot). Create / drop / alter / info each get direct unit tests.
- **Create-persists-before-OK invariant pinned.** Inject a `RocksStore` whose `put_search_meta`
  fails (closed/poisoned CF), call `create`, assert `Err(Persist)`, and assert there is **no**
  live-map entry and **no** orphan tantivy directory left behind. Today this property is untestable
  because the persist failure is swallowed and OK is returned regardless.
- **Idempotent recovery.** `recover` twice over the same data dir yields the same manager state;
  asserted directly on `RecoveryResult`.
- **Corrupt-index recovery surfaced.** Write a valid `SearchIndexDef` to the CF but a garbage
  tantivy directory → `recover` yields `RecoveryOutcome::Corrupt`, the index is quarantined (absent
  from the live map, metadata retained), and recovery completes for the other indexes. Mirrors the
  proposal 06 seam-test style (synthesize disk state, call the seam, assert on the result).
- **Drop partial-failure.** `drop_index` with a failing `delete_search_meta` reports the failure and
  does not leave the live map and disk in contradictory states (see Risks).
- **Existing suites unchanged.** The Redis-compat FT.* integration tests remain the end-to-end net;
  the migration subtracts the boot dependency from lifecycle-correctness coverage rather than
  duplicating it.

## Risks / open questions

- **Non-`Send` handles.** Resolved by keeping the manager strictly per-shard and running `recover`
  at worker-spawn time (above). If a coordinator-level *summary* of search state is ever needed
  (e.g. cluster FT.INFO), it must be a `Send` plain-data projection, not the manager itself —
  deferred until a consumer appears.
- **Partial-failure cleanup on drop.** Drop touches three things — alias map, RocksDB metadata,
  tantivy files — and they can fail independently. The order is load-bearing: deleting metadata
  *before* destroying files means a crash between them orphans disk files (harmless, re-droppable);
  destroying files *before* deleting metadata risks the resurrection bug flagged below. The manager
  must define and test one order (proposed: delete metadata first, then destroy files, then drop the
  live entry) and report which step failed.
- **Recovery error taxonomy — recoverable vs fatal.** `Corrupt` / `Undeserializable` are per-index
  and recoverable (quarantine + continue). But an `iter_search_meta` failure on the CF itself
  (`shards.rs:279-281`) is currently also a `warn!` — should a CF-level read error abort startup
  rather than silently bring the shard up with zero indexes? The seam makes this a deliberate
  policy decision in one place; proposed: CF-level read failure is fatal, per-index failure is
  quarantined.
- **Per-shard vs coordinator placement.** The manager lives in `frogdb-core` next to the worker
  (where the handles are), not in the server `recovery/` orchestrator. The orchestrator (proposal
  06) calls it per shard; this keeps the `Send` boundary clean but means search recovery is invoked
  from `shards.rs`, not from `recover()`. Documented, intentional.
- **Reserved-key namespace collision.** The `__`-prefix convention means an index literally named
  `__config__` would collide. Centralizing the constants makes this checkable (reject reserved names
  at create time) — a fold-in, not required for the extraction.
- **Schema semantic validation.** Today validation is syntactic (`parse_ft_create_args`,
  `search/schema.rs:116`) plus the duplicate-field check in alter (`index_mgmt.rs:152-161`). Deeper
  semantic validation (vector dims, geo bounds, conflicting field options) has a natural home on
  `IndexLifecycleManager::create`/`alter` but is its own treatment; this proposal only gives it a
  home, it does not specify it.
- **Vector field-state desync is out of scope.** The four usearch maps
  (`vector_indexes` / `vector_key_map` / `vector_next_id` / `vector_reverse_map`,
  `search/src/index.rs:201-208`) and their consistency under document update/delete are a **separate
  proposal (19)**. This proposal owns *which* indexes exist and that they persist before OK; it does
  not own the internal consistency of an individual index's vector state. The lifecycle seam is
  where proposal 19's per-index repair would later hook in.

## Correctness flags

Each verified against the cited source; all are silent data-loss / divergence bugs the seam fixes by
construction (persist-before-OK + surfaced recovery outcomes).

- **FT.CREATE persistence failure returns OK, then the index is lost on restart.**
  `core/shard/search/create.rs:51-58` logs the `put_search_meta` error and falls through;
  `:60` inserts into the live map and `:64`/`:115` return OK. A `serde_json::to_vec` failure
  short-circuits the `&&`-chain and is not even logged. Recovery (`server/server/shards.rs:233-282`)
  only sees what is in the CF, so the index silently disappears after a restart.
- **FT.CREATE initial-scan commit failure is swallowed.** `core/shard/search/create.rs:110-112`
  logs an `idx.commit()` error and `:115` returns OK; the scanned documents are not on disk, so a
  restart reopens an index that is missing the data the client was told was indexed.
- **FT.ALTER commit + persistence failure returns OK, then the schema reverts on restart.**
  `core/shard/search/index_mgmt.rs:214-216` (commit) and `:219-224` (persist) both only log; `:226`
  returns OK. The new field was already applied in memory (`reopen_with_def`, `:168-169`), so the
  live index serves the new schema while RocksDB keeps the old definition — on restart the new field
  silently vanishes although clients received OK and may have written to it.
- **Recovery silently skips a corrupt/undeserializable index with no signal.**
  `server/server/shards.rs:258-266` (tantivy `open` fails) and `:268-275` (def fails to
  deserialize) emit only a `warn!`. The metadata stays in the `search_meta` CF, so the divergence
  (metadata says the index exists; the live map says it does not) is permanent and unsurfaced —
  FT.SEARCH returns "Unknown index name" for an index the operator believes exists.
- **FT.DROPINDEX metadata-delete failure resurrects an empty index on restart.**
  `core/shard/search/index_mgmt.rs:18-35`: the live-map entry is removed and the tantivy files are
  destroyed, but if `delete_search_meta` (`:20-24`) fails it only logs and OK is returned. The
  metadata survives, so on restart `ShardSearchIndex::open` — which is `open_or_create`
  (`search/src/index.rs:213-218`) — recreates a fresh **empty** index under the dropped name, and
  the dropped index reappears.
</content>
</invoke>
