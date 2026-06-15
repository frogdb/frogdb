# Proposal: Vector-Index Field-State Manager

Status: proposed
Date: 2026-06-15

## Problem

`ShardSearchIndex` keeps the state of every vector field as **four parallel `HashMap`s** keyed by
field name, with no single owner and no enforced relationship between them. The interface a caller
must understand is the union of all four maps plus the implicit invariant that ties them together —
a shallow seam where the interface (four maps you must update in lockstep) is exactly as complex as
the implementation. The deletion test fails: you cannot delete any one of the four maps without
breaking the others, because the correctness of vector search lives in the *agreement* between them,
not in any one of them.

The four fields (`frogdb-server/crates/search/src/index.rs:200-207`):

```rust
vector_indexes: HashMap<String, usearch::Index>,          // field -> HNSW sidecar
vector_key_map: HashMap<String, HashMap<u64, String>>,    // field -> (usearch id  -> redis key)
vector_next_id: HashMap<String, u64>,                     // field -> next id to allocate
vector_reverse_map: HashMap<String, HashMap<String, u64>>,// field -> (redis key  -> usearch id)
```

The load-bearing invariant is a **bijection** between live usearch ids and redis keys, per field:

```
reverse_map[field][key] == id   ⇔   key_map[field][id] == key   ⇔   usearch[field] contains id
```

Nothing in the type system or any method enforces it. Instead, the bijection is hand-maintained
across the codebase at **every site that mutates vector state**, each of which must touch the right
subset of the four maps *and* the usearch index in the right order:

| Site | What it must keep in sync | Where |
|------|---------------------------|-------|
| `index_vector` | usearch add/remove + `key_map` + `reverse_map` + `next_id` | `index.rs:1050-1111` |
| `delete_vector` | usearch remove + `key_map` + `reverse_map` (per field) | `index.rs:1114-1131` |
| `create_vector_indexes` (load) | usearch load + `key_map` + `reverse_map` + `next_id` from disk | `index.rs:1270-1366` |
| `reopen_with_def` | wholesale reassign all four | `index.rs:1028-1044` |
| `save_vectors` | persist usearch + `key_map` + `next_id` | `index.rs:1134-1156` |
| `knn_search` | read usearch + `key_map` together | `index.rs:1159-1190` |

Because the four maps and the usearch index are mutated by separate, individually-fallible
statements — and **every usearch op's result is discarded with `let _ =`** (`index.rs:1080`, `:1095`,
`:1100`, `:1124`) — a partial failure silently desyncs them. The result is orphaned usearch ids
(present in usearch, absent from the maps) or stale map entries (present in the maps, absent from
usearch). No code path detects or repairs the divergence; it surfaces later as silent data loss
(an "indexed" vector that knn never returns) or, after a partial disk load, as an **id collision**
that returns the wrong key for a vector (see `## Correctness flags`).

This is a deepening opportunity: collapse the four parallel maps and the usearch handle behind one
deep module, `VectorFieldManager`, whose narrow interface (index / delete / lookup / save / load)
hides the bijection and makes every mutation all-or-nothing. The leverage is high: one module
becomes the single home of the invariant, and `ShardSearchIndex` shrinks from four coupled maps to
one field.

## Current state

### The four field declarations (`index.rs:200-207`)

```rust
/// Sidecar usearch indexes for VECTOR fields (field_name -> usearch::Index).
vector_indexes: HashMap<String, usearch::Index>,
/// Mapping from usearch key (u64) to Redis key string, for vector fields.
vector_key_map: HashMap<String, HashMap<u64, String>>,
/// Next available usearch key ID per vector field.
vector_next_id: HashMap<String, u64>,
/// Reverse map: Redis key -> usearch key, for delete/update.
vector_reverse_map: HashMap<String, HashMap<String, u64>>,
```

These four travel together everywhere: they are constructed together (`open` `:228`, `open_in_ram`
`:263`), reassigned together (`reopen_with_def` `:1041-1044`), and returned together as a 4-tuple
from `create_vector_indexes` (`:1273-1280`, flagged `#[allow(clippy::type_complexity)]` at `:1269`
— the type system already complaining that this is one thing wearing four hats).

### `index_vector` — five mutations, no atomicity (`index.rs:1050-1111`)

```rust
pub fn index_vector(&mut self, field_name: &str, key: &str, blob: &[u8]) {
    // ... dim lookup + blob length check (return on mismatch) ...

    let vec_idx = match self.vector_indexes.get(field_name) {
        Some(idx) => idx,
        None => return,
    };

    // Remove old vector if key was previously indexed
    if let Some(rev_map) = self.vector_reverse_map.get(field_name)
        && let Some(&old_id) = rev_map.get(key)
    {
        let _ = vec_idx.remove(old_id);          // (1) usearch remove — result discarded
    }                                            //     NOTE: key_map[old_id] is NOT removed

    // Allocate new ID
    let id = self.vector_next_id.entry(field_name.to_string()).or_insert(0);
    let vec_id = *id;
    *id += 1;                                    // (2) next_id bumped before add succeeds

    // Reserve capacity if needed
    let size = vec_idx.size();
    let capacity = vec_idx.capacity();
    if size >= capacity {
        let _ = vec_idx.reserve(capacity.max(64) * 2);   // (3) reserve — result discarded
    }

    let floats = bytemuck_cast_f32(blob);
    let _ = vec_idx.add(vec_id, &floats);        // (4) usearch add — result DISCARDED

    // Update maps (always run, even if (4) failed)
    self.vector_key_map
        .entry(field_name.to_string())
        .or_default()
        .insert(vec_id, key.to_string());        // (5a) key_map insert
    self.vector_reverse_map
        .entry(field_name.to_string())
        .or_default()
        .insert(key.to_string(), vec_id);        // (5b) reverse_map overwrite
}
```

Five effects ((1) usearch remove, (2) `next_id`, (3) reserve, (4) usearch add, (5a/5b) two map
inserts) with no transaction boundary. If (4) fails the maps still record `vec_id`, so usearch and
the maps disagree. The update path never removes the **old** `key_map[old_id]` entry, so re-indexing
an existing key leaves a permanent orphan there while `reverse_map` is overwritten — the bijection is
already broken on the happy path (see flags).

### `delete_vector` — per-field cleanup of three of the four (`index.rs:1114-1131`)

```rust
pub fn delete_vector(&mut self, key: &str) {
    for field_def in &self.def.fields {
        if !matches!(field_def.field_type, FieldType::Vector { .. }) {
            continue;
        }
        let fname = &field_def.name;
        if let Some(rev_map) = self.vector_reverse_map.get_mut(fname)
            && let Some(id) = rev_map.remove(key)        // reverse_map cleaned
        {
            if let Some(vec_idx) = self.vector_indexes.get(fname) {
                let _ = vec_idx.remove(id);              // usearch remove — result discarded
            }
            if let Some(key_map) = self.vector_key_map.get_mut(fname) {
                key_map.remove(&id);                     // key_map cleaned
            }
        }
    }
}
```

This is the *correct* shape — reverse, usearch, and key_map are all cleaned for the deleted key — and
precisely illustrates the problem: the same three-step lockstep is restated here, by hand, with a
different ordering than `index_vector`, and the usearch result is again discarded. Two sites, two
hand-rolled spellings of the same invariant, zero shared enforcement.

### Independent disk load (`create_vector_indexes`, `index.rs:1313-1346`)

```rust
if let Some(base) = base_path {
    let vec_path = base.join(format!("__vec_{}.usearch", field_def.name));
    let map_path = base.join(format!("__vec_{}_map.json", field_def.name));
    if vec_path.exists()
        && let Err(e) = vec_idx.load(vec_path.to_str().unwrap_or(""))
    {
        tracing::warn!(error = %e, "Failed to load vector index, starting fresh");
    }                                                     // usearch load may fail INDEPENDENTLY
    if map_path.exists()
        && let Ok(data) = std::fs::read(&map_path)
        && let Ok(map_data) = serde_json::from_slice::<serde_json::Value>(&data)
    {
        // ... rebuild km / rm / next_id from JSON ...     // maps load INDEPENDENTLY
    }
}
// ...
vector_next_id.entry(field_def.name.clone()).or_insert(0); // defaults to 0 if JSON missing/failed
```

The usearch file and the `_map.json` are loaded by two independent fallible blocks with no
all-or-nothing guard. Either can succeed while the other fails, producing exactly the desynced state
the rest of the design is trying to prevent — including the `next_id == 0` id-collision flagged below.

## Proposed design

Introduce a deep module, `VectorFieldManager`, that **owns the four maps** (and the usearch handles)
and exposes only high-level operations. `ShardSearchIndex` drops the four fields and holds one:

```rust
vectors: VectorFieldManager,   // replaces the four maps at index.rs:200-207
```

Internally the manager is a single map of per-field bundles, so the bijection is enforced *locally*,
per field, by the type that owns all of that field's state:

```rust
/// One module owning all vector-field state. The single source of truth for the
/// usearch-id <-> redis-key bijection.
pub struct VectorFieldManager {
    fields: HashMap<String, VectorField>,
    path: Option<PathBuf>,
}

/// All state for ONE vector field, behind private fields so the bijection cannot be
/// mutated out from under the invariant.
struct VectorField {
    index: usearch::Index,
    key_map: HashMap<u64, String>,      // usearch id -> redis key
    reverse_map: HashMap<String, u64>,  // redis key  -> usearch id
    next_id: u64,
    dim: usize,                         // for blob length validation, owned here
}
```

### Interface (everything a caller must know)

```rust
impl VectorFieldManager {
    /// Build from a definition, loading per-field state from disk if present.
    /// Absorbs `create_vector_indexes`; load is all-or-nothing per field (see below).
    pub fn new(def: &SearchIndexDef, path: Option<&Path>) -> Result<Self, SearchError>;

    /// Index (add or replace) a vector for `field` under `key`.
    /// All-or-nothing: on any usearch failure NOTHING is mutated and Err is returned.
    /// On replace, the prior id is removed from usearch AND both maps.
    pub fn index(&mut self, field: &str, key: &str, blob: &[u8]) -> Result<(), SearchError>;

    /// Remove `key` from every vector field. Idempotent; cleans usearch + both maps.
    pub fn delete(&mut self, key: &str);

    /// Resolve a usearch id back to its redis key (used by knn_search result mapping).
    pub fn lookup_key(&self, field: &str, id: u64) -> Option<&str>;

    /// Resolve a redis key to its usearch id (for callers that need it).
    pub fn lookup_id(&self, field: &str, key: &str) -> Option<u64>;

    /// Run a KNN search and return (redis_key, distance) pairs, already mapped.
    /// Owns the usearch-handle + key_map pairing so callers never touch both.
    pub fn knn(&self, field: &str, query: &[f32], k: usize)
        -> Result<Vec<(String, f32)>, SearchError>;

    /// Persist every field's usearch index + map sidecar atomically (temp + rename).
    pub fn save(&self) -> Result<(), SearchError>;

    /// Debug-only invariant check; called from debug_assert! at every mutation seam.
    #[cfg(any(test, debug_assertions))]
    pub fn check_invariant(&self) -> Result<(), String>;
}
```

The interface is narrow: callers index, delete, look up, search, save. The four maps, the id
allocator, the COW-style replace-then-add ordering, the usearch error handling, and the bijection are
all *implementation*. `ShardSearchIndex` no longer mentions `vector_*` anywhere except by delegating.

### The bijection enforced at the seam (all-or-nothing)

```rust
impl VectorField {
    fn index(&mut self, key: &str, blob: &[u8]) -> Result<(), SearchError> {
        if blob.len() != self.dim * 4 {
            return Err(SearchError::SchemaError("vector blob size mismatch".into()));
        }
        let floats = bytemuck_cast_f32(blob);

        // Capacity first; propagate failure instead of discarding it.
        if self.index.size() >= self.index.capacity() {
            self.index
                .reserve(self.index.capacity().max(64) * 2)
                .map_err(usearch_err)?;
        }

        // Snapshot the prior id so we can roll back the replace if `add` fails.
        let prior = self.reverse_map.get(key).copied();
        if let Some(old_id) = prior {
            self.index.remove(old_id).map_err(usearch_err)?;
        }

        let new_id = self.next_id;
        // usearch op FIRST; only mutate maps after it succeeds.
        self.index.add(new_id, &floats).map_err(usearch_err)?;
        self.next_id += 1;

        // Map mutations are infallible and happen as one unit, after usearch committed.
        if let Some(old_id) = prior {
            self.key_map.remove(&old_id);          // <-- the orphan the old code leaked
        }
        self.key_map.insert(new_id, key.to_string());
        self.reverse_map.insert(key.to_string(), new_id);

        debug_assert!(self.invariant_holds());
        Ok(())
    }

    fn delete(&mut self, key: &str) {
        if let Some(id) = self.reverse_map.remove(key) {
            let _ = self.index.remove(id);   // best-effort; maps are authoritative for "absent"
            self.key_map.remove(&id);
        }
        debug_assert!(self.invariant_holds());
    }

    #[cfg(any(test, debug_assertions))]
    fn invariant_holds(&self) -> bool {
        self.reverse_map.len() == self.key_map.len()
            && self.reverse_map.iter().all(|(k, id)| self.key_map.get(id) == Some(k))
    }
}
```

One subtlety the manager makes explicit and decides *once*: usearch's `add` may not be losslessly
rollback-able, and `remove` may fail. The chosen rule (documented at the interface) is **maps are
authoritative**: a usearch op that fails *before* the maps are touched aborts the whole `index`
(nothing recorded); a usearch `remove` that fails during `delete`/replace is best-effort, and the
maps are updated so the entry is logically gone. Either way the bijection `reverse_map ⇔ key_map`
always holds; the only tolerated drift is a *garbage* usearch id with no map entry, which `knn` can
never surface because it filters every hit through `lookup_key` (see Risks for the periodic-compaction
follow-up). This is a deliberate, single-site policy decision instead of four `let _ =`s that each
silently pick "ignore".

### Before / after: `index_document`'s call into the seam

Before — `ShardSearchIndex::index_vector` is 62 lines (`index.rs:1050-1111`) reaching into four
fields, discarding every usearch result, and leaking the old `key_map` entry on replace.

After — `index_vector` collapses to a delegation, and the caller (`index_document`, `:357-364`) is
unchanged:

```rust
// ShardSearchIndex::index_vector  (was index.rs:1050-1111)
pub fn index_vector(&mut self, field_name: &str, key: &str, blob: &[u8]) {
    if let Err(e) = self.vectors.index(field_name, key, blob) {
        tracing::warn!(error = %e, field = field_name, key, "vector index failed");
    }
}

// ShardSearchIndex::delete_vector  (was index.rs:1114-1131)
pub fn delete_vector(&mut self, key: &str) {
    self.vectors.delete(key);
}

// ShardSearchIndex::knn_search body  (was index.rs:1159-1190)
let hits = self.vectors.knn(field_name, query_vector, k)?;  // already (key, distance)
```

`reopen_with_def` (`:1028-1044`) drops four assignments for one:

```rust
self.vectors = VectorFieldManager::new(&new_def, self.path.as_deref())?;
```

### Why this is the right depth

- **Locality.** The bijection lives in `VectorField`/`VectorFieldManager` and nowhere else. The two
  hand-rolled lockstep spellings in `index_vector` and `delete_vector`, plus the disk-load rebuild in
  `create_vector_indexes`, collapse into one implementation. A future change to id allocation,
  replace semantics, or persistence format is a one-module edit.
- **Leverage.** One module removes the `#[allow(clippy::type_complexity)]` 4-tuple
  (`index.rs:1269-1280`), the four constructor blocks (`open` `:228-246`, `open_in_ram` `:263-281`),
  the four-line reassign in `reopen_with_def`, and every `let _ =` on a usearch op — and it deletes
  the whole class of partial-failure desync bugs flagged below, plus the happy-path orphan leak.
- **Deletion test.** The four fields at `index.rs:200-207` are *deleted* and replaced by one
  (`vectors: VectorFieldManager`). `create_vector_indexes` is deleted (folded into
  `VectorFieldManager::new`). `save_vectors` is deleted (folded into `VectorFieldManager::save`). If
  the new interface could not absorb and delete these, it would be the wrong shape. It can.
- **Shrinks the surface.** `ShardSearchIndex` goes from carrying four coupled maps a reader must
  mentally re-correlate to one struct with a documented invariant. The struct's `#[allow(type_complexity)]`
  admission that "this is really one thing" becomes literally true.
- **No new adapter layer.** This is not a wrapper callers may bypass; the four maps become private to
  the manager, so there is no raw path left that could desync them. usearch remains the only external
  dependency, reached only through the manager.

## Migration plan

Behavior-preserving and phased; each phase compiles and passes `just test frogdb-search`.

1. **Phase 0 — introduce the module, no behavior change.** Add `VectorFieldManager` +
   `VectorField` in a new `frogdb-server/crates/search/src/vector.rs`. Move `create_vector_indexes`
   and `save_vectors` logic into `VectorFieldManager::new` / `::save` *verbatim* (still using
   `let _ =` on usearch ops, still loading the two files independently) so semantics are identical.
   Keep the four `HashMap`s as private fields of the manager. `just check frogdb-search`.
2. **Phase 1 — encapsulate: route every access through the manager.** Replace the four
   `ShardSearchIndex` fields with `vectors: VectorFieldManager`. Rewrite `index_vector`,
   `delete_vector`, `knn_search`, `hybrid_search`'s knn call, `reopen_with_def`, and the constructors
   to delegate. No invariant change yet — `index`/`delete` reproduce the current (buggy) behavior so
   this phase is a pure move. Existing tests stay green.
3. **Phase 2 — enforce the bijection + all-or-nothing.** Inside `VectorField::index`/`delete`: order
   the usearch op first, propagate its `Result`, remove the leaked old `key_map[old_id]` entry on
   replace, and add `debug_assert!(self.invariant_holds())` at each seam. This is the first
   *behavior* change (it fixes the desync/leak); the Redis-compat vector tests and new unit tests
   below are the safety net.
4. **Phase 3 — atomic load + save.** Make `VectorFieldManager::new` load usearch + map as a unit
   (if either is missing/corrupt for a field, start that field fresh — never half-loaded), and make
   `::save` write each sidecar to a temp file and rename (durability). Removes the id-collision and
   half-load desync flags.
5. **Phase 4 — delete the dead code.** Remove `create_vector_indexes`, the standalone `save_vectors`,
   and the `#[allow(clippy::type_complexity)]`. FrogDB is pre-production — no shims.

## Testing impact

- **Unit-testable without tantivy.** `VectorFieldManager` depends only on usearch, not on the
  tantivy `Index`/`writer`/`reader`. Tests construct a manager directly from a minimal
  `SearchIndexDef` with one `FieldType::Vector` and exercise index/delete/knn/save/load with no
  search-index scaffolding. Today these paths can only be reached through a full `ShardSearchIndex`.
- **Bijection property test.** Drive a random sequence of `index`/`delete` ops against a manager and
  assert `check_invariant()` after each: `reverse_map` and `key_map` are mutual inverses and equal in
  size, and every live id is in usearch. Today the invariant is unstated and untested.
- **Partial-failure / rollback test.** Force `usearch::add` to fail (e.g. blob whose length passes
  the dim check but a stubbed/seamed add returns `Err`, or a zero-dim edge) and assert: `index`
  returns `Err`, neither map gained an entry, and `next_id` did not advance — i.e. no orphan. The
  symmetric replace case: a failed `add` during replace leaves the *prior* (key, id) intact in both
  maps and usearch (not a half-deleted entry).
- **Replace-no-leak (deletion) test.** Index `key` twice and assert `key_map.len() == 1` and
  `reverse_map.len() == 1` afterward — pins the fix for the happy-path orphan that the current
  `index_vector` leaks.
- **Round-trip persistence test.** `index` several keys, `save`, build a fresh manager via `new`
  from the same path, and assert `knn` returns identical results and `check_invariant()` holds.
  Add a corruption case: truncate/delete the `_map.json` and assert the field loads *fresh* (empty,
  `next_id == 0`, no usearch entries) rather than half-loaded — pins the id-collision fix.
- **Existing suites unchanged.** Phases 0-1 are behavior-preserving; the existing search/vector tests
  in `index.rs` (`:1683`+) and the Redis-compat vector suite are the end-to-end check that no reply
  changed.

## Correctness flags

Verified against `frogdb-server/crates/search/src/index.rs` at the cited lines:

- `index.rs:1100,1103-1110`: `index_vector` discards the `usearch::Index::add` result (`let _ =`)
  but unconditionally inserts into `vector_key_map` and `vector_reverse_map`; on add failure the maps
  claim a vector that usearch never stored — a desynced "indexed" vector that knn silently never
  returns. Make `index` all-or-nothing (usearch op first, maps only on `Ok`).
- `index.rs:1076-1106`: the replace path removes `old_id` from usearch (`:1080`) and overwrites
  `reverse_map` (`:1107`) but never removes `vector_key_map[old_id]`, so re-indexing any existing key
  leaves a permanent orphan in `key_map` and breaks the bijection on the *happy* path (one leaked
  entry per update). Remove the prior `key_map[old_id]` on replace.
- `index.rs:1124`: `delete_vector` discards the `usearch::Index::remove` result; on failure the maps
  are cleaned but the vector stays in usearch as an orphaned id. (Maps stay mutually consistent, so
  knn filters it out — but it is unbounded leaked usearch state.) Decide remove-failure policy once,
  in the manager.
- `index.rs:1313-1346,1356`: `create_vector_indexes` loads the `.usearch` index and the `_map.json`
  in two independent fallible blocks. If the map JSON is missing or fails to parse while the usearch
  file loads, `vector_next_id` falls back to `0` (`:1356 or_insert(0)`) while usearch still holds
  vectors at ids `0..N` — the next `index_vector` allocates id `0` and **collides with an existing
  usearch id, returning the wrong redis key for that vector**. The mirror case (usearch load fails,
  maps load) leaves the maps referencing ids absent from a fresh usearch index. Load the pair
  atomically per field.
- `index.rs:1134-1156` (`save_vectors`, called from `commit` at `:385`): the `.usearch` index and
  `_map.json` are written as two separate, non-fsynced `write`s with no temp-file + rename. A crash
  between them leaves usearch and the map sidecar inconsistent on the next `open`, feeding the
  id-collision above. Persist each field's pair atomically (temp + rename).
