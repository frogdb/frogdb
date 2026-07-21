# Proposal 09 — Give `ShardSearchIndex` a document-level write Interface

## Summary (2-3 sentences)

The invariant "a hash document must feed both the inverted (text) index and the vector sidecar in
lockstep" is not owned by the search crate. Its public write Interface is field-granular
(`index_document` + `has_vector_fields` + `index_vector`), so every ShardWorker caller re-derives
the lockstep by hand — read the value from the **Store**, lossy-project it to
`Vec<(String, String)>`, call `index_document`, gate on `has_vector_fields()`, then loop the *raw*
field bytes calling `index_vector`. This block is copy-pasted across four ShardWorker sites, and the
copies have already silently diverged from `index_document`'s own internal vector handling (which
indexes vectors from lossy-projected bytes and therefore fails). This proposal replaces the leaky
field-granular seam with a document-level `index_hash(key, &[(Bytes, Bytes)])` / `index_json(...)`
Interface that owns both projections internally, so callers shrink to read-value-then-one-call and
the lockstep becomes structurally unbreakable.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/search/src/index.rs` | 1857 | Defines the field-granular write Interface: `index_document` (L379), `delete_document` (L400), `index_vector` (L999), `delete_vector` (L1006), `has_vector_fields` (L1086), plus `extract_json_fields` (L128) |
| `frogdb-server/crates/search/src/vector.rs` | 832 | `VectorFieldManager::index` (L73) — no-ops on non-vector fields (L76), rejects wrong-length blobs (L300); `VectorField::index` (L298) upserts by key |
| `frogdb-server/crates/core/src/shard/search_hook.rs` | 166 | Write-path hook. `reindex_hash_key` (L79) contains the hash copy (L110-120); `reindex_json_key` (L124) the JSON copy (L142-149); `delete_from_search_indexes` (L153) the delete copy (L159-164) |
| `frogdb-server/crates/core/src/shard/search/create.rs` | 84 | `execute_ft_create` initial-scan closure re-derives the same hash block (L50-67) and JSON block (L45-49) |
| `frogdb-server/crates/core/src/shard/search/index_mgmt.rs` | 172 | `execute_ft_alter` re-index closure re-derives the same hash block (L139-154) |
| `frogdb-server/crates/core/src/shard/dispatch_search.rs` | 30 | The *separate* persistence seam: `FlushSearchIndexes` → `idx.commit()` (L8-17), the Pre-Snapshot Hook path — untouched by this proposal |
| `frogdb-server/CONTEXT.md` | — | Domain: **Store**, **ShardWorker**, **Internal Shard**, **Pre-Snapshot Hook** |

## Problem (concrete verified evidence)

**The interface is field-granular, so the document invariant lives in the callers.** The public
write surface of `ShardSearchIndex` speaks in fields, not documents. To index one hash key a caller
must orchestrate three separate calls in the right order. Here is `reindex_hash_key`
(`search_hook.rs:110-120`), verbatim:

```rust
for idx in self.search.indexes.values_mut() {
    if idx.matches_prefix(key_str) {
        idx.index_document(key_str, &hash_fields);
        if idx.has_vector_fields() {
            for (field_name, raw_val) in &hash {
                let fname = String::from_utf8_lossy(field_name);
                idx.index_vector(&fname, key_str, raw_val);
            }
        }
    }
}
```

`hash_fields` is a lossy `Vec<(String, String)>` (built at L100-108 via `from_utf8_lossy`), while
`hash` is the *raw* `Vec<(Vec<u8>, Vec<u8>)>` read from the **Store**. The caller must keep *both*
projections alive because it feeds the lossy one to the text index and the raw one to the vector
sidecar.

**This exact block is copy-pasted across four ShardWorker sites, three of them the hash variant:**

1. `search_hook.rs:110-120` — `reindex_hash_key`, live write path (after every HSET/HDEL/RENAME).
2. `create.rs:50-67` — `execute_ft_create` initial-scan closure (raw source `hash.iter()`,
   `index_vector(&fname, key_str, &v)`).
3. `index_mgmt.rs:139-154` — `execute_ft_alter` re-index closure (identical shape).

A JSON variant of the same read-project-index shape appears twice more —
`reindex_json_key` (`search_hook.rs:142-149`) and the JSON branch of `execute_ft_create`
(`create.rs:45-49`) — differing only in that it calls `extract_json_fields` and omits the vector
loop entirely.

**`index_document` already tries to index vectors — from the wrong bytes.** This is the sharp edge.
`index_document` (`index.rs:387-394`) *itself* loops its `hash_fields` and indexes any vector field:

```rust
// Index vector fields into usearch sidecar
for (field_name, value) in hash_fields {
    if let Some(field_def) = self.def.fields.iter().find(|f| f.name == *field_name)
        && matches!(field_def.field_type, FieldType::Vector { .. })
    {
        self.index_vector(field_name, key, value.as_bytes());
    }
}
```

But `value` here is the **lossy-projected `String`**, so `value.as_bytes()` is *not* the raw f32
blob. A vector blob is raw little-endian f32 bytes; `from_utf8_lossy` replaces every invalid byte
run with the 3-byte U+FFFD sequence, so the length almost always changes. `VectorField::index`
(`vector.rs:300-306`) then rejects it with a size-mismatch `Err`, which `index_vector`
(`index.rs:1000-1002`) swallows as a `tracing::warn!`. **So `index_document`'s built-in vector
indexing silently fails for real vectors and emits a warning per write.** The caller's *external*
raw-bytes loop is the only path that indexes vectors correctly — it re-does the work `index_document`
botched, this time from `raw_val`. The external loop is not redundant ceremony; it is a load-bearing
correction for a broken internal path, and nothing in the interface tells a caller that.

**The delete side has the mirror-image redundancy.** `delete_document` (`index.rs:400-408`) already
calls `self.delete_vector(key)` internally (L405). Yet `delete_from_search_indexes`
(`search_hook.rs:159-164`) calls *both*:

```rust
if idx.matches_prefix(key_str) {
    idx.delete_document(key_str);
    idx.delete_vector(key_str);   // already done inside delete_document
}
```

`delete_vector` is idempotent so this is harmless, but it is direct evidence that callers neither
know nor trust what the document methods already own — on write they *must* re-do the vector step
(because the internal one is broken), and on delete they *needlessly* re-do it (because the internal
one works but is invisible). Two opposite mistakes, one root cause: the seam is field-granular.

**The `has_vector_fields()` gate is dead ceremony.** `VectorFieldManager::index`
(`vector.rs:73-77`) already returns `Ok(())` for a field it doesn't manage (`None => Ok(())`), so
`index_vector` is a safe no-op on any non-vector field. The `if idx.has_vector_fields()` guard
around the loop gates nothing that `index_vector` doesn't already gate itself; it exists only as a
micro-optimization to skip a loop, and it is duplicated at all three hash sites.

## Why it is shallow/fragmented (architecture vocabulary)

**A shallow Interface that leaks its Implementation.** By Ousterhout's measure, `ShardSearchIndex`'s
write surface is shallow: `index_document`, `index_vector`, `has_vector_fields`, `delete_document`,
`delete_vector` are five public methods whose correct *composition* — "index the text doc, then
index every vector field from the raw bytes, in that order" — is not encapsulated by any of them.
The knowledge that a hash document is two coupled indexes is pushed up into every caller. A deep
module would expose one method ("index this document") and hide the tantivy-vs-usearch split behind
it; instead the split is the interface.

**The lockstep invariant has no owner (poor Locality).** The fact "text index and vector sidecar
move together" is smeared across four call sites in two crates (`core`'s ShardWorker,
`search`'s `ShardSearchIndex`) with no single point of truth. `search_hook.rs`, `create.rs`, and
`index_mgmt.rs` each independently reconstruct the ordering, the lossy projection, the raw-bytes
loop, and the `has_vector_fields` gate. Consistency is maintained by hand, and it has **already
failed**: `index_document`'s internal vector loop (lossy bytes) and the callers' external loop (raw
bytes) are two Implementations of the same step that disagree, and one is silently wrong.

**The Adapter is missing at the crate boundary.** The **Store** speaks `Bytes`; the text index
speaks lossy `String`; the vector sidecar speaks raw `&[u8]`. Today each ShardWorker call site *is*
the adapter that reconciles these three representations — so the adapter is duplicated four times and
lives on the wrong side of the crate seam. The projection from raw hash entries to "what tantivy
needs" and "what usearch needs" belongs *inside* `search`, where the field schema (`self.def.fields`)
already lives, not re-derived by a caller that has no schema.

**Deletion test.** Delete the external vector loop from `reindex_hash_key` (`search_hook.rs:113-118`)
and leave only `index_document`. Behavior *changes*: vectors stop being indexed for live HSETs
(because `index_document`'s internal loop indexes them from lossy bytes and the size check rejects
them). So the external loop is load-bearing — which is exactly the problem: a load-bearing step that
lives in the caller instead of behind the interface. Conversely, delete the redundant
`idx.delete_vector(key_str)` from `delete_from_search_indexes` (`search_hook.rs:162`) and *nothing*
changes, because `delete_document` already owns it. One interface, two callers, opposite answers to
"is this line load-bearing?" — the signature of a seam drawn in the wrong place.

**The JSON path proves the asymmetry.** `reindex_json_key` and the JSON branch of `create.rs` call
*only* `index_document`, with no external raw-bytes vector loop. So JSON documents rely entirely on
`index_document`'s internal (broken, lossy) vector handling. Hash and JSON — two callers of the same
document concept — are indexed by two different, inconsistent code paths. A document-level interface
would make both go through the same owned Implementation.

## Proposed change (plain English)

Raise the write Interface from field-granular to document-granular, and move the raw→text/raw→vector
Adapter inside `search` where the schema lives.

1. **Add `index_hash(&mut self, key: &str, entries: &[(Bytes, Bytes)])` to `ShardSearchIndex`.** It
   takes the *raw* hash entries straight from the **Store** and internally: (a) builds the lossy
   `Vec<(String, String)>` for the tantivy document (the projection that currently lives in every
   caller), and (b) indexes each vector field **from the raw `Bytes`**, not from the lossy string.
   Because both projections happen in one method that owns `self.def.fields`, the lockstep is
   structural — a caller cannot forget the vector step, and the vector step can no longer be fed the
   wrong bytes. This *fixes* the lossy-vector bug by construction.
2. **Add `index_json(&mut self, key: &str, json_data: &serde_json::Value)`.** It calls
   `extract_json_fields(self.definition(), json_data)` internally and indexes the result, so the
   `extract_json_fields → index_document` pairing stops being re-derived by callers. (If/when JSON
   vector fields are supported, they get corrected in exactly one place.)
3. **Delete `index_document`'s internal vector loop** (`index.rs:387-394`) — the broken one — and
   fold its correct raw-bytes replacement into `index_hash`. Keep `index_document` as a private
   text-only helper, or inline it into `index_hash`.
4. **Keep `delete_document` as the document-level delete** (it already deletes both indexes), and
   drop the redundant external `delete_vector` call from `delete_from_search_indexes`.
5. **Demote `index_vector`, `delete_vector`, `has_vector_fields` to `pub(crate)`/private.** They stop
   being part of the write Interface the ShardWorker sees; they become Implementation details of the
   document methods and the KNN/hybrid read paths that legitimately need them.

Net: the four copy-pasted blocks collapse to `idx.index_hash(key_str, &entries)` /
`idx.index_json(key_str, &json_data)` / `idx.delete_document(key_str)`. The projection and the
lockstep live once, behind the seam, next to the schema.

## Before / After

### Before — the caller owns the lockstep (`search_hook.rs:110-120`, real code)

```rust
for idx in self.search.indexes.values_mut() {
    if idx.matches_prefix(key_str) {
        idx.index_document(key_str, &hash_fields);      // lossy projection, built above at L100
        if idx.has_vector_fields() {                    // dead gate: index_vector already no-ops
            for (field_name, raw_val) in &hash {        // raw bytes — the real vector source
                let fname = String::from_utf8_lossy(field_name);
                idx.index_vector(&fname, key_str, raw_val);
            }
        }
    }
}
```

…plus the lossy projection the caller must pre-build (`search_hook.rs:100-108`):

```rust
let hash_fields: Vec<(String, String)> = hash
    .iter()
    .map(|(k, v)| (String::from_utf8_lossy(k).to_string(),
                   String::from_utf8_lossy(v).to_string()))
    .collect();
```

The same ~20 lines are repeated in `create.rs:50-67` and `index_mgmt.rs:139-154`.

### After — the caller states intent; the interface owns the invariant (illustrative)

```rust
// Caller (search_hook.rs). `hash` is the raw Vec<(Bytes, Bytes)> from the Store.
for idx in self.search.indexes.values_mut() {
    if idx.matches_prefix(key_str) {
        idx.index_hash(key_str, &hash);
    }
}
```

```rust
// search/src/index.rs — the projection + lockstep move here, once, next to self.def.
pub fn index_hash(&mut self, key: &str, entries: &[(Bytes, Bytes)]) {
    // Text side: lossy projection for tantivy.
    let text_fields: Vec<(String, String)> = entries
        .iter()
        .map(|(k, v)| (String::from_utf8_lossy(k).to_string(),
                       String::from_utf8_lossy(v).to_string()))
        .collect();
    self.write_text_document(key, &text_fields);

    // Vector side: RAW bytes, gated by the schema this index already owns.
    for (field, raw) in entries {
        let fname = String::from_utf8_lossy(field);
        // index_vector is a private no-op for non-vector fields; no has_vector_fields() gate needed.
        self.index_vector(&fname, key, raw);
    }
    self.dirty = true;
}
```

The lossy-vector bug is gone (vectors are indexed from `raw`, never from `value.as_bytes()`), the
`has_vector_fields()` gate is gone (the no-op inside `index_vector` subsumes it), and the four caller
copies collapse to one line each.

### Caller-site reduction

| Site | Before | After |
| --- | --- | --- |
| `search_hook.rs` hash (`reindex_hash_key`) | ~L100-120: pre-project + `index_document` + gate + raw loop | `idx.index_hash(key_str, &hash)` |
| `search_hook.rs` JSON (`reindex_json_key`) | L142-149: `extract_json_fields` + `index_document` | `idx.index_json(key_str, &json_data)` |
| `search_hook.rs` delete | L159-164: `delete_document` + redundant `delete_vector` | `idx.delete_document(key_str)` |
| `create.rs` (`execute_ft_create`) | L45-67: JSON block **and** hash block inline in the scan closure | two one-liners |
| `index_mgmt.rs` (`execute_ft_alter`) | L139-154: hash block inline in the re-index closure | one one-liner |

## Testability improvement

**Today the lockstep is only exercisable through the ShardWorker.** There is no unit test that "a
hash with a vector field indexes both indexes from the correct bytes," because the correct behavior
only emerges from the *composition* the caller performs — `index_document` + external raw loop. The
`search` crate's own tests can call `index_document` and would see the vectors silently dropped (size
mismatch → warn), which looks like "vectors aren't indexed" rather than a bug, so the crate can't
catch its own defect in isolation. Verifying the real path needs a booted ShardWorker driving HSET
through `update_search_indexes`.

**After the change, the invariant becomes a `search`-crate unit test.** `index_hash` is a pure
method on `ShardSearchIndex`: construct an index whose schema has a TEXT and a VECTOR field, call
`index_hash("doc:1", &[(b"title".into(), b"hi".into()), (b"embedding".into(), raw_f32_blob)])`,
commit, then assert the text query matches *and* a KNN query returns `doc:1`. That test — impossible
to write meaningfully today because `index_document` would drop the vector — directly pins the
lockstep and would have caught the lossy-bytes regression. A second test asserts `index_hash` and
`index_json` share the same delete/overwrite semantics via `delete_document`. No ShardWorker, no
**Store**, no socket.

The existing vector unit tests (`vector.rs:532+`, `index_non_vector_field_is_noop` at L556) already
cover the primitives; this adds the missing document-level test one layer up.

## Risks / open questions

- **Behavioral parity on the vector bytes.** This change *intentionally* alters behavior:
  `index_hash` indexes vectors from raw `Bytes`, whereas `index_document`'s internal loop used lossy
  bytes and effectively indexed nothing. The external caller loops already used raw bytes, so for the
  three hash sites the *observable* result is unchanged (the correct vector was already written by
  the external loop, and the internal attempt was a rejected no-op). The one real behavior change is
  the disappearance of the per-write size-mismatch `warn!` spam. Worth a careful diff and a note in
  the changelog.
- **JSON vector fields.** If any index defines a vector field with a `json_path`, today it is indexed
  only by `index_document`'s broken internal loop, i.e. not at all. Routing JSON through `index_json`
  with the same raw-bytes discipline would *start* indexing them — a latent behavior change. Confirm
  whether JSON+vector is a supported/reachable combination before deciding to fix vs. explicitly
  reject it. Either way the fix lives in one place after this change.
- **Signature choice for `index_hash`.** `&[(Bytes, Bytes)]` matches what `create.rs`/`index_mgmt.rs`
  hold (`value.as_hash()` yields borrowable entries) and what `search_hook.rs` builds. Confirm the
  `HashValue` iterator can hand `&[(Bytes, Bytes)]` (or a cheap `impl Iterator`) without a forced
  clone; a borrowing iterator param avoids the `to_vec()` copies currently at `search_hook.rs:92`.
- **The persistence seam is not touched.** The **Pre-Snapshot Hook** flush path
  (`dispatch_search.rs:8-17` → `idx.commit()`), the 1-second periodic commit, and shutdown commit all
  operate on the `dirty` flag and `commit()`, strictly downstream of the write projection. `index_hash`
  sets `self.dirty = true` exactly as `index_document` does today (`index.rs:396`), so snapshot
  durability is unaffected. Called out so reviewers don't conflate the two seams.
- **Visibility churn.** Demoting `index_vector`/`delete_vector`/`has_vector_fields` to `pub(crate)`
  requires checking no out-of-crate caller remains. Grep confirms the write callers are the ones
  listed here; the KNN/hybrid read paths that use the vector sidecar are inside `index.rs` itself.

## Effort estimate

**S–M.** The mechanical core is small: add two methods to `ShardSearchIndex` (`index_hash`,
`index_json`), move the lossy projection and raw-bytes vector loop inside them, delete
`index_document`'s broken internal vector loop, and replace four caller blocks (~70 lines total)
with one-liners. It spans two crates (`search`, `core`) but the `core` side is pure deletion, and the
compiler flags every changed call site. It edges toward M only because of the JSON-vector open
question (a genuine latent behavior decision) and the `pub(crate)` visibility sweep; the projection
move itself is a lift-and-shift of code that already exists and is already correct at the hash call
sites.
