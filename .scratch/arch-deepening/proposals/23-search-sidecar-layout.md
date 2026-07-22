# Proposal 23 — Give the search-index sidecar layout a single owner shared by the writer and the Checkpoint copier

## Summary

The on-disk layout of the search-index sidecar — `<data_dir>/search/<index>/shard_<n>/` — is
known independently in **two crates**. Core's `IndexLifecycleManager::index_dir` **writes** that
tree and is self-described as "the single source of truth for the search-dir path layout." The
`SnapshotStager` in `persistence` **re-derives the same layout by hand** to copy the sidecar into a
**Checkpoint**: it hardcodes the `search/` root name and blindly walks a two-level directory
structure, plus a tantivy-specific rule for which files may be hard-linked. Nothing links the two:
no shared type, no compile edge, no test that spans both. A layout change on the writer side
silently produces an empty or malformed sidecar copy, with no compile-time or test-time signal. The
divergence is not theoretical — the stager fixtures already spell the shard directory as `0` while
core produces `shard_0`, a benign spelling difference the name-agnostic copier cannot even observe,
but a concrete demonstration that the two halves are edited in isolation. This proposal extracts the
layout into one owned type consumed by both sides, turning the silent-drift seam into a compile link
plus a round-trip test.

**Motivation is drift-prevention / code hygiene, not data-loss prevention.** An earlier draft framed
the payoff as averting a "silent restore gap." That framing is wrong and is corrected below: no code
in the tree reads a Checkpoint's search sidecar *back* on restore (verified — see Problem and
Blast-radius). The copy path's output is currently an **unconsumed artifact**, so a drifted copy
produces a stale/empty on-disk sidecar, not a runtime data-loss symptom. The case for this refactor
therefore rests on eliminating a real cross-crate silent-drift seam and consolidating three
scattered concerns, *and* is contingent on `copy_indexes` remaining (or becoming) load-bearing — see
the honest cost/benefit note at the end of Problem.

## Problem (concrete verified evidence)

The layout `<data_dir>/search/<index>/shard_<shard_id>/` is authored in exactly one place and
re-implemented in another, with no connection between them.

**Writer side — `IndexLifecycleManager::index_dir`
(`frogdb-server/crates/core/src/shard/search/lifecycle.rs:101-106`):**

```rust
pub(crate) fn index_dir(&self, name: &str) -> PathBuf {
    self.data_dir
        .join("search")
        .join(name)
        .join(format!("shard_{}", self.shard_id))
}
```

Its own doc-comment (`lifecycle.rs:98-100`) calls it "**The single source of truth** for the
search-dir path layout (previously re-typed in create, drop, and recovery)." Within `core` that is
true: `create` (`:224`), `drop_index` (`:266`), and `recover` (`:378`) all route through it. But the
claim of single ownership stops at the crate boundary.

**Copier side — `SnapshotStager` re-derives the same layout
(`frogdb-server/crates/persistence/src/snapshot/stager.rs`):**

```rust
fn copy_indexes(&self) -> Result<(), SnapshotError> {          // :120-126
    let src = self.data_dir.join("search");                   // :121  hardcoded root name
    if src.exists() {
        Self::copy_search_indexes(&src, &self.tmp.join("search"))?; // :123
    }
    Ok(())
}
```

`copy_search_indexes` (`stager.rs:168-201`) walks **two nested directory levels** — outer entry =
`<index>`, inner entry = `shard_<n>` — then copies the files inside:

```rust
for ie in std::fs::read_dir(src)? {                    // level 1: <index> dirs
    if !ie.file_type()?.is_dir() { continue; }
    let in_ = ie.file_name();
    for se in std::fs::read_dir(ie.path())? {          // level 2: shard_<n> dirs
        if !se.file_type()?.is_dir() { continue; }
        let sd = dst.join(&in_).join(se.file_name());
        std::fs::create_dir_all(&sd)?;
        for fe in std::fs::read_dir(se.path())? {      // level 3: files
            ...
            let ns = fn_.to_string_lossy();
            if ns == "meta.json"                       // :191-195
                || ns.starts_with('.')
                || std::fs::hard_link(&sp, &dp).is_err()
            {
                std::fs::copy(&sp, &dp)?;
            }
        }
    }
}
```

Three couplings to the writer's layout live here, none enforced by the compiler:

1. **The root name `"search"`** — hardcoded at `stager.rs:121`, `:123`, and again at `:133`
   (metadata size). It must match `lifecycle.rs:103`.
2. **The nesting depth (exactly two levels of directory before files)** — encoded structurally by
   the two nested `read_dir` loops. It must match the `.join(name).join(format!("shard_{}", ...))`
   depth in `lifecycle.rs:104-105`.
3. **Tantivy file knowledge** — the `meta.json` / dotfile rule at `stager.rs:191-195`. (Correction
   to the exploration note: this is *not* an exclusion. `meta.json` and dotfiles are always
   **byte-copied** so the Checkpoint holds an inode independent of the live index; every other file
   is **hard-linked** with a byte-copy fallback. It encodes which tantivy files stay mutable under
   the live writer, so hard-linking them would let a later live mutation corrupt the Checkpoint.)

**A latent-hazard demonstration: the fixtures already diverge (benignly).** The persistence stager
test builds its sidecar as `data_dir/search/idx/**0**`
(`frogdb-server/crates/persistence/src/snapshot/tests.rs:223`, asserted again at `:245-248`):

```rust
let shard = data_dir.join("search").join("idx").join("0");   // tests.rs:223 — "0"
```

while the *actual writer* produces `shard_**0**` (`lifecycle.rs:105`; confirmed by
`core/.../lifecycle.rs:609` and `server/tests/search.rs:1332`, which both use `shard_0`). The stager
test passes anyway because `copy_search_indexes` is **name-agnostic within its two levels** — it
copies whatever directories it finds. This spelling divergence is therefore *benign*: the copier
cannot observe it, so it is not an active bug. It is cited only as evidence that the two halves are
authored and edited in isolation — the exact precondition for a *non-benign* drift. The
drift-sensitive couplings are #1 (root name) and #2 (depth), not the `shard_<n>` spelling. Flatten
the layout in core to `search/<index>/<files>` and the inner loop would iterate *files* (skipping
them all, `is_dir()` false) and copy **nothing** — a complete, silent sidecar *copy* failure. Rename
the root and `src.exists()` is false — again nothing copied. In both cases the snapshot still reports
complete (`finalize_metadata` at `stager.rs:129-144` marks it complete regardless), so the Checkpoint
on disk holds an **empty search sidecar** with no error raised at snapshot time. What that broken
copy *costs* depends on who reads it back — addressed next.

**No test spans the two crates.** Core's lifecycle tests exercise `index_dir` (e.g.
`lifecycle.rs:491-509`); the persistence stager tests exercise `copy_search_indexes`
(`tests.rs:231-262`, `:297-339`). Neither uses the other's layout function, so no test would fail if
the two diverged — the two suites are already testing *different* layouts today.

**Blast-radius scoping (verified, keeps this in bounds).** The search sidecar is a **local
Checkpoint artifact only**. Replication full sync does **not** ship it: `serve_full_resync` cuts its
own RocksDB checkpoint into `fullsync_<id>` and streams a flat file list
(`replication/src/replica_session.rs:447-519`), never touching `data_dir/search`. And
`load_staged_checkpoint` (`persistence/src/rocks/checkpoint.rs:28-101`) installs only the RocksDB
directory. So the drift hazard is confined to the local BGSAVE/periodic-snapshot write path — this
proposal does not need to touch replication or the staged-checkpoint installer.

**Cost/benefit honesty: `copy_indexes`'s output has no current reader.** Verified against the tree:
the *only* startup consumer of a snapshot is `load_latest_metadata`
(`snapshot/rocks_coordinator.rs:65-88`), which reads **only** `metadata.json` to resume the epoch
counter and populate `last_snapshot_metadata`; it never opens `snapshot_NNNNN/search` or
`snapshot_NNNNN/checkpoint`. `load_staged_checkpoint` installs only the replication full-sync RocksDB
dir. A repo-wide search finds the sidecar path (`join("search")`) authored in exactly two live
places — the writer (`lifecycle.rs`) and the copier (`stager.rs`) — and no reader that copies a
Checkpoint's `search/` sidecar back into a live `data_dir/search`; restore rebuilds indexes from the
`search_meta` CF via `IndexLifecycleManager::recover`. **Consequence for this proposal:** a drifted
copy yields an *unconsumed, stale artifact*, not runtime data loss, and the refactor hardens a copy
path (`copy_indexes` / `copy_search_indexes`) whose bytes nothing currently reads. That is the honest
cost/benefit picture:

- If `copy_indexes` is **dead-in-practice** (the sidecar bytes are never read back and there is no
  planned restore-from-sidecar consumer), the higher-leverage move is arguably to **delete
  `copy_indexes` entirely** rather than extract and test it — removing the seam by removing one of
  its two authors. That deletion is out of scope here but is the cheaper alternative and should be
  weighed first.
- If a restore-from-sidecar consumer **exists elsewhere or is planned** (e.g. a warm-open path that
  avoids re-indexing from `search_meta` by reusing the Checkpoint's tantivy/usearch files), then the
  copy path is load-bearing, the silent-drift seam is a genuine correctness hazard, and this
  extraction is warranted. **This proposal should not be committed until that reader is confirmed to
  exist or is on the roadmap.** Absent that confirmation, treat it as code-hygiene churn of positive
  but modest value (one seam removed, three concerns named), not as data-loss prevention.

## Why it is shallow/fragmented (architecture vocabulary)

**The layout is a fact with two authors and no owner.** A **deep module** hides a decision behind a
narrow **interface**; here the decision "how search files are laid out on disk" is exposed as raw
`PathBuf` joins in `core` and re-expressed as `read_dir` nesting depth in `persistence`. The
**locality** is the worst possible: the two halves of one invariant sit in different crates, kept in
agreement only by a human remembering to edit both. `index_dir`'s doc-comment claims single
ownership, but the **seam** it forms leaks — the copier reaches *around* the interface and
reconstructs the layout from first principles rather than calling it, because there is nothing to
call across the crate boundary.

**It fails the change test.** Change the layout in core (rename the root, add or remove a nesting
level) and the compiler is silent; the copier keeps compiling and silently copies the wrong tree or
nothing. There is no signal at all today — not even a runtime one, precisely because nothing reads
the copy back (see Problem's cost/benefit note): the broken sidecar sits unnoticed in the Checkpoint
until and unless a future reader depends on it, at which point the failure surfaces far from the
one-line cause. An invariant that cannot be changed in one place without a hidden second edit has
**low leverage** and negative depth: the interface is thinner than the knowledge behind it, so
callers re-implement the knowledge.

**The copier mixes three concerns at one altitude.** `copy_search_indexes` interleaves (a) the path
layout, (b) the hard-link-vs-copy durability policy, and (c) the tantivy mutable-file set, all as
inline literals in one function. None is named; each is a separate piece of knowledge that belongs
to a different owner (layout ↔ the writer, link policy ↔ the Checkpoint machinery, tantivy files ↔
the search engine).

## Proposed design (Rust interface sketch — signatures/types only)

Extract the layout into one owned type. **Home: `frogdb-persistence`** — respecting the dependency
direction. `persistence` depends only on `frogdb-types`; `core` already depends on `persistence`
(for `RocksStore`); `search` depends only on `types`. So `persistence` is the lowest crate both the
copier (already there) and the writer (`core → persistence` edge already exists) can share without
adding any new edge, and it is where the copy machinery and the `"search"` string already live.
(`core` cannot own it — `persistence` must not depend on `core`. See Risks for the `types`-owned and
`search`-owned alternatives.)

```rust
// frogdb-server/crates/persistence/src/search_sidecar.rs  (new)

/// The on-disk layout of the search-index sidecar: `<data_dir>/search/<index>/shard_<n>/`.
/// The single source of truth for the sidecar root name and the per-(index, shard) nesting,
/// shared by the writer (core `IndexLifecycleManager`) and the Checkpoint copier
/// (`SnapshotStager`). Owning the layout in one type turns a cross-crate silent-drift seam
/// into a compile edge.
pub struct SearchSidecar<'a> {
    data_dir: &'a Path,
}

impl<'a> SearchSidecar<'a> {
    /// Directory name of the sidecar root under a data dir. The one place `"search"` is spelled.
    pub const ROOT: &'static str = "search";

    pub fn new(data_dir: &'a Path) -> Self;

    /// `<data_dir>/search` — the sidecar root.
    pub fn root(&self) -> PathBuf;

    /// `<data_dir>/search/<index>/shard_<shard_id>` — one index's tantivy + usearch files
    /// on one shard. The layout `IndexLifecycleManager::index_dir` delegates to.
    pub fn index_dir(&self, index: &str, shard_id: usize) -> PathBuf;

    /// Copy the whole sidecar tree into `dst_root` (a Checkpoint's `search/`), preserving the
    /// two-level `<index>/shard_<n>/` structure. Hard-links files that are immutable under the
    /// live writer and byte-copies the rest (see [`is_link_safe`]); byte-copies on any
    /// hard-link failure. Caller checks `root().exists()` or this no-ops on an absent root.
    pub fn copy_into(&self, dst_root: &Path) -> std::io::Result<()>;
}

/// Whether a sidecar file may be hard-linked into a Checkpoint (shares an inode with the live
/// index) or must be byte-copied to stay immutable while the live writer keeps mutating it.
/// Encodes tantivy's mutable-file set: `meta.json` and lock/dotfiles are copied, segment
/// files are linked.
fn is_link_safe(file_name: &str) -> bool;
```

Writer side collapses to a delegation (core keeps `index_dir`'s in-crate callers unchanged):

```rust
// frogdb-server/crates/core/src/shard/search/lifecycle.rs
pub(crate) fn index_dir(&self, name: &str) -> PathBuf {
    SearchSidecar::new(&self.data_dir).index_dir(name, self.shard_id)
}
```

Copier side collapses to a call (deleting the two hand-rolled loops and the inline tantivy rule):

```rust
// frogdb-server/crates/persistence/src/snapshot/stager.rs
fn copy_indexes(&self) -> Result<(), SnapshotError> {
    let sidecar = SearchSidecar::new(&self.data_dir);
    if sidecar.root().exists() {
        sidecar.copy_into(&self.tmp.join(SearchSidecar::ROOT))?;
    }
    Ok(())
}
```

Now the root name, the nesting depth, and the tantivy-file policy each have exactly one home, and
core reaches its own layout through the same type the copier uses.

## Migration plan (ordered steps)

1. **Add `SearchSidecar` to `persistence`** (`search_sidecar.rs`, re-exported from the crate root)
   with `ROOT`, `root`, `index_dir`, `copy_into`, and the private `is_link_safe`. Move the body of
   `copy_search_indexes` (`stager.rs:168-201`) into `copy_into`, and lift the `meta.json`/dotfile
   test into `is_link_safe`. Unit-test it (see Test plan) — this is a pure extraction, behavior
   unchanged.
2. **Rewire `SnapshotStager`.** Replace `copy_indexes`/`copy_search_indexes` (`stager.rs:120-126`,
   `:168-201`) with the `SearchSidecar` call above; replace the three hardcoded `join("search")`
   sites (`stager.rs:121`, `:123`, `:133`) with `SearchSidecar::ROOT` / `sidecar.root()`. Delete the
   now-dead private method. Existing stager tests must stay green.
3. **Repoint core.** Rewrite `IndexLifecycleManager::index_dir` (`lifecycle.rs:101-106`) to delegate
   to `SearchSidecar::new(&self.data_dir).index_dir(name, self.shard_id)`. Its three in-crate callers
   (`:224`, `:266`, `:378`) are untouched. This is the edit that installs the compile edge.
4. **Fix the drifted fixtures.** Change `write_search_sidecar` (`tests.rs:220-227`) and the happy-path
   assertion (`tests.rs:245-248`) to build/expect the path via
   `SearchSidecar::new(data_dir).index_dir("idx", 0)` instead of the literal `.join("idx").join("0")`
   — so the fixture is generated from the same layout function as production and can never silently
   diverge again. Same for `lifecycle.rs:609` if convenient (already correct at `shard_0`).
5. **Add the cross-crate drift-guard test** (Test plan). Run `just test frogdb-persistence` and
   `just test frogdb-core`; then a whole-suite `just tb-run "just test"` on a testbox (this touches
   two crates that many others depend on).

## Test plan

- **`SearchSidecar::copy_into` unit tests (persistence):**
  - Round-trips a two-level tree: build a source via `index_dir("idx", 0)`, write `segment.dat` +
    `meta.json` + a `.lock` dotfile, `copy_into` a dest, assert all three land at
    `dst/idx/shard_0/…`.
  - **Link policy:** assert `segment.dat` in the dest shares an inode with the source
    (`std::os::unix::fs::MetadataExt::ino` equal), while `meta.json` and the dotfile have **distinct**
    inodes (byte-copied) — pinning the tantivy-immutability rule that today is an untested inline
    literal.
  - No-op when the root is absent; error surfaces when the root is a file (folds in the current
    `test_stager_search_copy_failure_aborts_preserving_previous`, `tests.rs:297-339`).
- **Fixture-from-layout (persistence):** `write_search_sidecar` builds through
  `SearchSidecar::index_dir`, so the fixture equals the production layout by construction (kills the
  `0` vs `shard_0` divergence at its root).
- **Cross-crate drift guard (the win):** a test that constructs the source sidecar via the *same*
  `SearchSidecar::index_dir(data_dir, "idx", 0)` and drives the full `SnapshotStager::run`, then
  asserts the Checkpoint contains `search/idx/shard_0/segment.dat`. Because both the writer's
  `index_dir` and this assertion now flow through one type, changing the layout in one place moves
  the writer, the copier, and this test together — a flatten/rename that today silently produces an
  empty sidecar copy becomes a compile error or a red test. This is the test the current architecture
  makes impossible to write.
- **Regression:** the existing stager suite (`tests.rs:231-262`, `:297-339`) and the lifecycle suite
  (`lifecycle.rs:491-679`) stay green unchanged (except the fixture fixups in step 4).

## Risks & alternatives

- **Home choice — `persistence`-owned (chosen) vs `types`-owned vs `search`-owned.**
  - *`persistence` (chosen):* smallest new surface — the copy op and the `"search"` literal already
    live here, and `core → persistence` is an existing edge. Mild conceptual oddity: `core` is the
    *writer/authority* for these files yet gets its layout from a lower crate. Acceptable because the
    dependency graph forbids the reverse (`persistence` must not depend on `core`), so the layout
    must be extracted downward regardless.
  - *`types`-owned (leading alternative):* put the *pure path descriptor* (`root`/`index_dir`) in
    `frogdb-types`, the leaf both crates already depend on, and keep `copy_into` in `persistence`
    consuming it. Cleanest by dependency neutrality and separates path-structure from copy-policy,
    but splits one concept across two files. Pick this if the team prefers the layout to sit in the
    shared-vocabulary crate rather than the persistence crate.
  - *`search`-owned (rejected):* the tantivy file knowledge morally belongs to `frogdb-search`, but
    `persistence` does not depend on `search` today and `copy_into` would force a new
    `persistence → search` edge (no cycle — `search → types` only — but a wider footprint). Deferred.
- **Tantivy knowledge stays in `persistence`.** `is_link_safe` encodes tantivy's mutable-file set
  outside the crate that wraps tantivy. This is a small, stable string rule; a doc-comment pointing
  to `frogdb-search` is the pragmatic seam. Promoting it to a `frogdb-search` export (with the new
  edge above) is the clean-ownership alternative if the rule ever grows.
- **Behavior parity.** The extraction must be byte-for-byte behavior-preserving on the copy path
  (hard-link with copy-fallback, `meta.json`/dotfile copy). The inode-level unit test guards this; do
  the extraction first (step 1) with tests green before rewiring callers.
- **Precondition: confirm the copy path is (or will be) load-bearing.** The single most important
  gate on this proposal is not technical risk but justification: `copy_indexes`'s output has no
  current reader (Problem cost/benefit note). Before committing the extraction, confirm one of: (a) a
  planned restore-from-sidecar consumer, or (b) a decision to keep the copy for forward-compat. If
  neither holds, prefer the **delete-`copy_indexes`** alternative (remove one author, kill the seam)
  over extract-and-test. This proposal is written for case (a)/(b); it is explicitly *not* justified
  as data-loss prevention.
- **Breaking change is fine.** FrogDB is pre-production; even a layout *rename* is acceptable, and
  this proposal makes such a rename a one-line, compiler-checked edit instead of a silent hazard.
- **ADRs untouched.** The data path never goes through Raft (`0001-raft-cluster-metadata.md`); this
  is a local Checkpoint-artifact refactor and touches neither the Raft Metadata Plane nor
  replication. `0002-single-database` is unaffected. Glossary terms used per
  `frogdb-server/CONTEXT.md`: **Checkpoint** ("a RocksDB checkpoint plus search-index sidecar and
  `metadata.json`"), **Snapshot**, **Snapshot Epoch**, **Internal Shard**, **Store**,
  **Pre-Snapshot Hook**.

## Effort

**S–M.** One new small type in `persistence` (three path methods + a lifted copy body + a one-line
predicate), a one-method delegation in `core`, three literal swaps and a dead-method deletion in the
stager, and test fixups plus the new drift-guard test. Blast radius is `persistence` (one new file,
one rewired method) plus a single method in `core`; no async, no signature churn across crates, no
replication or restore-path changes. The extraction is mechanical and compiler-guided; the M edge is
purely the care needed to keep the copy path byte-for-byte identical, which the inode test pins.

## Related

None.

## Adversarial review

**Verdict: AMEND** (revised in place). The reviewer opened every cited `file:line` and confirmed the
core architectural premise: the sidecar layout `<data_dir>/search/<index>/shard_<n>/` has two
independent authors (writer `IndexLifecycleManager::index_dir`, copier `SnapshotStager`) with no
shared type, no compile edge, and no cross-crate test; the proposed `persistence`-owned
`SearchSidecar` is feasible with no new dependency edge and no borrow/async problem; and there is no
conflict with ADRs or replication. The seam and the fix are real. One major and one minor defect were
raised and are resolved below.

- **Major — data-loss framing unsupported by the restore path.** The reviewer showed that no code
  reads a Checkpoint's search sidecar back: the only startup consumer, `load_latest_metadata`
  (`snapshot/rocks_coordinator.rs:65-88`), reads only `metadata.json`; `load_staged_checkpoint`
  installs only the RocksDB dir. So a drifted copy is an *unconsumed artifact*, not runtime data loss
  — and the proposal half-admitted this in its own blast-radius aside, making the Problem section
  internally inconsistent. **Verified independently** (read `rocks_coordinator.rs:65-88`; grepped the
  sidecar path — authored only by writer + copier, no reader). **Resolved:** the motivation is
  reframed throughout as drift-prevention / code hygiene, not data-loss prevention. The Summary now
  carries an explicit "not data-loss prevention" note; the Problem section's conclusion says the
  drifted copy yields an empty on-disk sidecar with no error, not "zero search indexes on restore";
  the blast-radius aside is promoted to a first-class **cost/benefit honesty** note stating plainly
  that `copy_indexes`'s output has no current reader, with two branches — delete `copy_indexes` if
  dead-in-practice, or extract only if a restore-from-sidecar consumer exists/is planned — and a new
  Risks entry gating the whole proposal on confirming that precondition.
- **Minor — "the drift has already happened" oversells a benign divergence.** The reviewer agreed the
  facts are correct (fixtures use `0`, writer produces `shard_0`, copier is name-agnostic so it
  cannot observe the difference) but noted the headline framed a benign divergence as an active bug.
  **Resolved:** the Summary and Problem headings now frame it as a *latent-hazard demonstration* —
  evidence that the two halves are edited in isolation — and explicitly call the spelling difference
  benign and unobservable by the copier, not an active bug.
