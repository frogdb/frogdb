# Proposal 43 — `frogdb-recovery` phase wrappers, the staged-checkpoint owner, and the function-catalog error policy

## Summary

Three findings in `frogdb-recovery`, one of which is a **decision request**, not a change.

1. **The phase wrappers are thin, and folding them is still wrong — on
   doc-locality grounds.** Three of the six phase modules (`checkpoint.rs` 39
   lines, `cluster.rs` 27, `functions.rs` 28) hold ~8 real lines each behind a
   `pub(crate)` function with exactly one caller, so the deletion test says the
   complexity **moves**. It does. The reason not to fold is narrower than the
   first draft claimed: each file is the home of a phase's **contract prose**,
   and `recover`'s body is already carrying four multi-line ordering comments
   (lib.rs:183–187, 197–202, 229–234, 246–248). Folding pushes three more
   doc-comment blocks into that body and gains nothing in **depth**,
   **leverage**, or **locality**. **Ruled: do not fold.** What the deletion test
   *does* kill is one thing inside `functions.rs`: the string `"functions.fdb"`,
   authored independently by the reader (`functions.rs:20`) and the writer
   (`server/src/function_store.rs:337`).
2. **The `StagedCheckpoint` owner names two of its three location facts; the
   third is composed one line away, in a crate that already re-exports the
   constant.** The real, costed finding here is **not** the missing method — it
   is that **27 sites hand-spell the two protocol strings**, 8 of them in
   `frogdb-recovery`'s own tests across **six locked failure-mode rows**.
3. **`functions.rs` swallows every error to `Ok(vec![])` — and this is
   spec-pinned, not an oversight.** **FM-PERSISTENCE-037** states the invariant
   verbatim and already names the cost ("a silently smaller `FUNCTION LIST` with
   no counter or metric to notice it by"). So the error policy is a **spec
   question over a LOCKED row**, and changing it is an *edit* to FM-037. The
   decision is posed below as **binary**: approve Option A now, defer Option B.

## Files involved (verified paths + counts)

| Path | Lines | Role |
|------|-------|------|
| `frogdb-server/crates/recovery/src/lib.rs` | 270 | The one **seam**: `recover` (182–265), `RecoveryInputs`, `RecoveredState`, `RecoveryPhase` (137–155, **7 variants**) |
| `frogdb-server/crates/recovery/src/checkpoint.rs` | 39 | Phase 1 wrapper — `install_staged` (22–39), 1 caller (lib.rs:205). Doc = **12 lines** (module 1–10, fn 20–21), **no FM tag** |
| `frogdb-server/crates/recovery/src/cluster.rs` | 27 | Phase 6 wrapper — `open_storage` (18–27), 1 caller (lib.rs:253) |
| `frogdb-server/crates/recovery/src/functions.rs` | 28 | Phase 4 wrapper — `restore` (19–28); authors `"functions.fdb"` at line 20; swallows at 23–26 |
| `frogdb-server/crates/recovery/src/replication.rs` | 75 | Phase 5 — real reconcile logic; the staged-metadata calls at 42 and 54 |
| `frogdb-server/crates/recovery/src/shards.rs` | 183 | **Two** phases — `open_rocks` (20, lib.rs:211) and `restore` (lib.rs:213). Real policy (`report_decode_failures`, 95–144). **Owned by proposal 39's boundary; not edited here** |
| `frogdb-server/crates/recovery/src/data_dir.rs` | 166 | Phase 0 — real logic, called **twice** (`verify` lib.rs:203, `stamp` lib.rs:209). Carries FM tags at :8, :39 |
| `frogdb-server/crates/recovery/src/tests.rs` | 1580 | Seam tests; the 8 hardcoded staged literals (314, 337, 381, 416, 590, 641, 1516, 1558); `continue_policy()`/`refuse_policy()` helpers at 46, 53 |
| `frogdb-server/crates/persistence/src/rocks/staged.rs` | 204 | The declared owner: `StagedCheckpoint` (50–91, field `dir` at 52), the two constants (28, 33), `staged_paths_are_the_cross_crate_contract` (doc 137–141, fn 143, ends 164) |
| `frogdb-server/crates/persistence/src/rocks/mod.rs` | — | `pub mod staged` at line 7; the constants are plain `pub`, no feature gate |
| `frogdb-server/crates/replication/src/state.rs` | 958 | `STAGED_METADATA_FILE` **re-export** of the persistence constant (25–26, doc 18–24 says "this is a re-export, not a second definition"); `staged_metadata_path` (410–412, **one `join`**) |
| `frogdb-server/crates/scripting/src/persistence.rs` | 440 | `load_from_file` (207–227) / `save_to_file` — both take a `&Path`; neither owns the name |
| `frogdb-server/crates/server/src/function_store.rs` | 394 | The catalog **writer**; re-authors `"functions.fdb"` at line 337 |
| `frogdb-server/crates/types/src/metrics/definitions.rs` | — | `RecoveryKeysFailed("frogdb_recovery_keys_failed_total")` at **233** — the typed-handle precedent Option A must follow |
| `frogdb-server/crates/config/src/recovery.rs` | 164 | `RecoveryConfig::on_decode_failure` (45–60) — the boot-time decode policy that does *not* reach the catalog |
| `frogdb-server/crates/recovery/Cargo.toml` | 26 | `frogdb-persistence` is a **dev-dependency only** (line 23, `features = ["test-support"]`) — recovery has no production path to `StagedCheckpoint` |
| `.scratch/hardening/specs/persistence-failure-modes.md` | 732 | FM-024 (371), FM-025 (383, Outcome variant 391), FM-027 (417–427; Invariant **424**, Forced by **426**), FM-037 (641–651; NOT observable **647**, Invariant **648**, Forced by **650**), FM-039 (665–675). Header: `Status: LOCKED` |

## Problem

### Part 1 — the wrappers: the deletion test, applied honestly

The lane candidate asks whether the phase wrappers are pass-throughs. Applying
the deletion test to `checkpoint.rs` (39 lines, of which 22–39 is the body):

```rust
pub(crate) fn install_staged(inputs: &RecoveryInputs<'_>) -> Result<bool> {
    fs::create_dir_all(inputs.data_dir)?;
    match RocksStore::load_staged_checkpoint(inputs.data_dir) {
        Ok(true) => { info!("Loaded staged checkpoint from replica full sync"); Ok(true) }
        Ok(false) => Ok(false),
        Err(e) => { error!(error = %e, "Failed to load staged checkpoint");
                    Err(anyhow::anyhow!("Failed to load staged checkpoint: {}", e)) }
    }
}
```

Delete it and the `create_dir_all` plus the three-arm match land in `recover`.
Nothing reappears across N callers, because there is exactly one caller.
`cluster.rs` (gate + `join("raft")` + open + message) and `functions.rs` (join +
downgrade) are the same shape. Verified: every `pub(crate)` phase function in the
crate is called exactly once, from `recover` (lib.rs:203, 205, 209, 211, 213,
215, 249, 253). By the letter of the deletion test all three are pass-throughs.

**Two arguments that do *not* support the ruling, retracted explicitly:**

- *"They are internal seams."* LANGUAGE.md:36 defines an internal seam as one
  "private to its implementation, **used by its own tests**." Verified: **no test
  calls any phase function.** The only callers of `install_staged`,
  `open_storage`, `functions::restore`, `shards::open_rocks`, `shards::restore`,
  `data_dir::verify`, `data_dir::stamp`, and `replication::restore_state` are the
  eight lines in `recover`; every test in `tests.rs` reaches them through
  `recover`. They are not seams under this vocabulary — they are private helpers.
  The clause does not apply and must not be cited.
- *"Folding would break the one-module-per-phase symmetry."* There is no such
  symmetry to break. There are **six modules for seven `RecoveryPhase` variants**:
  `shards.rs` owns both `OpenRocks` (lib.rs:211) and `RestoreShards` (lib.rs:213),
  and `data_dir.rs` is entered twice for the single `VerifyDataDir` variant
  (lib.rs:203 and 209). The mapping is already many-to-many.

**The argument that does hold is doc-locality.** The crate's **interface** is one
function — `recover(inputs) -> Result<RecoveredState, RecoveryError>` — and it is
genuinely **deep**: plain-data in, plain-data out, over filesystem + RocksDB
internals, with no live components (lib.rs:10–13). The six files are `pub(crate)`,
so they cost callers and tests nothing to learn, and each one is where a phase's
contract prose lives. `checkpoint.rs`'s 12 doc lines carry the install contract
(validate `CURRENT` before touching the live dir; prune displaced-database backups
to the retention limit) — that text has to exist somewhere, and `recover`'s body
is already carrying four multi-line ordering comments of its own (lib.rs:183–187,
197–202, 229–234, 246–248). Folding three phases inlines three more doc blocks
into a body whose readability *is* the recovery-order invariant (lib.rs:22–25
states exactly that). The trade is: pay for three files, keep `recover` scannable
top-to-bottom. **Ruled: do not fold.** This is the DELETE branch the deletion test
declines to support, and the ruling is a judgement about where documentation
lives, not a claim about seams.

*(Note: `checkpoint.rs` carries **no** `// FM-` tag. Only `data_dir.rs` does,
at :8 and :39. So no `just lint-failure-modes` obligation is created or
destroyed by this ruling either way.)*

What survives the ruling is one genuine finding *inside* `functions.rs`. The
catalog filename is authored twice in production:

- reader — `recovery/src/functions.rs:20`: `let functions_path = inputs.data_dir.join("functions.fdb");`
- writer — `server/src/function_store.rs:337`: `let path = PathBuf::from(self.config.data_dir()).join("functions.fdb");`

`frogdb-scripting` owns `save_to_file`/`load_from_file` but both take a `&Path`
(persistence.rs:207), so nothing owns the name. This is a cross-crate on-disk
protocol with no owner — the same class of problem `StagedCheckpoint` was created
to solve (staged.rs:19–20: *"Before this module the dir/file names were string
literals duplicated across the three crates"*).

### Part 2 — the staged-checkpoint protocol: 27 hand-spelled sites, 6 locked rows

`StagedCheckpoint` (staged.rs:50–91) exposes `for_db_dir`, `in_parent`, `dir`,
`exists`, `is_complete_db`, `replication_metadata_path`. Its doc (staged.rs:1–20)
names three collaborating parties and calls recovery the **Orchestrator** that
*"afterwards consumes the replication metadata that the install carried into the
data dir."*

The protocol has **three** path facts:

| Fact | Location | Where the path is composed |
|---|---|---|
| the staged dir | `<parent>/checkpoint_ready` | `StagedCheckpoint::dir` (staged.rs:72) — owner |
| metadata *inside* the staged dir | `<parent>/checkpoint_ready/replication_metadata.json` | `StagedCheckpoint::replication_metadata_path` (staged.rs:88) — owner |
| metadata *after* the install | `<data_dir>/replication_metadata.json` | `ReplicationState::staged_metadata_path` (replication/state.rs:410–412) — **one `join` of the owner's own constant**, which `state.rs:25–26` re-exports from `frogdb_persistence::rocks::staged` and documents as *"a re-export, not a second definition"* |

**Correction to the first draft.** The third fact is *not* independently
authored. `frogdb-replication` does not spell `"replication_metadata.json"`; it
re-exports the owner's constant and composes `data_dir.join(STAGED_METADATA_FILE)`
in three lines. So there is **no duplicated string** here and **no rename hazard**
in production. What is absent is a *named* home for the relationship "the install
rename moves the file from `staged.dir()/F` to `data_dir/F`" — the relationship
FM-PERSISTENCE-027's Invariant states in prose (spec:424: *"the install is what
carries `replication_metadata.json` into the data dir for phase 5 to find"*).
`staged.rs`'s own test `staged_paths_are_the_cross_crate_contract` (fn at 143,
body through 164) pins only the *pre-rename* half: that the metadata sits inside
the staged dir "so the commit rename carries it". The post-rename half is checked
only end-to-end, in another crate, through a hardcoded literal
(`staged_replication_metadata_is_adopted_and_consumed`, tests.rs tagged at
299–300, literal at 314).

Recovery has **no production dependency on `frogdb-persistence`** — Cargo.toml:23
lists it under `[dev-dependencies]` — and `frogdb_core` does not re-export
`StagedCheckpoint` (verified: the persistence re-export block, core/src/lib.rs:116–122,
does not name it). So the orchestrator the owner's doc names cannot reach the
owner in production. That is a real observation, but it is a *statement of the
crate graph*, not a defect: recovery reaches the protocol through
`frogdb_core::{read,consume}_staged_replication_metadata` (core/src/lib.rs:134–135),
which is the intended route.

**The costed finding is the hand-spelling.** The two constants are defined once
(staged.rs:28, 33) and re-spelled by hand at **27 sites**, every one of them a test:

| Crate / file | Sites | Notes |
|---|---|---|
| `recovery/src/tests.rs` | **8** — `replication_metadata.json` at 314, 337, 381, 416; `checkpoint_ready` at 590, 641, 1516, 1558 | Dev-dep on `frogdb-persistence` already present |
| `persistence/src/rocks/tests.rs` | **8** — 463, 493, 525, 546, 579, 611, 640, 674 | **The owning crate's own tests** — cheapest of all, `use super::*` already in scope |
| `server/tests/integration_persistence.rs` | **5** — 1721, 1821, 2056, 2187, 2345 | |
| `server/tests/integration_replication.rs` | **6** — 5846, 5851, 5890, 5972(+5973), 6117, 6121 | |

Production code is clean (verified by grep across `frogdb-server/`: outside the
tests above, every occurrence is in a doc comment — `recovery/src/lib.rs:125`,
`recovery/src/replication.rs:4`, `replication/src/fullsync/stager.rs:7,88,97,223`,
`replication/src/replica_session.rs:3251`).

The recovery 8 are the ones that matter most, because the tests holding them force
**six locked rows**:

| Literal sites | Test tagged at | Rows forced |
|---|---|---|
| 314, 337 | tests.rs:299–300 | FM-PERSISTENCE-027, -039 |
| 381, 416 | tests.rs:356 | FM-PERSISTENCE-039 |
| 590 | tests.rs:584–585 | FM-PERSISTENCE-025, -027 |
| 641 | tests.rs:636 | FM-PERSISTENCE-024 |
| 1516 | tests.rs:1510–1511 | FM-PERSISTENCE-027, -051 |
| 1558 | tests.rs:1553 | FM-PERSISTENCE-049 |

A rename of either constant leaves all six rows' forcing tests silently exercising
a path the product no longer uses — green, and testing nothing.

### Part 3 — the function catalog: a spec question, not a bug

`recovery/src/functions.rs:19–28`:

```rust
pub(crate) fn restore(inputs: &RecoveryInputs<'_>) -> Result<Vec<(String, String)>> {
    let functions_path = inputs.data_dir.join("functions.fdb");
    match frogdb_core::load_from_file(&functions_path) {
        Ok(libraries) => Ok(libraries),
        Err(e) => {
            warn!(error = %e, "Failed to load persisted functions");
            Ok(Vec::new())
        }
    }
}
```

**The lane classified this "LIVE-by-design-unknown". Verified: it is by-design,
and the design is already written down and LOCKED.** FM-PERSISTENCE-037
(persistence-failure-modes.md:641–651):

- *Observable* (spec:646) — **two distinct failures in one column**:
  "Unreadable file → no functions and a `WARN`" (phase 4, this function) **and**
  "Readable file with one bad library → the others still load, per-library, with
  a `WARN` naming the failed one" (the **wiring layer**, `init.rs`, outside the
  recovery seam).
- *NOT observable* (spec:647), third clause — the one most on point:
  > "And the mirror image of the tolerance: a *well-formed* `functions.fdb`
  > coming back empty, which would make a valid library disappear exactly as
  > silently as a corrupt one."
- *Invariant* (spec:648): "Phase 4 cannot fail: every read/parse error is
  downgraded to `Ok(Vec::new())`. … **The known cost is a silently smaller
  `FUNCTION LIST` with no counter or metric to notice it by.**"
- *Forced by* (spec:650): `corrupt_functions_file_is_tolerated`,
  `persisted_functions_are_restored`

So there is no undocumented hole. What there is, is an **asymmetry the spec itself
flags in its last sentence**: the shard-data path treats an undecodable value as
*skip, count, and surface* (FM-033), refuses outright when nothing decoded
(FM-045), and offers `recovery.on-decode-failure = refuse` as the operator opt-in
(FM-047, config/recovery.rs:45–60). The function catalog gets the first half
(skip) and none of the second (count, surface, policy). `RecoveryStats` carries
`keys_failed` and there is a `RecoveryKeysFailed` typed handle (shards.rs:14,
100); there is no `functions_failed` analogue anywhere.

The observable gap: a boot where `functions.fdb` is truncated and a boot where no
library was ever stored are **indistinguishable** to everything except a `WARN`
line. `RecoveredState.functions` is `vec![]` in both.

## Proposed change

Three parts, independently landable, in the order below.

### Part 1 — own the catalog filename (S)

Add the name next to the code that reads and writes it, in `frogdb-scripting`:

```rust
// frogdb-server/crates/scripting/src/persistence.rs
/// The on-disk function catalog, a sibling of the RocksDB dir inside the data
/// directory. Written by the server's `FunctionStore::persist`, read by
/// recovery phase 4 — the two only meet through this name.
pub const FUNCTIONS_FILE: &str = "functions.fdb";
```

Re-export through `frogdb_core` alongside `load_from_file`/`save_to_file`
(core/src/lib.rs:93–98 — the `functions::` block; `load_from_file` at 96,
`save_to_file` at 97), and use it at both `functions.rs:20` and
`function_store.rs:337`. **No behavior change.** Do **not** fold the phase
modules — see the ruling above.

### Part 2a — replace the hand-spelled protocol literals (S, zero production risk) — **land this first**

Replace the eight literals in `recovery/src/tests.rs` with
`frogdb_persistence::rocks::staged::{STAGED_CHECKPOINT_DIR,
STAGED_REPLICATION_METADATA_FILE}`. Then the same substitution in the owning
crate's own tests (`persistence/src/rocks/tests.rs`, 8 sites — `use super::*` is
already in scope there, so it is a pure find/replace).

**Why this is zero-risk, verified:**

- No production code is touched, so there is no new production mutation surface.
- No test is **renamed or moved**, so no `Forced by` list changes and
  `just lint-failure-modes` is unaffected in both directions.
- The dependency is already wired: `recovery/Cargo.toml:23` is
  `frogdb-persistence = { workspace = true, features = ["test-support"] }`, and
  the path is reachable without that feature anyway — `rocks/mod.rs:7` is
  `pub mod staged` and both constants are plain `pub`, not feature-gated. No
  manifest change.

The remaining 11 sites (`server/tests/integration_persistence.rs` ×5,
`server/tests/integration_replication.rs` ×6) are the same substitution in an
unlocked crate; take them opportunistically, not as a blocker.

### Part 2b — name the post-install location and pin the rename (S; **optional, weakest part of this proposal**)

**Honest re-cost.** The delegation half of this part buys almost nothing: it
replaces `replication/state.rs:410–412`'s single
`data_dir.join(STAGED_METADATA_FILE)` — already composing the owner's own
re-exported constant — with a call into the owner. That is **one line of path
composition traded for two locked-crate mutation re-gates**
(`frogdb-persistence` and `frogdb-replication`, both at 0.85). On that trade
alone, 2b does not clear the bar.

What *does* clear the bar is the **test**, and the test does not require the
delegation. FM-027's Invariant (spec:424) claims the install carries the metadata
into the data dir; nothing in the owning crate asserts it. Add, in `staged.rs`:

```rust
// FM-PERSISTENCE-027
/// The install is `rename(checkpoint_ready -> <db>)`, so the file the writer
/// stamped *inside* the staged dir must be findable at the same name inside the
/// live data dir afterwards. `staged_paths_are_the_cross_crate_contract` pins
/// the pre-rename half; this pins the half phase 5 depends on.
#[test]
fn the_install_rename_carries_the_replication_metadata_into_the_data_dir() {
    let tmp = TempDir::new().unwrap();
    let db_dir = tmp.path().join("frogdb");
    let staged = StagedCheckpoint::for_db_dir(&db_dir).expect("db dir has a parent");
    std::fs::create_dir_all(staged.dir()).unwrap();
    std::fs::write(staged.replication_metadata_path(), br#"{"replid":"x"}"#).unwrap();

    // The install, reduced to the one filesystem operation that matters.
    std::fs::rename(staged.dir(), &db_dir).unwrap();

    let installed = StagedCheckpoint::installed_replication_metadata_path(&db_dir);
    assert!(installed.exists(), "phase 5 must find what phase 1 carried");
    assert_eq!(std::fs::read(&installed).unwrap(), br#"{"replid":"x"}"#);
}
```

This performs the rename and asserts the stamped bytes are at the installed path.
It is not a restatement of two `join`s; a mutant that changes either constant, or
that composes the installed path from anything but the file name, kills it.

The accessor it needs:

```rust
impl StagedCheckpoint {
    /// Where the staged replication metadata lives **after** the install.
    /// Associated (not a method): `StagedCheckpoint` holds only the *staged*
    /// directory (`dir`, staged.rs:52) and cannot derive the live data dir from
    /// it, so the caller supplies it.
    pub fn installed_replication_metadata_path(data_dir: &Path) -> PathBuf {
        data_dir.join(STAGED_REPLICATION_METADATA_FILE)
    }
}
```

Making `ReplicationState::staged_metadata_path` delegate to it is **optional and
recommended-against for now** — it drags `frogdb-replication` (the *replication*
locked pair) into a re-gate for a one-line rewrite with no duplication removed.
Land the accessor + test in `frogdb-persistence` only; revisit the delegation if
some future change already opens `frogdb-replication`.

**Three LOCKED-spec row edits, all in `persistence-failure-modes.md`, all in the
same commit as the test:**

1. FM-PERSISTENCE-027 **Forced by** (spec:426) gains
   `the_install_rename_carries_the_replication_metadata_into_the_data_dir`.
   Without this, the new `// FM-PERSISTENCE-027` tag is a tagged test with no row
   entry and `just lint-failure-modes` — hence `just lint` — fails.
2. FM-PERSISTENCE-027 **Invariant** (spec:424) may optionally cite the new
   accessor by name; prose only, not enforced by the linter.
3. (If the delegation is taken after all) no further row changes — the behavior is
   identical.

**Deliberately out of scope:** moving the *typed* read/consume into
`frogdb-persistence`. `StagedReplicationMetadata` is a replication type and
persistence cannot depend on replication. `recovery/src/replication.rs:42,54`
keeps calling the `frogdb_core` re-exports unchanged. Whether `frogdb_core` should
also re-export `StagedCheckpoint` is an **open question**, not proposed here.

### Part 3 — the function-catalog error policy: **DECISION REQUIRED (binary)**

FM-PERSISTENCE-037 is a LOCKED row stating the current behavior as intended, so
every option is a spec **edit** (row first → forcing test → code). Both
`continue_policy()` and `refuse_policy()` helpers already exist
(`recovery/src/tests.rs:46, 53`), so the harness is in place.

**Option C is disposed of here, not deferred.** "Fail recovery unconditionally"
contradicts FM-037's NOT-observable column and its stated rationale ("the keyspace
is the valuable thing and a function library is recoverable by re-uploading it").
Adopting it would overturn a locked decision, not fill a gap. **Rejected.**

**B ⊇ A.** Option B's counter/metric half *is* Option A; B adds only a config
path on top. The proposal's own risk section says so, and says A must land first
regardless. So the ruling is genuinely one bit:

> **Approve Option A now. Defer Option B to a separate, later decision.**

**Option A — keep the tolerance, make it observable.** Phase 4 still cannot fail;
it additionally reports *why* it returned nothing.

- **Which failure the counter counts — decide this explicitly.** FM-037's
  Observable column (spec:646) covers **two** events: (a) the phase-4
  read/parse failure in `functions::restore`, and (b) the wiring layer's
  per-library **compile** failure at registration time, which happens in
  `init.rs` after `recover` has returned. A counter incremented inside
  `functions::restore` counts **(a) only**, and it is boot-scoped and
  effectively 0-or-1 — the whole file either read or did not. It is *not* a
  count of failed libraries. **Name it for that**: something like
  `frogdb_recovery_function_catalog_unreadable_total`, not
  `..._functions_failed_total`, which would imply a per-library number and make
  the metric uninterpretable the moment (b) is also instrumented. If (b) should
  also be counted, that is a second handle emitted from the wiring layer and a
  second sentence in the spec — say so in the ruling rather than letting one
  name straddle both.
- *Spec edit*: FM-037 **Invariant** (spec:648) drops "with no counter or metric
  to notice it by"; **Observable** (spec:646) gains the counter, scoped to (a).
  **NOT observable** (spec:647) is untouched — the refusal is still forbidden.
- *New forcing test*: `corrupt_functions_file_is_counted`, asserting the counter
  is 1 after a truncated catalog and 0 after a fresh boot. Add its name to
  FM-037's **Forced by** (spec:650) in the same commit.
- *Cost — five items, not one*:
  1. **Row edit** to FM-037 (Invariant + Observable).
  2. **Forced-by edit** to FM-037 (spec:650) naming the new test.
  3. **Metric definition** — emission must go through a typed handle generated by
     `define_metrics!` in `frogdb-server/crates/types/src/metrics/definitions.rs`,
     following the `RecoveryKeysFailed` precedent at line 233 (consumed at
     `recovery/src/shards.rs:14, 100`). A raw string-named `increment_counter`
     is rejected by `just lint-metrics-chokepoint` (Justfile:1198), which is in
     `just lint-gates` (Justfile:329) and therefore runs on every commit and in
     CI's `seam-gates` job.
  4. **Generated-artifact regen** — the metric name propagates out of the
     definitions file into tracked artifacts: `website/src/data/metrics.json`
     (`just docs-gen`), `frogdb-server/ops/grafana/frogdb-overview.json`
     (`just dashboard-gen`), and the Helm chart's bundled copy at
     `frogdb-server/ops/deploy/helm/frogdb/dashboards/frogdb-overview.json`
     (`just helm-gen`, which **must run after** `dashboard-gen` — Justfile:791–794;
     `just generate` sequences them). CI runs the `--check` variants, so skipping
     regen fails the build. Whether the counter also earns a dashboard *panel* is
     a `dashboard-gen` decision to make deliberately, not a side effect.
  5. **Mutation re-gate** on `frogdb-recovery`.
- Still the smallest blast radius: no new config surface, no client-visible
  behavior change. Directly answers the gap FM-037's own last sentence names.

**Option B (deferred) — extend `recovery.on-decode-failure` to cover the
catalog.** Under the default `continue`, A's behavior; under the opt-in `refuse`,
an unreadable `functions.fdb` refuses the boot with a
`RecoveryPhase::RestoreFunctions` error.

- *Spec edit*: FM-037 gains an opt-in sibling exactly as FM-033 gained FM-047 —
  a new row, or an amended 037 with a policy clause. FM-037's NOT-observable must
  be narrowed to "*under the default*", the qualification FM-033's already carries.
- *New forcing tests*: `corrupt_functions_file_refuses_under_refuse_policy`; and
  `corrupt_functions_file_is_tolerated`'s existing `continue_policy()` argument
  (tests.rs:227) becomes load-bearing rather than incidental.
- *The open question to settle before approving B*: one knob would cover two
  different artifact classes (keyspace values and a script catalog). An operator
  who wants strictness about their *data* does not obviously want it about their
  *scripts*; a shared knob asserts they are inseparable.
- *Required input* (per CLAUDE.md's "research what Redis, Valkey, and DragonflyDB
  do"): Redis stores functions *inside* the RDB rather than in a separate file, so
  its corrupt-catalog behavior is entangled with its corrupt-RDB behavior and does
  not transfer directly. FrogDB's split into `functions.fdb` is the deviation that
  creates the question, and it should be documented in the spec's "Redis
  deviations" section. **This comparison is not settled here** — it is input to
  the *B* ruling, and A does not need it (A changes no behavior).

## Testability improvement

**Correction to the lane candidate: the corrupt-catalog test is not "unwritable"
— it already exists and passes.** `corrupt_functions_file_is_tolerated`
(tests.rs:210–238) writes `b"not a valid function dump"` and asserts
`recovered.functions.is_empty()`.

The real testability gap is finer, and it is the one FM-037's NOT-observable
column already names (spec:647). Today `RecoveredState.functions == vec![]` is
the only observable of the failure path, and it is produced identically by a fresh
boot with no catalog, a boot with an empty catalog, and a boot with a truncated or
garbage catalog.

**Corrected mutant prediction.** The first draft claimed a whole-body
`Ok(Vec::new())` mutant survives. It does not: `persisted_functions_are_restored`
(tests.rs:1230, tagged FM-PERSISTENCE-037 at 1224) writes a real library through
`frogdb_core::save_to_file` and asserts the exact `(name, source)` pair back at
tests.rs:1259–1263. It lives in the same crate, so `cargo mutants -p
frogdb-recovery` runs it, and it kills any mutant that empties the success path.

What survives is **narrower**: a mutant that deletes the `warn!` (tests.rs:210's
assertion cannot see a log line), and a mutant that replaces the `Err` arm alone
with `Ok(Vec::new())` — which is a no-op, since that is what the arm already
does, making the arm's *diagnostic* content unforced. So the conclusion "the `Err`
arm is weakly forced" stands, at smaller scale: the arm's **control flow** is
pinned, its **reporting** is not. Under Option A the reporting becomes an
assertable value, which is precisely the "silent skipping with no counter, or a
counter that reaches nobody" argument FM-033's NOT-observable column already makes
for keys, applied to the artifact it was never applied to.

*(The surviving-mutant claim above is reasoning from test bodies, not a measured
`cargo mutants` run — no builds were run for this proposal. Confirm with `just
mutants-diff frogdb-recovery`.)*

Part 2b adds a second, smaller improvement: the install's move of
`replication_metadata.json` from the staged dir to the data dir is currently
asserted only end-to-end, from another crate, through a literal. Pinning it in
`staged.rs` puts the assertion in the crate that owns the fact — which matters for
the gate, since `cargo mutants -p <crate>` runs only that package's own tests.

## Risks / scope boundaries

### `persistence-failure-modes.md` has **four** editors this round

Verified against the siblings' current on-disk text. The spec file is a single
732-line document and every one of these lands a diff in it, so **the four edits
must be serialized** — whichever lands second rebases.

| Proposal | Its edit to `persistence-failure-modes.md` | Overlap with 43 |
|---|---|---|
| **38** snapshot-layout-fs-read | **Prose only**: FM-PERSISTENCE-017, -018 and -023 name `load_latest_metadata` / `cleanup_old_snapshots` / the `SnapshotFs` method list in their Invariant text; renaming those into `SnapshotLayout` leaves the prose stale (`scripts/failure-modes.py` checks `Forced by` names, not prose). 38 states it will update the prose in the same commit and rename **no** forced test | **None** — disjoint rows. Text-level rebase only |
| **39** recovery-replay-driver | Re-points **FM-PERSISTENCE-047's Invariant** (spec:525, names `recover_all_shards`) and appends to **FM-033/041 `Forced by`** for tests that move with the file | **None** — disjoint rows |
| **41** persistence-small-dedups | Rewrites `Forced by` for **FM-PERSISTENCE-015** (spec:252) and **FM-PERSISTENCE-016** (spec:264). *(41's reviewer is reported to have narrowed this to FM-015 only; 41's on-disk text as of this writing still names both. Either way it is disjoint from 43.)* | **None** — disjoint rows |
| **43** (this) | **FM-PERSISTENCE-027 `Forced by`** (spec:426) + Invariant prose (2b); **FM-PERSISTENCE-037 Observable/Invariant/`Forced by`** (spec:646, 648, 650) (Part 3 Option A) | — |

No two proposals edit the same row. The serialization constraint is purely
mechanical (one file, four diffs).

### Correction to proposal 41's expectation of this proposal

`41:427` describes 43's scope as *"`frogdb-recovery` phase wrappers,
`staged.rs`/`replication.rs`/`functions.rs`, **checkpoint/cluster module fold**"*.
**That expectation is wrong and 43 explicitly rules against it** — see Part 1:
`checkpoint.rs` and `cluster.rs` are **not** folded. 41 should not plan around a
fold that will not happen. The rest of 41's row is correct: 41 hands
`RecoveredState.installed_staged_checkpoint` to 43 and makes no change in
`frogdb-recovery`.

**On that handed-over item — do not delete it.** It has **no production reader at
all** (verified: `lib.rs:126` is the *field declaration*, `lib.rs:262` is the
struct-literal *write* in `recover`; the only readers are four test assertions at
tests.rs:135, 167, 613, 1569), which makes it look like a deletion-test candidate.
It is **spec-visible**: FM-PERSISTENCE-025's *Outcome variant* (spec:391) names
`RecoveredState::installed_staged_checkpoint` by hand, and FM-PERSISTENCE-039's
(spec:673) names it again. Removing it would silently unforce two rows.
**Ruled: keep, out of scope here**; if ever pursued it is spec-first.

### vs. proposal 39 — no file overlap

| Proposal | Owns |
|---|---|
| **39** | `core/src/persistence/store_recovery.rs`, `persistence/src/recovery.rs`, `core/src/persistence/mod.rs`, core's recovery tests, plus one line at `recovery/src/shards.rs:56` (the `duration_ms` deletion) |
| **43** | `recovery/src/{functions,tests}.rs`, `persistence/src/rocks/{staged,tests}.rs`, `scripting/src/persistence.rs`, `server/src/function_store.rs`, `types/src/metrics/definitions.rs` (Option A) |

`recovery/src/shards.rs` is the one file both proposals *reason* about; 43 does
not edit it. 39's own boundary note (39:450–456) anticipates that 43 might reshape
`shards.rs:44–67` — **it will not**; 43's Part 1 ruling leaves `shards.rs`
untouched, so 39 can take its one line there without coordination.

### Locked-area discipline

`frogdb-recovery` + `frogdb-persistence` are the persistence locked pair, gate
0.85; the spec header (line 3–4) records the phase-2 run at `frogdb-persistence
99.1%, frogdb-recovery 100%`. Every part requires `just mutants-diff <crate>`
before push:

- **Part 1**: `frogdb-recovery` + `frogdb-scripting`/`frogdb-server` (unlocked).
  Constant substitution only — re-gate expected trivial.
- **Part 2a**: tests only, two crates. No production mutation surface changes.
- **Part 2b**: `frogdb-persistence` (one associated fn + one test). Skipping the
  `frogdb-replication` delegation keeps the *replication* locked pair out of it
  entirely — that is the whole reason to skip it.
- **Part 3 / Option A**: `frogdb-recovery` + `frogdb-types`, and **spec-first** —
  the FM-037 edit and its forcing test land before `functions.rs` changes.
  `just lint-failure-modes` enforces spec↔test agreement in both directions.

### No `frogdb-persistence` production dependency for `frogdb-recovery`

Part 2b is deliberately shaped to avoid adding one: the accessor and its test live
entirely inside `frogdb-persistence`, and recovery keeps reaching the protocol
through `frogdb_core` re-exports. Part 2a uses the existing **dev**-dependency
(Cargo.toml:23). The crate doc (lib.rs:10–20) makes a point of recovery depending
on neither the server nor an async-network crate, and of its tests building
without the server's 130K-LOC test binary; widening the production dependency
graph for a path constant would be a poor trade.

## Effort estimate

- **Part 1 — S.** One constant, three call sites, one re-export. No behavior
  change. Independently landable.
- **Part 2a — S, land first.** 16 literal substitutions across two crates (8 in
  `frogdb-recovery`, 8 in the owning crate's own tests), 11 more optional in the
  server integration tests. No manifest change, no production code, no `Forced by`
  change. Removes a rename hazard from **six** locked rows' forcing tests at
  essentially zero risk.
- **Part 2b — S, optional.** One associated fn, one test, one `Forced by` line.
  Recommend **dropping the `frogdb-replication` delegation** — one line of path
  composition is not worth a second locked-crate re-gate. Without it, this is a
  single-crate change.
- **Part 3 — blocked on one binary ruling.** **Approve A / defer B.** A is S–M:
  spec row edit + `Forced by` edit + typed metric definition + generated-artifact
  regen (`just docs-gen`, `just generate`) + one forcing test + the metrics
  chokepoint gate. B is M and additionally needs the Redis/Valkey/DragonflyDB
  comparison and the "one knob, two artifact classes" argument settled; it cannot
  start before A has landed anyway, since B ⊇ A.
