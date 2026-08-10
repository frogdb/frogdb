# Proposal 43 — `frogdb-recovery` phase wrappers, the staged-checkpoint owner, and the function-catalog error policy

## Summary

Three findings in `frogdb-recovery`, one of which is a **decision request**, not a change.

1. **The phase wrappers are thin, and that is correct.** Three of the six phase
   modules (`checkpoint.rs` 39 lines, `cluster.rs` 27, `functions.rs` 28) hold
   ~8 real lines each behind a `pub(crate)` function with exactly one caller.
   The deletion test says the complexity **moves**, it does not concentrate —
   but that is the wrong question here, because these are not modules at a
   **seam**. The crate's **interface** is one function, `recover`; the phase
   modules are **internal seams** private to its **implementation**. Folding
   them would trade a legible one-module-per-phase decomposition for a longer
   `recover` and no gain in **depth**. **Ruled: do not fold.** What the deletion
   test *does* kill is one thing inside `functions.rs`: the string `"functions.fdb"`,
   authored independently by the reader (`functions.rs:20`) and the writer
   (`server/src/function_store.rs:337`).
2. **The `StagedCheckpoint` owner owns two of its three location facts.**
   `persistence/src/rocks/staged.rs` declares itself "one typed owner for the
   on-disk protocol" and names `frogdb-recovery` as one of its three parties.
   It models the staged dir and the metadata file *inside* it — but not where
   that metadata lands **after** the install, which is the only location
   recovery ever reads. That third fact is authored in
   `replication/src/state.rs:410-412`, and `recovery/src/replication.rs:42,54`
   reaches it through `frogdb_core` re-exports of replication free functions.
   Recovery cannot name `StagedCheckpoint` at all today. Meanwhile recovery's
   own tests hand-spell `"replication_metadata.json"` and `"checkpoint_ready"`
   eight times — exactly the duplication that module's doc says it exists to
   end.
3. **`functions.rs` swallows every error to `Ok(vec![])` — and this is
   spec-pinned, not an oversight.** **FM-PERSISTENCE-037** already states the
   invariant verbatim ("Phase 4 cannot fail: every read/parse error is
   downgraded to `Ok(Vec::new())`") and already names the cost ("a silently
   smaller `FUNCTION LIST` with no counter or metric to notice it by"). So the
   error policy is a **spec question over a LOCKED row**, and changing it is an
   *edit* to FM-037, not a new row. This proposal presents three options and
   **picks none**; it requires a decision before any code moves.

## Files involved (verified paths + counts)

| Path | Lines | Role |
|------|-------|------|
| `frogdb-server/crates/recovery/src/lib.rs` | 270 | The one **seam**: `recover` (182–265), `RecoveryInputs`, `RecoveredState`, `RecoveryPhase` |
| `frogdb-server/crates/recovery/src/checkpoint.rs` | 39 | Phase 1 wrapper — `install_staged` (22–39), 1 caller (lib.rs:205) |
| `frogdb-server/crates/recovery/src/cluster.rs` | 27 | Phase 6 wrapper — `open_storage` (18–27), 1 caller (lib.rs:253) |
| `frogdb-server/crates/recovery/src/functions.rs` | 28 | Phase 4 wrapper — `restore` (19–28); authors `"functions.fdb"` at line 20; swallows at 23–26 |
| `frogdb-server/crates/recovery/src/replication.rs` | 75 | Phase 5 — real reconcile logic; the staged-metadata calls at 42 and 54 |
| `frogdb-server/crates/recovery/src/shards.rs` | 183 | Phases 2–3 — real policy (`report_decode_failures`, 95–144). **Owned by proposal 39's boundary; not edited here** |
| `frogdb-server/crates/recovery/src/data_dir.rs` | 166 | Phase 0 — real logic (marker verify/stamp). Not a wrapper |
| `frogdb-server/crates/recovery/src/tests.rs` | 1580 | Seam tests; hardcodes the staged literals at 314, 337, 381, 416 (metadata) and 590, 641, 1516, 1558 (`checkpoint_ready`) |
| `frogdb-server/crates/persistence/src/rocks/staged.rs` | 204 | The declared owner: `StagedCheckpoint` (51–91), the two constants (28, 33) |
| `frogdb-server/crates/replication/src/state.rs` | 958 | `read_staged_replication_metadata` (52–78), `consume_…` (83–99), `ReplicationState::staged_metadata_path` (410–412) — the unowned third location |
| `frogdb-server/crates/scripting/src/persistence.rs` | 440 | `load_from_file` (207–225) / `save_to_file` — both take a path; neither owns the name |
| `frogdb-server/crates/server/src/function_store.rs` | 394 | The catalog **writer**; re-authors `"functions.fdb"` at line 337 |
| `frogdb-server/crates/config/src/recovery.rs` | 164 | `RecoveryConfig::on_decode_failure` (45–60) — the existing boot-time decode policy that does *not* reach the catalog |
| `frogdb-server/crates/recovery/Cargo.toml` | 26 | `frogdb-persistence` is a **dev-dependency only** (line 23) — recovery has no production path to `StagedCheckpoint` |
| `.scratch/hardening/specs/persistence-failure-modes.md` | 732 | FM-027 (417–427), FM-037 (641–651), FM-039 (668–679). Header: `Status: LOCKED` |

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
Nothing reappears across N callers, because there is exactly one caller. By the
letter of the test it is a pass-through. `cluster.rs` (gate + `join("raft")` +
open + message) and `functions.rs` (join + downgrade) are the same shape, all
three at one caller each — verified: every `pub(crate)` phase function in the
crate is called exactly once, from `recover` (lib.rs:203, 205, 209, 211, 213,
215, 249, 253).

**But the test is being pointed at the wrong thing.** LANGUAGE.md: *"A module
can have internal seams (private to its implementation, used by its own tests)
as well as the external seam at its interface."* The **module** here is the
crate, its **interface** is `recover(inputs) -> Result<RecoveredState,
RecoveryError>`, and that interface is genuinely **deep** — plain-data in,
plain-data out, over filesystem + RocksDB internals, with no live components
(lib.rs:10–13). The six phase files are `pub(crate)`: they are not part of the
interface, so they cost callers and tests nothing to learn. They exist to give
each of the seven `RecoveryPhase` variants a documentation home — and
`checkpoint.rs`'s 21-line doc comment carrying the FM-024/025 install contract
is real value that has nowhere better to live.

Folding three of six would also break the symmetry that makes the crate
readable: three phases inline, three in files, for no gain in **depth**,
**leverage**, or **locality**. **Ruled: do not fold.** This is the DELETE branch
that the deletion test declines to support.

What survives the ruling is one genuine finding *inside* `functions.rs`. The
catalog filename is authored twice in production:

- reader — `recovery/src/functions.rs:20`: `let functions_path = inputs.data_dir.join("functions.fdb");`
- writer — `server/src/function_store.rs:337`: `let path = PathBuf::from(self.config.data_dir()).join("functions.fdb");`

`frogdb-scripting` owns `save_to_file`/`load_from_file` but both take a `&Path`,
so nothing owns the name. This is a cross-crate on-disk protocol with no owner —
the same class of problem `StagedCheckpoint` was created to solve for the
staged-checkpoint names (staged.rs:19–20: *"Before this module the dir/file names
were string literals duplicated across the three crates"*).

### Part 2 — the staged-checkpoint owner is missing its third location

`StagedCheckpoint` (staged.rs:51–91) exposes `for_db_dir`, `in_parent`, `dir`,
`exists`, `is_complete_db`, `replication_metadata_path`. Its doc (staged.rs:1–20)
names three collaborating parties and calls recovery the **Orchestrator** that
*"afterwards consumes the replication metadata that the install carried into the
data dir."*

The protocol therefore has **three** path facts, not two:

| Fact | Location | Authored at |
|---|---|---|
| the staged dir | `<parent>/checkpoint_ready` | `StagedCheckpoint::dir` (staged.rs:72) ✅ owner |
| metadata *inside* the staged dir | `<parent>/checkpoint_ready/replication_metadata.json` | `StagedCheckpoint::replication_metadata_path` (staged.rs:88) ✅ owner |
| metadata *after* the install | `<data_dir>/replication_metadata.json` | `ReplicationState::staged_metadata_path` (replication/state.rs:410–412) ❌ **not the owner** |

The third is the only one recovery reads. `recovery/src/replication.rs`:

```rust
42:    match frogdb_core::read_staged_replication_metadata(inputs.data_dir) {
54:                Ok(()) => frogdb_core::consume_staged_replication_metadata(inputs.data_dir),
```

Both are `frogdb-replication` free functions re-exported through
`frogdb_core` (core/src/lib.rs:134–135). And recovery has **no production
dependency on `frogdb-persistence`** — Cargo.toml:23 lists it under
`[dev-dependencies]` with `features = ["test-support"]` only — while
`frogdb_core` does not re-export `StagedCheckpoint` at all (verified: the
persistence re-export block, core/src/lib.rs:116–121, does not name it). So the
orchestrator the owner's doc names *cannot* reach the owner.

The consequence is that the load-bearing relationship — *rename 2 moves the
metadata from `staged.dir()/F` to `data_dir/F`*, which is FM-PERSISTENCE-027's
Invariant in prose ("the install is what carries `replication_metadata.json`
into the data dir for phase 5 to find") — is nowhere expressed as code the owner
controls. `staged.rs`'s own test `staged_paths_are_the_cross_crate_contract`
(133–164) pins that the metadata sits *inside* the staged dir "so the commit
rename carries it", and stops there. The post-rename half is only checked
end-to-end, by `staged_replication_metadata_is_adopted_and_consumed`
(tests.rs:302–342), which reaches it by writing a **hardcoded literal**.

Which is the second half of the finding. Recovery — one of the three named
parties — spells the protocol names by hand eight times in its own tests:

- `"replication_metadata.json"` at tests.rs:314, 337, 381, 416
- `"checkpoint_ready"` at tests.rs:590, 641, 1516, 1558

Production code is clean (verified by grep across the workspace: only
staged.rs:28,33 define them, and no other production site re-spells them). The
duplication is entirely in tests — including the tests that *force*
FM-PERSISTENCE-025, -027 and -039. A rename of either constant would leave those
forcing tests silently testing a path the product no longer uses.

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

**The lane classified this "LIVE-by-design-unknown". Verified: it is
by-design, and the design is already written down and LOCKED.**
FM-PERSISTENCE-037 (persistence-failure-modes.md:641–651):

- *Observable*: "The server starts. Unreadable file → no functions and a `WARN`."
- *NOT observable*: "A database refusing to start because of a stored script — the keyspace is the valuable thing and a function library is recoverable by re-uploading it."
- *Invariant*: "Phase 4 cannot fail: every read/parse error is downgraded to `Ok(Vec::new())`. … **The known cost is a silently smaller `FUNCTION LIST` with no counter or metric to notice it by.**"
- *Forced by*: `corrupt_functions_file_is_tolerated`, `persisted_functions_are_restored`

So there is no undocumented hole. What there is, is an **asymmetry the spec
itself flags in its last sentence**: the shard-data path treats an undecodable
value as *skip, count, and surface* (FM-033), refuses outright when nothing
decoded (FM-045), and offers `recovery.on-decode-failure = refuse` as the
operator opt-in (FM-047, config/recovery.rs:45–60). The function catalog gets
the first half of that treatment (skip) and none of the second (count, surface,
policy). `RecoveryStats` carries `keys_failed` and there is a
`RecoveryKeysFailed` metric handle (shards.rs:14, 100); there is no
`functions_failed` analogue anywhere.

The observable gap: a boot where `functions.fdb` is truncated and a boot where
no library was ever stored are **indistinguishable** to everything except a
`WARN` line. `RecoveredState.functions` is `vec![]` in both.

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
(core/src/lib.rs:96), and use it at both `functions.rs:20` and
`function_store.rs:337`. **No behavior change.** Do **not** fold the phase
modules — see the ruling above.

### Part 2 — give `StagedCheckpoint` its third location, and stop hand-spelling it

**2a (independently landable hotfix, S, zero production risk).** Replace the
eight literals in `recovery/src/tests.rs` with
`frogdb_persistence::rocks::staged::{STAGED_CHECKPOINT_DIR,
STAGED_REPLICATION_METADATA_FILE}`. The dev-dependency is already wired
(recovery/Cargo.toml:23), so this needs no manifest change and touches no
production code. It closes the rename hazard on the tests that force FM-025,
-027 and -039.

**2b (S–M).** Add the missing fact to the owner and make the other authors
delegate:

```rust
// frogdb-server/crates/persistence/src/rocks/staged.rs
impl StagedCheckpoint {
    /// Where the staged replication metadata lives **after** the install.
    ///
    /// The install is `rename(checkpoint_ready -> <db>)`, so the file the
    /// writer stamped inside the staged dir arrives at the same name inside
    /// the live data dir. Phase 5 of recovery reads it from here — the second
    /// half of the relationship [`replication_metadata_path`] describes.
    pub fn installed_replication_metadata_path(data_dir: &Path) -> PathBuf {
        data_dir.join(STAGED_REPLICATION_METADATA_FILE)
    }
}
```

`ReplicationState::staged_metadata_path` (replication/state.rs:410–412) becomes
a delegation to it — legal, because `frogdb-replication` already depends on
`frogdb-persistence` (state.rs:26 imports the constant from it; the reverse
dependency does not exist and must not be introduced).

Pin the relationship in `staged.rs`'s own test, tagged **FM-PERSISTENCE-027**:
that `for_db_dir(db).replication_metadata_path()` and
`installed_replication_metadata_path(db)` are the *same file name* on the two
sides of the install rename — the property that makes phase 5 find what phase 1
carried.

**Deliberately out of scope:** moving the *typed* read/consume into
`frogdb-persistence`. `StagedReplicationMetadata` is a replication type and
persistence cannot depend on replication. `recovery/src/replication.rs:42,54`
keeps calling the `frogdb_core` re-exports unchanged; only the path derivation
consolidates. Whether `frogdb_core` should also re-export `StagedCheckpoint` so
recovery can name the owner directly is an **open question** — it would let a
future phase assert the staged location itself, but it widens core's re-export
surface for one consumer. Not proposed here.

### Part 3 — the function-catalog error policy: **DECISION REQUIRED**

**This proposal does not choose.** FM-PERSISTENCE-037 is a LOCKED row that
states the current behavior as intended, so every option below is a spec **edit**
(row first → forcing test → code), not a new row bolted on beside it. Both
`continue_policy()` and `refuse_policy()` helpers already exist in
`recovery/src/tests.rs:46,53`, so the harness for any of them is in place.

**Option A — keep the tolerance, make it observable.** Phase 4 still cannot
fail; it additionally reports *why* it returned nothing. Either a
`frogdb_recovery_functions_failed_total` counter through the injected
`metrics_recorder` (mirroring `RecoveryKeysFailed`, shards.rs:100), or a typed
outcome on `RecoveredState` the wiring layer can surface in `INFO persistence`.

- *Spec edit*: FM-037 **Invariant** drops "with no counter or metric to notice
  it by"; **Observable** gains the counter. **NOT observable** is untouched —
  the refusal is still forbidden.
- *New forcing test*: `corrupt_functions_file_is_counted_and_metered`, asserting
  the counter is 1 after a truncated catalog and 0 after a fresh boot.
- *Cost*: smallest blast radius, no new config surface, no client-visible
  behavior change. Directly answers the gap FM-037's own last sentence names.

**Option B — extend `recovery.on-decode-failure` to cover the catalog.** Under
the default `continue`, today's behavior plus Option A's counter. Under the
opt-in `refuse`, a `functions.fdb` that will not read refuses the boot with a
`RecoveryPhase::RestoreFunctions` error.

- *Spec edit*: FM-037 gains an opt-in sibling exactly as FM-033 gained FM-047 —
  either a new row `FM-PERSISTENCE-0NN — recovery.on-decode-failure = refuse
  also refuses an unreadable function catalog`, or an amended 037 with a policy
  clause. FM-037's NOT-observable must be narrowed to "*under the default*",
  the same qualification FM-033's NOT-observable already carries.
- *New forcing tests*: `corrupt_functions_file_refuses_under_refuse_policy`;
  existing `corrupt_functions_file_is_tolerated` re-pinned explicitly to
  `continue_policy()` (it already passes `continue_policy()` at tests.rs:219 —
  the pin becomes load-bearing rather than incidental).
- *Cost*: one knob covering two different artifact classes (keyspace values and
  a script catalog). Argue whether an operator who wants strictness about their
  *data* necessarily wants it about their *scripts* — the two are separable and
  a shared knob asserts they are not.

**Option C — fail recovery unconditionally.** Stated so it is rejected
explicitly rather than by omission: it contradicts FM-037's NOT-observable
column and its stated rationale ("the keyspace is the valuable thing and a
function library is recoverable by re-uploading it"). Adopting it would mean
overturning a locked decision, not filling a gap.

**Required input to the decision** (per CLAUDE.md's "research what Redis,
Valkey, and DragonflyDB do"): Redis stores functions *inside* the RDB rather
than in a separate file, so its corrupt-catalog behavior is entangled with its
corrupt-RDB behavior and does not transfer directly. FrogDB's split into
`functions.fdb` is the deviation that creates the question, and the deviation
should be documented in the spec's "Redis deviations" section whichever option
is chosen. **This comparison must be researched properly before the ruling — it
is not settled here.**

## Testability improvement

**Correction to the lane candidate: the corrupt-catalog test is not "unwritable"
— it already exists and passes.** `corrupt_functions_file_is_tolerated`
(tests.rs:210–238) writes `b"not a valid function dump"` and asserts
`recovered.functions.is_empty()`.

The real testability gap is finer, and it is the one FM-037's last sentence
names. Today `RecoveredState.functions == vec![]` is the *only* observable, and
it is produced identically by:

- a fresh boot with no catalog,
- a boot with an empty catalog,
- a boot with a truncated or garbage catalog.

So `corrupt_functions_file_is_tolerated`'s assertion cannot distinguish "the
downgrade fired" from "the success path returned nothing". Concretely, that
makes the `Err` arm weakly forced: a mutant that replaces the whole body with
`Ok(Vec::new())` still satisfies it, and so does one that deletes the `warn!`.
Under Option A or B the distinction becomes an assertable value — a counter or a
typed outcome — which is precisely the "silent skipping with no counter, or a
counter that reaches nobody" argument FM-033's NOT-observable column already
makes for keys, applied to the artifact it was never applied to.

*(The surviving-mutant prediction is a hypothesis to confirm with `just
mutants-diff frogdb-recovery`, not a measured result — no builds were run for
this proposal.)*

Part 2 adds a second, smaller improvement: the install's move of
`replication_metadata.json` from the staged dir to the data dir is currently
asserted only end-to-end, through a literal. Pinning it in `staged.rs` puts the
assertion in the crate that owns the fact — which also matters for the gate,
since `cargo mutants -p <crate>` runs only that package's own tests.

## Risks / scope boundaries

**vs. proposal 39 (`recovery-replay-driver`) — no file overlap.** Verified
against 39's own files table:

| Proposal | Owns |
|---|---|
| **39** | `core/src/persistence/store_recovery.rs`, `persistence/src/recovery.rs`, `core/src/persistence/mod.rs`, and core's recovery tests. 39 explicitly states `recovery/src/shards.rs` is **not edited** (its `restore` signature is preserved) |
| **43** | `recovery/src/{lib,checkpoint,cluster,functions,replication,tests}.rs`, `persistence/src/rocks/staged.rs`, `replication/src/state.rs`, `scripting/src/persistence.rs`, `server/src/function_store.rs` |

`recovery/src/shards.rs` is the one file both proposals *reason* about; neither
edits it. Two real coordination points: (a) both touch
`persistence-failure-modes.md` — 39 amends FM-047's Invariant, 43 amends FM-037
(and adds an FM-027 tag in `staged.rs`) — so they must not land the spec edit in
one commit; (b) both trigger a `frogdb-persistence` mutation re-gate, so
whichever lands second re-runs it.

**Locked-area discipline.** `frogdb-recovery` and `frogdb-persistence` are the
persistence locked pair, gate 0.85; the spec header records the phase-2 run at
`frogdb-persistence 99.1%, frogdb-recovery 100%`. Every part below requires
`just mutants-diff <crate>` before push:

- Part 1: `frogdb-recovery` + touches `frogdb-scripting` and `frogdb-server`
  (unlocked). Constant substitution only — re-gate expected trivial.
- Part 2a: tests only. No production mutation surface changes.
- Part 2b: `frogdb-persistence` (new method + new test) and `frogdb-replication`
  (delegation). Note `frogdb-replication` is in the *replication* locked pair
  (gate 0.85) — a one-line delegation, but it is a locked crate and re-gates
  there too.
- Part 3: `frogdb-recovery`, and **spec-first** — the FM-037 edit and its forcing
  test land before `functions.rs` changes. `just lint-failure-modes` enforces
  spec↔test agreement in both directions, so a new row with no tagged test (or a
  tagged test with no row) fails `just lint`.

**Do not delete `RecoveredState.installed_staged_checkpoint`.** It has no
production reader (verified: readers are lib.rs:126, 262 and four test
assertions at tests.rs:135, 167, 613, 1569) and so looks like a deletion-test
candidate — the persistence lane lists it as one. It is **spec-visible**:
FM-PERSISTENCE-025's *Outcome variant* names
`RecoveredState::installed_staged_checkpoint` by hand. Removing it would silently
unforce that row. Out of scope here; if pursued it is spec-first.

**Part 3's scope creep risk.** Option B's counter/metric half overlaps Option A
entirely. If B is chosen, land A's observability first as its own change so the
policy knob lands on top of an already-observable phase rather than introducing
both at once.

**No `frogdb-persistence` production dependency for `frogdb-recovery`.** Part 2b
is deliberately shaped to avoid adding one: recovery keeps reaching the protocol
through `frogdb_core` re-exports. The crate doc (lib.rs:10–20) makes a point of
recovery depending on neither the server nor an async-network crate, and its
tests building without the server's 130K-LOC test binary; widening the
dependency graph for a path constant would be a poor trade.

## Effort estimate

- **Part 1 — S.** One constant, three call sites, one re-export. No behavior
  change. Independently landable.
- **Part 2a — S, independently landable hotfix.** Eight literal substitutions in
  one test file. Dev-dependency already present; no production code, no manifest
  change. **This is the piece to land first** — it removes a real rename hazard
  from three locked rows' forcing tests at essentially zero risk.
- **Part 2b — S–M.** One method, one delegation, one test. The M comes from
  touching two locked crates (`frogdb-persistence`, `frogdb-replication`) and
  therefore two mutation re-gates.
- **Part 3 — blocked on a decision; M once ruled.** Option A is S–M (counter +
  spec edit + one test). Option B is M (adds a config path, a second forcing
  test, and a narrowing edit to FM-037's NOT-observable column). Neither may
  start before the ruling, and the Redis/Valkey/DragonflyDB comparison is
  required input.
