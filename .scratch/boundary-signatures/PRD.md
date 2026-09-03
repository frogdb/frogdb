# Boundary signatures — a compact, high-signal test suite

Status: **approved 2026-09-02** — all §9 decisions ruled in the design session
Author: 2026-09-02
Related: [testing-improvements-round2 BRIEF](../testing-improvements-round2/BRIEF.md) (test
abstraction ladder, §"Test-design guidance"), [ADR-0005](../../adr/0005-truthful-redis-86-surface.md)
(`core-profile` is a build-speed tier, not a product tier), `specs/*.md` + `scripts/spec-lint.py`
(the row↔forcing-test mechanism this design reuses).

## 1. Problem

FrogDB has one runnable notion of "the test suite": `just test` = `cargo nextest run --all`
(plus `just concurrency` for shuttle/turmoil). Every edit to a boundary crate or to a command
implementation pays for the whole thing, and most of that cost is per-datatype surface testing —
56 per-datatype/per-boundary integration files in `frogdb-server/tests/`, 102 files in
`redis-regression`, every command family's own `integration_<family>.rs`.

At the application boundaries — the WAL sink, the replica stream, cluster routing, transaction
queueing, the blocking-waiter table — most of those commands are indistinguishable. `SET`,
`HSET`, `ZADD`, `GEOADD`, `JSON.SET`, `PFADD`-without-merge, `TS.ADD` all present the same shape:
one key, one post-execution value persisted, replicated verbatim, routed by a single slot, waking
nobody. Testing all of them against each boundary buys nothing the first one did not.

The goal is a **compact suite** that exercises every *distinct* boundary shape once per boundary,
selectable by one filter, kept honest by a lint, and runnable from a `just` recipe locally,
pre-push, and as an early CI job. Iteration on a boundary crate or a command implementation then
runs the compact suite; the full suite stays the merge gate.

## 2. What exists today (findings, 2026-09-02)

No prior initiative tiered or tagged tests. The closest prior art is authoring guidance:
round-2's five-level abstraction ladder (`BRIEF.md:102-124`) and its infrastructure Tier A/B/C
scheme (`INFRASTRUCTURE.md:20-28`), neither of which is mechanized into test selection.

Test selection today is by location and string convention only:

- nextest filtersets keyed on binary name (`binary(/^cluster_/)`), module path
  (`test(integration_replication::)`), or package — `Justfile` `core-test` / `core-test-e2e`,
  `.config/nextest.toml` overrides;
- `#[ignore = "..."]` free-text reasons for nightly tiers;
- `// FM-<AREA>-NNN` comment tags binding `specs/*.md` rows to forcing tests, enforced
  bidirectionally by `scripts/spec-lint.py` against `cargo nextest list`.

The command-classification metadata already exists and is compile-enforced. `CommandSpec`
(`frogdb-server/crates/core/src/command_spec.rs`) carries, per command, five parallel
projections of "what this command does", and each application boundary reads a different one:

| Boundary | Spec field(s) read | What actually crosses |
|---|---|---|
| Persistence (WAL) | `wal: WalStrategy` → `WalAction` set | key + post-execution value (state-shipping), tombstone, HLL merge delta, or whole-shard clear. Never command bytes. |
| Replication | `CommandFlags::{WRITE, NO_PROPAGATE, NONDETERMINISTIC}`; runtime `ReplicationOverride` | RESP-encoded command bytes (statement-shipping), or a rewritten form (`SPOP` → `SREM`/`DEL`), or nothing |
| Cluster routing / txn queueing | `strategy: ExecutionStrategy`, `keys: KeySpec`, `requires_same_slot` | extracted key list → slot; `Deferral` derives purely from `ExecutionStrategy` |
| Blocking | `wakes: WaiterWake` | which waiter kinds a write wakes |
| VLL | none — generic over `Operation` | key list + `LockMode` |

`ReindexSpec`'s doc comment notes it mirrors `WalStrategy` variant-for-variant. A single
"keyspace effect" enum is latent in the spec but not extracted; extracting it is out of scope
here (§10) and this design's classification function is its seed.

Only one command rewrites its propagation at runtime (`SPOP`, `commands/src/set.rs`), and it
carries `NONDETERMINISTIC`; `post_execution.rs` asserts the pairing. So the replication shape is
derivable from flags alone today.

## 3. The model: `BoundarySignature`

A **read-only classification** computed from `CommandSpec`. It changes no engine behavior: the
dispatcher, router, WAL and replication paths keep reading the spec fields they read today.
`signature()` is called by the census generator, one conformance test, and humans naming tags.

```rust
// frogdb-server/crates/core/src/signature.rs
pub struct BoundarySignature {
    pub persist:   PersistShape,    // what the WAL sink sees
    pub propagate: PropagateShape,  // what the replica stream sees
    pub route:     RouteShape,      // what cluster routing + txn queueing see
    pub wake:      WakeShape,       // what the blocking-waiter table sees
}

impl CommandSpec {
    /// Pure projection of the spec's boundary-facing fields. No side effects,
    /// no runtime information.
    pub const fn signature(&self) -> BoundarySignature { /* match */ }
}
```

### 3.1 Axes and derivation

Each axis collapses existing fields to the distinctions the boundary can actually observe.

**`PersistShape`** — from `WalStrategy`, keyed on the `WalAction` set it resolves to:

| variant | source `WalStrategy` |
|---|---|
| `None` | `NoOp` (reads, connection-level, admin) |
| `Upsert` | `PersistFirstKey`, `PersistDestination` |
| `Delete` | `DeleteKeys` |
| `UpsertOrDelete` | `PersistOrDeleteFirstKey` |
| `TwoKeyMove` | `RenameKeys`, `MoveKeys` |
| `MergeDelta` | `MergeDeltaOrPersistFirstKey` |
| `ClearShard` | `ClearShard(..)` |
| `Dynamic` | `Dynamic` (resolved at runtime from `keys_with_flags`) |

**`PropagateShape`** — from flags:

| variant | condition |
|---|---|
| `None` | `!WRITE`, or `WRITE ∧ NO_PROPAGATE` |
| `Verbatim` | `WRITE ∧ !NONDETERMINISTIC` |
| `Rewritten` | `WRITE ∧ NONDETERMINISTIC` (must deposit a `ReplicationOverride`) |
| `Control` | function-library mutators shipped on the control shard (`FUNCTION LOAD/DELETE/FLUSH/RESTORE`) |

**`RouteShape`** — from `ExecutionStrategy` × `KeySpec` × `requires_same_slot`. The op enums
are reused, not re-declared, and **the op stays in the shape**: `MGET` and `MSET` are different
paths through `dispatch_scatter`, `PubSub` crosses the cluster bus while `Admin` never leaves the
node.

| variant | condition |
|---|---|
| `Keyless` | `Standard` ∧ `KeySpec::None` |
| `SingleKey` | `Standard` ∧ exactly one key position |
| `MultiKeySameSlot` | `Standard` ∧ multi-key spec (incl. `requires_same_slot`) |
| `ScatterGather(ScatterGatherOp)` | `ScatterGather(op)` |
| `Blocking` | `Blocking { .. }` |
| `DynamicKeys` | `KeySpec::Dynamic` (⇔ `MOVABLEKEYS`) |
| `ConnectionLevel(ConnectionLevelOp)` | `ConnectionLevel(op)` |
| `ServerWide(ServerWideOp)` | `ServerWide(op)` |
| `Raft` | `RaftConsensus` |
| `AsyncExternal` | `AsyncExternal` |

**`WakeShape`** — `WaiterWake` as-is: `None`, `Kind(WaiterKind)`, `All`.

Transaction deferral is not an axis: `TxnHost::deferral_of` is a pure function of
`ExecutionStrategy`, so `RouteShape` already determines it. VLL sees only keys and lock mode, so
`RouteShape` covers it too.

### 3.2 Deliberately excluded

`EventSpec`, `ReindexSpec`, `LookupSpec`, `AdminSurface`, ACL categories, `docs.group`. These
are in-memory or client-surface concerns, not application boundaries. Tests for them belong to
the full suite.

### 3.3 Naming is mechanical

A signature's name is its axes joined, upper-cased, `SIG-` prefixed:

```
SIG-UPSERT-VERBATIM-1KEY-NOWAKE            SET, HSET, ZADD, GEOADD, JSON.SET, ...
SIG-DELETE-VERBATIM-SCATTER_DEL-ALL        DEL
SIG-NONE-NONE-SCATTER_MGET-NOWAKE          MGET
SIG-UPSERT-VERBATIM-1KEY-WAKE_LIST         LPUSH, RPUSH, LINSERT, ...
SIG-UPSERTORDELETE-REWRITTEN-1KEY-NOWAKE   SPOP
SIG-NONE-NONE-CONN_PUBSUB-NOWAKE           PUBLISH, SPUBLISH
```

The census emits the name, the spec section heading is the name, the test tag is the name. No
join table, no drift, and the test site reads as a description of what the command does at
every boundary. An axis rename renames every tag by `sed`; that is acceptable.

### 3.4 Dynamic buckets are the one exception to representative-only

`PersistShape::Dynamic` (`SORT ... STORE`, `GEORADIUS ... STORE`, `BITOP`, some string and
stream commands) and `RouteShape::DynamicKeys` (`EVAL`, `SORT`, `ZUNIONSTORE`, `MIGRATE`,
`XREAD`, JSON multi-path, ...) hide the real boundary behavior behind a runtime decision. One
representative proves nothing about the others. A Dynamic signature's spec section therefore
lists **every member** with its own `Forced by` row per boundary (§6). This also leaves a
standing incentive to make strategies static where possible; that work is out of scope.

## 4. Census

`docs-gen` (`frogdb-server/ops/docs-gen`) already links `frogdb-server` with `cmd-full` and
emits `website/src/data/commands.json` with a `docs-gen-check` drift job in CI. It gains a
second output, `website/src/data/signatures.json`:

```json
{ "_generated": { ... },
  "count": <distinct signatures>,
  "signatures": [
    { "name": "SIG-UPSERT-VERBATIM-1KEY-NOWAKE",
      "axes": { "persist": "Upsert", "propagate": "Verbatim", "route": "SingleKey", "wake": "None" },
      "members": ["APPEND", "GEOADD", "HSET", ...] },
    ...
  ] }
```

The census is the ground truth for "which commands share a signature" and is never
hand-maintained. Expected order of magnitude: 35–45 distinct signatures over 391 commands; the
real number comes from running it, and the spec (§6) is written from that output.

**Conformance test.** CI's `cmd-full` job only type-checks; no test ever runs under `cmd-full`.
So the check that *every* signature has a `core-profile` member cannot be a test over the full
registry. Instead, a normal test in `frogdb-server` (core-profile build) computes the census of
its own registry and asserts that the set of names in the checked-in `signatures.json` (full
census) is a subset. Consequence: the compact suite never needs `cmd-full` — `SET` stands in for
`GEOADD` and `JSON.SET` at every boundary.

## 5. The compact suite: location, not tooling

Compact suite = two greppable sets, selected by one nextest filterset:

1. The boundary crates, entire: `frogdb-txn`, `frogdb-vll`, `frogdb-persistence`,
   `frogdb-recovery`, `frogdb-replication`, `frogdb-replication-runtime`, `frogdb-cluster`,
   `frogdb-cluster-runtime`. These already test datatype-free, in-crate.
2. Signature binaries: `frogdb-server/crates/server/tests/sig_{persistence,replication,cluster,txn}.rs`
   at `TestServer` level, plus shard-driver-level binaries where the boundary needs no socket —
   `sig_persistence` and `sig_wake` under `frogdb-server/crates/shard-harness/tests/` (the wake
   axis has no crate of its own; `specs/blocking.md` is its spec). At least one test per
   (signature × boundary), driving the signature's representative command.

```
-E 'package(/^frogdb-(txn|vll|persistence|recovery|replication|replication-runtime|cluster|cluster-runtime)$/) | binary(/^sig_/)'
```

**Level per boundary** (default, adjusted per test as the boundary demands; moving a test
between locations later is cheap): persistence and wake at `shard_driver` level (real dispatch,
real WAL seam, no connection); replication, cluster routing and txn at `TestServer` level.
Cluster signature tests use single-node cluster mode wherever the routing verdict
(`MOVED`/`ASK`/`CROSSSLOT`/`TRYAGAIN`) is computable from a fixed topology; multi-node
scenarios stay in the full suite.

**Existing representatives move, they are not tagged in place.** `integration_persistence.rs`
already contains "SET survives restart"; that test `git mv`s into `sig_persistence.rs`. The
`integration_*.rs` files shrink to surface and edge tests. Rule enforced by lint: a `// SIG-`
tag outside set 1 or set 2 is an error ("move the test or drop the tag"). Selection therefore
stays a one-line filter with no tooling in the hot path, and "is this test core or surface?" is
answered by `ls`.

**Recipes.** `just test-core [area]` replaces both `core-test <area>` and `core-test-e2e
<area>`, which are deleted (references in `.scratch/hardening/` updated). No area = the whole
filter above; an area = that area's crates + `binary(=sig_<area>)`. Areas are `txn`,
`persistence`, `replication`, `cluster`, and `blocking` (no crates, `binary(=sig_wake)` only). `just test` is unchanged
and remains the full suite. `redis-regression` stays outside both, on its own recipe.

## 6. The honesty gate: `specs/signatures.md` + `spec-lint`

A hand-authored spec, same grammar as the locked specs, no `Status: LOCKED` line (there is no
mutation gate behind it; it is a coverage contract). One `## SIG-<NAME>` section per census
signature:

```markdown
## SIG-UPSERT-VERBATIM-1KEY-NOWAKE

| | |
|---|---|
| Axes | persist=Upsert, propagate=Verbatim, route=SingleKey, wake=None |
| Representative | `SET` |
| Forced by (persistence) | `sig_persistence::upsert_1key_survives_restart` |
| Forced by (replication) | `sig_replication::upsert_1key_reaches_replica_verbatim` |
| Forced by (cluster) | `sig_cluster::upsert_1key_moved_on_foreign_slot` |
| Forced by (txn) | `sig_txn::upsert_1key_queued_and_applied_on_exec` |
```

Member lists are **not** repeated in the spec (they live in `signatures.json`), except for
Dynamic signatures (§3.4), whose sections list every member with per-member `Forced by` rows.

**Which boundaries a section must force.** A row is required exactly when the signature has
something to observe at that boundary; the lint derives the requirement from the `Axes` cell:

| row | required when |
|---|---|
| `Forced by (persistence)` | `persist ≠ None` |
| `Forced by (replication)` | `propagate ≠ None` |
| `Forced by (cluster)` | always — the routing verdict (including "node-local, exempt") is itself the behavior |
| `Forced by (txn)` | always — queued, deferred, or refused inside `MULTI` is itself the behavior |
| `Forced by (wake)` | `wake ≠ None` |

A row present when not required is a lint error (it would force a non-behavior); a row absent
when required is the usual `MISSING`.

`scripts/spec-lint.py` gains:

- the `SIG-` prefix in its section and tag regexes (it already handles `FM-`, `TR-`, `LV-`,
  `CO-`);
- a census-agreement check: every name in `signatures.json` has a section and vice versa; each
  section's `Representative` is a census member of that signature; Dynamic sections' member
  lists equal the census;
- the location rule from §5;
- the existing `Forced by` resolution against `cargo nextest list`, `MISSING ([gap: ...])`
  handling, and gap-issue allowlist, extended to accept `.scratch/boundary-signatures/issues/`.

Effect: a new command with a new signature fails `just lint-spec` until it has a section and
forcing tests. A command whose signature drifts (someone changes its `WalStrategy`) fails until
the census, the spec and the tests agree again. `lint-spec` already runs inside `just lint` and
the CI `lint` job, so this rides for free.

## 7. Change-based selection: `just test-affected`

`scripts/affected.py` maps changed paths (diff against `git merge-base HEAD main`, base
overridable) to a run plan through an ordered glob table, first match wins:

| path pattern | runs |
|---|---|
| `frogdb-server/crates/{txn,vll}/**` | `test-core txn` |
| `frogdb-server/crates/{persistence,recovery}/**` | `test-core persistence` |
| `frogdb-server/crates/{replication,replication-runtime}/**` | `test-core replication` |
| `frogdb-server/crates/{cluster,cluster-runtime}/**` | `test-core cluster` |
| `frogdb-server/crates/core/src/shard/persistence.rs`, WAL seam files | `test-core persistence` |
| `frogdb-server/crates/core/src/{command.rs,command_spec.rs,signature.rs,shard/post_execution.rs,...}` | `test-core` (all areas) |
| `frogdb-server/crates/commands/src/<family>/**` | `test-core` + `integration_<family>` |
| `frogdb-server/crates/server/tests/sig_<area>.rs` | `test-core <area>` |
| anything else | full `just test` |

Unknown → full is the safety valve. `--dry-run` prints the plan. The map lives in Python, not a
data file, so it is unit-testable with fixtures the way `spec-lint.py` is. The map is expected
to be coarse while `frogdb-core` remains monolithic; the crate split in §10 sharpens it later.

`test-affected` is local and pre-push only. It is not wired into CI (§8).

## 8. CI

`.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py` gains one job,
`core-tests` = `just test-core`, triggered on any rust change, with no dependency on
`unit-tests`, so it reports first. `unit-tests` (full `nextest run --all`) is unchanged and stays
required for merge. Change-based skipping is deliberately not in CI: a map that misses a
dependency would merge a break, and CI wall-clock is dominated by build, not test. The SIG lint
rides in the existing `lint` job; census drift in the existing `docs-gen-check` job.

## 9. Decisions log (design session, 2026-09-02)

| # | decision | ruling |
|---|---|---|
| Q0 | target loop | all three (local iteration, pre-push, CI) through one mechanism |
| Q0' | crate restructuring | no split now; file the split as an issue (§10) |
| A/B/C | approach | A: derived signature + lint. B (`KeyspaceEffect` extraction) filed, deferred. C (hand tags, no model) rejected |
| Q1 | test level / budget | no time budget; level per boundary as §5 default, adjustable per test |
| Q2 | spec member lists | representative-only in spec; members in census JSON |
| Q3 | existing representatives | move into `sig_*`, do not tag in place |
| Q4 | naming | mechanical, axes-joined |
| Q5 | Dynamic buckets | per-member `Forced by` rows |
| Q6 | op granularity in `RouteShape` | op stays in the shape |
| Q7 | affected map | Python, ordered globs, unknown → full |
| Q8 | CI | `core-tests` early job; full suite unchanged; no change-based skipping in CI |
| Q9 | recipes | `test-core [area]` replaces `core-test` + `core-test-e2e`, old ones deleted |
| Q10 | where this lives | `.scratch/boundary-signatures/`, `specs/signatures.md` carries no `Status:` |
| Q11 | order | §11 |

## 10. Non-goals and deferred work (filed as issues)

- **Crate split** — extract `CommandSpec`, `WalStrategy`, `ExecutionStrategy`, `CommandFlags`,
  `KeySpec`, `signature.rs` into a leaf crate `frogdb-command-spec` so datatype crates stop
  depending on `frogdb-core`. Sharpens §7's map from module-glob to crate-graph granularity.
  Deferred at the user's request; filed as issue 10.
- **`KeyspaceEffect` extraction (approach B)** — materialize the latent enum that `WalStrategy`,
  `ReindexSpec` and the event shape all mirror, and have persistence, replication, reindex and
  notification consume one mutation description. Fixes replication's missing per-command
  classifier (statement-shipping bug source per round-1 PRD). Spec-first across four locked
  crates; filed as issue 11, seeded by `signature.rs`.
- Making `WalStrategy::Dynamic` / `KeySpec::Dynamic` members static where possible.
- Any change to engine behavior. This design adds a projection and tests only.

## 11. Issue plan and order

| # | issue | depends on |
|---|---|---|
| 01 | `signature.rs` + census in `docs-gen` (`signatures.json`) + core-profile-covers-all conformance test | — |
| 02 | `test-core [area]` recipe, empty `sig_*` binaries, delete `core-test`/`core-test-e2e`, update docs | 01 |
| 03 | `specs/signatures.md` (from census) + spec-lint `SIG-` support + census agreement + gap-issue allowlist; sections start `MISSING` citing 04–07 | 01, 02 |
| 04 | `sig_persistence` + `sig_wake` (both shard-driver level) — move representatives, write missing, close their `MISSING` rows | 03 |
| 05 | `sig_replication` — same | 03 |
| 06 | `sig_cluster` — same | 03 |
| 07 | `sig_txn` — same | 03 |
| 08 | `scripts/affected.py` + `just test-affected` | 02 |
| 09 | CI `core-tests` job in `workflow_gen` | 02 |
| 10 | crate split: extract `frogdb-command-spec` (deferred) | 01 |
| 11 | `KeyspaceEffect` extraction, approach B (deferred, spec-first) | 01 |

04–07 are independent and may run in parallel worktrees. 08 and 09 may run alongside 04–07.
