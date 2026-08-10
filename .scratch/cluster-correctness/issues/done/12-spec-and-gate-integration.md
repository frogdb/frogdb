# 12 — Spec and gate integration

Status: done

## Parent

[PRD](../../PRD.md) §3 W6.

## What to build

- Cross-reference: each catalog invariant cites the FM rows it generalizes; rows whose
  invariant is now universally checked note the invariant ID.
- `lint-failure-modes` gains an optional `INV-*` vocabulary check (warn on dangling
  references) — same script, small addition.
- Mutation re-baseline: full `just mutants` + `just mutants-gate` runs for
  `frogdb-cluster` (0.80) and `frogdb-cluster-runtime` on current code — recorded scores
  predate rows 084–102 entirely. The catalog + property tests should move in-crate kill
  coverage for the 29 rows currently forced only from server-side integration tests.
- Fix the two mis-tagged rows (campaign-2 issue 09,
  `.scratch/hardening-2/issues/`) while in the file.

## Acceptance criteria

- [x] Every catalog invariant ↔ FM row cross-reference in place, lint warns on dangling
      `INV-*`
- [x] Fresh mutation scores recorded for both crates; gates pass or survivors documented
      at the code
- [x] Mis-tagged rows fixed; `just lint-failure-modes` green

## Blocked by

- Issue 02 and issue 03 (`.scratch/cluster-correctness/issues/`) — the re-baseline
  measures their in-crate coverage.

## Resolution

### Cross-reference

Recorded on both sides, so neither can drift silently:

- **Catalog → spec.** Each of the eleven `check_*` functions in
  `frogdb-server/crates/cluster/src/invariants.rs` gained a "Generalizes …" paragraph naming
  the rows it covers, chosen by one test: *would deleting the code that row names make this
  entry fire?* Rows that merely touch the same field are not cited. `INV-SLOT-1` says the
  opposite explicitly — it generalizes no row, because FM-CLUSTER-018 derives the slot range
  by hashing and FM-CLUSTER-075 enforces it at the `SlotRange` parse boundary, but neither
  states it of the replicated slot map. `INV-REF-3B` cites FM-CLUSTER-005 as the row it would
  *complete*, not one it checks — it is the catalog's one `DocumentedException`, reported and
  never asserted until issue 14 lands.
- **Spec → catalog.** Twenty rows gained an optional `Catalog` field: 001, 002, 003, 005, 006,
  010, 011, 012, 032, 033, 036, 040, 041, 042, 076, 084, 086, 088, 090, 100. The field is
  documented in the spec's "How to read a row" table and in a new "The `Catalog` field"
  section that states why a state-wide entry and a per-transition row are different claims.
  Rejected after reading them: FM-CLUSTER-013 (`MarkNodeFailed` bumps only the cluster
  counter, so `INV-EPOCH-1` is preserved trivially), 004, 018, 039, 075, 078, 087.
- The catalog citations are **prose, never a `// FM-…` tag line**: a tag is the lint's claim
  that the item below it *forces* that row, and a catalog entry is not a test. Verified no
  added comment line consists solely of FM ids.

### Lint

`scripts/failure-modes.py` gained a third direction. `load_catalog_ids` parses the `id:` fields
out of the `CATALOG` static — bounded to the static, so the catalog's own test fixtures
(`INV-TEST-HARD`) cannot widen the vocabulary — and `check_invariant_vocabulary` flags every
`INV-*` a spec cites that the catalog does not define. Dangling is an **error**, not the warning
the issue text proposed: it is always a rename or a deletion, never a judgement call, and a
warning in a gate that runs on every commit is a warning nobody reads. The unused direction is
deliberately not checked (see `INV-SLOT-1`).

`just lint-failure-modes` is green and now reports the new axis:

```
OK: 278 failure modes (BLOCKING, CLUSTER, PERSISTENCE, REPLICATION, TXN, VLL),
    1395 test references, 1395 tags, 41 invariant citations over 11 catalog entries
```

Zero dangling `INV-*` found. Shrinking the catalog to one id in a scratch run produced 34
errors, so the check is not vacuous.

### Mis-tagged rows (campaign-2 issue 09)

- **FM-CLUSTER-059** — the row's `NOT observable` is "a knob that only takes effect at
  startup", but all five cited tests drove `ClusterRuntimeFlags` directly and none issued a
  `CONFIG SET`, so none could witness it. Added `cluster_flag_sets_reach_the_live_flags`
  (`server/src/runtime_config.rs`) to `Forced by` and tagged it there; it goes through
  `ConfigManager`, which is the path that made the knob live.
- **FM-TXN-040** — the two cited tests count `validate_queued_batch` calls and witness the
  `Invariant`, not the `Observable` (a withheld `EXEC` reply that arrives on release).
  Cross-tagged `write_exec_parks_on_a_slot_barrier_and_commits_after_release`
  (`server/tests/cluster_pause_barrier.rs`, which keeps its FM-CLUSTER-083 tag) and added it
  to that row's `Forced by`. It reaches the same `TxnHost::wait_if_paused` through a slot
  pause rather than `CLIENT PAUSE`.

### Mutation re-baseline

Full runs on the current tree (local mode, `--jobs 2`; no `--iterate`, so no
`previously_caught` inflation):

| Crate | Total | Caught | Missed | Unviable | Timeout | Score | Gate 0.80 |
|---|---|---|---|---|---|---|---|
| `frogdb-cluster` | 545 | 400 | 3 | 142 | 0 | **99.3%** | PASS |
| `frogdb-cluster-runtime` | 199 | 173 | 2 | 24 | 0 | **98.9%** | PASS |

Zero timeouts in either run, so the `timeout_multiplier` in `.cargo/mutants.toml` is still
right. All five survivors were already documented at the code by earlier waves, and each was
re-read here rather than taken on trust:

- `state.rs:1063` `begin_receiving_snapshot` → `Ok(Box::new(Cursor::new(vec![])))`: a true
  equivalent, `vec![]` and `Vec::new()` being the same value.
- `storage.rs:249` `MetaDurability::write_opts` → `Default::default()`: `WriteOptions` is
  opaque and an fsync has no in-process witness; the classification it renders
  (`MetaDurability::for_key`) is forced instead. Carries a `DOCUMENTED EQUIVALENT` marker.
- `types.rs:155` `NodeFlags::connected` → `Default::default()`: "connected" *is* the zero
  value, so the mutant is the identical program.
- `bus.rs:260` `negotiate_framing`: `#[cfg(feature = "turmoil")]`, which the default-feature
  mutation run never builds; the turmoil suite is its witness.
- `failure_detector.rs:70` `server_state` → `Default::default()`: a field read off a live
  `openraft::Raft`'s metrics watch, unconstructible from this crate; the decision taken from
  it (`is_leader`) is forced.

### Also in this branch

`just test frogdb-cluster` killed `failover_model_smoke` as a TIMEOUT while the machine was
shared with a mutation run, then passed in 10.3s alone — two thirds of nextest's default 15s
hard kill. Both smoke model configs now get the 30s/3 headroom the other legitimately heavy
per-commit tests carry (`.config/nextest.toml`), rather than shrinking the depth bound.
