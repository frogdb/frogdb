# 35: slot-handoff drain/prepare budget derived from the heartbeat interval, not a fixed 50 ms

Status: needs-triage
Type: AFK
Origin: issue 33 investigation + build-toolchain D5, 2026-09-04
Area: frogdb-cluster (LOCKED, gate 0.80) + frogdb-server `slot_migration` finalizer; `specs/cluster.md`
Phase: 5 — regression fix
Size: M

## Parent

`.scratch/build-toolchain/PRD.md` (D2, D5); investigation record in
[issue 33](../done/33-cluster-tests-regressed-after-output-buffer-series.md).

## Why

The finalizer in `frogdb-server/crates/server/src/slot_migration/mod.rs::complete()` waits for
its own prepare to become visible and for the source's drain confirmation with a fixed
`HANDOFF_DRAIN_WAIT_MS = 50` budget (`frogdb-cluster/src/types.rs:665`), polled every
`HANDOFF_POLL_INTERVAL_MS = 2`. That constant was sized against a quiet-machine residual, not
against Raft round-trip latency. Under nextest's `cluster` group (`max-threads = 2`) a debug
Raft round trip routinely exceeds 50 ms, so a *slow but live* source is treated like a wedged
one: `TRYAGAIN … source did not drain in 50ms`, then `SETSLOT NODE failed`. Issue 33 bisected
the tipping point to `e67002d6f` and showed the cost is cumulative — no revert fixes it.

## Decision (D5)

The budget is derived, not measured: `max(HANDOFF_DRAIN_WAIT_MS, k × heartbeat_interval_ms)`,
exposed through cluster config, and the prepare/drain waits poll until that budget runs out
**inside an unchanged `HANDOFF_BARRIER_MS`** (the FM-CLUSTER-095 fence is not touched). Not the
fix: raising `HANDOFF_BARRIER_MS`; dropping `cluster.max-threads` to 1; an adaptive/EWMA
budget. Spec-first (locked area): row → failing test → fix.

## What to build

1. **Spec row.** Add `## FM-CLUSTER-108 — a slow-but-live source still finalizes` to
   `specs/cluster.md` in the handoff section beside FM-CLUSTER-091 (the investigator's draft
   called it 104; that number is taken). Contract: the drain/prepare wait budget is
   `clamp(k × heartbeat_interval_ms, HANDOFF_DRAIN_WAIT_MS, HANDOFF_BARRIER_MS − reserve)`
   where `reserve` is the headroom kept for the `CompleteSlotMigration` round trip; a source
   whose drain confirmation lands anywhere inside that budget finalizes; a source that never
   confirms still aborts within the budget with the migration record intact (FM-CLUSTER-091
   unchanged — add a cross-reference line to that row). Name the forcing tests in the row per
   `just lint-spec` conventions (`FM-CLUSTER-108` tag on each test).
2. **Derivation in `frogdb-cluster`** (so the forcing unit test lives in the mutated crate):
   a pure fn beside the constants in `types.rs`, e.g.
   `pub fn handoff_wait_budget_ms(heartbeat_interval_ms: u64) -> u64`, with `k` and `reserve`
   as named constants documented the same way the existing `HANDOFF_*` ones are. Unit tests
   pin: quiet default (`heartbeat 250` → capped at barrier − reserve), small heartbeat
   (`50` → `max(50, k×50)` capped), floor (`heartbeat 1` → `HANDOFF_DRAIN_WAIT_MS`), and that
   the result is always `< HANDOFF_BARRIER_MS`. Mutation-kill each arm (gate 0.80).
3. **Config surface.** `ClusterConfig` already carries `heartbeat_interval_ms`; thread it to
   the finalizer (the `SlotMigrationCoordinator`/whatever owns `complete()` gets the budget
   at construction, not a global). No new CLI flag unless one is needed to reach the value —
   if the server's cluster config plumbing does not already expose `heartbeat_interval_ms`
   to the finalizer, report the shortest honest path in the report rather than adding a
   parallel knob.
4. **Finalizer.** `poll_handoff` takes the budget instead of `HANDOFF_DRAIN_WAIT_MS`; the
   `TRYAGAIN … did not drain in {}ms` message prints the budget actually used. Nothing else
   in `complete()` changes (prepare → await seq → await drained → complete, abort on miss).
5. **Integration forcing test** in `frogdb-server` (`cluster_handoff_barrier.rs` or a sibling)
   tagged `FM-CLUSTER-108`: a source delayed past 50 ms but inside the budget still
   finalizes; the wedged-source test for FM-CLUSTER-091 still passes.

## Risk to report, not to fix silently

`prepared_at_ms` is minted *before* the `PrepareSlotHandoff` round trip, so the 100 ms barrier
must also cover prepare RTT + shard drain + confirm RTT + complete RTT. The derived budget
recovers the tail whose total fits inside the barrier; a tail that does not will move from
the `did not drain` arm to the `handoff barrier window elapsed` arm. If after the change the
diagnostic run below still fails on that third arm, report the counts per arm — raising the
barrier is a new ruling (issue 33 "open rulings"; both CockroachDB and FoundationDB derive
the window too), not something to do here.

## Acceptance criteria

- [ ] `specs/cluster.md` has FM-CLUSTER-108 with forcing tests named; FM-CLUSTER-091 cross-references it; `just lint-spec` green
- [ ] `frogdb-cluster` unit tests pin the derivation (floor, heartbeat-scaled, barrier cap); `just mutants-diff frogdb-cluster` shows no surviving mutant in the new fn
- [ ] `HANDOFF_BARRIER_MS`, `HANDOFF_DRAIN_TIMEOUT_MS`, `HANDOFF_LEASE_MS`, `.config/nextest.toml` unchanged
- [ ] the 3-binary diagnostic run passes 46/46 three times in a row on this machine with `cluster.max-threads = 2`:
      `cargo nextest run -p frogdb-server -E 'binary(cluster_handoff_barrier) + binary(cluster_migration) + binary(cluster_finalization_window)' --no-fail-fast --retries 0`
      (env `ROCKSDB_LIB_DIR=/opt/homebrew/lib SNAPPY_LIB_DIR=/opt/homebrew/lib DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib`; check the `Justfile` for a recipe that sets these first) — or, if not, the per-arm failure counts in the report
- [ ] `just test frogdb-cluster`, `just lint frogdb-cluster`, `just lint-spec`, `just lint-gates` green
- [ ] `just generate` re-renders `website/src/content/docs/specifications/cluster.md` from the spec (it is generated; never edit it by hand); `just generate-check` green

## Files likely touched

- `specs/cluster.md`
- `frogdb-server/crates/cluster/src/types.rs`
- `frogdb-server/crates/server/src/slot_migration/mod.rs`
- `frogdb-server/crates/server/tests/cluster_handoff_barrier.rs` (or sibling)
- whichever server file constructs the finalizer with cluster config
- `website/src/content/docs/specifications/cluster.md` (regenerated)

## Blocked by

[23](23-replica-feed-networkoutput-accounting.md) — no file overlap; sequenced after it so the
cluster suite's green is attributable to one change. Issue 34 stays separate.

## Decisions

D2, D5
