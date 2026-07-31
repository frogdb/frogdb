# Cross-slot Elle workload never runs under fault injection

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: jepsen

## Context

`cross_slot.clj:382` implements a rigorous conservation checker for cross-slot multi-key operations (the same pattern recommended for reuse in fixing the slot-migration checker, see task 08). However `run.py:201-208` only ever runs it with nemesis `none` — no partition, no kill, no migration-under-fault variant. This means the strongest cross-slot correctness checker the harness has never actually observes a fault while it's checking, so any cross-slot atomicity regression that only manifests under partition/kill (the realistic failure mode for a VLL cross-shard commit) is invisible to this workload.

## What to build

- Add `cross-slot-partition` `TestDefinition`: `cross_slot.clj` workload composed with a partition nemesis, in the raft/cluster suite.
- Add `cross-slot-kill` `TestDefinition`: `cross_slot.clj` workload composed with a process-kill nemesis.
- Ensure both run against a multi-shard cluster topology so cross-slot ops actually span shards under the fault.

## Acceptance criteria

- [ ] `run.py` `TESTS` table includes `cross-slot-partition` (cross_slot workload + partition nemesis).
- [ ] `run.py` `TESTS` table includes `cross-slot-kill` (cross_slot workload + kill nemesis).
- [ ] Both run successfully (or with a triaged, understood failure) in a manual harness invocation before merging.
- [ ] Both are reachable via the same suite grouping mechanism as the existing `cross-slot` (none) entry.

## Blocked by

None - can start immediately.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/cross_slot.clj:382` (conservation checker)
- `testing/jepsen/frogdb/run.py:201-208` (current `cross-slot` entry, nemesis `none` only)

## Resolution

Added two fault-injecting `TestDefinition`s to `testing/jepsen/run.py`, both in the
`("raft", "all")` suites alongside the existing `cross-slot` (none) entry:

- `cross-slot-partition` — `cross-slot` workload + `partition` nemesis, 60s.
- `cross-slot-kill` — `cross-slot` workload + `kill` nemesis, 60s.

Both use the `kill`/`partition` nemeses already proven on the raft topology
(key-routing-kill, slot-migration-partition, leader-election-partition).

### Checker made fault-aware (`cross_slot.clj`)

The prior checker gated `:valid?` on `(and conservation all-local)` unconditionally
— it would false-positive under a kill. The account balances are hash-tag
co-located to a **single shard** (`{account}:balance-a/-b`), so every atomic
transfer is a single-shard Lua EVAL that applies both SET legs or neither. Rewrote
the checker into three properties following the `key_routing.clj` /
`membership_routing.clj` patterns:

- **Slot co-location** (ALWAYS gates) — pure client-side CRC16, fault-independent.
- **Cross-slot rejection safety** (ALWAYS gates) — a cross-slot multi-key op that
  is silently accepted is recorded `:fail :should-have-failed` (fault-independent
  client-side slot check); this is now gated, where before it was only counted.
  `:keys-unexpectedly-same-slot` (probe-key collision, a test-data artifact) is
  surfaced but does not gate.
- **Balance conservation** (KILL-GATED) — under a partition (committed data never
  destroyed) conservation is enforced; under a SIGKILL it is surfaced loudly
  (totals + balances retained) but does not fail the run, because FrogDB does not
  promise failure atomicity across a crash: single-owner raft shards can lose
  recent un-fsynced acked writes, and a cross-shard VLL EXEC may durably
  partial-commit by documented design (concurrency issue 05, option 4 fail-stop;
  failure-atomic recovery deferred to the durability phase). `:process-killed?` is
  detected via `:f :kill` in the history, mirroring the key-routing checker.

### Smoke runs (fresh raft topology, `--no-build`, reused `frogdb:latest`)

- `cross-slot-partition` (30s): **:valid? true (PASS)**. `:balance-conserved true`
  (initial=final=2000), `:conservation-enforced? true`, `:hash-tags-colocated true`,
  `:cross-slot-safe? true`, 70 transfers, 1 cross-slot attempt correctly rejected.
- `cross-slot-kill` (40s): **:valid? true (PASS)**. A kill of n1/n2/n3 fired during
  the transfer phase, so `:process-killed? true` and `:conservation-enforced? false`
  — the kill-gating path was genuinely exercised. `:balance-conserved true`
  (initial=final=2000): FrogDB preserved atomicity through the kill, so **no
  partial-commit anomaly surfaced** in this window. `:cross-slot-safe? true`,
  `:hash-tags-colocated true`, 68 transfers.

No anomalies beyond the documented partial-commit contract. `lein check` passes
(only pre-existing `lag.clj` reflection warnings). store/ artifacts excluded from
the commit.
