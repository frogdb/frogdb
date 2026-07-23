# Jepsen: slot-migration checker is a no-op (`:valid? true` unconditionally)

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 2/3 (score 6)
Area: jepsen

## Context

`slot_migration.clj:403` returns `{:valid? true}` unconditionally. The workload does compute rich
metrics — writes, reads, owners-seen, migrations (lines 385-411) — but none of them feed the
verdict. The docstring (lines 375-381) claims "No data loss during migration," but nothing checks
that. This workload runs under both the `raft` suite as `slot-migration` (nemesis `none`) and as
`slot-migration-partition` (nemesis `partition`) per `run.py:192-245` — meaning real value loss
under partition during a slot migration currently passes green.

Area F independently flagged the same defect (`jepsen-slot-migration-checker-noop`, F#4) and noted
that sibling workloads over the same migration machinery — `concurrent_migration.clj:292` and
`migration_recovery.clj:324` — *do* have real predicates, which is part of why this is scored as a
false-confidence class problem rather than "the only defense is broken": adjacent, better-built
checkers create an impression of coverage that this specific workload doesn't back up. The two
source reports initially disagreed on severity (G scored L3/C3, F scored L3/C2); the adversarial
verification pass reconciled on F's L3/C2 as final, given the mitigating context from the sibling
checkers.

## What to build

- Strengthen `slot_migration.clj`'s checker to actually use the metrics it already collects:
  - Track per-write value + index (not just counts).
  - Assert the final `:read` for each key equals the last acked write.
  - Assert final owner of each migrated slot equals the intended destination.
  - Assert zero unresolved redirects at test end.
- Reuse the conservation-checking pattern already proven in `cross_slot.clj:382`.

## Acceptance criteria

- [ ] `slot_migration.clj` `:valid?` fails when a write is lost/misread during migration (verified
      via a deliberately-introduced fault in a manual smoke test)
- [ ] Checker asserts final-owner == destination node for migrated slots
- [ ] Checker asserts zero unresolved redirects
- [ ] `slot-migration` and `slot-migration-partition` runs pass against current (working) migration
      code after the checker fix, confirming no false positives introduced

## Blocked by

None - can start immediately. This checker fix is a prerequisite for `10-jepsen-nightly-ci.md`,
which gates CI wiring on fixed checkers (this task plus 04, 05, 06).

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/slot_migration.clj:375-381` (docstring), `:385-411`
  (unused metrics), `:403` (`{:valid? true}`)
- `testing/jepsen/frogdb/src/jepsen/frogdb/concurrent_migration.clj:292` — sibling workload with a
  real predicate
- `testing/jepsen/frogdb/src/jepsen/frogdb/migration_recovery.clj:324` — sibling workload with a
  real predicate
- `testing/jepsen/frogdb/src/jepsen/frogdb/cross_slot.clj:382` — conservation-checker pattern to reuse
- `testing/jepsen/run.py:192-245` — slot-migration / slot-migration-partition test definitions
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md` `slot-migration-checker-noop`,
  `.scratch/testing-improvements/audit/verdicts-G.md` (same, "G framing wins over F#5")
- Source: `.scratch/testing-improvements/audit/F-cluster.md` gap #5 `jepsen-slot-migration-checker-noop`,
  `.scratch/testing-improvements/audit/verdicts-F.md` #5 (final score L3/C2, "dedupe w/ G; sibling workloads
  carry real predicates over same machinery -> false-confidence class, not sole defense")
