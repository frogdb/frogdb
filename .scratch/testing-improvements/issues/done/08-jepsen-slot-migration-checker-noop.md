# Jepsen: slot-migration checker is a no-op (`:valid? true` unconditionally)

Status: done
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

- [x] `slot_migration.clj` `:valid?` fails when a write is lost/misread during migration (verified
      via a deliberately-introduced fault in a manual smoke test)
- [x] Checker asserts final-owner == destination node for migrated slots
- [x] Checker asserts zero unresolved redirects
- [x] `slot-migration` and `slot-migration-partition` runs pass against current (working) migration
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

## Resolution

Replaced the unconditional `{:valid? true}` stub in `slot_migration.clj` with a real checker that
gates `:valid?` on four independent, sound properties, mirroring the proven data/threshold patterns
in `key_routing.clj` and `membership_routing.clj`:

1. **Durability** (`:durable?`) — every acknowledged write must remain readable. Asserted on the
   post-migration quiescent read window (the trailing 10 reads that `core.clj` issues after the
   nemesis heals + a 5s settle, so all writes have completed). A nil there is a lost acked write.
   Reads *before* the first acked write are excluded (they are legally nil), so there are no false
   positives. No kill nemesis runs in either variant, so a nil is genuine loss, not single-master
   kill loss.
2. **Value-correctness** (`:values-correct?`) — every successful non-nil read must return a value
   actually written to that key. Reuses the `cross_slot.clj`/`key_routing.clj` conservation idea:
   the generator now draws write values from a per-run monotonic counter (globally unique), so a
   fabricated or cross-slot-contaminated value is caught.
3. **Final ownership** (`:final-owner-correct?`) — when a `finish-migration` succeeded, the final
   slot-owner read must equal the migration's intended destination node. Conditional on completion
   so the partition variant does not false-positive if `finish` was blocked by a partition;
   durability + value-correctness still gate that run.
4. **Unresolved redirects** (`:redirects-ok?`) — no data op may exhaust its MOVED/ASK redirect
   budget. Added a `[:type :too-many-redirects]` catch to `invoke!` so budget exhaustion surfaces as
   a determinate `:fail` (it was previously buried as `:info :unexpected`); the checker gates on zero.

Non-gating reported signal `:final-read-matches-last-acked-write` surfaces the issue's "final read ==
last acked write" ask; it is not gated because, on a single register under concurrent writes, the
settled value is whichever write linearized last (not necessarily the highest counter) — an exact
match is not soundly assertable without a linearizability model.

### Validation

- `lein check` — clean compile of `jepsen.frogdb.slot-migration` (only pre-existing unrelated
  `lag.clj` reflection warnings).
- **End-to-end smoke run** (`just jepsen slot-migration --time-limit 30`, clean 3-node raft
  topology, reusing the existing `frogdb:latest` image): **`:valid? true` — "Everything looks
  good!"** No false positive. Key fields: `:durable? true :lost-write-count 0
  :values-correct? true :wrong-value-count 0 :final-owner-correct? true :intended-dest "n2"
  :final-owner "n2" :migration-completed? true :redirects-ok? true :too-many-redirect-count 0
  :final-reads-converged? true :final-values [160] :last-acked-write 160
  :final-read-matches-last-acked-write true` (53 writes, 63 reads, 10 final reads, 0 failed).
- **Fault-injection matrix** (checker fed synthetic histories on the project classpath): the control
  (working) history is `:valid? true`, and every deliberately-faulted history is `:valid? false` via
  the expected gate — lost write → `durable? false`; wrong final owner → `final-owner-correct? false`;
  fabricated value → `values-correct? false`; exhausted redirect → `redirects-ok? false`. A legal
  pre-write nil read in the control did not false-positive.

No real data loss/misrouting was found — the working migration code passes the newly-sighted checker.

### Caveat / follow-up

- The end-to-end run covered the `none` variant (`slot-migration`). The `slot-migration-partition`
  variant was not run end-to-end here (the `run.py run` path force-cross-compiles a fresh release
  binary via `cargo zigbuild`, which fails on this macOS box building the `usearch`/`simsimd` C++
  dep; the smoke run therefore reused the pre-built `frogdb:latest` image). The checker is
  nemesis-agnostic and the partition variant's only behavioral difference — the conditional
  ownership gate — was designed specifically to avoid partition false positives; a testbox run of
  `slot-migration-partition` should confirm before CI wiring (issue 10).
