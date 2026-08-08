# Foundation-Hardening Campaign — Retrospective

Date: 2026-08-05. Campaign span: 2026-07-31 (Phase 0) → 2026-08-05 (Phase 4 lock, `6cfdb026`).
All four core areas — transactions, persistence, replication, cluster — LOCKED.

## Outcome summary

| Phase | Area | Locked | Mutation gate | Score |
|---|---|---|---|---|
| 1 | Transactions / VLL | 2026-08-01 | 0.90 | `frogdb-txn` 100%, `frogdb-vll` 100% |
| 2 | Persistence / recovery | 2026-08-02 | 0.85 | `frogdb-persistence` 99.1% (1037 mutants), `frogdb-recovery` 100% |
| 3 | Replication | 2026-08-04 | 0.85 | `frogdb-replication` 98.7% (1180), `frogdb-replication-runtime` 100% viable |
| 4 | Cluster | 2026-08-05 | 0.80 | `frogdb-cluster` 99.6% (496), `frogdb-cluster-runtime` 99.0% (224) |

- Failure-mode specs: **FM-TXN/VLL/BLOCKING/PERSISTENCE/REPLICATION-001..061/CLUSTER-001..078**
  — 241+ specced modes, 1190+ test tags, spec↔test agreement lint-enforced in `just lint`.
- Every surviving mutant across all four areas is a documented equivalent at the code
  (29 total: 6 persistence, 15 replication, 4 cluster, plus txn's zero).
- Four crates extracted from the server monolith: `frogdb-txn`, `frogdb-recovery`,
  `frogdb-replication-runtime`, `frogdb-cluster-runtime` (+ `frogdb-net`, `frogdb-shard-harness`
  in Phase 0). Boundary ADRs 0002-0004.
- Inner-loop cost (warm check / test-build): txn 9.1s→**1.8s/2.6s**, persistence
  11.4s→**4.7s/4.0s**, replication 11.8s→**11.3s/3.6s**, cluster 11.2s→**0.5s/1.1s**
  (`.scratch/hardening/metrics/loop-cost.md`).

## Real bugs found and fixed (the campaign's justification)

The mutation-gate + spec-first discipline surfaced correctness bugs that line coverage and the
existing 5000+-test suite had certified as fine:

**Acked-write loss (worst class):**
- **Bug X / hardening 40** — single-key write on a migrating source acked `+OK`, key then
  orphaned at `SETSLOT`. Found by refusing to accept "harness flake" and re-reading jepsen
  logs forensically. Fix: Redis-parity pre-execution presence probe at every arity.
- **WAL flush tore multi-entry transactions** across batches (Phase 1) — checkpoint/recovery
  corruption dismissed for weeks as "flake 65". Fix: WAL write groups.
- **Fullsync checkpoint acked-write loss** (pre-campaign rework) — WAL drain missing.
- **Durable watermark advanced on unsynced commits** (Phase 2) — committed/synced split.
- **Backlog eviction hole** (Phase 3) — floor re-check under entries lock.

**Security:** fullsync path traversal (Phase 3). **Cluster CAS hole:** WATCH never
slot-validated (Phase 1). **Bare EVAL bypassed cluster slot validation** (Phase 4 rework 11).
**Dead auto-failover** (edge-triggered detector, pre-campaign rework). Plus: INFO lying
wholesale (bgsave status/durations hardcoded, LASTSAVE restamped at boot, CLUSTER INFO
fabricated zeros), split-brain config silently disabling partial resync, ring-buffer hang
holding a lock, quiesce silently dropping dead shards from checkpoint cuts, staged-fullsync
metadata destroyed on failed save, PERSIST immortalizing expired keys.

Pattern: the highest-value finds came from (a) treating *every* flake as a bug until proven
harness, and (b) mutation survivors pointing at asserted-nothing test coverage.

## What worked — keep doing

1. **Extract → spec → fix → mutate → gap-fill → lock, strictly serial per area.** Each phase
   finished in 1-2 days because nothing was interleaved.
2. **Mutation score as a floor with forcing tests in the owning crate.** The
   `cargo mutants -p` scoping rule (now in CLAUDE.md "Locked core areas") is why
   scores are honest.
3. **Spec-first bug fixing** (row → failing test → fix) and the two-way FM lint. Tags survived
   five test-file splits and dozens of merges without rotting.
4. **Documented equivalents at the code**, never blanket skips — every skip carries its
   unobservability argument where the next reader will see it.
5. **Flake forensics.** Three of the worst bugs (WAL tear, Bug X, MultiWaiter-adjacent finds)
   started as "flaky test, passes isolated".
6. **Research-first design** (Redis/Valkey/Dragonfly source reading) — e.g. the pause-barrier
   brief's convergent-design table, per-subcommand admin marks verified against live Redis 8.6.1.

## What hurt — process lessons

1. **Parallel worktree cargo-mutants runs saturate the machine** and starve agents into 600s
   watchdog kills. Rule now: gap-fill agents write tests only; ONE consolidated `--iterate`
   run from the main session.
2. **Disk pressure SIGTERMs background builds silently.** Worktree `target/` dirs (9-19G each)
   and `/private/tmp/claude-501/cargo-mutants-*.tmp` (up to 15G) must be swept before long
   runs; two full-suite verifications died before this was understood.
3. **`mutants-gate.py` originally scored only the final pass** — under `--iterate` it read
   34.1% where the true score was 84.5%. Gate scripts must count `previously_caught.txt`.
4. **Gap-fill agents ran `just check` but not clippy** — a `clone_on_copy` landed on main.
   Verification instructions to agents must name the lint step explicitly.
5. **Shared-tree orchestration traps:** bare `git commit -m` sweeps concurrent WIP (pathspecs
   always); spec `Forced by` cells conflict when two agents share the spec (union-merge);
   pipelines mask exit codes (`rebase | tail && push` nearly pushed a conflicted state).
6. **Blanket mutants exclusions hide real state machines** (`::record_` exclusion, Phase 2) —
   exclude sinks by name only.
7. **The monolith's warm compile time was never the bottleneck** — suite scope, per-crate
   mutation cost, and context isolation were. Extraction paid off through those, not through
   `cargo check` seconds (though cluster's 0.5s loop is a nice side effect).

## Standing rules going forward (post-campaign)

- Locked crates: behavior changes are **spec-first**; `just mutants-diff <crate>` before any
  push touching one (push discipline — CI does not enforce it).
- Gates on record: txn 0.90, persistence 0.85, replication 0.85, cluster 0.80. Full runs are
  testbox-class jobs; `mutants-diff` is the PR-viable form.
- The FM lint keeps specs honest — new failure modes get rows + forcing tests, not just tests.
- Observability accuracy is a hard rule: no plausible-looking constants in INFO/logs/stats.

## Exit tasks (in flight as of this retrospective)

Unfreeze redis-regression and triage drift (client-breaking changes were deliberate: rework 05
admin gating, `-READONLY` on replica FUNCTION mutations, error-text changes); restore CI with
change-gated nightlies; close remaining hardening issues (27-29, 30 in flight, 32, 41, 10, 11);
rework 02 measurement → build-vs-accept decision; testing-improvements 66 closed as fulfilled.
