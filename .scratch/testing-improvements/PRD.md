# Testing Improvements — Gap Audit Backlog

Origin: full-stack testing audit, 2026-07-22. Method: 3 exploration agents (Rust test
inventory, Jepsen suite map, feature/tracker context) → 7 area gap-finding agents (basic
commands, transactions/scripting, pubsub/streams/blocking, persistence, replication,
cluster, Jepsen harness/CI) → 7 independent adversarial verifier agents (one per area,
instructed to refute) → merge/dedupe. Compat baseline: Redis 8.6.0
(`website/src/data/versions.json` `redis_compat_target`). Where FrogDB architecture makes
Redis parity ambiguous, expected behavior is judged by least surprise for a Redis user,
stated per-issue.

## Scoring

Each issue carries `Severity: likelihood L/3, consequence C/3 (score L*C)`.

- **Likelihood** — probability the untested/missing behavior is broken today or regresses
  undetected. 3 = confirmed broken or hole targets bug-dense machinery; 2 = plausible,
  unverified; 1 = currently correct-by-construction, regression guard only.
- **Consequence** — 3 = data loss / corruption / guarantee violation / split-brain;
  2 = wrong results, errors, availability loss; 1 = compat nit / observability / process.

## Verification discipline

Every gap survived an adversarial pass that re-read cited code, hunted for missed tests,
and checked claimed Redis semantics. Refutations that occurred (and are NOT filed):

- "Subscribe-family in MULTI diverges from Redis" — verified against Redis source:
  SUBSCRIBE/PSUBSCRIBE are MULTI-exempt in Redis and FrogDB matches. Only the residue is
  filed (issue 57).
- "Sharded-pubsub subscribers not dropped on slot migration" — covered by
  `integration_pubsub.rs:1274` end-to-end.
- "INFO cluster_current_epoch must be ≤ max node epoch" — wrong per Redis semantics;
  reframed as issue 47.
- "Pubsub subscription cap has zero tests" — unit tests exist; only integration residue
  filed (issue 50).
- Stale premises corrected along the way: `integration_cluster.rs` "hardcoded standalone
  responses" doc-comment is stale (commands render live state); DUMP/RESTORE
  Stream/Bloom/TimeSeries stubs no longer exist (doc-comment stale) — both in issue 60.

## Headline findings

Two confirmed **live bugs** (not just missing tests), both from the replication model of
verbatim-command re-execution (`slowlog_tcl.rs:16` documents the missing rewrite layer):

1. **SPOP replicates verbatim; replica re-rolls RNG → permanent divergence** (issue 01).
2. **Served blocking pops are never broadcast → replica keeps popped elements** (issue 02).

Structural theme in Jepsen: the harness's strongest checkers and highest-value fault
workloads are wired but never run (orphaned workloads, Elle list-append, membership
nemesis), and two running checkers assert nothing (slot-migration `:valid? true`,
key-routing redirect-count-only under the harshest nemesis). Jepsen is absent from CI
entirely. Durability's fsync boundary has never been exercised by any crash test
(page cache survives every "crash" the suite performs).

## Coverage baseline (cargo llvm-cov nextest --all, aarch64 testbox, 2026-07-22)

Total line coverage **84.0%** (105,531/125,629). 6,824 tests: 6,823 pass, 1 flaky under
coverage env (`integration_cluster::test_frogdb_version_reports_cluster_info` — issue 60).

Per-crate: acl 94.5 · testing 92.7 · replication 92.4 · types 91.7 · config 91.5 ·
persistence 90.4 · telemetry 89.7 · vll 88.9 · cluster 88.3 · core 86.6 · search 85.9 ·
protocol 85.2 · commands 84.7 · scripting 82.9 · server 82.5 · debug 56.8 · frogctl 46.6 ·
frogdb-macros 0.0.

Worst server-relevant files (≥100 lines): `server/src/commands/info.rs` 0.8% (3/397 —
likely legacy/dead vs `server/src/info/sections.rs`; worth a wire-or-delete look),
`server/src/connection/builder.rs` 0%, `server/src/config/loader.rs` 29.8%,
`core/src/store/mod.rs` 34.7%, `server/src/admin/handlers.rs` 36.9%,
`server/src/connection/persistence_handler.rs` 40.9%, `server/src/connection/routing.rs`
52.1%, `core/src/conn_command.rs` 61.4%. Coverage is not tracked in CI
(`COVERAGE_ENABLED = False`) — issue 59.

## Issue index (by score)

Score 9 — 01 spop-replication-divergence (bug) · 02 blocking-serve-replication-divergence
(bug) · 03 jepsen-failover-durability-workload · 04 jepsen-orphaned-workloads-wire-or-delete ·
05 jepsen-membership-under-fault · 06 jepsen-raft-chaos-blind-checker ·
07 shuttle-multiwaiter-exactly-once-guard

Score 6 — 08 jepsen-slot-migration-checker-noop · 09 keyspace-new-keymiss-events-inert ·
10 jepsen-nightly-ci · 11 turmoil-cluster-raft-topology · 12 durability-fsync-boundary ·
13 bgsave-restore-path · 14 wal-recovery-mode-pin · 15 split-brain-lifecycle-e2e ·
16 cluster-epoch-persistence-assert · 17 source-crash-mid-migration-assert ·
18 leader-fd-false-failover · 19 cross-slot-standalone-multi-invariant ·
20 jepsen-elle-list-append-schedule · 21 jepsen-cross-slot-fault-variants ·
22 jepsen-register-fault-coverage · 23 turmoil-replication-topology

Score 4 — 24 scan-full-iteration-stress · 25 restore-corrupt-payload ·
26 script-undeclared-key-policy · 27 stream-notification-class-assert ·
28 pubsub-mode-command-gate · 29 pubsub-slow-subscriber-bound ·
30 pubsub-disconnect-dereg-e2e · 31 blocking-crossslot-negative ·
32 replica-expiry-ttl-drift · 33 fence-min-replicas-e2e · 34 promotion-replid-asserts ·
35 broadcast-lag-resync-e2e · 36 cluster-slots-fail-accounting · 37 wait-in-cluster ·
38 jepsen-weak-checkers-strengthen · 39 jepsen-expiry-clock-skew ·
40 fuzzing-continuous-corpus · 41 wgl-downgrade-threshold

Score 3 — 42 tiered-storage-restart-recovery · 43 checkpoint-cross-shard-cut

Score 2 — 44 errorstats-e2e · 45 persistence-cmd-replies · 46 ryw-monotonic-readonly-table ·
47 epoch-fold-observability · 48 chained-replication-contract ·
49 cross-slot-multi-assert-tighten · 50 subscription-cap-integration ·
51 wait-in-multi-guard · 52 blmpop-bzmpop-nonblocking-multi · 53 xread-no-wake-pin ·
54 resp3-double-nonfinite-pin · 55 multi-exec-migration-boundary ·
56 consistency-design-intent-conversion

Score 1 — 57 subscribe-in-multi-parity-residue · 58 cluster-keyspace-notify-scan-semantics ·
59 coverage-ci-enable · 60 stale-test-docs-cleanup

## Relationship to existing backlog (not duplicated here)

`.scratch/concurrency-testing/issues/05` (VLL phase-3 partial commit), `06` (durability
txn framing), `10` (notification-capture rollout), `11` (nightly findings — issue 07 here
adds the missing deterministic guard, cross-referenced); `.scratch/arch-deepening/issues/16`
(flaky ssubscribe test, JSON reindex gap).
