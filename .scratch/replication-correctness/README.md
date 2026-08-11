# Replication correctness — invariant-driven validation

State: active

PRD: [PRD.md](PRD.md) — per-area port of the cluster-correctness validation pattern to
`frogdb-replication` and `frogdb-replication-runtime`: `ReplicationView` projection + invariant
catalog, property-based permutation testing, model checking over production code, seeded fault
schedules, `DEBUG REPLICATION CHECK` + Jepsen sweep, spec/gate integration. All §8 decisions
ruled 2026-08-10.

Issues: 15 filed (2026-08-10), dependency order 01 → (02, 06, 07, 11 parallel) → … → 15. Closed
so far under [issues/done/](issues/done/): 01, 02, 06, 07, 08, 09, 11. The validation layers have
since filed four defects of their own under [issues/open/](issues/open/) — 16 (issue 08's
promotion model), 17 and 18 (issue 02's catalog), 19 (issue 03's `DEBUG REPLICATION CHECK`, on
its first run against a live pair), all in shipped replication behavior.
