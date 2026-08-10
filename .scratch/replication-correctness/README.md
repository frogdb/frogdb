# Replication correctness — invariant-driven validation

State: active

PRD: [PRD.md](PRD.md) — per-area port of the cluster-correctness validation pattern to
`frogdb-replication` and `frogdb-replication-runtime`: `ReplicationView` projection + invariant
catalog, property-based permutation testing, model checking over production code, seeded fault
schedules, `DEBUG REPLICATION CHECK` + Jepsen sweep, spec/gate integration. All §8 decisions
ruled 2026-08-10.

Issues: 15 filed (2026-08-10), dependency order 01 → (02, 06, 07, 11 parallel) → … → 15. 14 open
under [issues/open/](issues/open/), 1 closed under [issues/done/](issues/done/) — issue 07, the
decision/IO seam W3 rests on.
