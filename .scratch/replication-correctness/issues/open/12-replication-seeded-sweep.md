# 12 — Seeded fault schedules on a replication topology

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W4; sequencing against cluster issue 23 ruled in §8 D9; scheduler split
ruled in §8 D8.

## What to build

`frogdb-server/crates/server/tests/simulation/replication_scheduler.rs` — a **replication topology
arm** (one primary plus N replicas, no Raft) driving issue 11's shared derivation. Extend the
existing scheduler; do not build a second one.

Four families beside the reusable `Healthy` / `CrashRestart` / `Mixed`:

- **`LinkDrop`** — the primary↔replica edge held and healed at deliberately chosen partial-sync
  boundaries: inside the backlog window (expect `+CONTINUE`), just outside it (expect
  `+FULLRESYNC`), and straddling an eviction.
- **`PromotionMidStream`** — `REPLICAOF NO ONE` on a replica with frames in flight, then the
  ex-primary demoted toward it and re-syncing. The level-4 witness for retro-validation revert (b).
- **`SlowReplica`** — one edge slowed past the lag-disconnect and `min-replicas-max-lag`
  thresholds so the self-fence and `min-replicas-to-write` arm and disarm under backpressure.
- **`FullSyncInterrupt`** — the link dies mid-payload, in both payload shapes (staged checkpoint
  and live dataset), including after the trailer but before the install.

Quiesce checking in three layers, as in cluster: the catalog on every surviving node via
`DEBUG REPLICATION CHECK`; the existing client-visible assertions (no acked-write loss,
convergence); and the cross-node checks a single-node view cannot express —

- **XREPL-1** no acked write is absent from the promoted node's keyspace,
- **XREPL-2** a replica's applied history is a prefix of the primary's,
- **XREPL-3** `WAIT`'s answer never exceeded `connected_slaves` at any sample, which closes spec
  GAP-5 at level 4, where it belongs.

Recipe `just replication-seeds SEEDS='500'` with the budget in the recipe default so CI does not
duplicate it, laptop-runnable as a plain `just` invocation; nightly through a **generated**
`replication_nightly.py` alongside `cluster_nightly.py` in
`.github/workflows/workflow_gen/src/workflow_gen/workflows/`.

**§8 D9 ruling — land the sweep early, hold the regression file.** No regression-seed file and no
`EXPECTED-FAILURE` muzzles are committed until cluster issue 23
(`.scratch/cluster-correctness/issues/`, same-seed fingerprint diverges under host load) closes,
because a muzzle is a claim about reproducibility. Exit criterion 5 alone blocks on 23; the rest of
this issue does not. A second topology that does *not* reproduce the divergence is itself evidence
for 23, so run and report it.

Out of scope here: durability faults. Replica-crash-mid-sync and lose-unsynced-writes-on-kill ride
campaign 2's `CrashTestHarness` (`frogdb-server/crates/core/src/persistence/test_harness.rs`) —
turmoil has no disk model. This arm consumes that machinery; it does not duplicate or own it.

## Acceptance criteria

- [ ] `simulation/replication_scheduler.rs` runs a primary + N-replica topology off issue 11's
      shared derivation, with no Raft in the arm
- [ ] All four families implemented including their boundary cases — `LinkDrop`'s three
      partial-sync boundaries and `FullSyncInterrupt`'s two payload shapes with the
      post-trailer/pre-install case
- [ ] Quiesce runs `DEBUG REPLICATION CHECK` on every surviving node plus XREPL-1, XREPL-2 and
      XREPL-3
- [ ] `just replication-seeds SEEDS='500'` is laptop-runnable with the budget in one place, and the
      nightly is generated as `replication_nightly.py` (`just workflow-gen --check` green)
- [ ] No regression-seed file or `EXPECTED-FAILURE` muzzle is committed while cluster issue 23 is
      open, and the hold is stated where the next reader will see it (recipe or module docs)

## Blocked by

- Issue 02 (`.scratch/replication-correctness/issues/`) — the quiesce catalog.
- Issue 03 (`.scratch/replication-correctness/issues/`) — the sweep reads violations through
  `DEBUG REPLICATION CHECK`.
- Issue 11 (`.scratch/replication-correctness/issues/`) — this arm is built on the extracted
  derivation, not on a fork of `scheduler.rs`.
- Cluster issue 23 (`.scratch/cluster-correctness/issues/`) blocks exit criterion 5 (the
  regression file) only, not this issue as a whole.
