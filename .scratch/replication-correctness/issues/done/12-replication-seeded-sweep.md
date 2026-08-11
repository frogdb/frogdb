# 12 — Seeded fault schedules on a replication topology

Status: done

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

- [x] `simulation/replication_scheduler.rs` runs a primary + N-replica topology off issue 11's
      shared derivation, with no Raft in the arm
- [x] All four families implemented including their boundary cases — `LinkDrop`'s three
      partial-sync boundaries and `FullSyncInterrupt`'s two payload shapes with the
      post-trailer/pre-install case
- [x] Quiesce runs `DEBUG REPLICATION CHECK` on every surviving node plus XREPL-1, XREPL-2 and
      XREPL-3
- [x] `just replication-seeds SEEDS='500'` is laptop-runnable with the budget in one place, and the
      nightly is generated as `replication_seeds_nightly.py` (`just workflow-gen --check` green —
      the plain `replication_nightly.py` name is issue 04's proptest tier, see the Resolution)
- [x] No regression-seed file or `EXPECTED-FAILURE` muzzle is committed while cluster issue 23 is
      open, and the hold is stated where the next reader will see it (recipe or module docs)

## Resolution (2026-08-11)

Built as `frogdb-server/crates/server/tests/simulation/replication_scheduler.rs`
— the replication sibling of `simulation/scheduler.rs` over issue 11's shared
`Arm`/`Schedule` seam. One primary and two replicas, no Raft; the `LEADER`
sentinel binds to the current primary and the driver tracks it across
promotions. All seven families derive from one `u64`: the reusable `Healthy` /
`CrashRestart` / `Mixed` plus `LinkDrop` (three partial-sync boundaries, chosen
by *sizing the backlog* — 8192 entries cannot evict, 2 evicts on the first write
of any hold, 24 straddles), `PromotionMidStream`, `SlowReplica` and
`FullSyncInterrupt` (both payload shapes; the post-trailer instant is not
externally observable, so it is searched for by chopping the link across the
whole sync window rather than scheduled).

Three check layers, all reported as `Violation`:

1. the catalog on every surviving node through `DEBUG REPLICATION CHECK` (issue
   03), with the DOCUMENTED-EXCEPTION tiers dropped — those are rulings;
2. client-visible assertions at quiesce: convergence and no acked-write loss;
3. `check_cross_node` — `XREPL-1`, `XREPL-2` (three shapes: no replica ahead of
   its primary on one replid, no offset rewind under an unchanged replid outside
   a churn window, no value read back that nobody wrote) and `XREPL-3`, the
   level-4 sampled `WAIT` bound that closes spec GAP-5 where it belongs.

`just replication-seeds SEEDS='500'` holds the budget; the per-PR default suite
runs a seven-seed smoke sweep (one per family, asserting both a `+CONTINUE` and
a resync were actually realized, so a silent loss of coverage fails) plus a
same-seed determinism double-run, plus seed 204 as a regression pin for the
sampling-skew fix below. Nightly is generated from
`workflow_gen/workflows/replication_seeds_nightly.py` into
`replication-seeds-nightly.yml` at 04:11 UTC, clear of the cluster nightly,
passing no seed count so the recipe default stays the single source (`just
workflow-gen --check` green). Named `-seeds-` rather than plain
`replication-nightly.yml` because issue 04's proptest tier claimed that name on
main while this was in flight; the two are budgeted and gated separately.

**§8 D9 held.** No `replication-regression-seeds.txt` and no `EXPECTED-FAILURE`
seed muzzle is committed;
`test_no_regression_seed_file_is_committed_while_cluster_issue_23_is_open`
enforces that and names the condition for lifting it, and the module docs say
why. The two named gaps below are signature-keyed, not seed-keyed: they cannot
hide a seed that fails another way.

### What the sweep found

Three harness defects and two product findings, at 500 seeds in 345s (4 jobs).

- **Harness, fixed:** every node's data dir was a bare `tempfile::tempdir()`,
  and a full sync stages into `checkpoint_ready` as a *sibling* of the data dir
  — so every node of every concurrently running seed staged into one shared
  `$TMPDIR/checkpoint_ready`. Cross-seed keyspace bleed and "staged checkpoint
  is incomplete". Nesting the data dir one level down, as `test-harness`'s
  `create_temp_dir` already does, fixed seeds 77, 84 and 106.
- **Harness, fixed:** `XREPL-1`'s quiesce convergence check demanded the newest
  acked write for every key with no promotion scoping, so seed 317 — `write
  charlie` acked `confirmed=0` on a primary an isolate had cut off, node 2
  promoted three ops later — reported correct behaviour as acked-write loss.
  `History::settleable_values` now floors the claim at the newest write that
  must have survived and accepts anything after it.
- **Harness, fixed:** an observation round is not an atomic snapshot — each node
  costs a connect and an `INFO`, and the primary keeps producing offsets in
  between, replication pings included. Seed 204 read the primary first and a
  replica three connects later, 66 offsets further on, and called the difference
  a lost prefix. `observe_round` now re-reads the primary after every other node,
  so its offset is sampled strictly later than every replica's and bounds them;
  the re-read replaces rather than maxes with the first sample, so a primary that
  rewinds mid-round is still reported rewound. Pinned by
  `test_sampling_skew_does_not_read_as_a_replica_ahead_of_its_primary`.
- **Product, filed as issue 24:** a restart keeps the replication id whose
  history it lost. `replication_state.json` is recovered role-gated, not
  persistence-gated, so a SIGKILLed node returns on its old id at offset 0 and
  every follower is trivially ahead of it; on resync it drags them down with it.
  Seeds 81, 122, 171, 211. Muzzled by `restart_tainted_replids`, keyed on an
  *observed* rewind under an id a restarted node held, so a fix empties the set
  and re-arms `XREPL-2a`/`XREPL-2b` with no edit.
- **Product, a second witness for issue 21:** the Hard-tier `INV-OFFSET-3`
  fires — "replica 2 acked 307 past live 278" — when a replica's `REPLCONF ACK`
  is credited above the primary's live head. Seeds 225 and 340. Filed from
  issue 04's proptest while this sweep was in flight; the sweep reaches the same
  shape from real servers over the simulated network, and adds the reachability
  path the generator cannot see — a promotion settling at *applied* below what a
  replica already received, with no restart involved, which bears on the
  reject-and-disconnect option. Recorded in issue 21 rather than re-filed.
  Muzzled by `known_panic_gap` on that one signature; gapped seeds are counted
  and named on stderr rather than skipped silently.

Diagnosing the panicking seeds needed two affordances that stayed: a chained
panic hook keeping the first message per worker thread (a task panic otherwise
unwinds as tokio's generic shutdown text, naming no invariant), and
`REPLICATION_SEED_TRACE` printing the schedule up front plus per-round node
views. The views are deliberately outside the fingerprint — a reconnect-count
wobble there would read as a determinism failure.

### Evidence

- 500 seeds: 7 red before the fixes, 0 after, in 240s. Seeds 225 and 340 stop at
  issue 21's named panic gap and are counted and named on stderr; the seeds that
  forced issue 24 (81, 122, 171, 211) are covered by its gap. Both counts are
  printed by a *passing* run, so nextest captures them unless the run fails —
  `--success-output immediate` shows them.
- 32/32 in the default scheduler tier (`just concurrency-turmoil
  replication_scheduler`), including the smoke sweep, the determinism replay and
  the seed-204 skew pin.
- `just workflow-gen --check` green.

Nothing in `frogdb-replication` / `frogdb-replication-runtime` src was touched,
so no mutation re-baseline was owed here; issues 21 and 24 carry that obligation
when they land.

## Blocked by

- Issue 02 (`.scratch/replication-correctness/issues/`) — the quiesce catalog.
- Issue 03 (`.scratch/replication-correctness/issues/`) — the sweep reads violations through
  `DEBUG REPLICATION CHECK`.
- Issue 11 (`.scratch/replication-correctness/issues/`) — this arm is built on the extracted
  derivation, not on a fork of `scheduler.rs`.
- Cluster issue 23 (`.scratch/cluster-correctness/issues/`) blocks exit criterion 5 (the
  regression file) only, not this issue as a whole.
