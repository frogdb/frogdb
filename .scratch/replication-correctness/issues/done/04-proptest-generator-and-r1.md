# 04 — Proptest link-sequence generator + R1

Status: done

## Parent

[PRD](../../PRD.md) §3 W2; topology scope ruled in §8 D5; budget inherited in §8 D1.

## What to build

The permutation harness in `frogdb-replication`. `proptest` is **already a dev-dependency** of
the crate (used exactly once today, for the checkpoint-header round trip at `fullsync.rs:1128`),
so no new dependency.

Hard rule inherited from the cluster campaign: **generation goes through the real code — no
shadow model.** Replication has no command enum, so the alphabet is the seam operations
themselves. `arb_link_sequence(len)` generates `LinkAction`: `Write(bytes)`,
`Ack{replica, offset}`, `Attach{replica, announcement}`, `Psync{id, offset}`,
`Detach{replica, departure}`, `Promote`, `Demote(addr)`, `HoldFeed(ms)`, `ReleaseFeed`,
`SaveState`, `RestoreState`, `StageMetadata`, `EvictBacklog`, `Freeze`, `AdmitDivergence`.

Generation is stateful — tracking live replicas, their phases and the backlog window — and biased
roughly 80/20 toward in-context-valid actions, with garbage retained **deliberately**: a
*rejected* action must also preserve every invariant, and the rejection path is exactly where
validate-then-mutate bugs live.

§8 D5 ruling: **multi-replica (bounded ≤3) from day one**, with replica identity part of the
action vocabulary — INV-SESSION-2, INV-OFFSET-3 and the whole `WAIT`-counting class cannot even
be expressed by a single-replica generator, and that is most of the value. **Chained topologies
sit behind a generator flag, off by default**, because chaining is a documented non-guarantee
today; generating chains by default would produce violations that are neither defects nor
exceptions. The flag is what gives INV-ROLE-1 a place to be tested.

**R1 — invariants always hold**: apply the sequence and assert `check_hard(view)` clean after
every action, including the rejected ones.

Muzzle pattern ported verbatim as three parts (`frogdb-cluster/src/properties.rs:738/1361/1449`):
`known_defect(view, action) -> Option<&'static str>` filtering only shapes under an open ticket;
one `pinned_issue_NN_*` `#[should_panic]` witness per muzzle entry that goes red the day the fix
lands; and a single `the_muzzle_only_covers_the_pinned_shapes` test enumerating the near-misses
so the muzzle cannot quietly widen. Defects R1 finds get filed as their own issues (§7), never
silently skipped.

Default case counts run in the normal suite; the boosted recipe is issue 05's.

## Acceptance criteria

- [x] `arb_link_sequence` covers all fifteen `LinkAction` variants, stateful and ~80/20 biased,
      with rejected actions retained on purpose
- [x] Multi-replica bounded ≤3 by default with replica identity in the action vocabulary; chained
      topologies behind a generator flag that is off by default
- [x] R1 asserts `check_hard(view)` clean after every action including rejections, driving
      production code paths only (no shadow model)
- [x] All three muzzle parts present; every muzzled shape cites an open issue and carries its
      `#[should_panic]` pin, and the near-miss scope test enumerates what the muzzle does *not*
      cover
- [x] Property lives in `frogdb-replication` (so the mutation gate sees it) and runs at default
      case counts in the normal suite

## Blocked by

- Issue 02 (`.scratch/replication-correctness/issues/`) — R1 asserts the catalog over the view.

## Resolution (2026-08-11)

Landed as `frogdb-server/crates/replication/src/properties.rs` (`#[cfg(test)]` module of
`frogdb-replication`), plus the `just replication-proptest` recipe, two `.config/nextest.toml`
profiles and a generated `replication-nightly.yml`.

**The harness.** `LinkNode` owns a real `ReplicationTrackerImpl`, `PrimaryReplicationHandler`,
`ReplicaFeedGate`, `AppliedOffset` and an on-disk `ReplicationState` over a `TempDir`; every one
of the fifteen actions is a call into production code (`broadcast_control_command`,
`ingest_replica_ack`, `register_announced_replica`, `handle_partial_sync_request`, `run_exit`,
`begin_primary_stint`/`end_primary_stint`, `feed_gate.publish`, `save_state`,
`ReplicationState::load_or_create`, `read_staged_replication_metadata`+`apply_staged_metadata`,
`expire_idle_backlog`, `AppliedOffset::freeze`, `admit_divergence`). There is no shadow model:
`LinkNode::view()` is only a composition — handler view + registry view + feed-gate view + role —
because no single seam in the crate assembles the whole-node projection the catalog wants.

Generation folds `Step` raw-entropy structs (so shrinking works on the source of the `prop_map`)
through a throwaway `LinkNode` under `catch_unwind`, biased `IN_CONTEXT_BIAS = 0.8` toward
in-context-valid actions; rejections are kept and R1 asserts over them too. Three replica slots,
identity in the vocabulary; `ChainPolicy::Off` by default with a scope test proving the on-path
reaches only the documented `Tier::DocumentedException`.

**Scope note.** `view.fence` is never set — the self-fence lives in
`frogdb-replication-runtime` — so `INV-FENCE-1` and `INV-SESSION-3` are *skipped* (not passed) by
the catalog here. That is issue 05's (R6) to force, and it is why issue 19 (self-fence arms only
on the write path) is unreachable from this harness and correctly carries no muzzle.

**Two new real defects found and filed**, both muzzled on the resolved effect (not a coarse
shape) with a `#[should_panic]` pin apiece and a shared near-miss scope test:

- [issue 20](../open/20-ack-above-live-head.md) — an ACK above the live head is admitted
  unclamped (`INV-OFFSET-3`)
- [issue 21](../open/21-duplicate-streaming-identity.md) — a reconnect streams beside the session
  it replaces (`INV-SESSION-2`)

Known defects 16, 17 and 18 need no muzzle: 16 requires an induced persist failure (and a frozen
gate is not a catalog violation), 17's `INV-OFFSET-2` is a `DocumentedException` that `check_hard`
skips, and 18 needs `adopt_replication_history` with a raw wire id while the staged path this
harness drives validates first.

**Budgets.** `DEFAULT_CASES = 96` in the normal suite (R1 in ~0.5s); `PROPTEST_CASES` raises it,
and `just replication-proptest` defaults to 200 000 for the nightly. A 30 000-case sweep ran clean
in 102s (~290 cases/s, debug build), putting the nightly budget near a quarter of an hour against
a 90-minute job ceiling. `just mutants-diff frogdb-replication` reports no mutants: the change is
test-only.
