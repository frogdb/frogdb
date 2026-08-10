# 04 — Proptest link-sequence generator + R1

Status: needs-triage

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

- [ ] `arb_link_sequence` covers all fifteen `LinkAction` variants, stateful and ~80/20 biased,
      with rejected actions retained on purpose
- [ ] Multi-replica bounded ≤3 by default with replica identity in the action vocabulary; chained
      topologies behind a generator flag that is off by default
- [ ] R1 asserts `check_hard(view)` clean after every action including rejections, driving
      production code paths only (no shadow model)
- [ ] All three muzzle parts present; every muzzled shape cites an open issue and carries its
      `#[should_panic]` pin, and the near-miss scope test enumerates what the muzzle does *not*
      cover
- [ ] Property lives in `frogdb-replication` (so the mutation gate sees it) and runs at default
      case counts in the normal suite

## Blocked by

- Issue 02 (`.scratch/replication-correctness/issues/`) — R1 asserts the catalog over the view.
