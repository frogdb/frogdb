# 04 — Properties P2 (snapshot lossless anywhere), P3 (replay determinism), P4 (event conservation)

Status: done

## Parent

[PRD](../../PRD.md) §3 W2.

## What to build

Three properties over the issue-03 generator:

- **P2 — snapshot/restore lossless at any point**: apply a prefix, round-trip through
  *both* snapshot vehicles (serialized `ClusterStateInner` openraft path and the
  `ClusterSnapshot` → `from_snapshot` DTO path — the audit showed they can disagree),
  apply the suffix, compare against the uninterrupted run. Retro-covers the whole
  audit-defect-2 class (FM-CLUSTER-100) for every field, forever.
- **P3 — replay determinism**: same sequence, two fresh states, identical results
  (closes round-2 87/F2); doubles as a purity guard against wall-clock/randomness in
  apply.
- **P4 — event conservation**: every `SlotHandoffPrepared` pairs with exactly one
  `SlotHandoffReleased` across the sequence, via the `release_events()` funnel.

## Acceptance criteria

- [x] P2 compares both restore vehicles against the uninterrupted run at every split
      point
- [x] P3 and P4 land beside it; all three in the default suite + nightly boosted pass
- [x] Reverting the FM-CLUSTER-100 fix makes P2 fail (retro-validation evidence for
      issue 13)
- [x] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 03 (`.scratch/cluster-correctness/issues/`) — shares the generator.

## Resolution

All three properties landed in `frogdb-server/crates/cluster/src/properties.rs` beside P1,
on the issue-03 generator (`arb_command_sequence`, stateful through the real `apply_to`).

### P2 — `p2_a_snapshot_restore_at_any_point_is_lossless`

Walks the baseline **once** and forks at *every* split point `0..=len` (48 commands, so 49
forks per case) rather than re-applying the prefix each time. At each split both vehicles are
exercised independently:

- **openraft**: `encode_snapshot()` → `serde_json` decode to `ClusterStateInner` →
  `restore_from_snapshot(inner, &meta)`, mirroring `RaftStateMachine::install_snapshot`.
- **DTO**: `snapshot()` → `ClusterState::from_snapshot(dto, self_node_id_atomic())`.

Two comparisons per fork: the restored state must equal the live state at that split, and the
state resumed from it (suffix applied) must equal the uninterrupted run's final state. The
comparison is full-field — pretty `serde_json` of `ClusterStateInner` plus its `Debug`
rendering, the same mechanism the issue-05 golden fixtures use — with a `first_difference`
helper naming the first differing line. The DTO vehicle cannot carry `last_applied_log` /
`last_membership`; rather than excluding those fields the property asserts
`openraft_bookkeeping_is_inert` on the baseline, so a future change that made the generator
move them would fail loudly instead of being silently skipped.

P2 is quadratic in sequence length where P1/P3/P4 are linear, so it draws
`HEAVY_CASE_DIVISOR = 8`-th of whatever budget is in force (`heavy_config()` over the same
`PROPTEST_CASES` machinery): 12 cases in the default suite, 25 000 under
`just cluster-proptest`.

### P3 — `p3_replaying_a_sequence_is_deterministic`

Records `(outcome, full state render)` per step on a first fresh state, then replays the same
sequence on a second fresh state and compares step for step. The second replay runs *after*
the first rather than interleaved, so a stray wall-clock read or RNG draw inside `apply` has a
real chance to differ — that is the purity guard, not a side effect.

### P4 — `p4_a_prepared_handoff_is_released_exactly_once`

The conservation claim as implemented (documented in full at the test): a ledger
`slot -> (seq, source_node)` is built from the `release_events()` event stream alone —
`SlotHandoffPrepared` inserts, `SlotHandoffReleased` removes — and after **every** command the
ledger must equal the set of prepared handoffs the state itself holds. Three consequences,
each asserted where it can be named:

1. **No release is spurious or doubled** — a `SlotHandoffReleased` must remove a ledger entry
   matching both its `seq` and its `source_node`.
2. **No prepared handoff disappears silently** — every arm that drops a migration record goes
   through `release_events`; a path that forgot the funnel leaves a ledger entry the state no
   longer has and the per-step equality fires.
3. **Supersession is the one licensed exception, and only under a lapsed lease** — a
   `PrepareSlotHandoff` over a lease-expired handoff replaces it with no release event. The
   property re-reads the *pre-command* state and requires the displaced record to be the one
   in the ledger and to have genuinely expired at the proposer's `proposed_at_ms`, so letting
   a *live* handoff be superseded without a release would fail here.

Handoffs still open when the sequence ends need no special case: they are exactly the entries
the state still holds, and the per-step equality already pins that. Their release is owed to a
command the sequence never issued — a truncated history, not a leak.

### Boosted path

`just cluster-proptest` filters on `test(/properties/)`, so it picked the three up with no
change. It did need a new `cluster-proptest` nextest profile (`.config/nextest.toml`): the
default profile hard-kills `properties::` tests at 120 s and boosted P2 legitimately runs for
minutes. The real bound stays the nightly job's `timeout-minutes`, following the
`seed_sweep_nightly` precedent.

### Retro-validation of FM-CLUSTER-100 (evidence for issue 13)

The fix was reverted locally (uncommitted, since restored) in two steps.

**A — `from_snapshot` back to `handoff_seq: 0`.** P2 fails on the *first* case at the default
budget (`successes: 0`), shrinking to three commands. The `from_snapshot` invariant hook wins
the race and reports the violation by name:

```
thread 'properties::p2_a_snapshot_restore_at_any_point_is_lossless' panicked at
  frogdb-server/crates/cluster/src/state.rs:163:9:
cluster state invariants violated after from_snapshot:
  - INV-HANDOFF-1: slot 7 carries handoff seq 1 above the generation counter 0
minimal failing input: commands = [
    AddNode { node: NodeInfo { id: 1, ... } },
    BeginSlotMigration { slot: 7, source_node: 1, target_node: 1 },
    PrepareSlotHandoff { slot: 7, source_node: 1, target_node: 1,
                         barrier_ms: 100, lease_ms: 40000, proposed_at_ms: 1000202 },
]
	successes: 0
```

**B — same revert, plus the `from_snapshot` hook disabled** so P2 has to name the divergence
itself rather than inheriting the hook's panic. It does, pointing at the vehicle, the split
point and the exact field:

```
Test failed: the Dto vehicle did not carry the state across a snapshot taken after 3 of 4 commands:
line 36:
  uninterrupted:   "handoff_seq": 1,
  restored:        "handoff_seq": 0,
minimal failing input: commands = [
    AddNode { node: NodeInfo { id: 4, ... } },
    BeginSlotMigration { slot: 2, source_node: 4, target_node: 4 },
    PrepareSlotHandoff { slot: 2, source_node: 4, target_node: 4,
                         barrier_ms: 0, lease_ms: 0, proposed_at_ms: 1000050 },
    BeginSlotMigration { slot: 1, source_node: 4, target_node: 4 },
]
	successes: 1
```

Both experiments were reverted; `state.rs` is untouched by this issue and the
`proptest-regressions/properties.txt` entries the broken builds wrote were discarded.

### Counterexamples

None. No new failing sequence was found against the fixed tree at either budget, so no known
defect (14/15/16) needed its muzzle widened and no issue 17 was filed. The only failures
observed were the two deliberately induced above.

### Verification

- `just test frogdb-cluster` — 283/283 pass (the 4 properties plus a unit test pinning the
  heavy-budget division).
- `just check frogdb-cluster`, `just fmt` — clean.
- `just lint-failure-modes` — OK, 278 failure modes, 1382 test references, 1382 tags. The
  properties carry no `FM-` tag, matching P1: they are universally quantified, not point
  witnesses for one row.
- `just scratch-check` — OK.
- `just mutants-diff frogdb-cluster` — `No mutants to filter`. The whole diff is `#[cfg(test)]`
  code (`mod properties` is test-gated) plus `.config/nextest.toml` and the `Justfile`, so
  cargo-mutants generates nothing. Vacuously triaged: no production code changed.
