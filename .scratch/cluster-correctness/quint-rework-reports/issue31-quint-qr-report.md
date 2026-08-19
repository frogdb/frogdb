# Phase QR — modularize and DRY the Quint cluster models

**Status:** DONE. Commit `0eba0a4a` on `spec-gaps-impl`. Not pushed.
**Nature:** behavior-preserving. No guard, action, invariant, witness or `run` test
changed semantics. Nothing outside `specs/quint/` was touched; `scripts/quint-models.sh`
and `scripts/quint-invariants.sh` are unchanged.

## Module map (file → single responsibility)

| File | Responsibility |
|---|---|
| `specs/quint/cluster_common_types.qnt` | **NEW.** The type vocabulary genuinely identical in both models: `NodeId`, `Option[a]`. No `var`, no `def`, no constants. |
| `specs/quint/cluster_admission_types.qnt` | Admission model's own state shape + its `NODES` sizing knob. |
| `specs/quint/cluster_admission_logic.qnt` | Pure `can*`/`apply*` over explicit state args. |
| `specs/quint/cluster_admission_machine.qnt` | `var` + `init` + actions + `step`. |
| `specs/quint/cluster_admission.qnt` | Runnable main model: witnesses, `inv_*`, `run` tests, model-wide header. |
| `specs/quint/cluster_migration_failover_types.qnt` | Migration model's state shape + its sizing knobs (`NODES`, `SLOTS`, `HOLD_BYTE_CAP`, ext-4 bounds). |
| `specs/quint/cluster_migration_failover_logic.qnt` | Pure guards/updates/helpers, now with a named node-shape predicate section. |
| `specs/quint/cluster_migration_failover_machine.qnt` | Wiring only: `var`s, `init`, frame keepers + the four named frame/postcondition bundles, one action per transition, `step`. Navigation index in the header. |
| `specs/quint/cluster_migration_failover.qnt` | Runnable main model: witnesses (family-indexed), `inv_*` grouped by family, `run` tests. |

Every module header now states what belongs and what does not. The satellite headers
also record that they must never declare an invariant value, because
`scripts/quint-models.sh` would promote the file to a runnable model.

## What was extracted, and what deliberately was not

**Extracted** into `cluster_common_types.qnt`:
- `type NodeId = int` — same meaning in both models (opaque, stable identity).
- `type Option[a] = Some(a) | None` — both models had a local, structurally-equal but
  *distinct* copy. One definition means the "why not `basicSpells`" deviation is argued
  once and the two models cannot drift into incompatible `Option`s.

**Not extracted (deliberate):**
- `NODES` / `SLOTS` / `HOLD_BYTE_CAP` and the ext-4 observation bounds. These are
  bounded-checking sizing knobs, not shared vocabulary. The two models agree on a
  4-node universe *today*; sharing the constant would silently rebound one model when
  the other's state space is retuned. Documented in the common module's header.
- Both models' `NodeState`. Same name, disjoint content (admission tracks
  bootstrap-vs-join intent and Raft provenance; migration tracks role/parent/keyspace and
  the run-identity machinery). Unifying would be forcing a merge on two different state
  spaces.
- No `cluster_common_logic.qnt` was created. The only model-agnostic helper in either
  model is `max2`, used by one model — a one-definition module would be ceremony.

Quint's `import M.*` is **not transitive** (proven by the pre-existing admission model,
which imports both `_types` and `_machine`), so every file that names `NodeId`/`Option`
imports the common module directly. No `export`, no ambiguity errors.

## Renames (old → new), all references updated

| Old | New | Why |
|---|---|---|
| `canComplete` | `canCompleteMigration` | `can<Transition>` matches the action it guards. |
| `canCancel` | `canCancelMigration` | same |
| `servingPrimary` | `isServingPrimary` | shape predicate → `is*`. |
| `recordIsStale` | `isRecordStale` | prefix consistency. |
| `refusalIsTerminal` | `isRefusalTerminal` | prefix consistency. |
| `observedProgress` | `hasObservedProgress` | predicate, not a value. |
| `abortableByBound` | `isAbortableByBound` | prefix consistency. |
| `canAssignSlot(migs, res, target: NodeState, s)` | `canAssignSlot(allNodes, migs, res, s, n)` | every other guard takes the node map + id; a bare `NodeState` arg was the odd one out. |
| `canBeginMigration(slotMap, migs, res, targetNode: NodeState, s, source, target)` | `canBeginMigration(allNodes, slotMap, migs, res, s, source, target)` | same |
| machine section header `Roles, failover and restarts.` | `Roles, failover and demotion.` | restarts have their own section directly below it; the old title was inaccurate. |

No `inv_*`, `witness*`, `run` test, action or `var` was renamed. Nothing outside
`specs/quint/` referenced any of the renamed predicates (checked across `*.md`, `*.rs`,
`*.sh`, `Justfile`).

## New named predicates (migration logic module)

Node-shape section (new):
- `isLivePrimary(allNodes, n)` — member ∧ Primary. The single most-repeated phrase in the
  model. Rewired: `shardPrimary`, `canSetRole`, `canCompleteMigration`, `isServingPrimary`,
  `beltOk`, `canDemoteNode`, `canDemoteNodeExternal`, and the machine's
  `attachTargetReplica`.
- `isSlotOwningPrimary(allNodes, slotMap, n)` — live primary that still owns a slot.
- `isInShardReplicaOf(allNodes, succ, p)` — the shard relationship `beltOk` and
  `canDemoteNode` both require of a successor.

Guard section:
- `canTargetAct(allNodes, t)` = `canAct ∧ ¬silent` — used by `reportTargetIngest`.
- `canTargetApply(allNodes, t)` = `canTargetAct ∧ ¬cannot_apply` — used by `applyAtTarget`
  and `coverageBatch`. Kept as two names because `reportTargetIngest` is gated only by the
  first (a target that cannot apply may still truthfully report what it holds) — this is
  the existing semantics, preserved exactly.
- `canApplyFailover(allNodes, slotMap, old, succ, requireSynced, obsEpoch, obsRole)` =
  `beltOk ∧ fenceOk` — the admission the three failover paths share. `failoverGraceful`
  now differs from `failoverAuto`/`failoverForced` only by what it adds.
- `barrierCoversMovedSlots(slotMap, fence, old)` = `fence ≠ ∅ ∧ movedSlotsOf ⊆ fence` —
  issue 26's graceful conjunct pair.
- `canArmFailoverFence(allNodes, slotMap, n)` = `isSlotOwningPrimary`.

**Mutation sensitivity is preserved:** the `defects.fence` and `defects.barrier` ghosts
still recompute `fenceOk` and `movedSlotsOf(...).subseteq(...)` *inline*, not through the
new predicates, so a mutant that weakens `canApplyFailover` or `barrierCoversMovedSlots`
is still caught by `inv_last_failover_fenced` / `inv_graceful_failover_barriered`. Same for
`defects.admission` vs `canCompleteMigration`. Comments at each site say so.

## Machine file navigation (no split, no reordering)

The machine file was **not** split into submodules: it declares the shared `var`s, and
splitting them across modules makes the imports awkward and conflict-prone for no
navigation gain. Per the task's stated preference, section grouping was used instead —
and **no action was moved**, since the existing blocks were already contiguous by family.

- Four recurring frames named from the *written* side, defined alongside the keepers and
  expanding to exactly the assignments they replace:
  `nodesOnly(m)` (15 call sites), `migrationsOnly(m)` (7), `releaseHoldRegion(s, src)` (4:
  `completeMigration`, `abortHandoff`, `cancelMigration`, `boundAbort`),
  `releaseLocalFence(s, src)` (2: `localReleaseOnCapBreach`, `selfFenceRelease` — these
  deliberately do **not** clear `barriers.disconnected`, and the two bundles keep that
  distinction explicit).
- New section headers inserted: *Shared postconditions over the barrier/hold variables*,
  *Handoff barrier and hold buffer (ext-2/ext-11)*, *Stream and target ingest (ext-1/ext-5)*,
  *Target-side replicas (ext-6/ext-14)*, *Migration lifecycle, continued: seal, batches,
  commit and the two aborts*.
- A navigation index in the file header lists the sections in true file order (the earlier
  draft index listed a family order the file does not have; corrected).

## Invariant / witness grouping (main files)

`cluster_migration_failover.qnt` — families, in the order the values already appear (no
reordering was needed):
- **Ownership and handoff** — `inv_slot_owner_valid`, `inv_migration_endpoints_valid`,
  `inv_handoff_owned`, `inv_handoff_seq_never_reused`
- **Epoch and reset** — `inv_epoch_monotone`, `inv_epoch_never_decreases`
- **Fencing, barrier and demotion** — `inv_last_failover_demoted`,
  `inv_last_failover_fenced`, `inv_graceful_failover_barriered`
- **Commit admission** — `inv_complete_requires_drained`
- **Residue and reaper** / **Identity and run** — reserved headings only (Q2 territory);
  written so they contain no text a discovery grep could mistake for a declaration.

Witnesses carry a family index in their section header; the witness values themselves were
left in place (reordering them would have produced diff churn for no gain).

`cluster_admission.qnt` — three family headers added (*Leadership / usurpation*,
*Identity / restart*, *Admission / absorption*) plus a family preamble, and the
Option-deviation and model-layout notes updated for the five-file layout.

## Discovery contract

Unchanged, and re-verified after the refactor:

```
$ bash scripts/quint-models.sh specs/quint
specs/quint/cluster_admission.qnt
specs/quint/cluster_migration_failover.qnt

$ bash scripts/quint-invariants.sh specs/quint/cluster_migration_failover.qnt
inv_slot_owner_valid inv_migration_endpoints_valid inv_handoff_owned
inv_handoff_seq_never_reused inv_epoch_monotone inv_epoch_never_decreases
inv_last_failover_demoted inv_last_failover_fenced inv_graceful_failover_barriered
inv_complete_requires_drained
```

`cluster_common_types.qnt` correctly does **not** appear as a model. Neither script
needed editing. The satellite headers that mention the discovery rule spell it as
`` `val inv_*` `` — the `*` is what keeps the grep from matching, and the same trick was
already in use before this phase.

## Gate results (baseline → post)

| Gate | Baseline (Q1, `5de6300e`) | Post-refactor |
|---|---|---|
| 1. Baseline captured before edits | migration 26 passing, admission 4 passing | — |
| 2. `just quint-check` | green (8 files) | **green (9 files)** |
| 3. `run` tests, identical counts | 26 / 4 | **26 passing / 4 passing** |
| 4. `just quint-run` | green | **green (exit 0)** |
| 5. Random-walk, conjunction of all invariants (`--max-samples=200 --max-steps=20`) | n/a | **`[ok] No violation found`**, 200 traces, max/min length 21 |

## Flags (things noticed, deliberately NOT changed)

1. **`inv_migration_endpoints_valid` does not state the target's role.** It requires both
   endpoints to be members but says nothing about the target being a Primary, while
   `canCompleteMigration` *does* require it (V6-C2/V10-C3). This is consistent with the
   main file's "deliberately not modeled" note, so it was left alone — but it means the
   ownership family has no state-level claim about who a migration is aimed at between
   `Begin` and `Complete`.
2. **`inv_handoff_owned`'s minting claim ignores the reset epoch.** It checks
   `spent.exists(t => t.seq == m.attempt)` while `spent` is keyed by `{epoch, seq}` (ext-10).
   A stamp minted under a *different* reset epoch satisfies it. The distinctness half
   (`inv_handoff_seq_never_reused`) is epoch-aware; this half is not. Possibly intentional
   (the migration record does not carry the epoch it was minted under), possibly a Q1 gap.
3. **`releaseLocalFence` vs `releaseHoldRegion` asymmetry on `barriers.disconnected`.**
   `localReleaseOnCapBreach` and `selfFenceRelease` clear `armed` and the held set but leave
   the slot in `disconnected` forever — nothing else removes it except a full teardown. That
   is the pre-existing behavior, now visible as two distinct named bundles rather than
   buried in duplicated assignment blocks; worth a ruling on whether the residual
   `disconnected` membership is intended.
4. **`demoteNodeExternal` was the one `nodes'`-only action the mechanical sweep skipped**
   (interleaved comment lines); it was converted by hand and its comment kept, so its frame
   is now `nodesOnly(...)` like the rest. Flagging only because it is the one site where the
   conversion was not mechanical.
5. **The 16 Q1 ambiguity flags were left untouched**, as instructed. None of them were
   resolved, weakened or strengthened by this phase.
