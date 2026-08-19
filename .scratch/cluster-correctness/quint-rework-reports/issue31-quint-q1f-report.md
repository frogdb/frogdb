# Q1F — corrective pass on the Quint migration model

**Status:** COMPLETE. Commit `4e1911a8b8c17178e1cbf6993b400c813c3a62ed` on `spec-gaps-impl`
(`specs/quint/` only, `--no-verify`, not pushed). `website/src/data/frogctl-cli.json` left
staged and untouched; `.claude/jobs/` left untracked.

Files touched: `specs/quint/cluster_migration_failover{,_logic,_machine,_types}.qnt`
(4 files, +328/−139). `cluster_common_types.qnt` and the admission model were not touched.

## Kept from b1a43cb6 (doc-aligned, unchanged by this pass)

- `resetCluster` rewrite + the restored literal `m.attempt <= ctl.handoff_seq` bound and the
  epoch-keyed minting claim (doc:2105-2226).
- `Migration.require_replica_ack` as a captured per-record field, with the global
  `ctl.count_replicas` and `setCountedReplicasKnob` gone (doc:8780-8786, V6-C2).

## FIX 1 — pruning on source failover restored (doc:8327-8331, 8229, 6291)

- `_logic.qnt`: deleted `rehomedMigrations`; `slotsSourcedBy` keeps its role and its comment
  now points at the prune/cancel users.
- `_machine.qnt`: `sourceFailover` writes `migrations' = v.migrations`, i.e. the shared
  failover postcondition that prunes every open migration naming the failed-over node on
  either leg, and pays the release events (barrier disarm, held exclusion, feed reset).
  Comment cites TR-CLUSTER-018 / FM-CLUSTER-036 / FM-CLUSTER-087 / FM-CLUSTER-104.
- Main file: `sourceFailoverRehomesMigrationTest` deleted; `sourceFailoverPrunesMigrationTest`
  added — asserts record gone, barrier/held/feed released, `residue == None`, shadow left
  in place, and ends with `prepareHandoff(1).fail()` (nothing to hand off).
  No cross-run Confirm-refusal trace was preserved: refusal is record-gone (doc:6706).

## FIX 2 — restart-invalidates-drain realigned

(a) Boot-arm cancellation binds to the **field write** (doc:7278-7281, V10-M1):

- `_logic.qnt`: `unsealedBySource` deleted, replaced by
  `cancelledBySource(migs, node)` which sets every slot whose record has `source == node`
  to `None`.
- `_machine.qnt`: `sourceRestart` now touches **no** record — frame is
  `nodes'`(restarted) + `held'`(cleared) + `ctl'`, everything else preserved.
  `restarted()` still preserves `Migration.fenced`, left as-is per the brief.
  `reportRunIdentity` gained a `replay: bool` argument; it computes
  `identityWritten = admitted and ns.stored_identity != Some(identity)` and, only then,
  cancels `slotsSourcedBy(migrations, n)` and pays the same release events as a prune
  (barrier `armed`/`disconnected` exclusion, `held` exclusion, `feed_bytes` reset), so no
  orphaned armed barrier can wedge. A topology-only re-proposal at equality changes no
  identity and cancels nothing.

(b) Complete admission conjunct (doc:6966):

- `_logic.qnt`: new `sourceRunIdentityOk(allNodes, m)` compares `m.source_log.run_id`
  against the source's **replicated** `stored_identity.run_id` (`None` ⇒ false), wired into
  `canCompleteMigration` alongside the restored one-tag-space drain comparison
  `d.run_id == m.source_log.run_id`. Reading `stored_identity` rather than the live
  `run_id` deliberately leaves open the doc-accepted window where the source has physically
  restarted but its boot report has not yet applied.

Test rework (the two pinning tests replaced):

- `sourceBootReportCancelsMigrationTest` (t1): restart leaves record + barrier intact →
  Boot report writing `Some({inc:1, seq:2, run_id:5})` cancels the record, releases
  barrier/held/feed, creates no residue → `completeMigration(1,1).fail()`.
- `replayedReportCancelsNothingTest` (t2): `reportRunIdentity(1, Boot, Primary, 0, true)`
  is refused (class `Ordering`, `identityOrderOk` strict domination) as a no-op, record
  intact, and `completeMigration(1,1)` still admits — the declared open window (doc:~10362).

## FIX 3 — `Residue.promotion_failures` reverted (doc:10240-10249)

- `_types.qnt`: `Residue` is exactly `{mig, source, target, promoted, source_gone,
  target_gone}`; the added counter field and its comment are gone.
- `_machine.qnt`: `completeMigration`'s residue initializer no longer sets it; `failPromotion`
  writes no counter and leaves the entry at `promoted == false`.
- Main file: `happyMigrationTest`'s residue expectation drops `promotion_failures: 0`.

## FIX 4 — orphan re-home arm added (doc:10259-10262)

- `_logic.qnt`: `canRehomeOrphanSlot(allNodes, res, s, dest)` — requires `r.promoted`,
  `r.target_gone`, `isLivePrimary(allNodes, dest)`, and, when `dest == r.source`, that the
  source still holds the slot's keys.
- `_machine.qnt`: `action rehomeOrphanSlot(s, dest)`, placed after `retargetSlotResidue` and
  wired into `step`. Two dispositions:
  - `dest == r.source`: slot re-assigned to the source, residue entry removed.
  - otherwise: slot re-assigned to `dest`, entry kept with `target = dest`,
    `target_gone = false`, and the slot unioned into `dest`'s `keys` so a `promoted` entry
    never names a target holding nothing; the entry then follows the ordinary
    reap-then-clear path.
  The reaper stays the only keyspace **deleter** — ext-12's only-one-remover rule intact.
- Main file: `orphanRehomeToSourceTest` and `orphanRehomeToAnotherPrimaryTest` (the latter
  promotes node 2 via `setRole(2, Primary, None)`, re-homes, then reaps and clears).
- `residueHasAnEffectiveRemover` was **not** written (Q2's).

## Test-count delta: 29 → 32 (migration model); admission unchanged at 4

Removed (3):
- `completeRefusesAfterSourceRestartTest` — pinned the reverted "restart mutates the record"
  mechanism; superseded by t1.
- `sourceRestartHandoffCanBeReEarnedTest` — same mechanism; superseded by t2.
- `sourceFailoverRehomesMigrationTest` — asserted the survival/re-home FIX 1 reverts.

Added (6):
- `sourceBootReportCancelsMigrationTest` (FIX 2a t1)
- `replayedReportCancelsNothingTest` (FIX 2a t2)
- `sourceFailoverPrunesMigrationTest` (FIX 1)
- `orphanRehomeToSourceTest` (FIX 4, disposition i)
- `orphanRehomeToAnotherPrimaryTest` (FIX 4, disposition ii)
- `failPromotionRefusedAfterSourceDepartedTest` (regression, see flag)

## Gates — all green

1. `just quint-check` — all 9 `.qnt` files typecheck.
2. `quint test specs/quint/cluster_migration_failover.qnt` — 32 passing, 0 failed.
   `cluster_admission*` — 4 passing, untouched.
3. `just quint-run` — admission 4 passing + `[ok] No violation found`;
   migration 32 passing + `[ok] No violation found` over all 10 invariants.
4. Random walk, conjunction of all 10 invariants, `--max-samples=8000 --max-steps=60`,
   8 independent runs, all `[ok] No violation found`. Seeds:
   `0x5f0af96bd7fbca14`, `0xdc96dfe2ecc06ebc`, `0xdf7bfcbb3aa08eea`, `0x86f91c38faab66f0`,
   `0x7109b9ecbe725ae`, `0x7136bbf57f86470f`, `0xd67fba6e6ca97650`, `0xb1dcd001851725f7`.

## Flags — 1

**F1 (pre-existing model defect, found by gate 4, fixed here).** The rollback arm
`failPromotion` assigned the slot back to `r.source` with no liveness check, so a rollback
after the source had departed left an assigned slot owned by a non-member — a genuine
`inv_slot_owner_valid` (INV-REF-1) violation. Trace: `slots[2] = Some(4)` with node 4 a
non-member and residue `{source: 4, promoted: false, source_gone: true}`. Confirmed
pre-existing by running the b1a43cb6 file copies under the same walk (1 of 6 walks
violated), so it is not fallout from FIXes 1-4. Fixed by a new
`canRehomeOrphanSlot`-style guard `canFailPromotion(allNodes, res, s)` requiring
`not(r.promoted) and isLivePrimary(allNodes, r.source)`, plus the regression test
`failPromotionRefusedAfterSourceDepartedTest`. Worth confirming against the doc in Q2 that
"rollback is refused once the source has departed (the residue is then the reaper's /
orphan arm's problem)" is the intended real-system rule rather than a modelling shortcut —
the design doc does not state the departed-source rollback case explicitly.
