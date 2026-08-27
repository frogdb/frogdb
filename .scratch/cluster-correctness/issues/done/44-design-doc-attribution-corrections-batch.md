# Design-doc attribution corrections + accepted-limitation notes (batch)

Status: done

Size: S

> **Ruling (2026-08-22,
> [work-item rulings](../../2026-08-22-work-item-rulings.md) R2):** the five
> wrong-attribution flags from the issue-31 Quint rework (Q2-Q4) are batched as one
> doc-fix; the four modelling-gap battery misses are accepted as documented limitations
> and recorded as such. Agent drafts the edits with citations; user reviews the diff
> before merge (small enough to eyeball).

## Scope

All edits target `.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md` and/or the
Q2-Q4 campaign ledger notes in `specs/quint/` comments — citation-correcting and note-adding only.
**No semantic changes**: anything that would alter a verdict, an invariant, or an arm belongs to
issue 43 or the campaign, not here.

### Attribution corrections (from the rework's flag list)

1. **F6/M65** — correct the flagged attribution (which battery/verdict actually covers the
   mutation) at its citation site.
2. **F7/M59** — same treatment.
3. **F13/M21** — same treatment.
4. **M38/M39** — attribution swap/correction between the two mutations.
5. **M34** — add the one-line note that `admitted ≡ identityWritten` in the model, which is why
   the mutation is equivalent (kill not expected).

For each: locate the flag's citation in the Q2-Q4 rework notes (the campaign ledger
`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md` and the batteries' comment
blocks), fix the attribution in place, and cite the correct kill/witness.

### Accepted-limitation notes (documented, not fixed)

Record at each mutation's site (battery comment or ledger) that the miss is an **accepted
limitation** per R2, with its one-line reason:

- **M06** — compound-only observable; battery drives single actions.
- **M22** — requires a temporal operator the invariant set deliberately excludes.
- **M32** — no doc-level observable distinguishes the mutant.
- **M37** — dataset-discard path outside the model's state scope.

Each note ends with: "revisit only if campaign work makes the observable cheap".

## Acceptance criteria

- [ ] All five attribution corrections landed with correct citations
- [ ] Four accepted-limitation notes landed at the mutation sites
- [ ] Diff is doc/comment-only (no `.qnt` semantics, no Rust); user reviews before merge
- [ ] Quint suite still green (comment-only edits — sanity run)

## Blocked by

None - can start immediately.

## Resolution (2026-08-23) — pending user diff review

Doc/comment-only, as ruled. **Every attribution was re-measured rather than transcribed**:
each mutation was applied to the *current* (post-R3/R4/R5, cluster issue 43) model one
edit at a time, then `quint test specs/quint/cluster_migration_failover.qnt` plus a walk
over the full 42-invariant conjunction (`quint run --max-samples=2000 --max-steps=40`,
seeds `0x1`/`0x2`; 500×20 per-invariant sweeps to name the violated one), then reverted.
The tree carries no mutation.

Two of the Q4-era "corrections" in the design doc turned out to be wrong themselves and
are now superseded — this is the substance of the batch, not a rubber stamp.

### Attribution corrections

1. **F6 / M65** — `.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md`,
   ext-18 mutation (15). Old: "only the `witnessFenceClears` half is operative". New:
   **neither** named half bites — `inv_no_record_outlives_its_registration` is not
   asserted (and R5's `inv_stale_record_never_admits` is untouched by this mutant), while
   `fenceClearsWithinThreeStepsTest` still **passes** because `completeAdoption` also
   drops `node_fenced`. Measured kill is **CAUGHT-T** on five run tests:
   `recordOutlivesRegistrationTest`, `staleRecordAfterForgetAndRejoinTest`,
   `staleRecordAfterOtherMemberResetTest`, `staleRecordAcrossWipeAndRebootTest`,
   `foreignRefusalDoesNotDisposeTest`; no invariant at 2000×40 on two seeds.
2. **F7 / M59** — same file, ext-17 mutation (9). Old: killed by
   `inv_no_live_record_disposed_by_a_foreign_refusal` / `foreignRefusalDoesNotDisposeTest`.
   New: those are the **other** half's detector — row **M68**, dropping `refusalBinds`'
   `reg_seq` conjunct (verified: that mutant fails exactly that one test). Dropping the
   `stage_id` conjunct — M59's own mutation — is **MISSED**: 93/93 tests pass, walk clean
   at 2000×40. Recorded with the reason (the surviving `reg_seq` half discriminates in
   every reachable battery trace) and the fixture that would close it.
3. **F13 / M21** — same file, ext-12 residue row. Unassign half confirmed unchanged
   (`inv_slot_owner_valid` at 500×20 + `orphanRehomeToSourceTest`,
   `removeNodeForceEvictsLiveOwnerTest`). Mark half refreshed: three failing tests
   (`reapDeferredWhileTargetGoneTest`, `orphanRehomeToSourceTest`,
   `orphanRehomeToAnotherPrimaryTest`), and `failPromotionRefusedAfterSourceDepartedTest`
   dropped — it no longer fails under the mutant.
4. **M38 / M39** — same file, ext-15 staged-flip row. Correction stands (both CAUGHT-T,
   `inv_member_keyspace_is_tracked` green at 2000×40 for each); M38's detector list gains
   R5's `staleRecordAcrossWipeAndRebootTest`.
5. **M34** — one-line note added **at the model**, on `identityWritten` in
   `specs/quint/cluster_migration_failover_machine.qnt`: `admitted ≡ identityWritten`
   because `identityOrderOk`'s `Some` arm is `lexGt` (strict domination) and its `None`
   arm makes `Some(identity) != None` trivial — so the admission-bound form is an
   **equivalent mutation** and no kill is expected. The design doc's existing note gained
   the same derivation.

### Accepted-limitation notes (each ends "revisit only if campaign work makes the observable cheap")

- **M06** — at `reportTargetIngest`, `specs/quint/cluster_migration_failover_machine.qnt`.
- **M22** — in the header of `specs/quint/cluster_migration_failover_temporal.qnt`.
- **M32** — at the `defects.crossShardSuccessor` field, same machine module.
- **M37** — at `completeAdoption`, same machine module.

M32 and M37 are **CAUGHT** since `2eb66e35`; the notes record the *residual* limitation
R2 named (M32's kill is a postcondition ghost restating the guard, not a doc-level
observable — the semantic one is phase-3 territory; M37's `keys` is a slot-id set, so the
modelled discard is the slot-claim projection of a byte-level dataset drop), not a claim
that either row is still missed.

The campaign ledger
[`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`](../../../formal-spec/2026-08-19-quint-completeness-campaign.md)
gained a "Batch disposition — cluster issue 44 / R2" table with the same measurements.

### Verification

- `just quint-check` — exit 0, 22 files type-check.
- `quint test specs/quint/cluster_migration_failover.qnt` — 93 passing, 0 failing
  (unchanged from the pre-edit baseline).
- `just scratch-check` — `OK: 15 feature dirs, 332 issues, tracker consistent`.
- Diff is docs + `.qnt` **comments** only; no invariant, arm, action or state changed.

### Follow-ups recorded, not fixed (semantic — issue 43 / campaign territory)

- **M59 has no forcing trace.** The stage-id half of `refusalBinds` is unobserved. Closing
  it needs a fixture: stage, adopt (`completeAdoption` clears the record without a
  registration change), re-stage under the **same** `registration_seq`, then deliver the
  first stage's refusal — `inv_no_live_record_disposed_by_a_foreign_refusal` already
  states the property, only the reaching trace is missing.
- **`failPromotionRefusedAfterSourceDepartedTest` lost its M21b kill-power** between the
  2026-08-20 measurement and today; worth a look when the residue rows are next touched.
