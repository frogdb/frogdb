# Design-doc attribution corrections + accepted-limitation notes (batch)

Status: ready-for-agent

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
