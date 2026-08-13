# 29 — Spec row-edit sweep from the 2026-08-13 rulings

Status: ready-for-agent

## Parent

[Adversarial design review — `specs/cluster.md` + the 2026-08-13 rulings](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-cluster.md),
finding **A5**. Spec-gap finding, distinct from the amended rulings on issues 14–20 and 25
themselves: those rulings each amend the one or two FM rows their own issue is *about*. A5's point
is that the rulings' effects ripple into rows nobody's issue explicitly owns — rows that state a
rationale, a trigger list, or an invariant the ruling silently invalidates. Left alone, those rows
keep asserting things that stopped being true the day the ruling landed, and the next reader
"restores" the behavior the ruling removed. This issue is a tracking/checklist issue for the
coordinated text-only edit set; it makes no behavior change itself.

## What is wrong

Fifteen `FM-CLUSTER-*` rows carry text that the settled + amended rulings on issues 14, 15, 16, 17,
18, 19, 20 and 25 (recorded 2026-08-13) invalidate or leave stale, once those issues' fixes land:

| Row | What the row currently claims | Why it goes stale | Owning issue |
|---|---|---|---|
| FM-CLUSTER-039 | "A *force* failover of an already-removed node is deliberately allowed, because that is precisely the situation an automatic failover fires in." | With the automatic path now proposing `force: false` (demote-don't-remove), that is no longer the situation an automatic failover fires in — the sentence describes a waiver the automatic path no longer needs. The automatic path now inherits `test_failover_graceful_requires_old_node`'s `NodeNotFound(old)` refusal, self-correcting via issue 18's level-triggered pass (the next tick re-scores). That must be *written*, not left implicit. | Issue 20 (ruling + amendment) |
| FM-CLUSTER-036 | Title/Trigger: "An automatic (force) failover of a primary that is the source or target of an open migration" cancels its migrations. | Two changes land on this row simultaneously: issue 15's ruling makes migration cancellation apply on *every* failover naming the demoted node, graceful or forced (not just the force branch); and issue 20 makes the automatic path force:false by default, so the "automatic (force)" framing in the trigger no longer matches which path is force vs. demote. | Issue 15 (cancellation scope) + Issue 20 (which path is force) |
| FM-CLUSTER-040 | Trigger: "An automatic failover (`force: true`) of a failed primary, and the primary-absorbs-primary case." | The automatic path is no longer `force: true` by default (issue 20); the primary-absorbs-primary case is deleted outright, not merely made deterministic (issue 28). Both clauses of the trigger need rewriting. | Issue 20 (automatic-path force flag) + Issue 28 (absorb case removed) |
| FM-CLUSTER-041 | Describes graceful failover as demote-transfer-promote-bump with no pause/drain/offset-parity step. | Issue 26 adds a pause/drain/offset-parity barrier ahead of the propose step for the planned path; the row's Invariant needs the new sequence, and a sibling row states the automatic path's honest lossy claim. | Issue 26 |
| FM-CLUSTER-101 | Trigger list: "a forced failover (`Failover { force: true }`) — client-issued or decided by the failure detector". | The failure-detector-decided case is no longer `force: true` by default (issue 20); the client-issued case named here is exactly the primary-absorb path issue 28 deletes. The trigger list needs both corrected. | Issue 20 + Issue 28 |
| FM-CLUSTER-013 | "The leader's reconciliation writes `MarkNodeFailed{node}`" and bumps `config_epoch` unconditionally on every mark. | Not incorrect on its own, but issue 19's amendment (global epoch fence reversed to a per-object fence, precisely *because* this row's unconditional bump starves a global fence under correlated failure) should be cross-referenced from here so a future reader does not "fix" the starvation by touching this row's bump instead of the fence. | Issue 19 (amendment) |
| FM-CLUSTER-014 | "An epoch bump on recovery [is NOT observable]... bumping there would let a flapping peer churn the epoch once per round." | Same cross-reference need as 013: the row's own anti-churn reasoning is part of what issue 19's amendment leans on when explaining why a *global* epoch fence was the wrong CAS granularity. | Issue 19 (amendment) |
| FM-CLUSTER-084 | `admits_complete_at = drained && !barrier_expired && !lease_expired`. | Issue 17's ruling drops the `barrier_ms` wall-clock admission window on `Complete`; this predicate's `!barrier_expired` clause is exactly what the ruling removes. Row must state the new resolution rule instead. | Issue 17 (ruling + amendment) |
| FM-CLUSTER-085 | Lease clauses `a_second_prepare_waits_for_the_lease_but_not_forever`, `complete_is_refused_once_the_lease_expired`. | Issue 17's ruling never says the lease itself is deleted, only the `barrier_ms` admission window — this row must state precisely which of its two bounds (`barrier_ms` vs. `lease_ms`) survives the ruling, since the review found the ruling text ambiguous on this exact point. | Issue 17 (ruling + amendment) |
| FM-CLUSTER-095 | Fence predicate description references the barrier/lease shape as of ruling time. | Downstream of 084/085's edit — the execute-seam fence text should stay consistent with whatever `admits_complete_at` becomes. | Issue 17 (ruling + amendment) |
| FM-CLUSTER-097 | `feed_hold_until`/`hold_deadline` derivation described as answering `None` once `clock::now()` passes the barrier's own deadline; "a feed that can wedge... the hold carries the barrier's own deadline". | Issue 17's amendment (byte-cap hold) changes what bounds the replica-feed hold once the wall-clock admission window is gone; this row's deadline-derivation text must match the amended mechanism, not the pre-review one. | Issue 17 (amendment) |
| FM-CLUSTER-001 | "The command always answers `ClusterResponse::Ok`." | Issue 14's ruling introduces a refusal path for `AddNode` when the incoming `primary_id` dangles or names a replica; this row's blanket "always `Ok`" claim needs the carve-out. | Issue 14 |
| FM-CLUSTER-005 | Catalog: `INV-REF-3B` is "the catalog's one `Tier::DocumentedException`" because `AddNode` and `SetRole` still admit a replica as a parent (citing issue 14). | Once issue 14 lands, `INV-REF-3B`'s status as a documented exception may close (repair-on-admit or refusal both retire it); the row's catalog note must be updated to match whichever remedy issue 14's ruling settles on, not left describing the pre-fix exception. | Issue 14 |
| FM-CLUSTER-003 | Validate-all-then-apply for `AssignSlots`, silent on open migrations. | Issue 16's ruling makes `AssignSlots` reject a slot with an open migration (source-only exception); this row's validation-pass description needs the new check named. | Issue 16 |
| FM-CLUSTER-004 | Validate-all-then-apply for `RemoveSlots`, silent on open migrations. | Issue 16 applies the same open-migration check to `RemoveSlots`; same edit as 003. | Issue 16 |

## What to build

A coordinated text-edit pass across the fifteen rows above, performed as each owning issue's fix
lands (not before — the wording must match the *final shipped* mechanism, not the draft ruling
text, which is why this issue is blocked on all of them). No row's `Forced by` test list should
need to change unless the owning issue's own acceptance criteria already call for that; this issue
is about the prose staying honest, not about re-deriving coverage.

Suggested order: land 14, 15, 16, 17, 18, 19, 20, 25 (independently, as already scheduled), then
sweep 001/005 (14), 003/004 (16), 036 (15+20), 084/085/095/097 (17), 013/014 cross-refs (19), and
finally 039/040/041/101 (20+26+28, in that dependency order since 26 and 28 are new issues that
also touch 040/041/101).

## Acceptance criteria

- [ ] All fifteen rows in the table above edited to match the shipped behavior of their owning
      issue(s); no row states a rationale, trigger, or invariant that the shipped code
      contradicts; `just lint-spec` green
- [ ] This is a text-only sweep with no behavior change of its own, so there is no new forcing
      test for this issue; each edited row's existing `Forced by` tests continue to force it, and
      `just lint-spec`'s row↔test agreement check is what catches a row whose wording drifted from
      its tests during the sweep
- [ ] `just mutants-diff` — not applicable to this issue directly (no code changes); confirm each
      owning issue's own `just mutants-diff` obligation already covers the crate(s) whose FM-row
      comments/doc-strings this sweep touches, and note here if any row edit turns out to require
      a code-level doc-comment change that a locked crate's mutants gate should re-triage

## Blocked by

Issues 14, 15, 16, 17, 18, 19, 20, 25 (all must land first — the sweep documents their final
shipped shape, not their ruling-time draft), and issues 26, 28 (new issues from this review that
also touch FM-CLUSTER-040/041/101).

## Ruling (2026-08-13)

**Tracking issue, no independent ruling.** The row-by-row ownership table above is this issue's
content; each row's edit is owed by the ruling already recorded on its owning issue (see the
`## Ruling` and, where present, `## Amendment` sections on issues 14, 15, 16, 17, 18, 19, 20, and
the rulings on issues 26 and 28 filed alongside this one). This issue exists so the edit set is
tracked as one sweep rather than dropped piecemeal by whichever agent happens to touch a
neighboring row.
