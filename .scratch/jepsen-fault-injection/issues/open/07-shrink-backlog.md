# 07 — `DEBUG REPLICATION SHRINK-BACKLOG` + resync-boundary nemesis

Status: needs-triage

## Parent

[PRD](../../PRD.md) W2.

## What to build

`DEBUG REPLICATION SHRINK-BACKLOG <n>`: clamp the replication backlog to `n` bytes live,
evicting history so a disconnected replica's resume offset falls off the retained window.
Nemesis pairing it with brief link drops so PSYNC arm selection (partial vs full, the
`select_psync_arm` seam from replication-correctness issue 07) is exercised at the boundary
under churn rather than only in unit tests.

## Acceptance criteria

- [ ] Command clamps live and restores on clear; backlog floor interaction with
      `plan_primary_stint`'s `backlog_floor` documented
- [ ] Locked-crate discipline (mutation gates; spec impact per D2)
- [ ] Boundary nemesis schedule: history shows both partial and full resync grants in one
      run; clean `:valid? true` store id cited
- [ ] The stateright promotion model's `select_psync_arm` coverage (replication-correctness
      issue 08) stays green — this must not fork arm-selection logic

## Blocked by

- Issue 01 (`.scratch/jepsen-fault-injection/issues/`) — plumbing.
- PRD rulings D1/D2.

## Comment (2026-08-13)

PRD rulings D1-D4 settled 2026-08-13 (see PRD). Remaining blocker: predecessor issues only.
