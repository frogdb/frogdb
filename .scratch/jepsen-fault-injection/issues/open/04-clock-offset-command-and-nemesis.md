# 04 — `DEBUG CLOCK-OFFSET` + clock-skew nemesis

Status: needs-triage

## Parent

[PRD](../../PRD.md) W2.

## What to build

A per-node injected clock offset applied at the seam-linted `clock::` chokepoint (the seam
lint guarantees every time read goes through it, so one injection point skews the whole
node), settable and clearable via `DEBUG CLOCK-OFFSET <ms>`. Then a clock-skew nemesis on
issue 01's plumbing replacing the current host-clock games in `replication-clock-skew`.
Research Redis/Valkey/DragonflyDB first: Redis has no such command (its jepsen-era skew
tests move the host clock); document the deviation as an improvement per repo guidelines.

Targets: feed-gate deadlines, `HANDOFF_BARRIER_MS`, lease/heartbeat logic under asymmetric
skew.

## Acceptance criteria

- [ ] Offset injected at the `clock::` chokepoint only; seam lint stays green proving no
      bypass
- [ ] `DEBUG CLOCK-OFFSET <ms>` set/clear round-trips; admin-gated; availability per D1
      ruling
- [ ] Locked-crate discipline for whichever crates carry the seam (mutation gates; spec
      impact per D2 ruling)
- [ ] Clock-skew nemesis wired; `replication-clock-skew` runs on injected skew with a clean
      `:valid? true` store id cited
- [ ] Forcing test: a deadline-bearing path (feed gate hold) observably shifts under
      injected offset

## Blocked by

- Issue 01 (`.scratch/jepsen-fault-injection/issues/`) — plumbing.
- PRD rulings D1/D2.

## Comment (2026-08-13)

PRD rulings D1-D4 settled 2026-08-13 (see PRD). Remaining blocker: predecessor issues only.
