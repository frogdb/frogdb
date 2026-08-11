# 09 — `DEBUG WAL-FAIL-NEXT` write/fsync error injection

Status: needs-triage

## Parent

[PRD](../../PRD.md) W2.

## What to build

`DEBUG WAL-FAIL-NEXT <n>`: make the next `n` WAL write or fsync operations return an IO
error without killing the process. The jepsen crash suite today cannot distinguish "handles
fsync failure" from "handles crash" — fsyncgate taught the industry those are different
bugs. Nemesis on issue 01's plumbing; a schedule mixing injected IO errors with client load
asserts the documented durability contract holds (error surfaced to the client or write
retried — per the persistence spec, not silently dropped).

This is the sharpest knife in the initiative (D1 applies with force) and belongs
doctrinally to the persistence campaign — sequencing per D4.

## Acceptance criteria

- [ ] Injection lands at the WAL write/fsync seam only; locked persistence crates —
      mutation gates, spec-first if any behavior row is touched (D2)
- [ ] Availability per D1 ruling (this issue is the strongest argument for a
      config/feature gate)
- [ ] Nemesis schedule with injected IO errors: analysis proves no acked write lost and
      no silent drop; store id cited
- [ ] Real defects found file into the persistence campaign tracker

## Blocked by

- Issue 01 (`.scratch/jepsen-fault-injection/issues/`) — plumbing.
- PRD rulings D1/D2/D4 (persistence-campaign sequencing).
