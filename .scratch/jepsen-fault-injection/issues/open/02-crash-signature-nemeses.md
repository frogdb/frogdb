# 02 — Crash-signature nemeses

Status: needs-triage

## Parent

[PRD](../../PRD.md) W1.

## What to build

Nemeses over the existing crash-shaped DEBUG commands so the crash suite distinguishes
crash *signatures* instead of one clean SIGKILL: `PANIC`, `SEGFAULT`, `OOM`,
`CRASH-AND-RECOVER`, and `RELOAD` (save+reload as a crash-recovery equivalence op). Each
becomes a schedulable fault via issue 01's plumbing; the existing crash workloads gain a
signature parameter rather than new workload copies.

## Acceptance criteria

- [ ] Each signature schedulable via the generic plumbing; recovery path (restart or
      built-in) verified per signature
- [ ] Crash workload(s) parameterized on signature; run matrix documented in the workload
      help text
- [ ] Run evidence: one clean `:valid? true` store id per signature on the crash workload
- [ ] Any signature that surfaces a real defect files it in the owning campaign's tracker,
      not fixed inline

## Blocked by

- Issue 01 (`.scratch/jepsen-fault-injection/issues/`) — plumbing.
