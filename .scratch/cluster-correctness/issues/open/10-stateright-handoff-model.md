# 10 — Stateright model 1: two-phase slot handoff

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W3; dev-dependency + nightly budget ruled in §8 D1.

## What to build

Add `stateright` as a dev-dependency of `frogdb-cluster`. Model the two-phase handoff:
coordinator + source + target + Raft-as-serializer, 3 nodes / 2 slots / bounded retries.
The transition function is production `apply_command` — the model layer contributes only
what Raft/network contribute in production: ordering, loss, duplication, leader changes.

Safety: no interleaving of Prepare/Confirm/Abort/Complete/leader-change admits writes on
two nodes for one slot; seqs never reused. Liveness: every prepared handoff reaches
Released or Completed.

Bounded-depth smoke config (<10 s) in the default suite; real exploration budget in the
nightly, recorded (states/minutes) in the model file header. If the state space defeats
the small-scope hypothesis, record the tried budget and the drop decision in this issue
and the PRD — that outcome is a legitimate close.

## Acceptance criteria

- [ ] Model embeds production `apply_command` (no hand-translated transition fn)
- [ ] Both safety properties + liveness checked; state-space size recorded
- [ ] Smoke config in default suite; full budget nightly
- [ ] Any counterexample checked in as a regression scenario replayed against the real
      state machine
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 02 (`.scratch/cluster-correctness/issues/`) — model states are judged by the
  catalog.
