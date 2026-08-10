# 10 — Restructure `replica_session.rs` into an explicit phase state machine

Status: needs-triage

## Parent

[PRD](../../PRD.md) §8 D2, ruling tier (iii) — "full restructure authorized", with the execution
discipline the ruling attaches to a locked-crate refactor of this size.

## What to build

`frogdb-server/crates/replication/src/replica_session.rs` (4574 LoC) becomes an explicit phase
state machine: a pure `step(view, event) -> (phase, effects)` and the async loop
(`replica_session::run`, `replica_session.rs:642`) as an interpreter over the returned effects.
This is the interpreter pattern the whole area benefits from, and D2 authorizes it explicitly
after tiers (i) and (ii) land as stepping stones.

The structural payoff beyond model-checkability: `ReplicaSession::set_phase`
(`replica_session.rs:591`) stops being an ad-hoc writer, because the phase transition becomes the
return value of `step`. INV-SESSION-1 — "a session's `Phase` only moves forward in the declared
order; `Disconnecting` is terminal" — is asserted in prose at `replica_session.rs:46` today and
checked by nothing; after this it is structural, with the catalog entry as the backstop.

**Execution discipline, ruled in D2 and not optional:**

- Tiers (i) and (ii) land first (issue 07). This issue is the third step, not a merge of all three.
- Every step is **spec-first** against `.scratch/hardening/specs/replication-failure-modes.md`:
  rows may move their file:line citations but not their meaning, and `just lint-failure-modes`
  stays green at each stage.
- Land in reviewable stages. A 4574-LoC locked-crate file does not land as one diff — this issue
  is expected to split itself into a numbered sub-issue chain, each stage green on the full suite.
- The **full** mutation gate re-runs at the end — `just mutants frogdb-replication` plus
  `just mutants-gate frogdb-replication 0.85` — not just `mutants-diff`, because the restructure
  moves most forcing-test targets. Any surviving mutant no test can kill is documented at the code
  with why it is unobservable, never blanket-skipped.

Issues 08 and 09 inform the shape without blocking it: whatever `Action`/`Outcome` vocabulary the
two models needed is the event/effect vocabulary that should generalize here.

## Acceptance criteria

- [ ] Explicit `Phase` state machine with a pure `step(view, event) -> (phase, effects)`; the
      async loop only interprets effects and performs I/O
- [ ] Landed as a reviewable chain of stages, each stage green on `just test frogdb-replication`
      and `just test frogdb-server`
- [ ] Every FM-REPLICATION row whose citation moves is re-pointed with its meaning unchanged;
      `just lint-failure-modes` green at each stage
- [ ] INV-SESSION-1 holds by construction, and its forcing test still goes red when the catalog
      check is deleted
- [ ] Full gate at the end: `just mutants frogdb-replication` + `just mutants-gate
      frogdb-replication 0.85` passing, with the in-crate share of forcing tests recorded

## Blocked by

- Issue 07 (`.scratch/replication-correctness/issues/`) — D2 requires tiers (i) and (ii) as
  stepping stones before (iii).
- Issues 08 and 09 (`.scratch/replication-correctness/issues/`) inform the event/effect vocabulary
  but do not block; if they run late, do not wait.
