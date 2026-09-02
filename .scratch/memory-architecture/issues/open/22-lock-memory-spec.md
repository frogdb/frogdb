# 22: lock specs/memory.md

Status: ready-for-agent
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R15
Area: specs/memory.md + frogdb-memory (+ table crate from phase 3) mutation gates
Phase: 6 — explicitly last. Do not start until every other memory-architecture issue is done.

## Why

R15's ruling has two halves. The seam-lint half shipped with [issue 05](../)
(`lint-budget-growth`, ratcheted). This issue is the other half: promote
[`specs/memory.md`](../../../../specs/memory.md) from `Status: DRAFT` to `Status: LOCKED`,
following the exact playbook that locked txn/persistence/replication/cluster — the spec
becomes the contract, every failure-mode row names its forcing tests, `just lint-spec`
enforces the agreement, and the owning crates get a mutation gate.

This is last on purpose: locking early would freeze seams that phases 4–6 are still moving.
Everything the spec rows describe must exist and be forced by tests before the header flips.

## What to build

### 1. FM rows

Collect the invariants the phase work established into `FM-MEMORY-NNN` rows, each with a
forcing test in the owning crate. The expected set (refine against reality at execution
time):

- **OOM verdicts**: a write refused by the broker's `maxmemory` verdict returns the OOM
  error and applies nothing; reads still served; verdict derives from the sampled upper
  bound (never claimed as live bytes). (Phase-3 table/eviction work, reserved
  [issues 10–12](../).)
- **Eviction invariants**: eviction only under verdict pressure; evicts only from the
  configured policy's candidate set; keyspace-event + metric per eviction; eviction
  terminates (no livelock when nothing is evictable → OOM verdict stands). (Reserved
  issue 12's rows.)
- **Snapshot copy-on-write**: a snapshot in progress bounds its COW overhead to the charged
  budget; snapshot memory is charged, visible in the breakdown, released on completion.
  (Phase-3 table design row.)
- **Txn caps**: cross-reference the rows [issue 21](../) wrote into
  `specs/txn.md`/`specs/persistence.md` (rows live where the behavior lives; memory.md's row
  states the budget-seam side: txn bytes are charged to `TxnBuffering`).
- **Budget seam**: a refused charge is observable (refusal counter + subsystem breakdown);
  no non-keyspace buffer grows without a charge (the lint enforces the static half; the row
  forces the dynamic half on one representative subsystem per disposition).
- **Output-buffer classes**: from [issue 18](../)
  — per-class hard/soft limit behavior.
- **Storage never aliases network buffers**: from
  [issue 19](../).
- Candidates flagged from [issue 20](../):
  sampler-off-shard-cores (include only if mechanically forceable; otherwise ADR prose).

Tag every forcing test `FM-MEMORY-NNN` per the lint-spec convention; every row names its
tests; `just lint-spec` green is the acceptance gate for agreement.

### 2. Lock the header + boundary ADR

Flip `specs/memory.md` to `Status: LOCKED`. Check whether the locked-area boundary needs an
ADR addendum ([adr/0006](../../../../adr/0006-memory-architecture-seams.md) already rules the
seams; `adr/0002`–`0004` set the precedent for boundary ADRs — a short addendum to 0006
naming the locked crates likely suffices; follow whatever the four existing areas did).
Update the CLAUDE.md locked-areas list (txn/persistence/replication/cluster + **memory**)
with the gate.

### 3. Mutation gate

Gate `frogdb-memory` and the phase-3 table crate (name TBD by reserved
[issues 10–12](../)). Run `just mutants <crate>` to find the achievable score; set the
gate at the measured-and-defensible level (existing areas: 0.80–0.90; broker/budget code is
small and pure — expect the high end). Forcing tests live in-crate (cargo-mutants runs only
the package's own tests). Surviving mutants no test can kill get documented at the code, per
house rule — no blanket skips. Add the `just mutants-gate` wiring and the CLAUDE.md row.

### 4. Sweep the drafts' spec debts

Issues 13–21 each carry a "Spec rows at R15" section; sweep them (plus the reserved 10–12
issues' equivalents) for rows promised but not yet written. The DRAFT spec should already
contain most of them if the phase issues kept their side of the bargain — this issue is the
audit that they did.

## Acceptance criteria

- [ ] `specs/memory.md` header `Status: LOCKED`; every FM-MEMORY row names live forcing
      tests; `just lint-spec` green.
- [ ] Every "Spec rows at R15" promise from issues 10–21 either rowed or explicitly ruled
      out in this issue's resolution.
- [ ] Mutation gates set for frogdb-memory (+ table crate) at measured levels;
      `just mutants-gate` passes; survivors documented at code.
- [ ] CLAUDE.md locked-areas section updated (five areas).
- [ ] ADR boundary note landed (0006 addendum or equivalent).
- [ ] `just lint`, `just scratch-check` clean.

## Test boundary

No new behavior — this issue writes rows for tests that exist, adds forcing tests only where
the audit finds a row without one, and gates. Any *behavior* gap found here becomes a new
issue, not scope creep in this one.

## Out of scope

New memory behavior of any kind, relitigating dispositions or limits, locking frogdb-types
(value representations are below the spec seam by design — `memory_size()` is the contract
surface).

Note for the spec text (from issue 13's review): `memory_size()` is a *contents hash*, not
an RSS estimate — block-backed values deliberately exclude `Vec`/`VecDeque` spare capacity
for run-stability, so true footprint can exceed the reported figure (worst case ~2× for a
freshly doubled buffer). The OOM-verdict rows should state which figure they bind to.

## Depends on

Everything: reserved [issues 10–12](../) (table, ownership, eviction) and issues
13–21. Last by construction.
