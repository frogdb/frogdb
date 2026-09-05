# 22: lock specs/memory.md

Status: done
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

## Resolution

Landed 2026-09-05 on `mem-arch-integration` (picks `e75f15666`..`45fd23b97`, 7
commits). Memory is the fifth locked core area.

What shipped: `specs/memory.md` is `Status: LOCKED (2026-09-05)` over eight rows,
FM-MEMORY-001..008. FM-MEMORY-008 (a shard's arena figure is that shard's own bytes, says so
before it is taken, and is never read on a shard core — 11 forcing tests already live in
`frogdb-telemetry`) is new; FM-MEMORY-004 was amended to name the figure the `maxmemory`
verdict binds to — the **accounted contents**, `Store::memory_used()`, exact at the boundary
(at the limit is inside it), with the ~2× real-footprint caveat — and to state that
`ShardEviction::per_shard_limit` is the only computation of the per-shard limit, both writers
(`new`, `update_config`) going through it, forced with `num_shards > 1`. The old "Planned
failure-mode groups" section is gone: what it promised and the code does not do is now
**Open items**, each naming its filed issue. The `snapshot handle` vocabulary and the "Rows
this spec will inherit" section were deleted rather than weakened. `CLAUDE.md` lists memory
beside txn, persistence, replication and cluster (`frogdb-memory` + `frogdb-table`, gate
0.85, boundary `adr/0006`); ADR-0006 carries an addendum recording the lock, narrowing §2
(the budget chokepoint exists, its guarantee does not yet) and §3 (the sampled upper bound
governs the arena figure, not the verdict), and stating that fragmentation is measured rather
than fought.

Mutation gates, measured at lock: `frogdb-memory` **97.1 %** (from 70.4 % before the audit's
forcing tests), `frogdb-table` **93.4 %** (from 90.9 %; its 21 timeouts are all mutations of
the `bits &= bits - 1` probe idiom, a legitimate non-terminating class). Gate **0.85** for
both — the number `frogdb-table` clears with margin and neither clears by accident, the same
figure as persistence and replication, inside the issue's 0.80–0.90 band; `just mutants-gate`
PASS for both. Every surviving mutant is documented at the code with why it is unobservable
(disjoint-bit `|`→`^` equivalents, `cfg` bodies, `Drop` leaks no in-process assertion sees,
a dead `else`, a split-sooner load-factor difference, `Charge::release` by value, the 2Q
step counter and `reconcile`'s Am threshold); the `scan`/`next_cursor` residue is issue 28.

R15 promise ledger: 13–17 ruled out (generic rows reach converted types through
`Entry::memory_size()`, which FM-MEMORY-004 now names); 18 discharged in FM-MEMORY-001/002
(both `Forced by` lists audited against the tests that exist); 19 discharged in FM-MEMORY-003
— its residue (fan-out width cap in the locked txn/vll crates, the Linux-rig benchmark) is
outside this spec's seam and stays ready-for-human on issue 19; 20 split — the
sampler-off-shard-cores half is FM-MEMORY-008, the "no active defrag; re-encode is manual
and O(value)" half is ADR-0006 prose (not mechanically forceable); 21 and 23 cross-referenced
(FM-TXN-054, FM-PERSISTENCE-062, FM-REPLICATION-069), no duplicate rows; 11's SCAN
exactly-once half ruled out as a keyspace-iteration contract, its sizing half absorbed into
FM-MEMORY-008 plus the accounted-contents vocabulary; 12 verified (every test named in
004..007 exists and is tagged; the suspected row-007 drift was a report undercount); the
inherit section (FM-REPLICATION-068 receive-path budget, TR-CLUSTER-016 hold cap) not
migrated — neither behaviour exists — and made explicit as Open items.

Review: round 0 (opus) found no Critical, two Important — FM-MEMORY-002's `Forced by` housed
tests forcing nothing the row states (the txn-default constants test now forces FM-TXN-054;
the `Budget::available` tests are untagged, `available` having no production caller; broker
`adopt` got its own Trigger clause backed by `RocksStore::open` → `builder.rs`), and
FM-MEMORY-004's "update_config is the only writer" was false and its division unforced — plus
five Minors (two change-detector tests, one mis-described survivor, incomplete survivor
coverage on FM-MEMORY-007's own arithmetic, draft 24's premise, draft 28's phase header). Fix
round 1 addressed all seven (`cold_candidates`' guard and `nominate_from`'s one lap are now
forced, not documented); re-review: all findings addressed, no new Critical/Important. Reviewer verified the equivalence claims
statically, the tag census in both directions (001=9, 002=13, 003=17, 004=8, 005=4, 006=5,
007=11, 008=11 at head), the allowlist figures (20 budgeted / 83 unconverted in 32 files)
against `scripts/budget-growth.py`, and that no non-test hunk changes behaviour.

Deviations: `frogdb-server/crates/telemetry/src/shard_arenas.rs`, `core/src/shard/types.rs`
and `specs/txn.md` were touched outside the brief's file list — tags and one `Forced by`
name only, which `just lint-spec` requires.

Follow-ups filed as drafts (`Status: needs-triage`): 24 (budget-growth allowlist burndown —
83 unconverted sites; `replication_backlog`, `wal_channel`, `fullsync_staging` declared but
charged by nobody), 25 (snapshot handle / bounded full sync), 26 (replication receive-path
`Budget`, FM-REPLICATION-068 amendment), 27 (replica-feed hold byte cap, after the cluster
campaign's TR-CLUSTER-016 rewrite), 28 (`frogdb-table` mutation survivor burndown — the
`Table::split` directory assertion that kills two survivors at once). Out-of-scope reviewer
notes, not filed: FM-MEMORY-002's heading is narrower than its Trigger now that `adopt` is in
it; `Budget::available` is public API with no production caller.

Gates at head: `cargo clippy --all-targets -D warnings` (direct; `just lint` is red from the
pre-existing `lint-turmoil` dead-code at `acceptor.rs:90`), `just lint-spec` (324 failure
modes, 1848 references ↔ 1848 tags), `just lint-gates`, `just scratch-check`, `just
fmt-check`; `just test` frogdb-memory 33/33, frogdb-table 97/97, frogdb-core 1062/1062,
frogdb-server 2151 passed / 5 skipped.
