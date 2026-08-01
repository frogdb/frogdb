# Non-deterministic writes (`VADD REDUCE`, `TOPK.ADD`) are propagated verbatim, so replicas diverge

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/07 F4 · proposals/07 F13 · MASTER.md §3 (consistency violations), §2 T7
Score: severity 5 · likelihood 3 · effort 3 · priority 18
Area: frogdb-types / frogdb-commands — vectorset, topk; replication propagation

## Context

Two write commands consume `rand` while executing and are then replicated **verbatim** —
the replica re-runs the same command text and draws different random numbers, so primary
and replica end up with different data under the same key. `VADD … REDUCE n` seeds its
projection matrix from `rand::random()`; `TOPK.ADD` uses `rand::random::<f64>()` in its
decay loop. Neither raises an error anywhere: `VSIM` simply returns different neighbours
depending on which node is read, and after a failover every previously-inserted REDUCE'd
vector is effectively garbage.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

**07/F4 — `VADD` REDUCE projection matrix** (severity 5 · likelihood 3 · effort 3 · priority 18)

`types/src/vectorset.rs:163` `vs.uid = rand::random();`; `:563`
`self.projection_matrix = generate_projection_matrix(self.uid, original_dim, self.dim)`; `:688`
`StdRng::seed_from_u64(uid)`. `persistence/src/serialization/search.rs:42,127` persists `uid` and
the matrix (so RDB/DUMP is fine), but `VADD` has `repl_override: None` → verbatim propagation.
`vectorset/vadd.rs` is 68.6% covered; `vsim.rs` 60.5%.

Why nothing catches it: full-sync is safe (the codec does persist `uid`), so only *live*
propagation diverges — which is the common case for keys created after the replica attached.

**07/F13 — `TOPK.ADD` decay** (severity 4 · likelihood 3 · effort 3 · priority 15)

`types/src/topk.rs:120` calls `rand::random::<f64>()` inside the decay loop
(`:118-124`, up to 100000 iterations per item per row). `integration_replication.rs:6995` lists
`TOPK.ADD` only in a smoke matrix; no test asserts primary/replica convergence.

## What to fix

1. Derive `VectorSetValue::uid` deterministically from the key name (or from a
   replicated/propagated seed) instead of `rand::random()`, so the projection matrix is a
   pure function of replicated inputs.
2. Make `TOPK.ADD` deterministic under replication — either seed the decay RNG from
   replicated state or set `repl_override` so the *effect*, not the command, propagates.
3. Sweep for other `rand::` uses on write paths with `repl_override: None` and fold them
   into the same table (theme T7).

## Options

Reproduced verbatim from proposals/07 F4:

- *Boundary 1* — assert `uid` derivation is a pure function of the key name. Cheap, fast, but
  only tests the fix, not the property; a future refactor could reintroduce randomness elsewhere
  (e.g. `VectorSetValue::new`).
- *Boundary 4/5, primary+replica* — asserts the real property (`VEMB` equality after sync).
  ~2s of harness time, catches any future source of nondeterminism.
- **Recommendation**: both. Boundary 5 as the property test, boundary 1 as the fast regression
  pin. The boundary-5 test should be generalised into a "verbatim-propagated write is
  deterministic" table covering `VADD` and `TOPK.ADD` (F13).

## Acceptance criteria

- [ ] A primary+replica test (`server/tests/integration_replication.rs`) does
      `VADD k REDUCE 4 VALUES 8 … elem`, waits for sync, and asserts `VEMB k elem` and
      `VSIM k VALUES 8 …` are identical on both nodes. Fails today, passes after the fix.
- [ ] The same table drives N `TOPK.ADD`s and asserts `TOPK.LIST WITHCOUNT` is byte-identical
      on primary and replica.
- [ ] A boundary-1 pin asserts `uid` is a pure function of the key name.
- [ ] The determinism table is written so a new verbatim-propagated command can be added as
      one row.

## Test boundary

Level 5 (per proposal 07/F4/F13), with a level-1 companion pin. Not level 4 alone because the
property being asserted *is* primary/replica convergence of live-propagated writes; a
single-node test cannot observe the second draw of the RNG.

## Depends on

issue 25 (theme T7 — determinism of propagated writes), `.scratch/testing-improvements-round2/issues/`
