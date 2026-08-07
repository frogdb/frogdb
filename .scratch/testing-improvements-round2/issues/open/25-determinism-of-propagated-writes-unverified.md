# Nothing verifies that verbatim-propagated writes are deterministic — two commands seed from `rand`

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §2 T7
Score: aggregate of 2 findings
Area: frogdb-types / vectorset + topk · frogdb-replication

## Context

A write that propagates verbatim must produce the same state on the replica as on the primary.
Nothing in the suite asserts that. Two commands are already known to violate it by seeding
themselves from `rand`, and both propagate verbatim, so the replica's state is unrelated to the
primary's with no error anywhere.

This is **one piece of work, not two command fixes**: a shared primary/replica determinism table
that drives a command on the primary, waits for sync, and asserts the observable state is
byte-identical on both nodes. `VADD` and `TOPK.ADD` are its first two rows; area 07 raised it
explicitly as a class and noted it "probably is not the only source". Adding a future
nondeterministic write becomes a one-row change that fails.

## Evidence

- **`VADD` seeds the REDUCE projection matrix from `rand::random()`.** *(07/F4)*
  `types/src/vectorset.rs:163` — `vs.uid = rand::random();`; `:563` —
  `self.projection_matrix = generate_projection_matrix(self.uid, original_dim, self.dim)`; `:688` —
  `StdRng::seed_from_u64(uid)`. The replica projects the *same* input vector through a *different*
  matrix, so its stored vectors are unrelated to the primary's, `VSIM` returns different neighbours,
  and after a failover every previously-inserted REDUCE'd vector is garbage.
  `persistence/src/serialization/search.rs:42,127` persists `uid` and the matrix, so RDB/DUMP and
  full-sync are safe — **only live propagation diverges**, which is the common case for keys created
  after the replica attached. `VADD` has `repl_override: None` → verbatim propagation.
  `vectorset/vadd.rs` is 68.6% covered; `vsim.rs` 60.5%.
- **`TOPK.ADD` decay uses `rand`.** *(07/F13)* `types/src/topk.rs:120` calls
  `rand::random::<f64>()` inside the decay loop (`:118-124`, up to 100000 iterations per item per
  row). So `TOPK.LIST`/`TOPK.COUNT` answer differently depending on which node you read, and a
  failover changes the answers. `integration_replication.rs:6995` lists `TOPK.ADD` only in a smoke
  matrix; **no test asserts primary/replica convergence** for it.

## What to fix

1. Build one table-driven primary/replica determinism test owned by the replication area: each row
   is (setup commands, the propagated write, the observation command). The test drives the write on
   the primary, waits for sync, and asserts the observation is byte-identical on both nodes.
2. Seed row 1 with `VADD k REDUCE 4 VALUES 8 … elem`, observed via `VEMB k elem` and
   `VSIM k VALUES 8 …`.
3. Seed row 2 with N `TOPK.ADD`s, observed via `TOPK.LIST WITHCOUNT`.
4. Fix both sources: derive `VectorSetValue::uid` deterministically from the key name (or set a
   `repl_override` that propagates the effective value), and make the TOPK decay draw from a
   deterministic, replicated seed.
5. Add the cheap regression pin beside each fix — a level-1 assertion that `uid` is a pure function
   of the key name — so a future refactor that reintroduces randomness elsewhere (e.g. in
   `VectorSetValue::new`) is caught without paying for a two-node run.

## Acceptance criteria

- [ ] A single table-driven test file hosts both rows; adding a third command is one table entry.
- [ ] Primary + replica, after `VADD k REDUCE 4 VALUES 8 … elem` and sync: `VEMB k elem` and
      `VSIM k VALUES 8 …` are identical on both nodes. Fails today.
- [ ] Primary + replica, after N `TOPK.ADD`s and sync: `TOPK.LIST WITHCOUNT` is byte-identical on
      both nodes. Fails today.
- [ ] A level-1 test asserts `uid` is derived deterministically from the key name (added *with* the
      fix, as the fast regression pin).
- [ ] Neither `types/src/vectorset.rs` nor `types/src/topk.rs` calls `rand::random` on a
      verbatim-propagated write path.

## Test boundary

**Level 5** for the table — this is genuinely a replication-divergence property, and no lower level
can observe two nodes disagreeing; `TestServer::start_primary`/`start_replica` already exist, so
the cost is roughly 2 s of harness time per row. **Level 1** for the companion `uid`-derivation pin,
which tests the fix rather than the property and is there to make regressions cheap to catch. The
audit recommends both, not one: the level-1 pin alone would not notice a new source of
nondeterminism introduced elsewhere.

## Depends on

Nothing. `TestServer::start_primary`/`start_replica` and the existing replication integration
harness are sufficient; no infrastructure item from issues 01–18,
`.scratch/testing-improvements-round2/issues/`, is required.

## Re-triage 2026-08-06

**Verdict: still-valid**

Both sources of nondeterminism are unchanged and no determinism table exists. Per-claim:

- **`VADD` REDUCE matrix — still valid.** `crates/types/src/vectorset.rs:163` is still
  `vs.uid = rand::random();`, `:563` still derives the projection matrix from that uid, `:688`
  still `StdRng::seed_from_u64(uid)`. No `repl_override` was added.
- **`TOPK.ADD` decay — still valid.** `crates/types/src/topk.rs:118` is still
  `if rand::random::<f64>() < prob`.
- `rg 'rand::random' crates/types/src/{vectorset,topk}.rs` returns exactly those two lines, so
  acceptance criterion 5 still fails.

**On the campaign's determinism work — it discharges neither half of this issue.** The clock-seam
sweep (`2fb1051c`), the OS-clock lint (`0fe2dd0a`, `just lint-clock-seam` →
`scripts/clock-seam.py`) and the XADD wall-clock virtualization (`8b62120f`) all address a
*different* nondeterminism source: **wall-clock reads**. `2fb1051c` touches 49 files and neither
`types/src/vectorset.rs` nor `types/src/topk.rs` is among them, and `clock-seam.py` gates
`Instant::now`/`SystemTime::now`, not `rand`. There is no lint gating `rand` on a
verbatim-propagated write path. So: the clock work closes the wall-clock half of "propagated
writes must be deterministic"; this issue's PRNG half — which is the whole of its evidence — is
untouched. The generic table-driven primary/replica determinism harness (criterion 1) also does
not exist.

Relationship to **issue 56**: 56 was closed on 2026-08-06 as **superseded by this issue** and moved
to `issues/done/` (same two findings, strictly narrower criteria). This issue owns the work and
must stay open.
