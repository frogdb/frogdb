# 14 — Spec cross-reference, per-area catalog lint, mutation re-baseline

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W6; exit criterion 4.

## What to build

Three pieces, all cheap once the catalog is in-crate.

**1. Cross-reference.** Each catalog invariant cites the FM rows it generalizes, and the rows in
`.scratch/hardening/specs/replication-failure-modes.md` whose claim is now universally checked name
the `INV-*` id. Close the spec's two live GAPs while in the file: **GAP-5** becomes INV-SESSION-2
plus `XREPL-3` (nothing today asserts `WAIT` can never exceed `connected_slaves` across a
reconnect, `tracker.rs:146-153`), and **GAP-6** (`-UNBLOCKED` on demotion racing `CLIENT UNBLOCK`,
`connection/blocking.rs:285-305`) is a point test no layer in this campaign reaches — so write the
point test.

**2. The dangling-INV lint, which is only half generic today.** `scripts/failure-modes.py` already
globs *every* `*-failure-modes.md` for `INV-*` citations (`INV_REF_RE` at `:107`,
`check_invariant_vocabulary` at `:306`), but the vocabulary it checks against is one hard-coded
file: `INVARIANTS_RS = REPO / "frogdb-server/crates/cluster/src/invariants.rs"` (`:52`), loaded
once by `load_catalog_ids` (`:274`). As it stands an `INV-REPLID-2` cited in
`replication-failure-modes.md` is flagged as dangling. Turn `INVARIANTS_RS` into a **per-area
catalog map** so each spec is checked against its own area's catalog — a replication row citing
`INV-HANDOFF-1` must be an error, not a pass, and a genuinely dangling `INV-*` in either spec must
still error. Deliberately generalized rather than special-cased, because the persistence port is
next and this is the piece that makes the third and fourth ports free. It is a dict, not a
framework (§7).

**3. Mutation re-baseline on current code**, for both crates: `just mutants frogdb-replication` +
`just mutants-gate frogdb-replication 0.85`, and the same for `frogdb-replication-runtime`. Record
not just the score but the **in-crate share of forcing tests** — the number ADR 0004 says the score
is really measuring, given that the crate's headline figure was reached by moving tests down rather
than by removing the cross-crate dependency. Re-check the ADR 0004-era survivors specifically
(`apply_single`, `apply_transaction`, `apply_group`, `export_live_dataset`, `install`,
`read_snapshot`) and record whether issue 05's R6 and the runtime crate's first `[dev-dependencies]`
moved them.

## Acceptance criteria

- [ ] Every catalog invariant cites the FM rows it generalizes, and those rows name the `INV-*` id
- [ ] Spec GAP-5 closed (INV-SESSION-2 + `XREPL-3`) and GAP-6 closed by a written point test
- [ ] `scripts/failure-modes.py` checks each spec against its own area's catalog via a per-area
      map; both a cross-area citation and a dangling `INV-*` error, proven by a fixture or test
- [ ] `just mutants-gate frogdb-replication 0.85` and `just mutants-gate
      frogdb-replication-runtime 0.85` pass on current code
- [ ] In-crate forcing-test share recorded alongside each score, with the ADR 0004 survivor list
      re-checked and its status recorded

## Blocked by

- Issue 02 (`.scratch/replication-correctness/issues/`) — there is no catalog to cite or to feed
  the per-area map until it lands.
- Issues 05 and 10 (`.scratch/replication-correctness/issues/`) move the mutation numbers; run the
  re-baseline after them if they are in flight, or expect to repeat it.
