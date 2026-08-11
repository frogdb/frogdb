# 14 — Spec cross-reference, per-area catalog lint, mutation re-baseline

Status: done

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

- [x] Every catalog invariant cites the FM rows it generalizes, and those rows name the `INV-*` id
- [x] Spec GAP-5 closed (INV-SESSION-2 + an end-to-end bound test; `XREPL-3` is a third witness
      that arrives with issue 12) and GAP-6 closed by a written point test
- [x] `scripts/failure-modes.py` checks each spec against its own area's catalog via a per-area
      map; both a cross-area citation and a dangling `INV-*` error, proven by a fixture or test

The mutation re-baseline (piece 3) moved to **issue 20**: its two criteria are blocked on issues 05
and 10, which are still open and both move the numbers, so a baseline recorded now would have to be
thrown away. Nothing landed here changes a mutable line in either replication crate.

## Blocked by

- Issue 02 (`.scratch/replication-correctness/issues/`) — there is no catalog to cite or to feed
  the per-area map until it lands.
- Issues 05 and 10 (`.scratch/replication-correctness/issues/`) move the mutation numbers; run the
  re-baseline after them if they are in flight, or expect to repeat it.

## Resolution (2026-08-11)

Three pieces landed; the fourth (mutation re-baseline) is issue 20.

**Per-area catalog map** (`5d0c1ee3`). `INVARIANTS_RS` became `INVARIANT_CATALOGS`, a dict from
area to catalog path (`CLUSTER` → `frogdb-server/crates/cluster/src/invariants.rs`, `REPLICATION` →
`frogdb-server/crates/replication/src/invariants.rs`); `load_catalog_ids` became `load_catalogs`,
returning a frozen `Catalog(area, path, ids)` per area. Each spec is checked against its **own**
area's catalog, and the three ways a citation can fail are three distinguishable errors: dangling
in its own catalog, defined but owned by another area (the message names the owner and its file),
or cited from an area with no catalog registered at all. A persistence or txn port is now one line
in the dict — deliberately a dict, not a framework (PRD §7).

The lint's teeth are covered by `scripts/tests/test_failure_modes.py` (8 tests, stdlib-only,
`uv run --script`), wired as `just test-failure-modes-lint` and made a dependency of
`just lint-failure-modes` — a green tree only ever exercises the passing direction, so the failing
directions need their own fixtures. They pin: own-area citation passes and is counted; a dangling
id errors; a cross-area citation errors *naming the owner*; an area with no catalog errors
distinctly; prose like `INV-*` is not a citation; ids are bounded to the `CATALOG` static (a
doc-comment mention is not an entry); a vacuous catalog errors; and the registered paths are real
and non-empty. The cross-area rule was also proven against the real spec, not only a fixture: an
explanatory paragraph naming a cluster id failed `just lint-failure-modes` until reworded
(`63fee819`).

**Cross-reference** (`690aadfd`). 41 `Catalog` citations over 22 rows in
`.scratch/hardening/specs/replication-failure-modes.md` (FM-REPLICATION-001, -008, -009, -012,
-013, -014, -015, -016, -019, -020, -021, -022, -023, -037, -039, -041, -043, -047, -049, -059,
-060, -062), each naming the catalog entries that make the row's claim universal rather than
point-wise, plus a `Catalog` line in "How to read a row" and a `### The Catalog field` section
explaining the both-ways rule. In the other direction, 15 check-fn doc comments in
`invariants.rs` name the rows they generalize. Locked-spec discipline held: citations and the new
field only, no invariant/trigger/observable text reworded.

`INV-GATE-1` is the one entry no replication row cites: the rows it generalizes are cluster rows
(the feed gate is FM-CLUSTER-097), and a cross-area `Catalog` citation there would be a lint error
by the rule this issue added. Recorded in both the spec section and the entry's doc comment, the
same way cluster records its uncited entry. `INV-ROLE-1` is cited as a documented non-guarantee.

**GAP-6** (`6a1da43a`). The WAIT race lived inline in `handle_wait_command`, so the interleaving the
gap names — a `CLIENT UNBLOCK ERROR` landing in the same poll as the demotion — was reachable from a
socket only by luck. Extracted `resolve_wait_race`, a free function over the wait future, an
`UnblockSignal` and a count closure (the treatment `reconcile_ack` already had), so a test can
present both arms ready in one poll.
`wait_released_by_a_demotion_reports_the_role_change_even_if_client_unblock_races` asserts the reply
is the role-change error and that the acked count is never even computed; three siblings pin the
other corners. Teeth proven by swapping the `select!` arm order: exactly the two tie tests fail,
with `left: "UNBLOCKED client unblocked via CLIENT UNBLOCK"` against the role-change string. The
coordinator's `MockUnblock` moved to a shared `test_support` module so both races read the CLIENT
UNBLOCK edge through one seam. The `MULTI`/deny-blocking half of the gap needed nothing: WAIT there
returns the live count immediately and never parks (FM-REPLICATION-037).

**GAP-5** (`035c73d1`). Closed from both directions rather than waiting on `XREPL-3` (issue 12, still
open): `INV-SESSION-2` is the universal half — at most one live session per announced identity,
checked at every seam that takes a view — and `test_wait_never_exceeds_connected_slaves` is the
end-to-end half. It churns the link with `REPLICAOF NO ONE` / `REPLICAOF` back at the same primary,
so the replica re-announces the *same* identity from the same port (a process restart would bind a
new port and miss the overlap window entirely), and asserts across eight rounds that `WAIT 5 200`
never exceeds `connected_slaves` sampled either side of the call, then that the set reconverges — a
bound held by losing the replica is not a bound. Stable over three consecutive runs, ~1.9s each.
`XREPL-3` will add a sweep-level witness when issue 12 lands; the bound does not depend on it.

**Evidence.** `just lint-failure-modes`: `OK: 279 failure modes (BLOCKING, CLUSTER, PERSISTENCE,
REPLICATION, TXN, VLL), 1434 test references, 1434 tags, 88 invariant citations over 27 catalog
entries (CLUSTER 41/11, REPLICATION 47/16)`, with `8 passed` from the lint's own unit tests.
`just test frogdb-server 'connection::blocking'`: 15/15. `just test frogdb-server
test_wait_never_exceeds_connected_slaves`: 1/1, three times.
