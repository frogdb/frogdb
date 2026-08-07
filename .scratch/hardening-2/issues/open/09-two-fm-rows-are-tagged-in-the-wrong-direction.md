# Two FM rows cite tests that do not witness them, while the tests that do are tagged elsewhere

Status: ready-for-agent
Type: bug (spec bookkeeping)
Severity: likelihood 3/3 (both rows read as forced today), consequence 2/3 (the invariant is
actually covered in both cases — the citation is what is wrong, not the coverage) — score 6
Area: failure-mode specs (cluster, txn)

## Problem

`just lint-failure-modes` checks that every name in a `Forced by` cell resolves to a test that
carries the matching `// FM-<AREA>-NNN` tag. It cannot check that the test witnesses the row. Two
rows fail that unchecked half, in opposite directions.

**FM-CLUSTER-059** (`.scratch/hardening/specs/cluster-failure-modes.md:849`) — "the self-fence knob
is live". Its `Trigger` is:

> `CONFIG SET cluster-self-fence-on-quorum-loss` and `cluster-auto-failover` on a running node with
> quorum already lost.

All five cited tests live in `cluster-runtime/src/flags.rs:165-234` and manipulate
`ClusterRuntimeFlags` **directly** — none issues a `CONFIG SET`, so none exercises the path from
the config parameter to the live flag, which is precisely where a knob becomes startup-only. The
row's `NOT observable` cell names "a knob that only takes effect at startup" as the thing being
excluded, and no cited test could detect it.

The test that does exist and is not cited: `cluster_flag_sets_reach_the_live_flags`
(`server/src/runtime_config.rs:4845`), which drives `manager.set("cluster-self-fence-on-quorum-loss",
"no")` and asserts the live `flags` object observes it. Its own comment (`:4838-4843`) states the
distinction the row cares about: *"a test that only asserted GET would pass against a parameter that
stores into the manager and reaches nothing."*

**FM-TXN-040** (`.scratch/hardening/specs/txn-failure-modes.md:506`) — "a write transaction waits at
the pause barrier and re-validates after it". Its two cited tests (`exec_outcomes.rs:608`, `:636`)
witness the `Invariant` cell — the validate-call counts — but nothing witnesses the `Observable`:

> The `EXEC` reply is withheld until the pause lifts, then the batch commits.

That end-to-end behaviour *is* tested, by
`write_exec_parks_on_a_slot_barrier_and_commits_after_release`
(`server/tests/cluster_pause_barrier.rs:343`) — which is tagged **FM-CLUSTER-083**, a different row
in a different area's spec.

Neither row is under-covered. Both are mis-attributed, which means the spec's picture of *which
layer proves what* is wrong — and that picture is what the next person uses to decide whether a
change is safe.

## Fix

1. Add `cluster_flag_sets_reach_the_live_flags` to FM-CLUSTER-059's `Forced by` and tag it
   `// FM-CLUSTER-059` (it may carry more than one tag).
2. Cross-tag `write_exec_parks_on_a_slot_barrier_and_commits_after_release` with `// FM-TXN-040`
   and add it to that row's `Forced by`, keeping the FM-CLUSTER-083 tag.
3. While in these files: both rows are examples of a `Forced by` list that covers the Invariant cell
   but not the Observable cell. W3d should check that split explicitly — for each row, which cited
   test witnesses the Observable, and which witnesses the Invariant. Where the answer is "the same
   unit test for both", the Observable is probably unwitnessed.

## Verification

`just lint-failure-modes` passes after the edits (it will, both directions are additive), and the
W3d re-read confirms each row has at least one cited test per non-empty Observable cell.

## Comments

Found by the campaign-2 witness audit, 2026-08-07. These two were found by hand while sampling 54
of 258 rows; the same defect at the same rate implies roughly ten more rows across the corpus.
