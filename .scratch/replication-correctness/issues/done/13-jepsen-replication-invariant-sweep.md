# 13 — Jepsen invariant sweep over `DEBUG REPLICATION CHECK`

Status: done

## Parent

[PRD](../../PRD.md) §3 W5 (the Jepsen half).

## What to build

`testing/jepsen/frogdb/src/jepsen/frogdb/invariant.clj` already implements the whole quiesce+final
pattern as a nemesis wrapper: `wrap-nemesis` intercepts one `:f` and delegates everything else, so
it composes with any nemesis package with zero per-workload changes, and `core.clj:268/276/295/307`
wires it for `cluster-mode?`. What it does not do is vary the command it calls or the result key it
reports under.

Parameterize both, then extend the wiring to `Topology.REPLICATION`. That is one change covering
all nine replication workloads (`testing/jepsen/run.py:256-380`): `replication`, `lag`, `zombie`,
`split-brain`, `register-partition`, `replication-chaos`, `replication-failover`,
`replication-failover-chain` and `replication-clock-skew`. The cluster wiring keeps its current
behavior — this is a generalization, not a rewrite.

Carry over the counting bug validation found on the cluster side: **every check op appears twice in
the history** (dispatch + completion), so the checker must filter on `(map? (:value op))`, not on
`:f` alone, or every sweep is counted once per node. Keep the calls at quiesce and final only, so
nemesis timing stays undistorted.

Validation mirrors cluster issue 07's evidence format, and the two runs are the acceptance
evidence, not a formality:

- **seeded violation** — one fault-injection run against a build carrying a throwaway always-firing
  invariant, which must come back `:valid? false` naming the invariant id in the analysis output;
  the throwaway patch is then reverted and the seeded id must appear nowhere in the source tree;
- **clean run** — one run on the clean build coming back `:valid? true` with sweeps run > 0 and
  violating sweeps 0.

Both cited by store run timestamp in this issue when it closes.

## Acceptance criteria

- [x] `invariant.clj` parameterized on the DEBUG command and the result key; the cluster wiring's
      behavior is unchanged
- [x] All nine `Topology.REPLICATION` workloads sweep `DEBUG REPLICATION CHECK` at quiesce and
      final, with no per-workload changes beyond the wiring
- [x] The `(map? (:value op))` filter is carried over, with evidence that sweep counts are right
      (not doubled)
- [x] Seeded-violation run fails naming the invariant id; the throwaway patch is reverted and the
      seeded id is absent from the tree
- [x] Clean run green (`:valid? true`, sweeps run > 0, violating sweeps 0); both store run ids
      recorded here

## Blocked by

- Issue 03 (`.scratch/replication-correctness/issues/`) — consumes the command, and needs it
  always-compiled because Jepsen runs release binaries.

## Resolution (2026-08-11)

Generalized, not rewritten: `invariant.clj` now takes a **surface** map and every function threads
it, so adding a third catalog is one `def`.

### Shape

```clojure
(def cluster-surface
  {:op-f :cluster-check, :command ["DEBUG" "CLUSTER" "CHECK"], :label "DEBUG CLUSTER CHECK",
   :result-key :cluster-invariants, :conn-fn cluster-db/conn-for-raft-node-single, :known {}})

(def replication-surface
  {:op-f :replication-check, :command ["DEBUG" "REPLICATION" "CHECK"],
   :label "DEBUG REPLICATION CHECK",
   :result-key :replication-invariants, :conn-fn client/conn-for-node-single, :known {}})
```

`check-node`/`check-all-nodes!`/`invariant-sweep!`/`wrap-nemesis`/`check-op`/`checker` all take the
surface as their first argument; `core.clj` picks one with a three-way `cond`
(`cluster-mode?` → cluster, `multi-node?` → replication, else no sweep) and uses it at all four
wiring sites (nemesis wrap, checker map, quiesce generator, final generator). The cluster path is
byte-for-byte the same ops, key and output as before — `:known` is empty on both surfaces, and the
`:known-*` analysis keys are emitted only when non-empty.

The `:known` allowlist mechanism exists (split out of the violation set, reported but non-gating,
keyed by issue number) but **no entry was needed**: the clean run did not trip `INV-FENCE-1`
(issue 19), so adding an allowlist entry speculatively would have masked a live catalog id.

Coverage is the `multi-node?` predicate, which is the replication-workload set plus the
`--replication` flag, so all nine `Topology.REPLICATION` workloads are covered — plus
`partition-recovery`, a tenth. No per-workload file changed. One caveat worth knowing: node
resolution is the unchanged `(or (:cluster-nodes test) (:nodes test))`, so a client-pinned test
(`register-partition` with `--node n1`) sweeps its pinned node only.

`testing/jepsen/frogdb/test/jepsen/frogdb/invariant_test.clj` is new: 10 tests / 38 assertions
pinning the double-count filter, dedup, allowlist non-masking, connectivity-error non-gating and
surface isolation.

### Run evidence

Both runs are `just jepsen split-brain --build --build-mode docker` (cargo-zigbuild is not
installed locally, so `--build-mode docker` — the `docker` profile inherits release).

- **clean** — store `20260811T005313.690-0400`
  (`testing/jepsen/frogdb/store/frogdb-split-brain-partition-docker-replication/`):
  `:replication-invariants {:valid? true, :sweeps-run 2, :violating-sweeps 0, :violation-ids [],
  :details (), :connectivity-errors 0}`, run verdict `Everything looks good!`, with
  "DEBUG REPLICATION CHECK clean across 3 nodes" logged at quiesce and at final.
- **not doubled** — that run's `history.edn` holds **four** `:replication-check` entries (two
  `:value nil` dispatches, two map completions) while the checker reports `:sweeps-run 2`. Without
  the `(map? (:value op))` filter the count would have been 4.
- **seeded** — store `20260811T010338.577-0400`, built with a throwaway always-firing
  `INV-SEEDED-TEST-1` appended to the replication `CATALOG`:
  `:replication-invariants {:valid? false, :sweeps-run 2, :violating-sweeps 2,
  :violation-ids ["INV-SEEDED-TEST-1"], :connectivity-errors 0}`, run verdict `Analysis invalid!`,
  `split-brain: FAIL`. The throwaway was then reverted; `grep -rn INV-SEEDED` over the tree hits
  only prose in cluster issue 07 and this file.

No new defects found. Docs: `website/src/content/docs/compatibility/testing-methodology.mdx` and
the `jepsen-testing` skill both describe the sweep and its analysis keys.
