# 08 — Close the raft-topology workload gaps

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W5 (audit blind spot B3 / workload inventory).

## What to build

The audit found `split-brain` and `zombie` workloads exist only for the replication
topology (`jepsen/run.py:264/272`), and 11 raft workloads plus 4 `raft-extended` ones
have no stored results. Port the two workloads to the raft topology, run all 15
result-less workloads, store results under the standard results path.

## Acceptance criteria

- [ ] `split-brain` and `zombie` runnable against the raft topology from `run.py`
- [ ] All 15 previously result-less workloads have stored, passing results (failures
      filed as issues, not buried)
- [ ] Issue-07 checker active in these runs once it lands (do not block on it)

## Blocked by

None - can start immediately.

## Progress

This pass ported both workloads to the raft topology and smoke-tested each once. The
15-workload result-less sweep (second acceptance-criteria bullet) is **not** in this pass's
scope — deferred to a separate compute pass (see below).

### What shipped

- `testing/jepsen/frogdb/src/jepsen/frogdb/split_brain_raft.clj` (new) and
  `.../zombie_raft.clj` (new), registered in `core.clj`'s `workloads`/`cluster-workload?`
  maps and as `TestDefinition`s in `testing/jepsen/run.py` (`split-brain-raft`,
  `zombie-raft`, both `Topology.RAFT`, `nemesis="partition"`, suites `raft`/`all`).
  `just jepsen-list` shows both alongside the existing raft workloads.
- Both reuse the **existing** `:partition` nemesis unchanged — no new nemesis.clj code.
  On the default 3-node `--cluster` bootstrap it isolates n1 from n2/n3, a genuine 2-vs-1
  Raft quorum split (majority keeps quorum, minority self-fences via
  `self_fence_on_quorum_loss`).
- Architectural adaptation (raft-cluster has no primary/replica ROLE split — ADR-0001:
  Raft carries cluster metadata only, so a fresh cluster is all primaries with disjoint
  slots and zero replicas by default, and the true Raft leader is not exposed over RESP,
  only via an HTTP debug endpoint the jepsen harness doesn't call):
  - `split-brain-raft`: per-node keys (one key per node's owned slot range, via
    `key-routing/key-for-slot`); asserts the isolated minority (n1) self-fences on writes
    (`-CLUSTERDOWN`) while the majority (n2/n3) never does, and the cluster converges
    (all nodes readable/unfenced) after heal.
  - `zombie-raft`: asserts no "phantom commit" — a write the server told the client was
    CLUSTERDOWN-rejected must never later be observed as n1's live value — and that n1
    rejoins unfenced after heal. This is the meaningful analogue of the
    replication-topology `zombie` workload's property in a topology with no
    primary-to-replica handoff to compare against.
  - Deliberate deviation from the reference `split_brain.clj`/`zombie.clj`: plain
    `try`/`catch` instead of slingshot's `try+` for CLUSTERDOWN classification —
    `client.clj`'s own `with-error-handling` docs a case where `try+`'s
    `ExceptionInfo`-unwrapping can let a plain Carmine RESP-error escape both a
    `clojure.lang.ExceptionInfo` catch and the trailing `Exception` catch uncaught, and
    both new workloads' correctness properties hinge on reliably catching CLUSTERDOWN.

### Smoke-test evidence (both PASS)

Ran via `just jepsen <workload> --time-limit 30 --no-build` (see note below on `--no-build`).

- `split-brain-raft`: PASS (0:57). `valid? true`, `isolated-writes-fenced 18`,
  `isolated-writes-ok 28`, `majority-writes-ok 67`, `majority-fenced-events 0`,
  `unexpected-errors 0`, `recovered? true`. Log:
  `.scratch/cluster-correctness/smoke-logs/split-brain-raft.log`. Results:
  `testing/jepsen/frogdb/store/frogdb-split-brain-raft-partition-docker-cluster/20260808T222533.673-0400/results.edn`.
- `zombie-raft`: PASS (0:54). `valid? true`, `accepted-writes 68`, `fenced-writes 46`,
  `phantom-commits #{}`, `unexpected-errors 0`, `recovered? true`. Log:
  `.scratch/cluster-correctness/smoke-logs/zombie-raft.log`. Results:
  `testing/jepsen/frogdb/store/frogdb-zombie-raft-partition-docker-cluster/20260808T222647.614-0400/results.edn`.

No database bugs found; no harness bugs found once written (both compiled clean via
`lein check` and passed on the first run against the existing `frogdb:latest` image).

### Environment note (not a code issue)

The default `just jepsen <workload>` auto-rebuild path (`needs_rebuild()` in `run.py`,
`--build-mode cross` via `cargo-zigbuild`) currently fails in this worktree: the `usearch`
crate's C++ build script fails cross-compiling `simsimd/c/lib.c` for
`x86_64-unknown-linux-gnu` under `zigcxx` (`error: invalid argument '-std=c++17' not
allowed with 'C'`). This is a pre-existing cross-compilation toolchain issue unrelated to
this pass's changes (`usearch`/`simsimd` build-script/zigcxx interaction, not Clojure or
core server code). Worked around here with `--no-build` against the already-built
`frogdb:latest`/`frogdb-runner:local` images. Not filed as a cluster-correctness issue
since it's a build-toolchain concern, not a database-correctness one — flagging here so
the next pass (the 15-workload sweep) doesn't hit it cold.

### Sweep pass (2026-08-10)

Executing the deferred second acceptance-criteria bullet: run the previously result-less
raft + raft-extended workloads, record results, triage failures against known issues
14-20/23. This pass does not fix product defects.

**Scope reconciliation.** The original per-workload list behind "11 raft workloads with no
stored results" was not preserved anywhere in the repo (the workload-inventory audit that
produced the count was an ephemeral analysis, not a checked-in doc — `store/` itself is
gitignored, so there is no durable record of what had results at PRD-authoring time).
Reconstructing it:

- The raft suite has 22 workloads (`just jepsen-list`). `split-brain-raft`/`zombie-raft`
  already have fresh, documented PASS results above (this pass's own smoke test) — out of
  scope here.
- Leftover local `store/` artifacts in this worktree (dated Feb 2026, ~6 months stale,
  pre-dating this campaign) show partial history for 8 of the remaining 20:
  `cluster-formation`, `leader-election`, `slot-migration`, `cross-slot`, `key-routing`,
  `leader-election-partition`, `key-routing-kill`, `slot-migration-partition` — the
  original raft-topology bring-up workloads. Nightly CI (`jepsen-nightly.yml`,
  `CORE_SUITES` includes `raft`) already exercises all 22 raft-suite workloads on every
  green main, so these 8 are not truly unexercised even though the local artifacts predate
  this campaign.
- That leaves 12 (not 11 — the exact original count could not be reconstructed; erring
  toward completeness) raft workloads with no trustworthy local record: `cross-slot-partition`,
  `cross-slot-kill`, `raft-chaos`, `list-append-raft`, `migration-recovery`,
  `concurrent-migration`, `membership-routing`, `rolling-restart`, `raft-membership`,
  `cluster-membership`, `cluster-replication`, `cluster-lag`.

Ran these 12 plus all 4 `raft-extended` workloads (16 total) against a freshly built image
(`just docker-build-debug`, in-Docker build — sidesteps the host zigbuild/usearch breakage
noted above) at current HEAD.

**Results:**

<!-- SWEEP_RESULTS_TABLE -->

### Remaining

- Issue-07's checker (third acceptance-criteria bullet) is active in these runs (issue 07 is
  done/merged) — `DEBUG CLUSTER CHECK` invariant assertions ran wherever the workload's
  checker calls it.
