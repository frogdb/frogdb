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

| # | Workload | Verdict | Notes |
|---|----------|---------|-------|
| 1 | `cross-slot-partition` | PASS (1:14) | `valid? true` all sub-checks; `cluster-invariants` 2 sweeps, 0 violations, 0 connectivity errors. |
| 2 | `cross-slot-kill` | PASS (1:16) | `valid? true` all sub-checks; `cluster-invariants` 2 sweeps, 0 violations, 0 connectivity errors. |
| 3 | `raft-chaos` | PASS (2:23) | `valid? true` (key-routing checker, durability+value-correctness); worker crashes on `CLUSTERDOWN` during quorum-loss windows correctly recorded `:info`/indeterminate, not counted as failures; `cluster-invariants` 2 sweeps, 0 violations, 0 connectivity errors. |
| 4 | `list-append-raft` | PASS (2:49) | Elle strict-serializable checker, `valid? true`, 0 anomalies; `cluster-invariants` 2 sweeps, 0 violations. First run crashed 100% of ops with a harness bug (see below), fixed and rerun clean: 869,626 `:ok` txns, 0 `:unexpected`. |
| 5 | `migration-recovery` | PASS (0:59) | `valid? true` (leader recovered after Raft-leader kill mid-migration, no data loss, migration resolvable); 346 `:ok` ops, 0 failed ops; `cluster-invariants` 2 sweeps, 0 violations. |
| 6 | `concurrent-migration` | PASS (0:39) | `valid? true` (all slots owned, no data loss across 4 parallel migrations); 460 `:ok` ops, 31 `:fail` (expected migration-window rejections), 0 `:unexpected`; `cluster-invariants` 2 sweeps, 0 violations. |
| 7 | `membership-routing` | **FAIL** (0:27-0:28, 3/3 attempts) | `Analysis invalid!` — `:start-migration` throws `ERR node 15092003070405494904 not found` immediately after a `REDIRECT -> retrying on leader n4`. Reproduced 3 times (original + 1 retry + 1 dedicated no-teardown repro), including across a full container teardown/recreate, ruling out a one-off scheduling fluke or stale docker volumes. Root-caused to a real server-side defect: a freshly-started, not-yet-joined node unconditionally self-bootstraps as a solo one-node Raft "leader" and stays externally reachable/redirectable for a ~5s window while the real cluster's `add_learner` promotion attempts fail against it. Filed as [issue 25](25-newly-started-node-briefly-usurps-leadership-via-solo-bootstrap.md) (`needs-triage`; originally drafted as 24, renumbered — 24 was taken by origin/main's issue-23 residue landed after this worktree branched) with full timestamped log evidence from both sides (joining node and real leader) plus a live `CLUSTER MYID` cross-check confirming the "not found" node was actually n3, an original bootstrap member — not the newly-joined node. `cluster-invariants` checker itself stayed green (2 sweeps, 0 violations) since it doesn't currently probe this admission-window race — noted in issue 25 as a candidate checker gap. |
| 8 | `rolling-restart` | PASS (0:57) | `valid? true`; `cluster-invariants` 2 sweeps, 0 violations, 0 connectivity errors. |
| 9 | `raft-membership` | **FAIL** (2:17) | Identical fingerprint to workload 7: `:add-node "n4"` → `:start-migration` → `REDIRECT -> retrying on leader n4` → `ERR node 15092003070405494904 not found` (n3's id again). Second independent witness of [issue 25](25-newly-started-node-briefly-usurps-leadership-via-solo-bootstrap.md), not re-diagnosed separately; `cluster-invariants` green (2 sweeps, 0 violations). |
| 10 | `cluster-membership` | PASS (2:17) | `valid? true`; `cluster-invariants` 2 sweeps, 0 violations. Uses `cluster-formation` workload under the `raft-cluster-membership` nemesis (nodes joining/leaving), not `membership-routing`'s add-node+migrate sequence, so it doesn't hit issue 25's window (no `CLUSTER SETSLOT` sent to a just-joined node). |
| 11 | `cluster-replication` | PASS (0:33) | `valid? true`; `cluster-invariants` 2 sweeps, 0 violations, 2 connectivity errors (expected — self-driven, nemesis `none`; the workload itself `docker stop`s the slot's primary mid-run to exercise `CLUSTER FAILOVER TAKEOVER`, producing one indeterminate `Connection refused` op, which the checker correctly treats as indeterminate rather than a failure). No add-node/join involved, doesn't hit issue 25. |
| 12 | `cluster-lag` | PASS (1:11) | `valid? true`, 599 ops, 0 fail, 0 info; lag-p99 6ms, lag-max 23ms, burst-success-rate 1.0, offsets monotone/advancing on both ends; `cluster-invariants` 2 sweeps, 0 violations, 0 connectivity errors. Self-driven (nemesis `none`), no add-node/join, doesn't hit issue 25. |
| 13 | `clock-skew` (raft-extended, `elle-rw-register` workload) | PASS (1:41) | `valid? true`, 810,502 txn ops, 0 fail, 0 info; Elle cycle checker clean; `cluster-invariants` 2 sweeps, 0 violations, 0 connectivity errors; `DEBUG CLUSTER CHECK` clean across 3 nodes. Fault-injection nemesis (clock skew), no add-node/join path, doesn't hit issue 25. |
| 14 | `disk-failure` (raft-extended, `elle-rw-register` workload) | PASS (1:30) | `valid? true`, 852,496 txn ops, 0 fail, 0 info; Elle cycle checker clean; `cluster-invariants` 2 sweeps, 0 violations, 0 connectivity errors; `DEBUG CLUSTER CHECK` clean across 3 nodes. Fault-injection nemesis (disk failure), no add-node/join path, doesn't hit issue 25. |
| 15 | `slow-network` (raft-extended, `elle-rw-register` workload) | PASS (1:25) | `valid? true`, 218,235 txn ops, 192 fail, 964 info (expected timeouts/retries under a network-slowdown nemesis — Elle checker still `valid? true`); `cluster-invariants` 2 sweeps, 0 violations, 0 connectivity errors; `DEBUG CLUSTER CHECK` clean across 3 nodes. No add-node/join path, doesn't hit issue 25. |

**Harness bug found and fixed (not a product defect):** the first `list-append-raft` attempt
crashed every op with `IllegalArgumentException: Don't know how to create ISeq from:
clojure.lang.ExceptionInfo`. Root cause: `exec-multi-ops` in
`testing/jepsen/frogdb/src/jepsen/frogdb/list_append.clj` only checked `(nil?
exec-results)` before `mapv`-ing the EXEC reply into txn shape. Carmine doesn't throw on a
pipelined per-command error — it returns the exception inline as that element's value, so
when EXEC itself errored (CLUSTERDOWN during a quorum-loss window under the `raft-cluster`
nemesis — the first time this workload has run against fault injection, since it has never
had stored results before), `exec-results` was a `clojure.lang.ExceptionInfo`, not nil, and
fell through to `mapv`, masking the real CLUSTERDOWN/MOVED underneath. Fixed by checking
`(instance? Throwable exec-results)` and re-throwing so the existing
`is-redirect-error?`/clusterdown handling classifies it correctly. Fix + rerun confirmed
clean (see row 4 above).

### Remaining

- Issue-07's checker (third acceptance-criteria bullet) is active in these runs (issue 07 is
  done/merged) — `DEBUG CLUSTER CHECK` invariant assertions ran wherever the workload's
  checker calls it.
