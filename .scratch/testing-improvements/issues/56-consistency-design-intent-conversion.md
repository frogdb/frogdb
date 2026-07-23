# Convert consistency.md [Design intent] rows into executable Jepsen/turmoil workloads

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Jepsen / Distributed testing infra

## Context

`consistency.md` documents several consistency guarantees as [Design intent] — asserted in docs but
never converted to executable tests: read-your-writes (line 27), monotonic reads (line 38),
acked-write-loss bound (line 86), replica staleness (line 89), within-connection ordering (line 138),
and pub/sub ordering (line 144).

The G-report assessment (CONFIRMED L1/C2) breaks these down by feasibility:
- **RYW** and **within-connection ordering** are cheaply testable today via a single-connection
  turmoil or Jepsen workload — no new harness capability needed.
- **Acked-write-loss bound** is the failover-durability gap already tracked as task 03
  (`jepsen-failover-durability-workload`, E#10) — do not duplicate; cross-reference only.
- **Pub/sub ordering** is partially addressable via the issue-10 capture seam, but lacks a
  cross-reconnect Jepsen workload to exercise the harder case.
- **Staleness** and **monotonic reads** (replica-side) need a dedicated replica-read client
  capability in the Jepsen/turmoil harness that doesn't exist yet — this is a harness prerequisite,
  not just a missing test.

## What to build

1. Single-connection RYW workload (turmoil and/or Jepsen): write then read on the same connection,
   assert the read observes the write.
2. Within-connection ordering workload: a sequence of operations issued on one connection, assert
   the server-observed order matches issue order.
3. Pub/sub-ordering Jepsen workload using the existing issue-10 capture seam, including the
   reconnect case where feasible.
4. New harness capability: a replica-read client for Jepsen/turmoil — a prerequisite for testing the
   monotonic-reads and staleness rows; scope explicitly as a deliverable of this task (or split into
   a tracked sub-task if large).
5. Update `consistency.md`, relabeling each row from [Design intent] to [Tested] as its
   corresponding test lands — no row should claim [Tested] without a merged test backing it.

## Acceptance criteria

- [ ] RYW single-connection workload implemented (turmoil or Jepsen) and passing.
- [ ] Within-connection ordering workload implemented and passing.
- [ ] Pub/sub-ordering Jepsen workload added, built on the issue-10 capture seam.
- [ ] Replica-read client capability added to the Jepsen/turmoil harness (prerequisite for
      monotonic-reads/staleness rows; may land as a follow-up if scoped separately).
- [ ] `consistency.md` rows relabeled [Design intent] → [Tested] individually as each corresponding
      test merges — never batch-relabeled ahead of actual test coverage.
- [ ] Acked-write-loss-bound row explicitly cross-referenced to task 03
      (`jepsen-failover-durability-workload`) rather than re-implemented here.

## Blocked by

Task 03 (`jepsen-failover-durability-workload`) covers the acked-write-loss-bound row — do not
duplicate that work in this task.

## References

- .scratch/testing-improvements/audit/G-jepsen-harness.md (`consistency-design-intent-rows-testable`)
- .scratch/testing-improvements/audit/verdicts-G.md (CONFIRMED L1/C2)
- consistency.md lines 27, 38, 86, 89, 138, 144

## Resolution

Built three single-node Jepsen workloads and relabeled the three consistency.md rows they
actually verify. All smoke-run on the CLEAN single-node topology (`--no-build`, reusing
`frogdb:latest`); `lein check` clean.

### Workloads added

- **`ryw`** (`ryw.clj`) — read-your-writes on a single connection. Each worker holds a dedicated
  single-connection pool (`conn-spec-single`) and a private UUID key; every op pipelines `SET v` +
  `GET` on that one connection and the checker asserts the read equals the value just written.
  Private key ⇒ no cross-worker interference, so a mismatch is a genuine RYW violation. The
  monotonically increasing value also makes each read strictly newer, so reads never regress on the
  connection.
  Smoke: **PASS** — `:ryw {:valid? true :ryw-ops 200 :violation-count 0}`.
- **`wc-order`** (`wc_order.clj`) — within-connection command ordering. Each op pipelines N ordered
  `RPUSH`es on one connection in a single round-trip, then `LRANGE`s the list back. Two orderings
  gate: server-observed execution order (`LRANGE` == `[0..N-1]`) and response order (pipelined
  return values == `[1..N]`). Any execution- or response-reordering fails.
  Smoke: **PASS** — `:ordered-ops 202 :violation-count 0`.
- **`pubsub-order`** (`pubsub_order.clj`) — pub/sub delivery ordering per channel. A dedicated
  Carmine pubsub listener records arrival order while a publisher publishes `0..N-1` in order on a
  private channel; the checker asserts arrivals are a strictly increasing (in-publish-order)
  subsequence of what was published — delivery may drop (at-most-once) but never reorder. Includes
  a **reconnect flavour** (drop + re-subscribe mid-stream) for the harder cross-reconnect case.
  `:valid?` also requires ≥1 delivered message run-wide, so a dead subscriber can't pass vacuously.
  Smoke: **PASS** — `:pubsub-ops 228 :total-received 4560 :violation-count 0`.

All three registered in `core.clj` and wired into `run.py` `TESTS` (single/crash/all suites for
`ryw`/`wc-order`; single/all for `pubsub-order`). Each workload supplies its own `:final-generator`
so the harness's default `{:f :read}` recovery phase (which these clients don't implement) never
runs — without it the generic `:stats` checker flags an all-`:info` `:read` bucket and reds the run.

### Doc rows relabeled (consistency.md) — [Design intent] → [Tested]

- **Read-Your-Writes** — relabeled; prose + table now credit the `ryw` workload and scope the claim
  to the same-connection scenario. The *reconnect* and *failover-to-replica* rows stay honest
  (reconnect = [Design intent]; failover = not guaranteed).
- **Ordering → Within a Single Connection** — relabeled; credits `wc-order`.
- **Pub/Sub Message Ordering** — relabeled; credits `pubsub-order`, notes at-most-once + no
  cross-channel ordering.

### Deliberately NOT relabeled / deferred (honest)

- **Monotonic Reads (single node)** and **Reads from a Replica / staleness** — left [Design intent].
  A replica-read client capability (issue deliverable #4) is a genuine new harness prerequisite and
  is **deferred as a tracked follow-up**, not built here (would need replica connections that read
  the async-lagging copy and a staleness-bound checker). The `ryw` workload's monotonic values give
  only incidental single-connection evidence, not a dedicated monotonic/staleness test, so no
  relabel.
- **Acked-write-loss bound (failover)** — left to task 03
  (`03-jepsen-failover-durability-workload`); the `replication-failover` workload already covers
  kill-primary → promote-replica → verify-acked-write-fate. Not duplicated here.

### Deviation from issue text

The issue suggested building the pub/sub workload "on the issue-10 capture seam". No such capture
seam exists in the Jepsen harness (the audit's internal issue-10 maps to the CI-absence issue in
this tracker, not a code seam), so `pubsub-order` is self-contained on a Carmine pubsub listener
instead. Same coverage, incl. the reconnect case.

### Acceptance criteria

- [x] RYW single-connection workload implemented and passing.
- [x] Within-connection ordering workload implemented and passing.
- [x] Pub/sub-ordering Jepsen workload added (Carmine pubsub listener; see deviation note).
- [ ] Replica-read client capability — **deferred as a follow-up** (harness prerequisite, out of
      scope for this task).
- [x] consistency.md rows relabeled individually, each backed by a passing workload (three rows;
      monotonic/staleness intentionally left honest).
- [x] Acked-write-loss-bound row cross-referenced to task 03 (not re-implemented).
