# Jepsen: kill-primary -> promote -> acked-write-fate workload is missing

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 3/3 (score 9)
Area: jepsen

## Context

There is no automated kill-primary -> promote-replica -> verify-acked-write-fate workflow anywhere
in the test suite. The acked-write-loss bound on failover is documented only as a
`consistency.md:75-90` [Design intent] row and has never been measured.

In the Jepsen harness, `client.clj:497-500` defines `promote-to-primary!` (issuing `REPLICAOF NO
ONE`) but it is never called from any workload. The `replication` suite (`run.py:145-172`) only
runs nemeses `none`/`partition`/`all-replication`; `all-replication`
(`nemesis.clj:1058-1096`) kills and restarts the primary but nothing ever promotes a replica in its
place — node `n1` simply comes back as primary. So the harness's harshest replication-topology
nemesis never actually exercises failover, only crash/restart. Extended nemeses (clock skew, disk
faults, slow network, memory pressure) are wired for the `raft-extended` suite only — the
`REPLICATION` topology gets none of them.

This is a merged gap: area E flagged it as `kill-primary-promotion-ackd-write-fate-untested`
(E#10, dedupe target) and area G independently flagged the harness-side manifestation as
`failover-durability-untested`. Both verdicts confirmed at L3/C3 and were folded into one task,
with the E (replication) framing winning since it states the invariant being tested (acked-write
loss bound) rather than just the harness mechanics.

## What to build

- New Jepsen workload: `replication-failover` — track acknowledged writes, kill the primary via
  the existing REPLICATION-suite nemesis, call `promote-to-primary!` on a replica, then assert the
  loss set (acked writes that are actually missing post-promotion) is within the documented bound
  from `consistency.md:75-90` (ideally: empty, for `WAIT`-acknowledged writes at full replica
  count).
- Add clock-skew and slow-network nemeses to the `REPLICATION` topology (currently
  `raft-extended`-only), so failover is exercised under realistic fault conditions, not just clean
  kill/restart.
- Wire the new workload into `run.py`'s `replication` suite alongside `none`/`partition`/
  `all-replication`.

## Acceptance criteria

- [ ] `replication-failover` Jepsen workload exists: tracked writes, primary kill, explicit
      `promote-to-primary!` call, and a checker that asserts acked-write loss is within the
      documented bound
- [ ] Workload registered in `run.py` under the `REPLICATION` topology/suite
- [ ] Clock-skew and slow-network nemeses available (and exercised in at least one suite
      combination) on the `REPLICATION` topology, not just `raft-extended`
- [ ] Workload run at least once on a testbox/CI environment with results attached to this issue

## Blocked by

None - can start immediately. Complements the deterministic-simulation equivalent covered by
`11-turmoil-cluster-raft-topology.md` (turmoil is faster-feedback per-PR coverage; this Jepsen
workload is the higher-fidelity/longer-running counterpart) and should be included once ready in
the CI wiring done under `10-jepsen-nightly-ci.md`.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/client.clj:497-500` — `promote-to-primary!`, never called
- `testing/jepsen/run.py:145-172` — replication suite nemesis list (none/partition/all-replication)
- `testing/jepsen/frogdb/src/jepsen/frogdb/nemesis.clj:1058-1096` — all-replication kills+restarts,
  no promotion
- `consistency.md:75-90` — [Design intent] acked-write-loss bound, unmeasured
- `server/tests/integration_replication.rs:1515-1560` — `test_secondary_replication_id_failover`
  (role-only check, not a durability workload; see also issue 34)
- Source: `.scratch/testing-improvements/audit/E-replication.md` gap #10, `.scratch/testing-improvements/audit/verdicts-E.md` #10
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md` `failover-durability-untested`,
  `.scratch/testing-improvements/audit/verdicts-G.md` (same, "DEDUP: fold into E#10; replication framing wins")

## Resolution

Resolved 2026-07-23. Built the `replication-failover` Jepsen workload plus the two extended
replication-topology nemesis variants requested. Smoke-run on a clean 3-node replication topology
came back `:valid? true` with **zero acked-write loss** across a real kill-primary -> promote-replica
failover — the durability bound the issue set out to measure holds for `WAIT`-acknowledged writes.

### Design

New workload `testing/jepsen/frogdb/src/jepsen/frogdb/replication_failover.clj`, a **self-driven**
workload (runs under the `none` nemesis, drives the fault sequence deterministically from the
generator, in the style of `rolling_restart.clj`) rather than a nemesis-driven one. This gives
strict kill -> promote -> verify ordering so the invariant check is deterministic.

Topology: 3-node REPLICATION (primary `n1` + replicas `n2`/`n3`). A namespace-level
`(defonce current-primary (atom "n1"))` coordinates the live primary pointer across worker threads,
reset in `setup!`.

Operation set and phases:
- **Phase 1 (seed):** interleaved `:write-durable` (`SET` then `WAIT 2` — full replica count, so an
  ack means the write is on *both* replicas and survives promotion of either) and `:write-async`
  (`SET` only, no wait — the informational carve-out for the documented lag-bounded loss window).
- **Phase 2 (failover):** `:kill-primary` (`db/docker-stop` the current primary container) then
  `:promote`, which calls `do-failover!` — this wires the previously-dead
  `client.clj:497-500 promote-to-primary!` helper (`REPLICAOF NO ONE`), selecting the freshest
  replica by highest `slave_repl_offset`, and best-effort repoints the surviving replica.
- **Phase 3 (post-failover):** `:write-post` (liveness — a plain `SET` proving the promoted node is
  writable), `:read`, `:read-all`.

Checker asserts `:valid?` = killed? AND promoted? AND at least one post-failover write succeeded AND
`durable-loss` empty (every `WAIT 2`-acked write readable on the new primary). Async loss is reported
(`:async-loss-count`) but **informational only** — it is the documented `consistency.md` [Design intent]
lag-bounded window, not a validity failure.

Registration:
- `core.clj` — workload added to the `workloads` map and to the `replication-workload?` set (so it
  auto-selects the REPLICATION topology; `run.py` does not need `--replication`).
- `run.py` — `replication-failover` test in the `replication`/`all` suites; plus
  `replication-clock-skew` and `replication-slow-network` in a new **`replication-extended`** suite
  (mirrors `raft-extended`), delivering the requested clock-skew + slow-network nemeses on the
  REPLICATION topology, kept out of the default suites so main stays green.

### Run evidence (clean replication topology, Docker, short limit)

`just jepsen-down` -> `just jepsen-up replication` -> smoke run:

```
:valid? true
:durable-loss-count 0
:durable-acked-count 40
:new-primary "n2"
:post-failover-writes-ok 9
:async-acked-count / :async-loss-count 0
192 ops, all :ok
```

The `replication-clock-skew` variant also validated green with correct container targeting.
`lein check` clean. Headline: **no acked-write loss observed across failover** — the durability
bound is met for `WAIT`-acknowledged writes at full replica count.

### Notable finding (NOT a durability bug)

FrogDB's **runtime** `REPLICAOF <host> <port>` does not live-attach a replica to a newly promoted
primary — it stages a checkpoint and logs `Checkpoint staged for loading - server restart required
to apply`. So after promotion the new primary shows `connected_slaves:0` until the surviving replica
is restarted; a post-failover `WAIT 2` would therefore block (only 1 node in the topology can ack).
This is why post-failover writes use liveness (`WAIT`-free) `SET`s, and why pre-failover durable
writes use `WAIT 2` (both replicas present while the original primary is alive). Data was never lost
— all 40 `WAIT 2`-acked writes were readable on the promoted primary, and `foo=bar` was readable on
both `n2` and `n3`. The restart-required re-attach behavior is documented in the workload docstring,
the `do-failover!` comment, and the `:write-post` op comment. Worth a follow-up issue if runtime
hot-reattach is desired, but it does not affect the acked-write-durability question this issue asked.

### Files

- `testing/jepsen/frogdb/src/jepsen/frogdb/replication_failover.clj` (new)
- `testing/jepsen/frogdb/src/jepsen/frogdb/core.clj` (register workload + topology set)
- `testing/jepsen/run.py` (suite wiring + `replication-extended` variants)

### Acceptance criteria status

- [x] `replication-failover` workload exists: tracked writes, primary kill, explicit
      `promote-to-primary!`, checker asserting acked-write loss within bound (empty for `WAIT 2`)
- [x] Registered in `run.py` under REPLICATION topology/suite
- [x] Clock-skew and slow-network nemeses available + exercised on REPLICATION topology
      (`replication-extended` suite); clock-skew variant smoke-validated
- [x] Workload run; results above (local Docker smoke run — nightly-CI wiring tracked by issue 10)
