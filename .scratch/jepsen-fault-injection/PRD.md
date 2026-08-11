# PRD — Jepsen fault injection via DEBUG commands

Status: draft — decisions D1-D4 need rulings before implementation issues leave triage.

## Motivation

The jepsen nemesis surface today is SIGKILL, SIGSTOP, and iptables partitions. Every fault
is process- or network-level; none exercises a failure the database can *see coming* or a
degradation short of death. Meanwhile the server already carries a DEBUG surface built for
exactly this (admin-gated, always compiled since the cluster/replication-correctness
campaigns), and none of it is reachable from a jepsen schedule.

Two consequences observed during the replication-correctness campaign:

- Known defect shapes (issue 16's stranded applied gate, issue 19's unarmed fence) are
  reachable only by luck in jepsen runs — the sweep landed in `:valid? true` runs because
  nothing could *force* a write-free streaming window or a failed promotion at the right
  moment. Deterministic injection would turn those from "wait for the schedule to find it"
  into a nemesis op.
- The invariant sweeps (`DEBUG CLUSTER CHECK`, `DEBUG REPLICATION CHECK`) are node-local by
  design. Cross-node claims — replication-id agreement, replica offset ≤ primary head,
  exactly one primary per shard — have no checker at all.

## Current state (investigated 2026-08-11)

Existing DEBUG surface useful for fault injection but unused by jepsen: `SLEEP`,
`CRASH-AND-RECOVER`, `RELOAD`, `PANIC`, `SEGFAULT`, `OOM`, `PAUSE-SLOT`,
`EXPIRE-BACKDATE`, `SET-ACTIVE-EXPIRE`, and the introspection set (`MEMORY-CHECK`,
`EXPIRY-INDEX-CHECK`, `LOCKTABLE`, `WAITQUEUE`). Each implementation issue re-verifies its
command's exact name/arity at the dispatch table before wiring — this list is an
investigation snapshot, not a contract.

Missing commands, by value:

1. **`DEBUG CLOCK-OFFSET <ms>`** — clock reads already funnel through the seam-linted
   `clock::` chokepoint, so a single injection point exists. Exercises feed-gate deadlines,
   `HANDOFF_BARRIER_MS`, lease logic without touching the host clock.
2. **`DEBUG REPLICATION DELAY-APPLY <ms>`** — replica-side apply throttle; forces real lag
   into WAIT paths, the applied gate, and backlog pressure. Forces issue-16/19 shapes.
3. **`DEBUG CHANGE-REPL-ID`** — Redis parity; forces fullsync storms, probes the issue-18
   class (wire replication-id validation).
4. **`DEBUG REPLICATION SHRINK-BACKLOG <n>`** — forces the partial→full resync boundary
   live (PSYNC arm selection under churn).
5. **`DEBUG CLUSTERLINK KILL`** — Redis precedent; app-layer per-peer link drop, asymmetric,
   cheaper than iptables.
6. **`DEBUG WAL-FAIL-NEXT <n>`** — fsync/write error injection; the jepsen crash suite
   currently cannot distinguish IO-error handling from crash handling. Persistence-campaign
   tie-in.
7. **`DEBUG REPLICATION VIEW`** (JSON dump) — raw `ReplicationView` per node lets jepsen
   assert cross-node invariants the catalogs cannot: replid agreement, replica offset ≤
   primary head, one primary per shard, per-node offset monotonicity over time.

## Decisions needing rulings

- **D1 — availability of injection commands.** The check/introspection commands are always
  compiled (ruled in prior campaigns). Injection commands that *corrupt or degrade* state
  (CLOCK-OFFSET, DELAY-APPLY, WAL-FAIL-NEXT, SHRINK-BACKLOG) are a different class: always
  compiled + admin-gated like Redis's DEBUG, or behind a build feature/config flag jepsen
  images enable? Redis ships them always-on behind DEBUG; matching that is the parity
  default, but WAL-FAIL-NEXT in a production binary is a sharper knife than Redis carries.
- **D2 — locked-crate discipline.** Every injection point lands inside a locked area
  (replication, cluster, persistence). Ruling needed on whether injection seams are
  spec-relevant (new failure-mode rows) or test-surface exempt (like the DEBUG check
  commands were). Mutation gates apply either way.
- **D3 — cross-node checker scope.** Issue 10's checker duplicates some single-node
  catalog claims at the fleet level. Rule whether it asserts only genuinely cross-node
  claims (replid agreement, offset dominance, primary uniqueness) or also re-checks
  node-local ones for defense in depth.
- **D4 — sequencing vs open campaigns.** WAL-FAIL-NEXT belongs naturally to the persistence
  campaign; DELAY-APPLY/CHANGE-REPL-ID interact with open replication rulings (16-22).
  Rule whether those issues wait on their campaign rulings or land behind them.

## Workstreams

- **W1 — wire what exists** (issues 01-03): generic DEBUG-command nemesis plumbing, crash
  signature nemeses, post-nemesis leak checks in final analysis.
- **W2 — new injection commands** (issues 04-09): one command + its nemesis + its forcing
  workload per issue, research-first (Redis/Valkey/DragonflyDB implementations per repo
  guidelines).
- **W3 — cross-node checking** (issue 10): `DEBUG REPLICATION VIEW` dump + fleet-level
  checker.

Issue 01 is the tracer bullet: the generic plumbing plus one end-to-end nemesis (`DEBUG
SLEEP` slow-node) proving the shape. Everything else parallelizes behind it.
