# Area G: Jepsen harness + dist-sys infra (agent report, verified evidence)

Scope: testing/jepsen/frogdb/src/jepsen/frogdb/*.clj, run.py, .github/workflows, Justfile, crates/testing/src/checker.rs, server/tests/{simulation.rs,common/invariants.rs}, consistency.md. No overlap w/ backlog issues 05/06/10/11/16.

### `slot-migration-checker-noop`
slot_migration.clj:403 returns `{:valid? true}` unconditionally; metrics (writes/reads/owners-seen/migrations, lines 385-411) never feed verdict; docstring (375-381) claims "No data loss during migration". Runs in raft/all suites as slot-migration (none) + slot-migration-partition (partition) (run.py:192-245) — real value loss under partition passes green.
Fix: track per-:write value+index; assert final :read = last acked write; final-owner == dest; zero unresolved redirects. Reuse cross_slot.clj:382 conservation pattern.
**L3 / C3.**

### `key-routing-checker-blind-to-data`
key_routing.clj:333 `:valid? (empty? redirect-failures)` (only :too-many-redirects, line 331). Wrong-value GET or silently dropped write invisible. This workload backs raft-chaos (harshest nemesis, run.py:246-254) + key-routing-kill/partition.
Fix: switch raft-chaos to elle-rw-register/list-append, or add key→last-acked-write tracking.
**L3 / C3.**

### `weak-single-node-checkers`
(a) cluster_formation.clj:229 `:valid? (or (= "ok" final-state) (nil? final-state))` — nil passes; node-count consistency never checked. (b) hash.clj:234 default weak path; linearizable independent-checker (204-211) gated behind --independent which run.py never passes. (c) sortedset.clj tracks latest-scores (274-281) but :valid? (291) never cross-checks final read against them.
Fix: wire hash-independent TestDefinition; sortedset assert final score == latest; cluster_formation fail on nil.
**L2 / C2.**

### `orphaned-cluster-workloads`
core.clj:95-100 registers + 143-147 routes migration-recovery, concurrent-migration, partition-recovery, membership-routing, rolling-restart; none in run.py TESTS (92-295). Highest-value scenarios never executed. Compounding: membership_routing.clj:287 :valid? = (and node-added? migration-completed?) despite docstring (10, 257-259) promising no-acked-write-loss — computes written-keys/reads-with-nil (268,273) but never asserts durability. Redirect exceptions downgraded to :info (169-171), unthresholded.
Fix: add TestDefinitions; strengthen membership_routing checker; threshold redirect-info counts; or delete unready workloads.
**L3 / C3.**

### `elle-list-append-never-invoked`
list_append.clj = RPUSH/LRANGE + jepsen.tests.cycle.append :strict-serializable (line 239), cluster-aware client, registered core.clj:93 — zero run.py TestDefinition. Strongest anomaly detector runs nowhere.
Fix: add list-append (none) to single; list-append + raft-cluster nemesis to raft.
**L2 / C3.**

### `membership-fault-testing-absent`
membership.clj + nemesis.clj:1195-1221 raft-cluster-membership composed nemesis dispatched at nemesis.clj:1254, accepted core.clj:223 — no TestDefinition uses it. cluster_formation.clj:172-203 membership-change-generator gated :membership-changes — no CLI flag (core.clj cli-opts 211-259). Join/leave under faults tested by nothing.
Fix: raft-membership TestDefinition (membership-routing or cluster-formation + raft-cluster-membership); add CLI flag.
**L3 / C3.**

### `failover-durability-untested`
client.clj:497-500 promote-to-primary! (REPLICAOF NO ONE) never called. replication suite (run.py:145-172) = none/partition/all-replication; all-replication (nemesis.clj:1058-1096) kills+restarts primary but nothing promotes replica — n1 returns as primary. Acked-write-loss bound on failover = consistency.md:75-90 [Design intent], never measured. Extended nemeses (clock/disk/slow-net/memory) raft-extended-only; REPLICATION topology gets none.
Fix: replication-failover workload — tracked writes, kill primary, promote replica, assert loss set within documented bound; add clock-skew/slow-network on REPLICATION.
**L3 / C3.**

### `cross-slot-no-fault-injection`
cross_slot.clj:382 rigorous conservation checker; run.py:201-208 runs nemesis none only.
Fix: add cross-slot-partition + cross-slot-kill to raft suite.
**L2 / C3.**

### `expiry-never-clock-skewed`
expiry.clj wall-clock based (System/currentTimeMillis, 2s tolerance; lines 30,107-155,366); runs single-node none/kill/rapid-kill (run.py:114-138). clock-skew nemesis bound to elle-rw-register raft-extended only. Checker itself would need server-authoritative time under skew.
Fix: expiry-clock-skew TestDefinition; derive expected expiry from server TTL/PTTL.
**L2 / C2.**

### `register-linearizability-thin-fault-coverage`
register.clj Knossos cas-register linearizable (161-165) runs only register/none + crash/kill single-node. Never on replication/raft; never clock/disk/net/memory/pause (extended swaps to Elle citing Knossos OOM, run.py:256-258). `:all` nemesis (nemesis.clj:1251) no TestDefinition; nemesis-pause (run.py:143) suites=() — unreachable via suites.
Fix: register+pause, register+partition (bounded time-limit); suite nemesis-pause; add :all TestDefinition or delete dead defs.
**L2 / C3.**

### `jepsen-absent-from-ci`
grep -ri jepsen .github/ → nothing (only Justfile 409-450). concurrency-nightly.yml cron pattern exists to clone. Distributed regressions only caught by manual runs.
Fix: jepsen-nightly.yml (or weekly) running raft + replication suites on Blacksmith runner, artifacts uploaded. Fix noop/blind checkers first.
**L3 / C2.**

### `fuzzing-not-continuous`
fuzz.yml workflow_dispatch only (line 12), -max_total_time=10 per target; testing/fuzz/.gitignore excludes corpus/ + artifacts/ — every run starts from empty corpus. 33 parsers effectively unfuzzed.
Fix: scheduled cron w/ multi-minute budget; persist corpus (cache/artifact); PR job replaying saved corpus as regression.
**L2 / C2.**

### `coverage-tracking-disabled`
test.py:28 COVERAGE_ENABLED=False gates coverage job (test.py:189). No CI coverage signal.
Fix: enable as nightly non-gating job; floor on core/vll/persistence later.
**L1 / C1.**

### `no-deterministic-sim-for-replication-or-raft`
simulation.rs = standalone scatter-gather + client↔server partition only (module doc line 6; test_network_partition_client_isolated is client isolation). No multi-node replication/raft turmoil topology. Multi-node = integration tests (no faults, no lin checking) or Jepsen (not in CI).
Fix: turmoil harness for ≥2 instances; replication-failover + raft-election sim tests feeding WGL check_linearizability; per-PR.
**L2 / C3.**

### `wgl-downgrade-rate-unmonitored`
checker.rs inconclusive handled (258-266, never silent pass); invariants.rs:133-154 downgrades keys over state budget to conservation-only w/ eprintln; downgraded_keys never thresholded in real sweeps (only unit test invariants.rs:335). All-keys-downgraded run passes having lin-checked nothing.
Fix: report downgrade ratio in sweep summary; warn threshold / nightly fail threshold; raise max_states for nightly.
**L2 / C2.**

### `consistency-design-intent-rows-testable`
consistency.md [Design intent] rows: RYW (27), monotonic reads (38), acked-write-loss bound (86), replica staleness (89), within-connection ordering (138), pubsub ordering (144). Assessment: RYW + within-connection ordering cheaply testable today (single-conn turmoil/Jepsen workload); acked-loss-bound = failover gap above; pubsub ordering partial via issue-10 capture seam, no cross-reconnect Jepsen workload; staleness/monotonic need replica-read client (new harness capability).
Fix: single-conn RYW + order workloads (turmoil per-PR); pubsub-ordering Jepsen workload; replica-read client; relabel rows as tests land.
**L1 / C2.**

**Top cluster (L×C=9):** slot-migration-checker-noop, key-routing-checker-blind-to-data, orphaned-cluster-workloads, membership-fault-testing-absent, failover-durability-untested. Theme: fault workloads that look like coverage either assert nothing, never run, or lack promotion path; jepsen-absent-from-ci compounds all.
