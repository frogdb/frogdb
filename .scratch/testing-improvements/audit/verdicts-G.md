# Verdicts G (Jepsen harness / infra) — all CONFIRMED
slot-migration-checker-noop: CONFIRMED L3/C3. (G framing wins over F#5.)
key-routing-checker-blind-to-data: CONFIRMED L3/C3. (note: "key-routing-partition" not in run.py; core claim stands — backs raft-chaos + key-routing-kill.)
weak-single-node-checkers: CONFIRMED L2/C2 (all three sub-claims verified; zero --independent in run.py).
orphaned-cluster-workloads: CONFIRMED L3/C3. Git: commit d7c60603 "implement 6 new Jepsen workloads from gap analysis plan" — intended to close gaps, never wired; no parked/flaky comment → undocumented orphan state. membership_routing durability computed-not-asserted; redirects → :info unthresholded.
elle-list-append-never-invoked: CONFIRMED L2/C3.
membership-fault-testing-absent: CONFIRMED L3/C3 (0 uses of raft-cluster-membership in run.py; no CLI flag for :membership-changes).
failover-durability-untested: CONFIRMED L3/C3 — DEDUP: fold into E#10 (replication framing wins; harness manifestation).
cross-slot-no-fault-injection: CONFIRMED L2/C3.
expiry-never-clock-skewed: CONFIRMED L2/C2 (currentTimeMillis ×6; checker needs server-authoritative time).
register-linearizability-thin-fault-coverage: CONFIRMED L2/C3 (:all 0 defs; nemesis-pause suites=()).
jepsen-absent-from-ci: CONFIRMED L3/C2.
fuzzing-not-continuous: CONFIRMED L2/C2 (fuzz.py:39 max_total_time=10; corpus gitignored).
coverage-tracking-disabled: CONFIRMED L1/C1.
no-deterministic-sim-for-replication-or-raft: CONFIRMED L2/C3 — DEDUP: F#6 framing wins for cluster half; keep replication half here or merge into one infra task.
wgl-downgrade-rate-unmonitored: CONFIRMED L2/C2 (downgraded_keys only asserted in unit test :310).
consistency-design-intent-rows-testable: CONFIRMED L1/C2 (doc honestly labeled; opportunity-to-test).
