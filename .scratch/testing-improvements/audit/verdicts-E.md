# Verdicts E (replication)
Corroboration: slowlog_tcl.rs:16 documents "FrogDB does not implement replication command rewriting (SPOP->DEL, ... blocked BLPOP->LPOP) for replication purposes" — gaps 1&2 = known real divergences.

1 spop-nondeterministic-verbatim-propagation: CONFIRMED-LIVE-BUG — L3/C3. Broadcast verbatim (post_execution.rs:396-402); SPOP WRITE|FAST no rewrite (set.rs:879-884); fresh rand::rng() per node (set.rs:284); replica full re-execution (executor.rs:56-82). NONDETERMINISTIC never consulted; NO_PROPAGATE only feeds COMMAND INFO. No SPOP replication test anywhere.
2 blocking-serve-not-propagated: CONFIRMED-LIVE-BUG — L3/C3. Served pop via strat.satisfy in drive_satisfaction_body (blocking.rs:291-320), WaiterSatisfaction idx4; only broadcast = waking write at idx8. No broadcast call in blocking.rs/dispatch_blocking.rs. Zero BLPOP/BRPOP/BLMOVE tests in integration_replication.rs.
3 replica-independent-expiry-and-relative-ttl-drift-untested: CONFIRMED — L2/C2. Sole test resolves all outcomes via eprintln (:2955).
4 self-fence-and-min-replicas-write-rejection-untested-e2e: CONFIRMED — L2/C2 (min-replicas sub-issue arguably C3; inert config real).
5 split-brain-divergence-lifecycle-untested-e2e: CONFIRMED — L2/C3. grep split.brain/divergen/discard in integration_replication.rs → nothing.
6 promotion-replication-id-semantics-untested: CONFIRMED — L2/C2. PSYNC2 tests accept "CONTINUE, FULLRESYNC, or OK" loosely (:1395,:1484).
7 read-your-writes-monotonic-staleness-untested: ADJUSTED — L2/C1. READONLY enforced centrally on WRITE flag; missing coverage pins design intent only.
8 broadcast-lag-disconnect-resync-untested: CONFIRMED — L2/C2. channel(10000), test writes 1000, never triggers Lagged.
9 chained-replication-behavior-undefined: CONFIRMED — L2/C1.
10 kill-primary-promotion-ackd-write-fate-untested: CONFIRMED, DEDUPE with G:failover-durability-untested — L3/C3. Track jointly.
