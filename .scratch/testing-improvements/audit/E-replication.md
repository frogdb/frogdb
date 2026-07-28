# Area E: Replication (agent report, verified evidence)

## Root-cause context
Steady-state replication ships ORIGINAL client command verbatim; replica re-executes: broadcast of record.handler.name()+record.args (core/src/shard/post_execution.rs:395-423); args = command.args.as_slice(), no rewrite hook (execution.rs:388-390,427-430); replica applies via CoreMsg::Execute full handler re-execution (server/src/replication/executor.rs:56-82,121-134). Local WAL persists resulting values (deterministic on restart). ⇒ any command whose re-execution doesn't reproduce primary's mutation diverges on replicas but not WAL. NO_PROPAGATE/NONDETERMINISTIC flags exist (core/src/command.rs:860-861; commands/src/basic.rs:227-233) but NEVER consulted in broadcast path.

### 1. `spop-nondeterministic-verbatim-propagation` — likely LIVE BUG
SpopCommand flags WRITE|FAST only (commands/src/set.rs:879-884); pops rand member (types/src/types/set.rs:279-288). Primary broadcasts "SPOP <args>"; replica re-executes w/ fresh RNG → different member. Set ≥2 members not fully drained → permanent divergence. tcl_spop_propagate_as_del_or_unlink checks local effect only (set_tcl.rs:1038-1075); test_set_operations_replicate uses SADD only (integration_replication.rs:3409). Redis rewrites SPOP→SREM/DEL.
- tests: primary+replica SADD 5, SPOP 2, WAIT, SMEMBERS equal both; turmoil property random SADD/SPOP → quiescent equality.
- **L3 / C3.**

### 2. `blocking-serve-not-propagated` — likely LIVE BUG
Served blocking pop mutates store inside blocking.rs w/ no replication broadcast (core/src/shard/blocking.rs:291-320). WaiterSatisfaction = index 4 of WRITE_EFFECT_ORDER, ReplicationBroadcast = index 8 (post_execution.rs:238-248). LPUSH k v waking BLPOP: primary ends empty; broadcast = "LPUSH k v" only; replica retains v. No blocking replication test (grep BLPOP/BRPOP/BLMOVE in integration_replication.rs → none). Redis propagates the serving pop.
- tests: replica + blocked BLPOP k 0; LPUSH k v; WAIT; assert LRANGE empty on replica. Repeat BRPOPLPUSH/BLMOVE (dest replicates too), BZPOPMIN, XREAD BLOCK.
- **L3 / C3.**

### 3. `replica-independent-expiry-and-relative-ttl-drift-untested`
Expiry = EffectScope::InternalRemoval replicate:false (post_execution.rs:141-150,371-377) — replicas expire on own clock. Relative-TTL commands shipped verbatim ("TTL propagated as absolute" = intentional-incompatibility NOT implemented, expire_tcl.rs:15) → replica deadline later by lag; window where replica returns value primary expired. Only test test_expire_propagated_as_del_not_expire accepts both outcomes via eprintln!, no assertion (integration_replication.rs:2955-3012).
- tests: SET+PEXPIRE 300, WAIT, post-expiry replica read asserting documented behavior (fake clock); PTTL replica <= primary + lag bound.
- **L2 / C2.**

### 4. `self-fence-and-min-replicas-write-rejection-untested-e2e`
self_fence_on_replica_loss wires ReplicationQuorumChecker (server/src/server/replication_init.rs:186-195) rejecting w/ "CLUSTERDOWN The cluster is down (quorum lost, writes rejected)" even standalone (guards.rs:286-296). Unit-only coverage (replication_quorum.rs:85-157; MockQuorumChecker guards.rs:794-825). No e2e: flag+kill replica → writes rejected → recovery. min_replicas_to_write consulted ONLY in wait_for_acks (replication/src/tracker.rs:254-262) — never gates writes; Redis user expects NOREPLICAS refusal. INERT config = certain gap.
- tests: e2e fence engage/recover; min-replicas-to-write 1 + zero replicas → write rejected (currently passes through — bug); turmoil partition fencing within timeout.
- **L2 / C2.**

### 5. `split-brain-divergence-lifecycle-untested-e2e`
Unit-tested parts only: divergence_record window (replication/src/primary/tests.rs:65-166); log round-trip + has_pending_logs (split_brain_log.rs:194-317); wiring cluster_init.rs:586-633. No integration/turmoil full lifecycle: demoted primary w/ writes past acked offset → correct capture, file written, SplitBrainOpsDiscardedTotal increments, writes actually discarded after full resync. SplitBrainBufferConfig (config/src/replication.rs:124-132) overflow-truncation untested; no rotation/retention test.
- tests: turmoil/jepsen partition-heal-promote; assert divergent writes absent post-resync, log contains exactly those ops or truncation marker, telemetry matches.
- **L2 / C3.**

### 6. `promotion-replication-id-semantics-untested`
test_secondary_replication_id_failover checks role==master; master_replid2 presence = eprintln! no assert; never verifies new replid minted or old preserved (integration_replication.rs:1515-1560). PSYNC2 cross-failover partial resync unverified (test_psync2_failover_partial_sync :3637 — confirm asserts +CONTINUE).
- tests: capture replid before/after REPLICAOF NO ONE; assert changed + replid2 = prior; old-offset replica gets +CONTINUE not FULLRESYNC.
- **L2 / C2.**

### 7. `read-your-writes-monotonic-staleness-untested`
consistency.md [Design intent] rows unasserted. No monotonic replica-read test; READONLY tests cover SET/DEL/ZADD only (integration_replication.rs:3940-3975); replica lazy-expiry-on-read behavior untested.
- tests: write-then-replica-read monotonic loop; table-driven READONLY across ALL WRITE-flagged commands via registry (GETEX/GETDEL/SETRANGE/EXPIRE/COPY/PFMERGE...).
- **L2 / C2.**

### 8. `broadcast-lag-disconnect-resync-untested`
Lagged → disconnect for resync (replication/src/replica_session.rs:734-738); channel 10000 (primary/mod.rs:120). test_replica_lag_behavior under-fills (1000 writes), asserts nothing (eprintln 5 keys), stale doc-comment "Issue #2: only warns — no resync" (integration_replication.rs:1798-1848).
- tests: stall replica socket, write >10k frames; primary progresses; replica reconnects; SCAN/checksum equality; WAIT drops then recovers.
- **L2 / C2.**

### 9. `chained-replication-behavior-undefined`
Replica constructed w/ NoopBroadcaster (replication_init.rs:177-181); applied frames under REPLICA_INTERNAL_CONN_ID (broadcast suppressed). Sub-replica never receives writes; PSYNC/REPLICAOF not rejected. No test/doc contract.
- tests: REPLICAOF replica→replica; assert data flows OR defined error/INFO state. Pin contract.
- **L2 / C1-2.**

### 10. `kill-primary-promotion-ackd-write-fate-untested` — DEDUPE w/ G:failover-durability-untested
No automated kill-primary→promote→verify ack'd-write fate; loss window unmeasured; WAIT-durability boundary unasserted outside zombie partition case.
- **L3 / C3.**

## Verified NOT gaps
Checkpoint checksum verify + mismatch-cleanup (fullsync/stager.rs:221-239), truncation EOF (receiver.rs:144); partial-resync +CONTINUE/fallback/ordering (integration_replication.rs:1365-1515); WAL-buffer overflow→full-resync + interrupted resume (:2051,:2386); offset persistence + staged-metadata corruption rejection (:4058,:4398); inter-shard ordering (single serialized consume_frames, apply.rs:101-218); Lua propagation as effects; MULTI atomic framing on replica (apply.rs tests); INCRBYFLOAT deterministic in-binary (no divergence).
