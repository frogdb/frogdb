# FINAL TASK LIST — .scratch/testing-improvements/issues/NN-<slug>.md
Format: NN | slug | source (area file + gap id) | final L/C | notes (verdict adjustments/reframes)
Source reports: .scratch/testing-improvements/audit/{A-basic-commands,B-transactions,C-pubsub-streams,D-persistence,E-replication,F-cluster,G-jepsen-harness}.md
Verdicts: .scratch/testing-improvements/audit/verdicts-{A,B,C,D,E,F,G}.md

01 | spop-replication-divergence | E#1 | L3/C3 | Type: bug (CONFIRMED-LIVE-BUG). Corroboration slowlog_tcl.rs:16. Fix = deterministic rewrite/suppression + replication convergence tests.
02 | blocking-serve-replication-divergence | E#2 | L3/C3 | Type: bug (CONFIRMED-LIVE-BUG). Served blocking pops never broadcast.
03 | jepsen-failover-durability-workload | E#10 + G:failover-durability (merged; E framing wins) | L3/C3 | kill-primary→promote→ackd-write-fate; use promote-to-primary!; add clock-skew/slow-net on REPLICATION topology.
04 | jepsen-orphaned-workloads-wire-or-delete | G:orphaned-cluster-workloads | L3/C3 | 5 workloads + membership_routing checker durability-assert fix + redirect-:info thresholding. Git d7c60603 context.
05 | jepsen-membership-under-fault | G:membership-fault-testing-absent | L3/C3 | raft-cluster-membership nemesis TestDefinition + :membership-changes CLI flag.
06 | jepsen-raft-chaos-blind-checker | G:key-routing-checker-blind-to-data | L3/C3 | swap raft-chaos to Elle workload or add value-tracking checker.
07 | shuttle-multiwaiter-exactly-once-guard | C:multiwaiter | L3/C3 | Deterministic shuttle model of real ShardWorker multi-key BLMPOP path; cross-ref concurrency-testing issue 11 (NOT a re-file); raise nightly cap post-fix.
08 | jepsen-slot-migration-checker-noop | G+F#5 (merged; final F score) | L3/C2 | Strengthen checker: last-acked-write, final-owner==dest, redirect thresholds.
09 | keyspace-new-keymiss-events-inert | C | L3/C2 | n/m classes parse but zero emission sites; implement or reject flags.
10 | jepsen-nightly-ci | G:jepsen-absent-from-ci | L3/C2 | Clone concurrency-nightly.yml pattern; gate on fixed checkers (03-08).
11 | turmoil-cluster-raft-topology | F#6 (+G raft half; F framing wins) | L3/C2 | Deterministic multi-node cluster sim: MOVED/ASK under leader partition mid-migration.
12 | durability-fsync-boundary | D#1 | L2/C3 | Page-cache-severing harness or fsync-seam injection; consistency.md:61 label overclaims [Tested] — fix label if not testable now. Sync WriteOptions wiring itself verified correct.
13 | bgsave-restore-path | D#2 | L2/C3 | Restore from actual snapshot_NNNNN artifact e2e; ops doc confirms no automated load path.
14 | wal-recovery-mode-pin | D#3 | L2/C3 | Pin WalRecoveryMode explicitly + mid-log corruption tests + metric on skipped keys.
15 | split-brain-lifecycle-e2e | E#5 | L2/C3 | Full partition→divergent-writes→heal→discard+audit-log lifecycle; buffer overflow truncation.
16 | cluster-epoch-persistence-assert | F#3 | L2/C3 | Assert per-node config_epoch equality across restart (not composite >=).
17 | source-crash-mid-migration-assert | F#4 | L2/C3 | test_source_dies_during_migration has zero assertions; add single-owner/no-orphan/exactly-one-readable.
18 | leader-fd-false-failover | F#7 | L2/C3 | Asymmetric partition (follower cut from leader, client-reachable); failure_detector.rs:10 leader-only.
19 | cross-slot-standalone-multi-invariant | B#2 | L2/C3 | Config-on MULTI must still CROSSSLOT; verify actual config field name before writing tests.
20 | jepsen-elle-list-append-schedule | G | L2/C3 | Add list-append TestDefinitions (single + raft w/ nemesis).
21 | jepsen-cross-slot-fault-variants | G | L2/C3 | cross-slot-partition + cross-slot-kill TestDefinitions.
22 | jepsen-register-fault-coverage | G:register-linearizability-thin | L2/C3 | register+pause/partition; suite nemesis-pause; wire or delete :all.
23 | turmoil-replication-topology | G:no-deterministic-sim (replication half) | L2/C3 | ≥2-instance turmoil replication failover sim + WGL histories.
24 | scan-full-iteration-stress | A#2 | L2/C2 | Force resizes mid-iteration; proptest interleavings.
25 | restore-corrupt-payload | A#4 | L2/C2 | Truncated/type-flipped/garbage payloads per type; restore_payload fuzz target. RESTORE already fails closed on parse-fail (persistence.rs:122-124) — residue = still-parses cases.
26 | script-undeclared-key-policy | B#6 | L2/C2 | Pin slot-based policy; wire or delete dead validate_key_access; fix scripting_tcl.rs exclusion header.
27 | stream-notification-class-assert | C | L2/C2 | Assert t-class events e2e; port excluded upstream test.
28 | pubsub-mode-command-gate | C | L2/C2 | RESP3 allow-all branch; RESP2 9-command boundary; RESET exits mode.
29 | pubsub-slow-subscriber-bound | C | L2/C2 | Unbounded mpsc + no output-buffer-limit; test pins policy (part feature gap).
30 | pubsub-disconnect-dereg-e2e | C | L2/C2 | CLIENT KILL + ungraceful close → counts drop across ≥2 shards (lifecycle.rs:191-193 path).
31 | blocking-crossslot-negative | C | L2/C2 | BLPOP/BLMPOP/BZMPOP/BLMOVE cross-slot → immediate CROSSSLOT.
32 | replica-expiry-ttl-drift | E#3 | L2/C2 | Deterministic post-expiry replica read assertion; PTTL bound.
33 | fence-min-replicas-e2e | E#4 | L2/C2 | Fence engage/recover e2e; min-replicas-to-write inert as write gate (sub-issue arguably C3); CLUSTERDOWN error string on standalone.
34 | promotion-replid-asserts | E#6 | L2/C2 | Assert new replid + replid2 preserved + CONTINUE not FULLRESYNC (tests currently accept anything).
35 | broadcast-lag-resync-e2e | E#8 | L2/C2 | >10k frame stall → disconnect → reconnect → checksum equality; fix stale doc-comment.
36 | cluster-slots-fail-accounting | F#2 | L2/C2 | slots_pfail/fail hardcoded 0; slots_ok unconditional.
37 | wait-in-cluster | F#10 | L2/C2 | Ack counting + failover behavior.
38 | jepsen-weak-checkers-strengthen | G:weak-single-node-checkers | L2/C2 | hash --independent wiring; sortedset final-score cross-check; cluster_formation nil-fail.
39 | jepsen-expiry-clock-skew | G | L2/C2 | expiry+clock-skew TestDefinition; server-authoritative time in checker.
40 | fuzzing-continuous-corpus | G | L2/C2 | Scheduled cron; persisted corpus; PR corpus-replay job.
41 | wgl-downgrade-threshold | G | L2/C2 | Report + threshold downgrade ratio in sweeps.
42 | tiered-storage-restart-recovery | D#4 | L1/C3 | Real-spill e2e seam (recover_warm_shard_into IS unit-tested — scope = integration).
43 | checkpoint-cross-shard-cut | D#5 | L1/C3 | Verifier: per-shard separate WriteBatches → torn checkpoint possible; concurrent BGSAVE test. Adjacent to issues 05/06, not dup.
44 | errorstats-e2e | A#1 | L2/C1 | errorstat_<PREFIX> section + rejected-vs-failed + total_error_replies e2e; fix stale info_tcl.rs doc. Note: introspection2_tcl.rs:630 partially covers.
45 | persistence-cmd-replies | D#6+D#7 merged | L2/C1 | BGSAVE AlreadyRunning branch (+status vs Redis -ERR) + LASTSAVE real-coordinator e2e + lossy conversion.
46 | ryw-monotonic-readonly-table | E#7 | L2/C1 | Table-driven READONLY across all WRITE-flagged; monotonic replica-read loop.
47 | epoch-fold-observability | F#1 | L2/C1 | Reframed per verdict: pin INFO-vs-NODES epoch relationship deliberately; original <=max assertion WRONG (Redis allows currentEpoch>config epochs).
48 | chained-replication-contract | E#9 | L2/C1 | Replica-of-replica silently no-data; pin support-or-reject.
49 | cross-slot-multi-assert-tighten | B#3 | L2/C1 | Pin queue-time CROSSSLOT + EXECABORT specifics.
50 | subscription-cap-integration | C (adjusted) | L2/C1 | Unit tests EXIST (subscribe_limit_and_80pct_latch etc.); missing = integration error text + all-or-nothing batch + duplicate-count quirk.
51 | wait-in-multi-guard | B#4 (adjusted) | L1/C2 | Correct-by-design (Standard strategy → non-blocking branch); add regression guard vs hang.
52 | blmpop-bzmpop-nonblocking-multi | B#5 | L1/C2 | Extend MULTI + Lua non-block suites.
53 | xread-no-wake-pin | C | L1/C2 | XTRIM/XDEL no-wake property.
54 | resp3-double-nonfinite-pin | A#3 (adjusted) | L1/C2 | Encoder correct upstream; pin regression w/ raw-byte assert.
55 | multi-exec-migration-boundary | F#8 | L1/C2 | Pin queue-vs-EXEC migration semantics deliberately.
56 | consistency-design-intent-conversion | G | L1/C2 | RYW + within-conn-order + pubsub-order workloads; relabel rows as tests land; staleness needs replica-read client.
57 | subscribe-in-multi-parity-residue | B#1 (headline REFUTED) | L1/C1 | FrogDB plain-family matches Redis (verified vs Redis src). Residue: SUNSUBSCRIBE/PUBSUB rejected where Redis allows; non-Redis error string; zero tests. Do NOT claim EXECABORT semantics.
58 | cluster-keyspace-notify-scan-semantics | F#11 | L1/C1 | Define+test which node emits events; SCAN per-node union.
59 | coverage-ci-enable | G | L1/C1 | COVERAGE_ENABLED=False; nightly non-gating job. Include audit's own coverage summary as baseline.
60 | stale-test-docs-cleanup | F premise + D notes + A notes | L1/C1 | integration_cluster.rs:6-9 stale; integration_dump_restore.rs:5 stale; info_tcl.rs:42-56 stale (if not fixed in 44); also flaky test_frogdb_version_reports_cluster_info under coverage env (investigate/deflake).

REFUTED (do not file): F#9 sharded-pubsub-migration-unsubscribe (covered integration_pubsub.rs:1274; failover-variant residue noted in 03/17 context).
