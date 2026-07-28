# Area D: Persistence / Durability / Recovery (agent report, verified evidence)

Orthogonal to backlog issues 05/06.

## 1. `durability-fsync-boundary-never-exercised`
Fsync durability boundary (point of sync vs periodic vs async) never tested; every "crash" preserves OS page cache so modes indistinguishable. Unit crash = handle drop: CrashTestHarness::crash() is `self.rocks = None` (core/src/persistence/test_harness.rs:183-186) — graceful Close. Jepsen kill = pkill -KILL (testing/jepsen/frogdb/src/jepsen/frogdb/db.clj:102-105); single-node compose sets NO durability mode (testing/jepsen/docker-compose.yml:16-19) → default Periodic{1000}. SIGKILL leaves acked-unfsynced WAL bytes in kernel page cache — kernel flushes anyway. Per-mode assertions vacuous: test_periodic_mode_within_window / test_async_mode_explicit_flush assert keys_loaded>=1 (crash_recovery_tests.rs:156-159,190-193); test_sync_mode_crash_recovery (line 45) would pass in async mode. Turmoil fake WAL "synchronously durable" (wal/fake.rs:221-223) — models append-failure, not unsynced loss.
- expected: consistency.md:59-69 labels Durability [Tested], "Per-mode crash windows are verified" — unproven. Redis appendfsync always/everysec/no equivalence requires power-loss-class event drops exactly the unsynced window per mode.
- tests: jepsen-crash nemesis severing page cache (VM hard reset / sysrq-b / dm-flakey loopback); or integration fsync-seam WriteObserver discarding unsynced writes on crash(), parameterized by mode. Minimum: downgrade label to [Design intent].
- **L2 / C3** (silent loss of acked writes; strongest durability promise).

## 2. `bgsave-snapshot-artifact-never-restored`
No test recovers a server from a BGSAVE checkpoint. test_bgsave_snapshot_survives_restart (server/tests/integration_persistence.rs:176-222) does BGSAVE, writes wal_key, graceful shutdown, reopens SAME live dir — recovery via ordinary WAL replay; snapshot_NNNNN never loaded. Boot only installs replica checkpoint_ready staged dir or opens live DB (operations/persistence.md:95-99); BGSAVE dirs + latest symlink (snapshot/stager.rs:60-66) consulted by no automated path. Stager unit tests (snapshot/tests.rs:251-486) = staging/rename/retention mechanics; load_staged_checkpoint tests (rocks/tests.rs:419-654) install hand-written 1-key DBs.
- expected: BGSAVE checkpoint = consistent recoverable point-in-time image (operator's DR artifact). Redis verifies RDB loadability on restart; no equivalent.
- tests: integration — BGSAVE, install checkpoint into fresh dir, start server, assert all keys/types; cover documented operator restore procedure end-to-end.
- **L2 / C3** (DR artifact silently unrecoverable).

## 3. `wal-recovery-mode-unconfigured-and-mid-log-corruption-untested`
Grep WalRecoveryMode/set_wal_recovery_mode/PointInTime/TolerateCorrupted across crates → nothing. Doc WAL-corruption table (architecture/persistence.md: truncated/checksum/invalid marker → Truncate) describes RocksDB default, neither pinned nor tested. Existing corruption tests: test_partial_recovery_on_corruption = invalid VALUE in live CF, key silently skipped (crash_recovery_tests.rs:793-808, keys_failed=1); test_truncated_metadata_recovery = snapshot metadata.json truncation (974-989). Neither flips a byte in the RocksDB WAL. Default kPointInTimeRecovery: mid-log checksum failure silently truncates ALL subsequent valid records.
- expected: doc policy (truncate torn tail only) safe; silently dropping valid suffix ≠ least surprise. Recovery mode should be explicit pinned choice.
- tests: unit — WAL w/ N records, corrupt byte in record k, reopen, assert exact survivors per chosen mode + truncation logged/counted; pin configured WalRecoveryMode; assert value-skip emits metric/log.
- **L2 / C3.**

## 4. `tiered-storage-spilled-keys-no-restart-recovery-test`
recovery.rs:145 implements recover_warm_shard_into; operations/persistence.md:102 documents hot-over-warm precedence. tiered_storage.rs never reopens a RocksStore — closest test asserts warm CF empty after clear (core/tests/tiered_storage.rs:226-232). No spill→reopen→recover→verify test; no integration-level tiered test at all (grep spill/warm_tier in server/tests empty). Spilled key's value exists ONLY in warm CF (test_memory_accounting_warm_keys:243-254).
- tests: spill several types, reopen, assert restore (KeyType, expiry, unspillable); hot+warm dual-presence resolves hot; spilled key w/ past TTL across restart.
- **L2 / C3** (spilled-key loss on restart).

## 5. `checkpoint-cross-shard-consistent-cut-untested`
create_checkpoint cuts at latest_sequence_number() (rocks/checkpoint.rs:7-21); cross-shard WriteBatch should be atomically in/out. Stager happy-path test (snapshot/tests.rs:251-315) checkpoints quiescent store — no concurrent cross-shard writer.
- tests: turmoil/integration — hammer cross-shard MSET/tx during BGSAVE; restore; assert no torn batch.
- **L1 / C3** (likely correct by construction via seqno semantics).

## 6. `bgsave-already-running-path-untested-and-reply-diverges`
handle_bgsave AlreadyRunning → Response::Simple("Background save already in progress") (server/src/connection/persistence_conn_command.rs:125-128). Unit tests use NoopSnapshotCoordinator (completes instantly, branch never fires; lines 242-258 document back-to-back Started). No integration overlap of two BGSAVEs on real coordinator. Redis 8.6 returns -ERR error reply; FrogDB returns + simple string (client treats as success).
- tests: integration — slow save, overlap second BGSAVE, assert AlreadyRunning observed; pin reply type (Redis = error).
- **L2 / C1.**

## 7. `lastsave-only-unit-tested-with-noop-and-lossy-conversion`
handle_lastsave = now.as_secs().saturating_sub(elapsed.as_secs()) (persistence_conn_command.rs:133-152) — double truncation ±1s; unit tests Noop only (277-294). No integration LASTSAVE 0→advance after real BGSAVE.
- **L2 / C1.**

## Verified NOT gaps (save future auditor time)
- Consumer-group/PEL, expiry-on-load, replication offsets, search-index rebuild across restart covered: integration_persistence.rs:1192,1100,1289; crash_recovery_tests.rs:488; integration_replication.rs:4058,4353; search.rs:1536.
- DUMP/RESTORE Stream/Bloom/TimeSeries NO LONGER stubbed — active round-trips integration_dump_restore.rs:188/209/229; zero #[ignore]; module doc-comment line 5 STALE (minor doc nit). ABSTTL/IDLETIME covered dump_tcl.rs:146,175.
- Serialization round-trip probabilistic/timeseries/search covered at persistence unit level; e2e server-restart for JSON/CMS/TopK/TDigest/Cuckoo/VectorSet/HashWithFieldExpiry not integration-tested but judged below bar (serialization layer covered).
