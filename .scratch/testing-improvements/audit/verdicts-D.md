# Verdicts D (persistence)
durability-fsync-boundary-never-exercised: CONFIRMED L2/C3. crash() graceful drop (test_harness.rs:183); compose no durability env → Periodic default; consistency.md:61 overclaims [Tested]. Sub-hypothesis REFUTED: sync WriteOptions ARE wired (flush.rs:358 is_sync → :175 set_sync) — gap = test vacuity + false doc label, not broken sync path.
bgsave-snapshot-artifact-never-restored: CONFIRMED L2/C3. Ops doc independently confirms "no snapshot-load step at startup … primary recovery never reads them".
wal-recovery-mode-unconfigured-and-mid-log-corruption-untested: CONFIRMED L2/C3. Grep confirms mode unpinned (RocksDB default kPointInTimeRecovery); corruption tests touch CF value + metadata.json only; persistence.md:149-157 tail-truncate table unverified.
tiered-storage-spilled-keys-no-restart-recovery-test: ADJUSTED L1/C3. recover_warm_shard_into IS unit-exercised (recovery.rs:408) incl. hot-wins + warm-expiry prune; startup wires warm recovery. True gap = missing real-spill e2e seam, not unproven logic.
checkpoint-cross-shard-consistent-cut-untested: CONFIRMED L1/C3. NOT purely correct-by-construction: flush.rs sinks per-shard separate WriteBatches → cross-shard MSET not one atomic batch; checkpoint could catch torn. Narrow window. Adjacent to (not dup of) issues 05/06.
bgsave-already-running-path-untested-and-reply-diverges: CONFIRMED L2/C1.
lastsave-only-unit-tested-with-noop-and-lossy-conversion: CONFIRMED L2/C1.
