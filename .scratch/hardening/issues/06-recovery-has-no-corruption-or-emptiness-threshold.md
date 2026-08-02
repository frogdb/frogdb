# Recovery has no corruption threshold and cannot tell an empty database from the wrong one

Status: open
Type: bug (data safety)
Severity: likelihood 2/3 (a mistyped data-dir is an ordinary operator slip; wholesale value
corruption is rare), consequence 3/3 (an empty keyspace comes up healthy and is then written to,
and the WAL/snapshot cadence starts overwriting the real state) — score 6
Area: recovery / persistence restore

## Problem

Two related holes in what recovery is willing to call success.

### 1. `keys_failed` is counted and never consulted

Per-key deserialization failures during restore are caught, counted into
`RecoveryStats::keys_failed`, and logged as `WARN` (`recover_shard_into`,
`frogdb-server/crates/core/src/persistence/store_recovery.rs`). Skipping is the right default — one
bad value must not take the whole keyspace down (FM-PERSISTENCE-033). But nothing anywhere reads
the counter back. A database where *every* value fails to decode — a format change, a truncated
file, a bit-rotted column family — recovers "successfully" with an empty keyspace, a clean startup,
and only `WARN` lines to say otherwise. The server then accepts writes and starts persisting over
what is left.

The `Serialization` variant on the recovery error enum exists but is unreachable from this path.

### 2. "No existing data" and "wrong data dir" are the same observation

`has_data()` is a first-key-exists probe across the shard column families. A boot against an empty
or wrong-but-writable directory logs `No existing data found, starting fresh` and comes up with an
empty keyspace (FM-PERSISTENCE-029) — which is correct on a genuine first boot and catastrophic
when `--data-dir` was mistyped, a volume failed to mount, or a container lost its bind mount. Redis
has the same shape for a missing RDB, but Redis' blast radius is smaller: it does not immediately
begin overwriting a directory it believes is fresh.

Note the shard-count and warm-tier guards (FM-PERSISTENCE-030/031) *do* refuse loudly — but only
when a column-family layout is already stamped. A directory with no layout at all passes every
guard.

## Candidate fixes

1. **Threshold on `keys_failed`.** Refuse to start when `keys_failed` exceeds a configured
   fraction (or an absolute count) of `keys_loaded + keys_failed`, with an override flag to force
   the boot. Default it to something conservative — 100% failure with a non-zero total is
   unambiguous and would already catch case 1. Cheap: the stats are already aggregated at the seam,
   which is the natural place for the check.
2. **Surface the stats.** Even without a threshold, `keys_failed` should reach `INFO` (Redis has
   `rdb_last_load_keys_loaded` / `rdb_last_load_keys_expired`, both currently hardcoded to `0` in
   `info/sections.rs`) so monitoring can alarm on it. Complements 1; useful alone.
3. **A marker file for case 2.** Stamp a small `frogdb_data_dir` marker at first initialization and
   require either the marker or an explicit `--init` / empty-directory confirmation to boot fresh.
   Turns "wrong directory" into a refusal without affecting genuine first boots. This is the same
   trick the shard-count guard already relies on, generalized to the directory itself.

## Forcing tests

Case 1's current behavior is pinned by `test_partial_recovery_on_corruption`; a threshold would
extend that test with an all-keys-corrupt variant asserting a `RestoreShards` error. Case 2 needs a
recovery-seam test that boots against a directory containing an unrelated marker file, which the
existing `frogdb-recovery` tests already have the fixtures for.
