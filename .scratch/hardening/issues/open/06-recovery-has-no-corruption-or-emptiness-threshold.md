# Recovery has no corruption threshold and cannot tell an empty database from the wrong one

Status: ready-for-human
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

## Resolution (partial — this issue stays open, narrowed)

Shipped: **the surfacing half of candidate 2 in full, and the unambiguous half of candidate 1**.
Case 2 (marker file, candidate 3) is untouched, and so is the *partial*-corruption threshold.

### 1. `keys_failed` is now consulted, and reaches operators

`frogdb_recovery::shards::restore` gained `report_decode_failures`, which runs once per boot
against the aggregate `RecoveryStats` (not per shard, so one boot produces one signal):

- **Refusal on total failure.** When data was found and *nothing* in it decoded —
  `keys_failed > 0` and `keys_loaded + keys_expired_skipped + warm_keys_loaded + warm_keys_stale
  == 0` — recovery fails the `RestoreShards` phase with the data dir, the count, and the
  remediation. That is candidate 1 restricted to the case the issue itself called unambiguous, so
  it needs no config and no override flag: a database that yielded not one decodable value is
  broken by any threshold anyone would pick. A key that decoded and was then dropped for being
  expired counts as decoded — the database is readable, so that boot takes the skip path.
- **A `keys_failed`-above-zero boot is loud.** One `ERROR` naming the data dir and the totals,
  plus `frogdb_recovery_keys_failed_total` (typed handle, `frogdb-types/src/metrics/definitions.rs`)
  incremented by the count, so monitoring can alarm without log scraping.

The *partial*-corruption threshold ("refuse above N%") is deliberately not implemented. It is
policy: it needs a configured fraction, a force-boot override, and an answer for what a replica
should do when it trips. Left open below.

### 2. The INFO load fields stopped lying

`rdb_last_load_keys_loaded` and `rdb_last_load_keys_expired` were hardcoded `0` — the same class
of misleading field as the hardcoded `rdb_last_bgsave_status:ok` from issue 03. Both now report
this boot's real numbers, joined by `rdb_last_load_keys_failed` (a FrogDB extension; Redis has no
counterpart because a failed load is fatal there). The stats reach INFO by riding on `AdminDeps`
next to the snapshot coordinator — plain data fixed at boot, since recovery has finished before
any connection exists. The scripting path's static `INFO` is unchanged and stays issue 10.

Forcing tests: `wholly_undecodable_database_refuses_to_start`,
`partial_decode_failure_is_counted_and_metered`,
`expired_keys_count_as_decoded_so_one_bad_key_does_not_refuse` (recovery seam), and
`persistence_renders_the_real_load_stats` (the INFO render).

Spec: FM-PERSISTENCE-033 retitled and rewritten to the surfaced behavior; the refusal is a new row,
FM-PERSISTENCE-045.

## Remaining scope (why this stays open)

1. **A partial-corruption threshold.** Refuse (or degrade) when `keys_failed` exceeds a configured
   fraction of the total, with a force-boot override. Needs a config knob and a policy decision;
   the 100%-failure case above is the floor, not the whole of candidate 1.
2. **Case 2 in full: "no existing data" vs "wrong data dir"** (candidate 3, the marker file).
   Untouched — a mistyped `--data-dir` still boots as a fresh database. Note the refusal added
   above does *not* help here: a wrong-but-writable directory has no data to fail decoding, so it
   takes the `has_data() == false` path (FM-PERSISTENCE-029) exactly as before.
