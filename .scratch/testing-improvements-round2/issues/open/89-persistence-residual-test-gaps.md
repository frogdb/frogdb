# persistence — residual test gaps (12 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/13 — residual findings after promotion to issues 19–76
Score: 12 findings, priority range 15–22
Area: `frogdb-server/crates/persistence/` (RDB / WAL / snapshots / tiered durability) + the core-side seam `frogdb-server/crates/core/src/persistence/`

## Context

This area is FrogDB's real durability substrate — RocksDB column families (`shard_<n>`,
`tiered_warm_<n>`, `search_meta_<n>`), a FrogDB-level WAL over RocksDB `WriteBatch`, and
RocksDB Checkpoint-API snapshots — plus the serialization frame shared with DUMP/RESTORE and
replication full-sync. `persistence/` is 6661 instrumented lines / 12338 regions at **92.0%**
line coverage (6129/6661 lines, 11345/12338 regions) across 17 inline `#[cfg(test)]` modules
and **no `tests/` dir**; the core-side seam adds `test_harness.rs` and
`crash_recovery_tests.rs` (1346 lines). Deduped depth over `persistence/src/` shows 687
functions: 258 `well-covered`, 236 `single-test`, 126 `monoculture`, 53 `untested`; over
`core/src/persistence/`, 144 functions with 103 `single-test` and 16 `untested`, **13 of
which are in `test_harness.rs`** — a byte-level crash-testing API with zero call sites (F15).
The proposal's verdict on the shape of that coverage: it is "broad but **shallow in exactly
the places that matter**" — the encode/decode of the 17 on-disk `TypeMarker`s is a per-type
single example test, the property tests that would generalise it cover 7 of 17 markers with 5
of those asserting only `len()`, and "the suite **cannot express a crash at an arbitrary byte
offset**". AOF does not exist and is out of scope (`BGREWRITEAOF`/`WAITAOF` are
`stub_command!` → `NotImplemented`; the surface fails closed). The proposal's
`## Deprioritised` section carries no F-numbers, so nothing there is a finding; it records
`SnapshotScheduler` shuttle coverage, `ColumnFamilyManifest::reconcile` examples,
`TmpDirGuard` RAII, AOF, warm-tier `RocksIterator` (same defect as F8, fixed by fixing F8),
`marker.rs`, snapshot metrics, and the `serialization/mod.rs` OOM guard as deliberately not
filed.

## Promoted elsewhere

- F3 → issue 71, `.scratch/testing-improvements-round2/issues/` (`FlushOutcomes::durable_sequence` advances on `sync = false` commits — "durable" means "handed to RocksDB").
- F5 → issue 72, `.scratch/testing-improvements-round2/issues/` (no on-disk format version or magic anywhere; the reserved `flags` byte is written 0 and discarded on read).
- F7 → issue 44, `.scratch/testing-improvements-round2/issues/` (`TS.CREATERULE` downsampling rules are never serialised and die on every restart — found independently by two agents, 07/F2 and 13/F7).
- F8 → issue 42, `.scratch/testing-improvements-round2/issues/` (`RocksIterator::next` maps a mid-iteration RocksDB error to `None` → silent shard truncation) **and** issue 20, `.scratch/testing-improvements-round2/issues/` (theme T2 — failure of a derived structure reported as success).
- F9 → issue 20, `.scratch/testing-improvements-round2/issues/` (theme T2 — HLL encode falls back to persisting an empty sketch, silently, with `Ok`).
- F11 → issue 33, `.scratch/testing-improvements-round2/issues/` (§4 tests that cannot fail — vacuous and self-referential assertions throughout `crash_recovery_tests.rs`).
- F18 → issue 33, `.scratch/testing-improvements-round2/issues/` (§4 — round-1's WAL-mode pin is tautological and two findings are `eprintln!`'d where an assert belongs).
- F20 → issue 74, `.scratch/testing-improvements-round2/issues/` (the stager fsyncs neither `metadata.json` nor any directory before promoting `latest`).

## Residual findings

### F1 — No content-level round-trip test for Bloom, Cuckoo, TopK, TDigest, CMS, or JSON

- **Severity** 5 — a field dropped or reordered in any of these encoders is silent data loss discovered only at restart; a sketch that decodes with the right cardinality but wrong registers gives permanently wrong answers with no error.
- **Likelihood** 4 — every restart of any deployment using these types; encoder edits are ordinary maintenance.
- **Effort** 1 — pure unit test next to the existing per-type tests in `serialization/`.
- **Priority** 22
- **Evidence**: `persistence/src/serialization/probabilistic.rs`, `.../search.rs:188` — `boundary_m_ef_round_trips` asserts only `m`, `ef` and cardinality; the vectors themselves, attributes, projection matrix, `uid` and `next_id` are never compared. `registry::every_marker_round_trips` does cover all 17 markers but asserts only `key_type()` equality — a codec that returned an empty value of the right type passes it. The per-type tests are `single-test` class.
- **Proposed test**: for each of Bloom, Cuckoo, TopK, TDigest, CMS, Json, VectorSet, TimeSeries: build a value with non-default parameters and ≥100 inserted items, `serialize` → `deserialize`, and assert **full structural equality** of the decoded value against the original (raw registers/buckets/centroids/heap/vectors/attrs, not just cardinality). For sketches without `PartialEq`, assert on the raw accessor slices the encoder itself reads (`buckets_raw()`, `as_dense()`, centroid list).
- **Boundary**: 1 — pure encode/decode of a value type; no store, no server, no async.

### F2 — Round-trip property tests cover 7 of 17 markers, and 5 of those assert length only

- **Severity** 5 — this is the exact generalisation of the `"00"` bug (commit `b353d9de`: `serialize_string` re-derived integer-ness from rendered bytes, so `"00"`, `"+5"`, `"-0"` were written as `StringInt` and canonicalised on load — silent byte corruption of persisted state). The property test that caught it exists **only for strings**.
- **Likelihood** 4 — encoder changes are routine; the collection props would not have caught the equivalent bug in a hash field name or a stream entry field.
- **Effort** 2 — extend an existing proptest file; needs `Arbitrary` strategies for the remaining value types (the crate-level work is generating the values, not the assertion).
- **Priority** 21
- **Evidence**: `core/tests/proptest_serialization.rs` — `roundtrip_string` and `roundtrip_integer_string` are byte-exact; `roundtrip_sorted_set`/`_hash`/`_list`/`_set`/`_stream` assert `len()` equality and nothing else; `corrupted_header_doesnt_panic` (:268) and `random_type_byte_doesnt_panic` (:287) contain no assertion beyond not panicking. There is no proptest at all for Bloom, Cuckoo, TopK, TDigest, CMS, HLL, TimeSeries, Json, VectorSet, HashWithFieldExpiry, or stream consumer groups.
- **Proposed test**: one `proptest` per `TypeMarker` asserting **byte-exact re-serialisation** — `serialize(deserialize(serialize(v))) == serialize(v)` *and* structural equality of the decoded value — over generated values that include the adversarial shapes the `"00"` bug lived in: leading zeros, `+`/`-` prefixes, `-0`, `i64::MIN`/`MAX` boundaries, empty and 8-bit-clean binary field names, NaN/±inf scores, duplicate members, and empty collections. Plus a meta-test that fails when a new `TypeMarker` is added without a corresponding proptest (iterate `TypeMarker::all()`).
- **Boundary**: 2 — crate-level; property generation over the public value types, no server. This is a property/fuzz gap, not an example-test gap: the failure mode is a *rare input*, not a rare code path.
- **OPTIONS**:
  - *(a)* Extend `core/tests/proptest_serialization.rs` with per-type strategies — matches where the string proptest already lives, keeps regression seeds in one place. **Recommended.**
  - *(b)* Move to a `persistence/tests/` proptest (the crate currently has no `tests/` dir) — better locality to the code under test, but splits the seed corpus and duplicates the `Arbitrary` strategies that `frogdb-core` tests already need.
  - *(c)* Wire the existing `testing/fuzz/` targets into CI instead — broader coverage of *decode*, but fuzzing does not check round-trip fidelity, only panic-freedom. Complementary, not a substitute.

Note: 15/F3 (issue 91, `.scratch/testing-improvements-round2/issues/`) claims the *types-side*
residue of the same gap — the compile-time totality guard over the `Value` enum. Dedupe on the
proptest body; both areas want the guard.

### F4 — Recovery silently drops any key it cannot deserialise — the `Err` branch has zero coverage

- **Severity** 5 — a binary downgrade after a newer version wrote a newer `TypeMarker` causes the older binary to **drop those keys and start serving happily**. Data loss with a WARN log and a `keys_failed` counter nobody reads. The dropped keys are then permanently lost at the next snapshot.
- **Likelihood** 3 — rolling upgrade/rollback is a plausible ops event; also reachable from any single corrupted value.
- **Effort** 1 — `recover_shard_into` takes a `RestoreSink`; a mock sink plus a hand-written bad value is a unit test.
- **Priority** 20
- **Evidence**: `persistence/src/recovery.rs:113-120` — `Err(e) => { tracing::warn!(...); stats.keys_failed += 1; }`, then the loop continues and the function returns `Ok(stats)`. The only test, `round_trips_format_through_mock_sink` (`recovery.rs:239`, `single-test`, reached only by the `frogdb_persistence::recovery` suite), covers exclusively the `Ok` path. `recover_warm_shard_into` has the identical shape.
- **Proposed test**: seed a CF with 3 good keys and 1 key whose payload is a valid header with an unknown marker; run `recover_shard_into`; assert `keys_loaded == 3`, `keys_failed == 1`, the 3 good keys reached the sink, **and** that the outcome is surfaced — either as a `RecoveryError` or via a metric/`INFO` field an operator can alert on. The test should encode the *decision* (fail-fast vs. continue-and-report), which today is undocumented.
- **Boundary**: 1 — pure function over a `RestoreSink`; no store or server needed.

### F6 — No restart round-trip at the server level for JSON, TimeSeries, VectorSet, sketches, or hash field TTLs

- **Severity** 5 — a type that only survives restart by luck is silent data loss. These types have no end-to-end evidence they survive at all.
- **Likelihood** 4 — any restart of a deployment using them.
- **Effort** 3 — `integration_persistence.rs` already has the restart pattern; this is adding cases to an existing harness.
- **Priority** 20
- **Evidence**: `server/tests/integration_persistence.rs` covers string/TTL/list/hash/set/zset, geo-store, sort-store, smove, HLL delta, msetex, bitop, streams + consumer groups, flushdb/flushall, and tiered spill — and nothing else. JSON.*, TS.*, VADD/VSIM, BF.*, CF.*, CMS.*, TOPK.*, TDIGEST.*, and HEXPIRE/hash-field-TTL have no restart case anywhere.
- **Proposed test**: for each type, write via RESP, restart the server, read back and assert the **full observable state** (JSON.GET of the whole document; TS.RANGE over all samples plus TS.INFO retention/labels/rules; VSIM neighbour ordering; BF.EXISTS on 100 inserted + 100 absent members with the false-positive bound respected; CMS.QUERY counts; TOPK.LIST order; TDIGEST.QUANTILE within tolerance; HTTL remaining within ±1 s).
- **Boundary**: 4 — genuinely needs process lifecycle. But see OPTIONS: most of the *fidelity* risk is already covered by F1/F2 at level 1–2, and doing it at level 4 for all eight types is slow.
- **OPTIONS**:
  - *(a)* Full server restart per type (level 4) — highest confidence, catches store/CF-routing bugs the codec tests cannot, but ~8 slow tests.
  - *(b)* Level 1–2 codec fidelity (F1/F2) plus **one** server restart test that writes *all* eight types then restarts once and verifies them all — one slow test, near-equal coverage. **Recommended.**
  - *(c)* `shard_driver` harness with a real `RocksStore` reopen (level 3) — faster, exercises real recovery, but does not exercise the command layer's read-back path.

Note: the proposal folds the end-to-end confirmation of the promoted F7 (`TS.CREATERULE`
rules, issue 44, `.scratch/testing-improvements-round2/issues/`) into F6's single restart
test — `TS.CREATERULE src dst AGGREGATION avg 1000`, restart, `TS.ADD src`, assert `dst`
receives the bucket.

### F10 — The suite cannot express a crash at an arbitrary byte offset

- **Severity** 5 — torn writes, truncated tails and partial fsyncs are *the* durability failure mode, and there is no test anywhere in the repo that produces one during writing. Whatever the recovery code does at those interruption points is unknown.
- **Likelihood** 4 — power loss / OOM-kill / container SIGKILL are ordinary failures for a database.
- **Effort** 5 — needs a new fault primitive.
- **Priority** 18
- **Evidence**: `core/src/persistence/test_harness.rs:183-190` — `CrashTestHarness::crash()` drops the handle **in-process**, so the OS page cache survives; the tree admits this at `crash_recovery_tests.rs:122-133`. `ClusterNode::kill()` is a graceful shutdown. Nothing in the repo forks a child or sends a signal. The only byte-level manipulation, `rocks/tests.rs:1155-1240`, corrupts a **quiesced** file after a clean drop — i.e. it tests "corrupt file on disk", not "crash mid-write". `wal/fake.rs` holds only `(kind, key, seq)` tuples — no bytes — so it cannot express a partial record either.
- **Proposed test**: a parameterised harness that, for a write workload, interrupts at offset *k* of the *n*-th physical write, then reopens and asserts the **prefix property**: every acknowledged write up to the last durable sequence is present, no un-acknowledged write is present, and no key decodes to a value that was never written (no torn frame accepted as valid).
- **Boundary**: 5 — new infrastructure.
- **OPTIONS**:
  - *(a)* **Subprocess + `SIGKILL`** at a controlled point (env-var-driven kill switch in a test binary). Real page-cache semantics, real RocksDB recovery. Cannot target a byte offset — only a logical point. Cheapest real crash. **Recommended as step 1.**
  - *(b)* **Expose `PageCacheSink`** (see F14) so `frogdb-core` and the `shard_driver` harness can model "written but not fsynced" and discard the unflushed tail. Deterministic, fast, no subprocess — but it models FrogDB's own WAL only, not RocksDB's internals.
  - *(c)* **Fault-injecting VFS / `libfiu`-style syscall interposition** for true arbitrary-offset torn writes including inside RocksDB. Highest fidelity, highest cost, platform-specific.
  - Recommendation: (a) + (b) together cover the acknowledged-write prefix property, which is the contract that matters; defer (c).

### F12 — Snapshot epoch silently resets to 0 on a missing `latest` symlink or unparseable metadata, and the new snapshot is then immediately reaped

- **Severity** 4 — the failure loops: epoch resets to 0 → next snapshot is `snapshot_00001` → `update_latest_symlink` points `latest` at it → `cleanup_old_snapshots` sorts by epoch **ascending** and deletes the oldest, which is now the brand-new snapshot → `latest` dangles → next startup resets to 0 again. The node ends up with no usable snapshot and, worse, deletes the good high-epoch ones it still had.
- **Likelihood** 3 — a crash between metadata write and symlink update, or a truncated `metadata.json` from an un-fsynced write (F20), produces exactly this.
- **Effort** 2 — construct a snapshot dir on disk, corrupt it, and drive the coordinator + stager.
- **Priority** 16
- **Evidence**: `persistence/src/snapshot/rocks_coordinator.rs:43` — `let (ie, lm) = Self::load_latest_metadata(&config.snapshot_dir).unwrap_or((0, None));` swallows every error, including `serde_json` parse failure, into epoch 0; `:49` `SnapshotScheduler::with_epoch(ie)`. `persistence/src/snapshot/stager.rs:125-130` promotes the symlink **before** `:196-220` runs the ascending-epoch retention scan. The only test touching this is the tautology at `crash_recovery_tests.rs:719`.
- **Proposed test**: create `snapshot_00007` + `snapshot_00008` with valid metadata and `latest` → 8; truncate `metadata.json`; construct the coordinator; assert (a) the epoch does **not** regress below 8, and (b) after one save with `max_snapshots = 2`, `latest` resolves to an existing directory containing a complete `metadata.json`. Add a direct stager test that retention never deletes the snapshot `latest` points at.
- **Boundary**: 2 — filesystem-level, no server; the coordinator and stager are directly constructible.

### F13 — No test anywhere produces an errno-typed I/O failure — ENOSPC, EACCES, EIO, EDQUOT, EROFS are absent

- **Severity** 4 — disk-full and permission-denied are the two most common real persistence failures. Every injected error in the repo is `io::Error::other(&str)` with `ErrorKind::Other`, so no production path can even *branch* on the errno, and no test asserts what happens when it can't.
- **Likelihood** 3 — a full data volume is a routine ops event.
- **Effort** 2 — a read-only directory gives real `EACCES` portably; a small `tmpfs`/`fallocate`-filled volume or a size-capped file gives real `ENOSPC` on Linux.
- **Priority** 16
- **Evidence**: `crash_recovery_tests.rs:8` claims "ENOSPC, I/O errors" coverage; `rg` finds no `ENOSPC`/`EACCES`/`EIO`/`ErrorKind::` construction in any injected failure across `persistence/` or `core/src/persistence/`. `wal/fake.rs`'s `FakeFailure` variants carry no error payload at all — a failure is a bare `bool` decision, so the *kind* of failure is not expressible.
- **Proposed test**: (a) `chmod 0555` the snapshot dir, trigger BGSAVE, assert the save fails with a distinguishable error, `PersistenceErrors{type=Snapshot}` increments, the previous `latest` is left intact, and the server keeps serving; (b) fill the data volume, issue writes in `Sync` mode, assert the write is **rejected** rather than acknowledged-then-lost, and that recovery after freeing space finds a consistent prefix.
- **Boundary**: 2 for the permission case (pure filesystem); 4 for ENOSPC (needs a size-capped volume in the harness, Linux-only — gate it).

### F14 — `PageCacheSink` — the only real fsync/power-loss model in the repo — is structurally unreachable from any test

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`) —
`MASTER.md` §5 lists `PageCacheSink` as unreachable because `WriteSink` is `pub(super)`. §5
cites no finding numbers, so it claims nothing on its own; this finding's recommendation is to
**export** the seam rather than delete it, and the two should be reconciled before either is
actioned.

- **Severity** 4 — the abstraction that could make durability testable exists and is walled off; every durability test outside the `wal` module is therefore forced to use the fake sink, which is documented as "synchronously durable" and hardcodes `flush_failures: 0, lost_ops: 0, last_flush_ok: true`.
- **Likelihood** 3 — this shapes every durability test written from now on, so the cost compounds.
- **Effort** 2 — visibility change plus a re-export; no new machinery.
- **Priority** 16
- **Evidence**: `persistence/src/wal/flush.rs` — the `WriteSink` trait is `pub(super)`, module-private to `wal`. It is the **only** seam that sees the `sync: bool`. `frogdb-core`, `core/tests/shard_driver/`, and every server integration test are all outside it.
- **Proposed test**: not a test — a seam. Export `WriteSink` + `PageCacheSink` under the existing `test-support` feature (alongside `sync_wal` and `commit_raw_batch`, which are already `#[cfg(any(test, feature = "test-support"))]` in `rocks/mod.rs:540+`), then add a `shard_driver` case: write in `Periodic` mode, drop the unflushed page-cache tail, reopen, assert every write below `durable_sequence()` survived and nothing above it did (this is the assertion F3 and F10 both need).
- **Boundary**: 3 — `shard_driver` is the right level: real command dispatch and real WAL seam, no socket.
- **OPTIONS**:
  - *(a)* Export under the existing `test-support` feature — consistent with `sync_wal`/`commit_raw_batch`, zero release-build impact. **Recommended.**
  - *(b)* Make `WriteSink` fully `pub` — simpler, but commits a durability-internal trait to the crate's public API.
  - *(c)* Add a `FakeFailure`-style byte-holding variant to `wal/fake.rs` instead — keeps the seam private, but re-implements page-cache semantics in a second place that can drift from the real one.

### F15 — `CrashTestHarness`'s byte-level and verification API is entirely dead code

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`) —
`MASTER.md` §5 lists the `CrashTestHarness` byte-level verify API (13 fns, zero call sites).
§5 cites no finding numbers, so it claims nothing on its own; this finding's proposal is
"use them first, delete only what remains unused", and `INFRASTRUCTURE.md`'s I2 entry names
the same harness as the *cheaper substitute* for a real SIGKILL primitive — so deletion and
issue 02 are in tension and should be decided together.

- **Severity** 4 — the harness advertises exactly the capabilities F10 says are missing; the next author will read it, believe crash testing is covered, and stop looking.
- **Likelihood** 3 — misleading test infrastructure causes real gaps to go unnoticed indefinitely.
- **Effort** 2 — either wire the helpers into real tests or delete them.
- **Priority** 16
- **Evidence**: deduped depth data shows 13 of `core/src/persistence/test_harness.rs`'s functions as `untested` with **zero call sites anywhere in the repo**: `corrupt_file` (:258, 16 regions), `append_garbage` (:267), `find_files_with_extension` (:280, 25 regions), `find_wal_files` (:296), `find_sst_files` (:301), `simulate_crash` (:190), `create_wal_writer` (:120), `count_keys` (:225), `total_key_count` (:233), `verify_store_contains` (:459), `verify_expiry_index_contains` (:489), `verify_sorted_set` (:501).
- **Proposed test**: use them. `find_sst_files` + `corrupt_file` at a chosen offset + reopen is the missing "corrupted SST" test that pairs with F8; `append_garbage` on a WAL file is the missing truncated/extended-tail test. Whatever remains unused after that should be deleted so the harness's surface reflects its real capability.
- **Boundary**: 2 — crate-level, using the existing harness.

### F16 — TTL drift across persist→load — every conversion resamples the wall clock against the monotonic clock

- **Severity** 3 — a key's TTL shifts by the persist→load interval, and a wall-clock jump (NTP step, VM migration, suspend) can expire a key early or resurrect an expired one. User-visible wrong answer on a data path.
- **Likelihood** 4 — every restart applies the conversion; NTP steps are ordinary.
- **Effort** 1 — pure unit test with an injected clock.
- **Priority** 16
- **Evidence**: `persistence/src/serialization/mod.rs:195-241` — `instant_to_unix_ms` / `unix_ms_to_instant` each independently sample `SystemTime::now()` and `Instant::now()` and compute the offset at call time. The only guard is `test_serialize_deserialize_sorted_set_with_expiry` (`mod.rs:498`), which asserts the TTL lands in `58..=62` seconds — a ±2 s window that would not notice a 1.5 s drift, and `test_instant_unix_ms_roundtrip` (:559), which round-trips within a single instant where the offset cannot have moved.
- **Proposed test**: with an injectable clock, serialise a key with a 60 s TTL, advance the wall clock by +30 s **without** advancing the monotonic clock (and separately −30 s), deserialise, and assert the remaining TTL is within ±10 ms of 60 s — i.e. the absolute expiry instant is preserved, not recomputed. Also assert that a key whose `expires_at` is in the past is reported expired regardless of clock direction.
- **Boundary**: 1 — pure conversion functions; needs a clock seam, which is otherwise the only cost.

### F17 — Every multi-shard durability test pins `num_shards: Some(1)`

- **Severity** 4 — round 1 issue 13's fix was the `FlushWal` **fan-out**, which is inherently multi-shard, and issue 43's checkpoint cut is a **cross-shard** consistency property. Both are verified at the one shard count where the property is trivially true.
- **Likelihood** 3 — multi-shard is the default production configuration.
- **Effort** 2 — change a parameter and add cross-shard assertions.
- **Priority** 16
- **Evidence**: every issue-13 and issue-42 test sets `num_shards: Some(1)`. `RocksStore::flush` (`rocks/mod.rs:520-530`) loops over all shards and returns on the **first** error, leaving later shards unflushed — a behaviour invisible at one shard. `spawn_periodic_sync` similarly flushes then watermarks across all shards.
- **Proposed test**: with `num_shards = 4`, interleave writes across shards, take a checkpoint, and assert the restored sequence numbers across the four shard CFs form a consistent cut (no shard reflects a write ordered after another shard's cut point). Separately, make shard 2's flush fail and assert shards 3 and 4 are not silently left unflushed with a success return.
- **Boundary**: 2 — crate-level over `RocksStore`; the cross-shard cut property does not need a server.

### F19 — `serialize_value` ends in `unreachable!` — a new `Value` variant compiles and panics at first persist

- **Severity** 4 — an added value type that nobody wired into the encode registry passes `cargo build` and `cargo clippy`, then panics the shard thread the first time that key is written to disk. Decode is safe (exhaustive `match` on `TypeMarker`, no wildcard, so a missing arm is a compile error) — encode is not.
- **Likelihood** 2 — requires adding a value type, but this is a pre-production DB that is still adding them.
- **Effort** 1 — an exhaustiveness test over the registry.
- **Priority** 15
- **Evidence**: `persistence/src/serialization/mod.rs:174-177` — the encode path is a linear scan over the codec registry terminating in `unreachable!("serialization registry has no codec for value of type {:?}", value.key_type())`. The nearest guard, `copy_codec_round_trips_all_value_variants` (`mod.rs:258`), hand-counts `values.len() == 15` — a constant that must be manually bumped and silently under-covers when it isn't.
- **Proposed test**: replace the hand-count with an exhaustive `match` over `Value` inside the test (so adding a variant is a **compile error** in the test, matching the decode side's guarantee), constructing one instance per variant and round-tripping each. Assert the registry claims every variant before the `unreachable!` is reached.
- **Boundary**: 1 — pure.

Cross-area: the proposal notes F19 is a `frogdb-types` ↔ `frogdb-persistence` contract — if the
types agent proposes a derive or registry macro for value variants, wiring encode
exhaustiveness into it closes F19 structurally rather than by test. 15/F3 (issue 91,
`.scratch/testing-improvements-round2/issues/`) asks for the same compile-time totality guard
from the types side.

## Acceptance criteria

- [ ] F1: for each of Bloom, Cuckoo, TopK, TDigest, CMS, Json, VectorSet and TimeSeries, a test builds a value with non-default parameters and ≥100 inserted items and asserts full structural equality (raw registers/buckets/centroids/heap/vectors/attrs) of `deserialize(serialize(v))` against `v` — not merely `key_type()` or cardinality.
- [ ] F2: a proptest exists per `TypeMarker` asserting `serialize(deserialize(serialize(v))) == serialize(v)` **and** structural equality of the decoded value, over generators covering leading zeros, `+`/`-` prefixes, `-0`, `i64::MIN`/`MAX`, empty and 8-bit-clean binary field names, NaN/±inf scores, duplicate members and empty collections; plus a meta-test over `TypeMarker::all()` that fails when a marker has no proptest.
- [ ] F4: a test seeds a CF with 3 good keys plus one valid-header/unknown-marker key, runs `recover_shard_into`, and asserts `keys_loaded == 3`, `keys_failed == 1`, the 3 good keys reached the sink, and that the failure is surfaced as an error or an operator-visible metric/INFO field rather than only a WARN log.
- [ ] F6: one restart test writes JSON, TimeSeries, VectorSet, Bloom, Cuckoo, CMS, TopK, TDigest and hash-field TTLs, restarts the server, and asserts full observable state per type (JSON.GET whole document; TS.RANGE + TS.INFO retention/labels/rules; VSIM neighbour ordering; BF.EXISTS over 100 present + 100 absent within the false-positive bound; CMS.QUERY counts; TOPK.LIST order; TDIGEST.QUANTILE within tolerance; HTTL within ±1 s).
- [ ] F10: a parameterised crash harness interrupts a write workload at offset *k* of the *n*-th physical write, reopens, and asserts the prefix property — every acknowledged write up to the last durable sequence present, no un-acknowledged write present, and no torn frame accepted as a valid value.
- [ ] F12: a test with `snapshot_00007` + `snapshot_00008`, `latest` → 8 and a truncated `metadata.json` asserts the coordinator's epoch does not regress below 8, and that after one save with `max_snapshots = 2` `latest` resolves to an existing directory containing a complete `metadata.json`; plus a stager test asserting retention never deletes the snapshot `latest` points at.
- [ ] F13: a test with a `chmod 0555` snapshot dir asserts BGSAVE fails with a distinguishable error, `PersistenceErrors{type=Snapshot}` increments, the previous `latest` is intact and the server keeps serving; and a (Linux-gated) ENOSPC test asserts a `Sync`-mode write is rejected rather than acknowledged-then-lost and that recovery finds a consistent prefix.
- [ ] F14: `WriteSink` + `PageCacheSink` are reachable from outside the `wal` module under the `test-support` feature, and a `shard_driver` case writes in `Periodic` mode, drops the unflushed page-cache tail, reopens, and asserts every write below `durable_sequence()` survived and nothing above it did.
- [ ] F15: each of the 13 listed `test_harness.rs` functions either has at least one real call site in a test that asserts an outcome (including a `find_sst_files` + `corrupt_file` + reopen case and an `append_garbage`-on-WAL case) or has been deleted.
- [ ] F16: a test with an injectable clock serialises a key with a 60 s TTL, advances the wall clock ±30 s without advancing the monotonic clock, deserialises, and asserts the remaining TTL is within ±10 ms of 60 s; and that a key with a past `expires_at` is reported expired regardless of clock direction.
- [ ] F17: a test at `num_shards = 4` interleaves writes across shards, takes a checkpoint and asserts the restored per-CF sequence numbers form a consistent cut; and a second test makes shard 2's flush fail and asserts shards 3 and 4 are not left unflushed behind a success return.
- [ ] F19: `copy_codec_round_trips_all_value_variants` (or its successor) uses an exhaustive `match` over `Value` so that adding a variant is a compile error, constructs one instance per variant, and round-trips each — the `values.len() == 15` hand-count is gone.

## Depends on

- issue 02, `.scratch/testing-improvements-round2/issues/` (I2 — subprocess-SIGKILL crash primitive; F10's recommended step 1, and the only way to reach real page-cache semantics. Note the tension recorded on F15: `INFRASTRUCTURE.md` names `CrashTestHarness` as I2's *cheaper substitute*, and this proposal asks that if built it live in `frogdb-test-harness` next to `TestServer`, not in `core/src/persistence/test_harness.rs`).
- issue 03, `.scratch/testing-improvements-round2/issues/` (I3 — injectable clock seam; F16 is unwritable without one. The proposal's explicit request: whoever builds one first owns it, and F16 adopts it rather than adding a second).
- issue 10, `.scratch/testing-improvements-round2/issues/` (I10 — fuzz CI; F2's option (c) and the proposal's cross-area note that `testing/fuzz/` runs in no PR or nightly job. Highest-value targets named by this area: `deserialize`, each per-type decoder, `RESTORE` payloads).
