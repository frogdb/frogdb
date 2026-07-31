# Persistence (RDB / AOF / WAL / snapshots / tiered durability) — testing gap audit (round 2)

## Scope

| path | src LOC (instrumented) | line cov | notes |
|---|---|---|---|
| `frogdb-server/crates/persistence/` | 6661 lines / 12338 regions instrumented (11.8k raw src LOC) | **92.0%** (6129/6661 lines, 11345/12338 regions) | 17 inline `#[cfg(test)]` modules, **no `tests/` dir** |
| `frogdb-server/crates/core/src/persistence/` | — | — | core-side seam: `test_harness.rs`, `crash_recovery_tests.rs` (1346 lines) |

Depth classes, **deduplicated by `(file, line_start, regions)` taking `max(test_count)`** — the raw
`depth.json` emits one record per monomorphisation, and the `::<_>` records report 0 tests while the
concrete instantiations report 40+. Every "untested" claim below uses the deduped view.

| scope | fns | well-covered | covered | single-test | monoculture | untested |
|---|---|---|---|---|---|---|
| `persistence/src/` | 687 | 258 | 14 | **236** | **126** | 53 |
| `core/src/persistence/` | 144 | 4 | 1 | **103** | 20 | 16 |

Of the 16 untested fns in `core/src/persistence/`, **13 are in `test_harness.rs`** — a large
byte-level crash-testing API with zero call sites (F15).

**AOF does not exist and is out of scope.** `BGREWRITEAOF`/`WAITAOF` are `stub_command!` →
`CommandError::NotImplemented`; INFO reports a hardcoded `aof_enabled:0`; there is no `appendonly`
config parameter. The surface fails closed. Likewise there is **no RDB file format** — "RDB" appears
only in replication full-sync (`create_minimal_rdb`) and in DUMP/RESTORE payloads, both of which use
the same `serialization/` frame audited here. The real durability substrate is: RocksDB column
families (`shard_<n>`, `tiered_warm_<n>`, `search_meta_<n>`) + a FrogDB-level WAL over RocksDB
`WriteBatch` + RocksDB Checkpoint-API snapshots.

## Summary

Persistence is at 92% line coverage and the coverage is broad but **shallow in exactly the places
that matter**: the encode/decode of the 17 on-disk `TypeMarker`s is a per-type single example test,
and the property tests that would generalise it cover **7 of 17 markers, 5 of those asserting only
`len()`**. The one bug this class has already produced in production code (`"00"` persisted as
`"0"`) was caught by the single *byte-exact* string proptest — the same class of bug in a Bloom
filter, TDigest, CMS, TopK, JSON document, vectorset or hash-with-field-expiry would ship silently
today. Two live code defects fall directly out of this audit: `TimeSeriesValue.rules` (TS.CREATERULE
downsampling rules) is **never serialised**, so downsampling silently stops at the next restart; and
`RocksIterator::next` maps a mid-iteration RocksDB error to `None`, which recovery cannot distinguish
from end-of-column-family — a single unreadable block silently truncates a shard's recovered
keyspace and the server comes up "healthy". The suite also **cannot express a crash at an arbitrary
byte offset**: `CrashTestHarness::crash()` drops a handle in-process (the page cache survives, and
the tree says so), `ClusterNode::kill()` is a graceful shutdown, every injected I/O error is
`io::Error::other(&str)` with no `ErrorKind` (so no ENOSPC/EACCES/EIO path is reachable), and the one
true fsync/power-loss model in the repo (`PageCacheSink`) sits behind a `pub(super)` trait,
structurally unreachable from any test outside the `wal` module. The bug that escapes today is a
silent one, discovered by a customer at restart.

## Existing test inventory

| surface | what it covers | notable strengths | notable blind spots |
|---|---|---|---|
| `persistence/src/serialization/*` inline tests | one example per type; header parse; truncation; unknown marker; huge length prefix | `test_deserialize_huge_length_prefix_no_oom`, `marker_bytes_are_stable` pins the 17 wire bytes, `from_byte_rejects_unknown` | content assertions absent for Bloom/Cuckoo/TopK/TDigest/CMS/Json; VectorSet asserts only m/ef/card; TimeSeries asserts only duplicate policy; `copy_codec_round_trips_all_value_variants` hand-counts `values.len() == 15` (drifts silently) |
| `core/tests/proptest_serialization.rs` | 1000 cases/prop; panic-freedom on garbage; round-trip on 7 markers | `roundtrip_string` is **byte-exact** — this is what found the `"00"` bug, seed checked in | 10 of 17 markers have **no** proptest; `roundtrip_{sorted_set,hash,list,set,stream}` assert **length only**; `corrupted_header_doesnt_panic` and `random_type_byte_doesnt_panic` assert nothing at all |
| `persistence/src/recovery.rs` | `round_trips_format_through_mock_sink` (single-test) — hot str/zset/TTL/expired/dup, HLL full + 2 delta operands, stream + group + consumer + PEL, warm-only/stale/expired | genuinely broad for the happy path; exercises the merge-operand fold | the `Err(_) => keys_failed` branch has **zero** coverage; no test asserts what happens when a key fails to deserialise |
| `persistence/src/snapshot/tests.rs` | 14 scheduler tests incl. the double-CAS reschedule handshake and the lost-wakeup double-check | the pure state machine is genuinely well tested — best-tested module in the crate | nothing tests the coordinator's `load_latest_metadata` → `with_epoch` seam, the stager's install/promote/retention ordering, or any partial-write |
| `core/src/persistence/crash_recovery_tests.rs` (1346 lines) | drop-and-reopen recovery, WAL replay, snapshot metadata shapes | large scenario surface | `assert!(result.is_ok() \|\| result.is_err())` (:719); `test_incomplete_snapshot_skipped` never calls the coordinator; module doc claims "ENOSPC, I/O errors" — no such test exists |
| `core/src/persistence/test_harness.rs` | `CrashTestHarness` | — | `crash()` is an in-process drop (page cache survives — acknowledged in-tree at `crash_recovery_tests.rs:122-133`); `corrupt_file`/`append_garbage`/`find_wal_files`/`find_sst_files`/`verify_*`/`simulate_crash` have **zero call sites** |
| `persistence/src/wal/fake.rs` | `FakeFailure::{None, AtWriteIndex(usize), Predicate(fn)}` over `WalEffectKind::{Set,Merge,Delete,Clear,FlushAsync,FlushThrough}` | deterministic, sticky-at-n (a failed write does not advance `write_index`), predicate form can key on payload | `flush_async`/`flush_through` are **infallible**; `durable_sequence()` is documented "synchronously durable"; `lag_stats()` hardcodes `flush_failures: 0, lost_ops: 0, last_flush_ok: true`; **the fake holds no bytes** — only `(kind, key, seq)` tuples, so no byte-level or torn-write scenario is expressible through it |
| `server/tests/integration_persistence.rs` | restart round-trip for string/TTL/list/hash/set/zset, geo-store, sort-store, smove, HLL delta, msetex, bitop, streams + consumer groups, flushdb/flushall, tiered spill | the common types are genuinely covered end-to-end | **no** restart round-trip for JSON, TimeSeries, VectorSet, Bloom, Cuckoo, CMS, TopK, TDigest, or hash field TTLs |
| `server/tests/integration_dump_restore.rs` | DUMP/RESTORE payload round-trip | shares the frame with persistence, so it doubles as format coverage | same type gaps as above |
| `testing/fuzz/` | fuzz targets exist | — | **not wired into CI**; no scheduled or PR job runs them |

## Findings

### F1: No content-level round-trip test for Bloom, Cuckoo, TopK, TDigest, CMS, or JSON
- **Severity** 5 — a field dropped or reordered in any of these encoders is silent data loss discovered only at restart; a sketch that decodes with the right cardinality but wrong registers gives permanently wrong answers with no error.
- **Likelihood** 4 — every restart of any deployment using these types; encoder edits are ordinary maintenance.
- **Effort** 1 — pure unit test next to the existing per-type tests in `serialization/`.
- **Priority** 22
- **Evidence**: `persistence/src/serialization/probabilistic.rs`, `.../search.rs:188` — `boundary_m_ef_round_trips` asserts only `m`, `ef` and cardinality; the vectors themselves, attributes, projection matrix, `uid` and `next_id` are never compared. `registry::every_marker_round_trips` does cover all 17 markers but asserts only `key_type()` equality — a codec that returned an empty value of the right type passes it. The per-type tests are `single-test` class.
- **Proposed test**: for each of Bloom, Cuckoo, TopK, TDigest, CMS, Json, VectorSet, TimeSeries: build a value with non-default parameters and ≥100 inserted items, `serialize` → `deserialize`, and assert **full structural equality** of the decoded value against the original (raw registers/buckets/centroids/heap/vectors/attrs, not just cardinality). For sketches without `PartialEq`, assert on the raw accessor slices the encoder itself reads (`buckets_raw()`, `as_dense()`, centroid list).
- **Boundary**: 1 — pure encode/decode of a value type; no store, no server, no async.

### F2: Round-trip property tests cover 7 of 17 markers, and 5 of those assert length only
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

### F3: `FlushOutcomes::durable_sequence` advances on `sync = false` commits — "durable" means "handed to RocksDB"
- **Severity** 5 — WAIT / LASTSAVE / replication acks built on this number report data as durable that is only in the RocksDB memtable. A power loss loses acknowledged writes. This is a durability violation, not a metrics bug.
- **Likelihood** 4 — `DurabilityMode::Periodic` and `Async` are the non-default-but-common modes; in `Sync` mode the two coincide, which is why no test notices.
- **Effort** 2 — crate-level: drive the `FlushEngine` with a sink that records `sync`, then assert the reported durable sequence.
- **Priority** 21
- **Evidence**: `persistence/src/wal/flush.rs` — `RocksSink::commit` calls `rocks.record_wal_watermark()` only when `result.is_ok() && sync`, but `FlushOutcomes` advances `durable_seq` regardless of `sync`. Round 1 issue 12 (fsync boundary) named this as an unfiled follow-up and it was never filed. `flush()` also early-returns `Ok(())` when `staged_len() == 0` **without** advancing `durable_seq`, so the two notions disagree in both directions.
- **Proposed test**: in `Periodic{interval_ms: 60_000}`, commit N ops, assert `durable_sequence()` does **not** advance past the last fsync'd sequence; then force a periodic sync and assert it advances to exactly N. Separately assert that a no-op `flush()` on an empty stage still reports the correct `durable_seq` rather than a stale one.
- **Boundary**: 2 — the WAL flush engine's own API; a server test could not distinguish the two notions without a power-loss primitive.

### F4: Recovery silently drops any key it cannot deserialise — the `Err` branch has zero coverage
- **Severity** 5 — a binary downgrade after a newer version wrote a newer `TypeMarker` causes the older binary to **drop those keys and start serving happily**. Data loss with a WARN log and a `keys_failed` counter nobody reads. The dropped keys are then permanently lost at the next snapshot.
- **Likelihood** 3 — rolling upgrade/rollback is a plausible ops event; also reachable from any single corrupted value.
- **Effort** 1 — `recover_shard_into` takes a `RestoreSink`; a mock sink plus a hand-written bad value is a unit test.
- **Priority** 20
- **Evidence**: `persistence/src/recovery.rs:113-120` — `Err(e) => { tracing::warn!(...); stats.keys_failed += 1; }`, then the loop continues and the function returns `Ok(stats)`. The only test, `round_trips_format_through_mock_sink` (`recovery.rs:239`, `single-test`, reached only by the `frogdb_persistence::recovery` suite), covers exclusively the `Ok` path. `recover_warm_shard_into` has the identical shape.
- **Proposed test**: seed a CF with 3 good keys and 1 key whose payload is a valid header with an unknown marker; run `recover_shard_into`; assert `keys_loaded == 3`, `keys_failed == 1`, the 3 good keys reached the sink, **and** that the outcome is surfaced — either as a `RecoveryError` or via a metric/`INFO` field an operator can alert on. The test should encode the *decision* (fail-fast vs. continue-and-report), which today is undocumented.
- **Boundary**: 1 — pure function over a `RestoreSink`; no store or server needed.

### F5: No on-disk format version or magic anywhere; the reserved `flags` byte is written 0 and discarded on read
- **Severity** 5 — there is no mechanism by which an older binary can *refuse* to load a newer on-disk layout. It will either drop the keys (F4) or, if a marker byte was reused, decode them as garbage. Silent corruption on downgrade.
- **Likelihood** 3 — a rolling upgrade with a rollback is a normal ops event for a pre-production DB about to add value types.
- **Effort** 1 — assertion on the frame header and on `ColumnFamilyManifest`; a version stamp is a production change, but the *test* that pins today's behaviour and the refusal contract is a unit test.
- **Priority** 20
- **Evidence**: `persistence/src/serialization/mod.rs:90-91` — `// Flags (1 byte) - reserved for future use` / `result.push(0)`; `mod.rs:125` — `let _flags = data[1];`, discarded. `persistence/src/rocks/manifest.rs` `ColumnFamilyManifest::reconcile` enforces shard count (`ShardCountMismatch`) and warm-tier presence (`WarmTierMismatch`) but stamps **no format/version** on disk. Only streams carry a per-type `STREAM_FORMAT_VERSION`.
- **Proposed test**: (a) a golden-bytes test that pins the exact 24-byte header layout `[type][flags][expires_at_ms:i64][lfu][pad:5][payload_len:u64]` and fails on any silent layout change; (b) a test asserting that a frame with a non-zero `flags` byte (i.e. a future format) is **rejected with a distinguishable error**, not silently accepted — this test fails today and documents the missing gate; (c) a manifest test asserting an unrecognised on-disk version stamp refuses to open rather than opening as garbage.
- **Boundary**: 1 — header parsing and manifest reconciliation are both pure functions.

### F6: No restart round-trip at the server level for JSON, TimeSeries, VectorSet, sketches, or hash field TTLs
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

### F7: `TimeSeriesValue.rules` (TS.CREATERULE downsampling rules) is never serialised — rules vanish on restart
- **Severity** 5 — **live bug, not just a test gap.** After a restart, a configured downsampling rule silently stops firing; the destination series simply stops receiving buckets. No error, no log, and `TS.INFO` reports no rules. Data that should have been aggregated is permanently lost.
- **Likelihood** 3 — requires TS.CREATERULE (a normal TimeSeries workflow) plus any restart.
- **Effort** 2 — crate-level round-trip assertion on `TimeSeriesValue`.
- **Priority** 19
- **Evidence**: `types/src/timeseries/value.rs:94-95` — `/// Downsampling rules attached to this source key.` / `rules: Vec<DownsampleRule>`, populated by `add_rule` (:493) from `commands/src/timeseries.rs:1259-1270` (`TS.CREATERULE`). `persistence/src/serialization/timeseries.rs:8-16` documents the complete payload — `retention_ms`, `duplicate_policy`, `chunk_size`, labels, chunks — and the encoder at :30-63 writes exactly those; `rules` is absent. Every `TimeSeriesValue` constructor initialises `rules: Vec::new()`, so decode always yields an empty rule set. The only serialization test for this type asserts the duplicate policy.
- **Proposed test**: build a `TimeSeriesValue`, `add_rule(DownsampleRule::new(dest, bucket_ms, agg))`, round-trip through `serialize`/`deserialize`, assert `rules()` matches. This test fails today. Pair it with a server-level case (folded into F6's single restart test): `TS.CREATERULE src dst AGGREGATION avg 1000`, restart, `TS.ADD src`, assert `dst` receives the bucket.
- **Boundary**: 1 for the codec assertion; the end-to-end confirmation rides along in F6.

### F8: `RocksIterator::next` maps a mid-iteration RocksDB error to `None` — recovery silently truncates a shard
- **Severity** 5 — recovery's `for (key, value) in rocks.iter_cf(shard_id)?` cannot distinguish "error" from "end of column family". One unreadable block (bad SST checksum, or a merge-operator failure) drops **every remaining key in that shard** with `Ok(stats)`, an under-reported `keys_loaded`, no error, no metric and no log. The truncated state then becomes the new truth at the next snapshot.
- **Likelihood** 3 — needs a read error, but the merge path makes one reachable from data alone: `full_value_merge`/`partial_value_merge` (`persistence/src/rocks/mod.rs:607-631`) return `Option<Vec<u8>>`, and `None` becomes a RocksDB Corruption status — reachable from a single undecodable HLL delta operand.
- **Effort** 2 — crate-level, and deterministic: put a good key, merge a garbage HLL operand under a middle key, put another good key, then iterate.
- **Priority** 19
- **Evidence**: `persistence/src/rocks/columns.rs:41-45` —
  ```rust
  fn next(&mut self) -> Option<Self::Item> {
      self.inner.next().and_then(|r| r.ok())
  }
  ```
  The `Item` type is `(Box<[u8]>, Box<[u8]>)`, so the error is structurally unrepresentable. `recovery.rs` consumes this iterator directly. `RocksStore::has_data` (`rocks/mod.rs:531-539`) has the same shape and would report "empty" for an unreadable CF.
- **Proposed test**: seed a CF with keys `a`, `m`, `z`; make `m`'s merge operand undecodable so the merge operator returns `None`; call `recover_shard_into` and assert it does **not** return `Ok` with `keys_loaded == 1` — the shard must either fail loudly or skip only `m`. Also assert `has_data()` does not report an unreadable CF as empty.
- **Boundary**: 2 — needs a real `RocksStore` with the merge operator installed, but no server.

### F9: HLL encode falls back to persisting an **empty** sketch, silently, with `Ok`
- **Severity** 5 — a HyperLogLog that is neither sparse nor dense is written as five zero bytes. The key still exists, PFCOUNT returns 0, and nothing anywhere reports a problem. Silent, unrecoverable data loss on a write path that returns success.
- **Likelihood** 2 — requires a `HyperLogLogValue` in a state the encoder does not recognise; today the type invariants make this hard to reach, which is exactly why nothing guards it.
- **Effort** 1 — a unit test asserting the encoder is total.
- **Priority** 18
- **Evidence**: `persistence/src/serialization/probabilistic.rs:181-183` —
  ```rust
  } else {
      // Shouldn't happen, but fallback to empty sparse
      (TypeMarker::HyperLogLog, vec![0, 0, 0, 0, 0])
  }
  ```
  No log, no metric, no error, and no test reaches this arm.
- **Proposed test**: assert that for every constructible `HyperLogLogValue` state, `as_sparse()` or `as_dense()` is `Some` — i.e. the fallback is provably dead; and assert that a round-trip of a value in each state preserves the register contents byte-exactly. If the arm is genuinely reachable, the test should demand it return an error rather than a silent empty value.
- **Boundary**: 1 — pure encoder.

### F10: The suite cannot express a crash at an arbitrary byte offset
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

### F11: Vacuous and self-referential assertions in `crash_recovery_tests.rs`
- **Severity** 4 — these tests are named for the scenarios an operator most cares about (corrupt snapshot metadata, incomplete snapshot skipped) and would pass with the production code deleted.
- **Likelihood** 3 — corrupt/incomplete metadata after a crash during snapshot install is exactly the case they claim to cover.
- **Effort** 1 — replace the assertions.
- **Priority** 17
- **Evidence**: `core/src/persistence/crash_recovery_tests.rs:719` — `assert!(result.is_ok() || result.is_err());` — a tautology. `crash_recovery_tests.rs:724` `test_incomplete_snapshot_skipped` never invokes the coordinator at all; it re-reads the `metadata.json` the test itself just wrote and asserts `!metadata.is_complete()` — it tests `serde`, not the skip logic. The module doc at :8 claims coverage of "Disk failure scenarios (ENOSPC, I/O errors)"; no such test exists anywhere (see F13).
- **Proposed test**: for corrupt metadata, assert the concrete contract — `RocksSnapshotCoordinator::new` succeeds, the recovered epoch is *not* silently reset (see F12), and a `PersistenceErrors{type=Snapshot}` metric is incremented. For incomplete-snapshot-skipped, actually call `load_latest_metadata` and assert it returns `(epoch, None)` so the incomplete snapshot is not adopted as `last_metadata`.
- **Boundary**: 1 — both are pure functions over a temp directory.

### F12: Snapshot epoch silently resets to 0 on a missing `latest` symlink or unparseable metadata, and the new snapshot is then immediately reaped
- **Severity** 4 — the failure loops: epoch resets to 0 → next snapshot is `snapshot_00001` → `update_latest_symlink` points `latest` at it → `cleanup_old_snapshots` sorts by epoch **ascending** and deletes the oldest, which is now the brand-new snapshot → `latest` dangles → next startup resets to 0 again. The node ends up with no usable snapshot and, worse, deletes the good high-epoch ones it still had.
- **Likelihood** 3 — a crash between metadata write and symlink update, or a truncated `metadata.json` from an un-fsynced write (F20), produces exactly this.
- **Effort** 2 — construct a snapshot dir on disk, corrupt it, and drive the coordinator + stager.
- **Priority** 16
- **Evidence**: `persistence/src/snapshot/rocks_coordinator.rs:43` — `let (ie, lm) = Self::load_latest_metadata(&config.snapshot_dir).unwrap_or((0, None));` swallows every error, including `serde_json` parse failure, into epoch 0; `:49` `SnapshotScheduler::with_epoch(ie)`. `persistence/src/snapshot/stager.rs:125-130` promotes the symlink **before** `:196-220` runs the ascending-epoch retention scan. The only test touching this is the tautology at `crash_recovery_tests.rs:719`.
- **Proposed test**: create `snapshot_00007` + `snapshot_00008` with valid metadata and `latest` → 8; truncate `metadata.json`; construct the coordinator; assert (a) the epoch does **not** regress below 8, and (b) after one save with `max_snapshots = 2`, `latest` resolves to an existing directory containing a complete `metadata.json`. Add a direct stager test that retention never deletes the snapshot `latest` points at.
- **Boundary**: 2 — filesystem-level, no server; the coordinator and stager are directly constructible.

### F13: No test anywhere produces an errno-typed I/O failure — ENOSPC, EACCES, EIO, EDQUOT, EROFS are absent
- **Severity** 4 — disk-full and permission-denied are the two most common real persistence failures. Every injected error in the repo is `io::Error::other(&str)` with `ErrorKind::Other`, so no production path can even *branch* on the errno, and no test asserts what happens when it can't.
- **Likelihood** 3 — a full data volume is a routine ops event.
- **Effort** 2 — a read-only directory gives real `EACCES` portably; a small `tmpfs`/`fallocate`-filled volume or a size-capped file gives real `ENOSPC` on Linux.
- **Priority** 16
- **Evidence**: `crash_recovery_tests.rs:8` claims "ENOSPC, I/O errors" coverage; `rg` finds no `ENOSPC`/`EACCES`/`EIO`/`ErrorKind::` construction in any injected failure across `persistence/` or `core/src/persistence/`. `wal/fake.rs`'s `FakeFailure` variants carry no error payload at all — a failure is a bare `bool` decision, so the *kind* of failure is not expressible.
- **Proposed test**: (a) `chmod 0555` the snapshot dir, trigger BGSAVE, assert the save fails with a distinguishable error, `PersistenceErrors{type=Snapshot}` increments, the previous `latest` is left intact, and the server keeps serving; (b) fill the data volume, issue writes in `Sync` mode, assert the write is **rejected** rather than acknowledged-then-lost, and that recovery after freeing space finds a consistent prefix.
- **Boundary**: 2 for the permission case (pure filesystem); 4 for ENOSPC (needs a size-capped volume in the harness, Linux-only — gate it).

### F14: `PageCacheSink` — the only real fsync/power-loss model in the repo — is structurally unreachable from any test
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

### F15: `CrashTestHarness`'s byte-level and verification API is entirely dead code
- **Severity** 4 — the harness advertises exactly the capabilities F10 says are missing; the next author will read it, believe crash testing is covered, and stop looking.
- **Likelihood** 3 — misleading test infrastructure causes real gaps to go unnoticed indefinitely.
- **Effort** 2 — either wire the helpers into real tests or delete them.
- **Priority** 16
- **Evidence**: deduped depth data shows 13 of `core/src/persistence/test_harness.rs`'s functions as `untested` with **zero call sites anywhere in the repo**: `corrupt_file` (:258, 16 regions), `append_garbage` (:267), `find_files_with_extension` (:280, 25 regions), `find_wal_files` (:296), `find_sst_files` (:301), `simulate_crash` (:190), `create_wal_writer` (:120), `count_keys` (:225), `total_key_count` (:233), `verify_store_contains` (:459), `verify_expiry_index_contains` (:489), `verify_sorted_set` (:501).
- **Proposed test**: use them. `find_sst_files` + `corrupt_file` at a chosen offset + reopen is the missing "corrupted SST" test that pairs with F8; `append_garbage` on a WAL file is the missing truncated/extended-tail test. Whatever remains unused after that should be deleted so the harness's surface reflects its real capability.
- **Boundary**: 2 — crate-level, using the existing harness.

### F16: TTL drift across persist→load — every conversion resamples the wall clock against the monotonic clock
- **Severity** 3 — a key's TTL shifts by the persist→load interval, and a wall-clock jump (NTP step, VM migration, suspend) can expire a key early or resurrect an expired one. User-visible wrong answer on a data path.
- **Likelihood** 4 — every restart applies the conversion; NTP steps are ordinary.
- **Effort** 1 — pure unit test with an injected clock.
- **Priority** 16
- **Evidence**: `persistence/src/serialization/mod.rs:195-241` — `instant_to_unix_ms` / `unix_ms_to_instant` each independently sample `SystemTime::now()` and `Instant::now()` and compute the offset at call time. The only guard is `test_serialize_deserialize_sorted_set_with_expiry` (`mod.rs:498`), which asserts the TTL lands in `58..=62` seconds — a ±2 s window that would not notice a 1.5 s drift, and `test_instant_unix_ms_roundtrip` (:559), which round-trips within a single instant where the offset cannot have moved.
- **Proposed test**: with an injectable clock, serialise a key with a 60 s TTL, advance the wall clock by +30 s **without** advancing the monotonic clock (and separately −30 s), deserialise, and assert the remaining TTL is within ±10 ms of 60 s — i.e. the absolute expiry instant is preserved, not recomputed. Also assert that a key whose `expires_at` is in the past is reported expired regardless of clock direction.
- **Boundary**: 1 — pure conversion functions; needs a clock seam, which is otherwise the only cost.

### F17: Every multi-shard durability test pins `num_shards: Some(1)`
- **Severity** 4 — round 1 issue 13's fix was the `FlushWal` **fan-out**, which is inherently multi-shard, and issue 43's checkpoint cut is a **cross-shard** consistency property. Both are verified at the one shard count where the property is trivially true.
- **Likelihood** 3 — multi-shard is the default production configuration.
- **Effort** 2 — change a parameter and add cross-shard assertions.
- **Priority** 16
- **Evidence**: every issue-13 and issue-42 test sets `num_shards: Some(1)`. `RocksStore::flush` (`rocks/mod.rs:520-530`) loops over all shards and returns on the **first** error, leaving later shards unflushed — a behaviour invisible at one shard. `spawn_periodic_sync` similarly flushes then watermarks across all shards.
- **Proposed test**: with `num_shards = 4`, interleave writes across shards, take a checkpoint, and assert the restored sequence numbers across the four shard CFs form a consistent cut (no shard reflects a write ordered after another shard's cut point). Separately, make shard 2's flush fail and assert shards 3 and 4 are not silently left unflushed with a success return.
- **Boundary**: 2 — crate-level over `RocksStore`; the cross-shard cut property does not need a server.

### F18: Round-1 residue — a tautological pin, and two findings printed instead of asserted
- **Severity** 4 — `wal_recovery_mode_is_pinned_to_point_in_time` is the test standing between the product and a silently-changed WAL recovery mode, and it would pass if the production pin were deleted.
- **Likelihood** 2 — requires someone to change the pin, but that is precisely what the test exists to catch.
- **Effort** 1 — rewrite three assertions.
- **Priority** 15
- **Evidence**: `persistence/src/rocks/tests.rs:1243` asserts `matches!(DBRecoveryMode::PointInTime, DBRecoveryMode::PointInTime)` — a tautology over a literal, not over the value read from the production options at `rocks/mod.rs:167`. Issue 25's type-flip residue and issue 43's cross-shard generation spread are `eprintln!`'d rather than asserted, and nextest discards captured stdout on pass, so nobody will ever see them. Issue 45's `handle_lastsave` truncation fix is guarded by a ±1 s bound that is looser than the bug it fixes. No fuzz target from `testing/fuzz/` is wired into CI. Issues 65 and 66 remain open/`needs-triage`, and two issue files are both numbered 66.
- **Proposed test**: assert on the value actually read back from the constructed `Options` (or from a `#[test]`-visible accessor on the production config path), so deleting the pin fails the test. Convert both `eprintln!` residues into assertions with explicit bounds. Tighten the LASTSAVE bound below the magnitude of the original truncation bug.
- **Boundary**: 1 — all three are unit-level.

### F19: `serialize_value` ends in `unreachable!` — a new `Value` variant compiles and panics at first persist
- **Severity** 4 — an added value type that nobody wired into the encode registry passes `cargo build` and `cargo clippy`, then panics the shard thread the first time that key is written to disk. Decode is safe (exhaustive `match` on `TypeMarker`, no wildcard, so a missing arm is a compile error) — encode is not.
- **Likelihood** 2 — requires adding a value type, but this is a pre-production DB that is still adding them.
- **Effort** 1 — an exhaustiveness test over the registry.
- **Priority** 15
- **Evidence**: `persistence/src/serialization/mod.rs:174-177` — the encode path is a linear scan over the codec registry terminating in `unreachable!("serialization registry has no codec for value of type {:?}", value.key_type())`. The nearest guard, `copy_codec_round_trips_all_value_variants` (`mod.rs:258`), hand-counts `values.len() == 15` — a constant that must be manually bumped and silently under-covers when it isn't.
- **Proposed test**: replace the hand-count with an exhaustive `match` over `Value` inside the test (so adding a variant is a **compile error** in the test, matching the decode side's guarantee), constructing one instance per variant and round-tripping each. Assert the registry claims every variant before the `unreachable!` is reached.
- **Boundary**: 1 — pure.

### F20: The stager fsyncs neither `metadata.json` nor any directory before promoting `latest`
- **Severity** 4 — a power loss after promotion can leave `latest` pointing at a snapshot whose `metadata.json` is zero-length or absent, which on the next startup resets the epoch to 0 and triggers F12's reap loop. Corruption caught at restart.
- **Likelihood** 3 — needs a crash inside the install window, which is small but is exactly when a crash is most likely (heavy I/O).
- **Effort** 4 — asserting an fsync happened needs either syscall interposition or a filesystem seam.
- **Priority** 14
- **Evidence**: `persistence/src/snapshot/stager.rs` — `run()` is `remove_dir_all(tmp)` → `TmpDirGuard` → `stage_checkpoint` → `finalize_metadata` → `install()` (rename tmp → final) → `guard.commit()` → best-effort `update_latest_symlink` → best-effort `cleanup_old_snapshots`. There is no `File::sync_all`, no `sync_data`, and no directory fsync anywhere in the file. `persistence/src/rocks/wal_watermark.rs` has the same shape: `write` is temp+rename and never fsyncs (the module documents that it can only under-report, which bounds the damage there but not here).
- **Proposed test**: route the stager's writes through a small filesystem seam that records `sync_all` calls; assert `metadata.json` is fsynced before `install()` and that the snapshot directory is fsynced before the symlink is repointed. Pair with a durability assertion: after truncating `metadata.json` to zero length, `load_latest_metadata` must not silently yield epoch 0.
- **Boundary**: 2 with the seam; 5 without one (needs syscall interposition). The seam is the cheaper path and is worth introducing.
- **OPTIONS**:
  - *(a)* Introduce a narrow `SnapshotFs` trait the stager writes through, faked in tests to record fsyncs (level 2). Cheap, deterministic, no platform dependency. **Recommended.**
  - *(b)* Rely on F10's subprocess-SIGKILL harness and assert only the *outcome* (post-crash `latest` always resolves to a complete snapshot). No new abstraction, but probabilistic — it will not reliably land inside the install window.

## Deprioritised

- **`SnapshotScheduler` concurrency under shuttle.** The double-CAS handshake is already the best-tested code in the crate (14 unit tests, with the lost-wakeup window reasoned out in the doc comment and driven deterministically via the `pub(super) arm_follow_up` seam). Shuttle would add cost without a plausible new finding.
- **`ColumnFamilyManifest::reconcile` example coverage.** Nine unit tests cover both invariants and the legal off→on warm transition. The gap is the *missing version stamp* (F5), not the reconciliation logic.
- **`TmpDirGuard` RAII cleanup.** Straightforward Drop-based cleanup with a `commit()` escape; a dedicated test would assert the obvious.
- **AOF.** Does not exist; `BGREWRITEAOF`/`WAITAOF` fail closed with `NotImplemented` and INFO reports constants. Testing a stub asserts nothing. Revisit if AOF is implemented — "rewrite atomicity while writes continue" then becomes a top-priority finding.
- **`RocksIterator` error handling for the *warm* tier specifically.** Same defect as F8; fixing F8 fixes both. Not filed separately.
- **`marker.rs` coverage.** Four tests already pin the wire bytes, exhaustiveness, uniqueness, and unknown rejection — this file is a model for what F1 should look like elsewhere.
- **Metrics assertions on snapshot duration/size.** Wrong values are severity 2 and the code paths are trivial `set` calls.
- **`serialization/mod.rs` OOM guard.** `test_deserialize_huge_length_prefix_no_oom` already covers the adversarial length prefix, and the fuzz targets extend it. The gap is CI wiring (folded into F18), not the test.

## Cross-area notes

- **`testing/fuzz/` is not wired into CI.** No PR or nightly job runs any fuzz target. This is shared infrastructure and affects every crate with a parser or decoder, not just persistence. Recommend a nightly job with a checked-in corpus; persistence's decode entry points (`deserialize`, each per-type decoder, `RESTORE` payloads) are the highest-value targets.
- **A crash primitive is shared infrastructure** (F10). Replication, cluster and persistence all need "kill the process at point X and reopen". Recommend the subprocess-SIGKILL harness live in `frogdb-test-harness` next to `TestServer`, not in `core/src/persistence/test_harness.rs`. Note that `ClusterNode::kill()` is currently a *graceful* shutdown, which is a misleading name the cluster agent should also see.
- **An injectable clock seam** (F16) would serve expiry, TTL, replication timeouts, and cluster elections. If another agent proposes one, F16 should adopt it rather than adding a second.
- **`TimeSeriesValue.rules` (F7) is a live product bug**, not only a test gap — the type lives in `frogdb-types` and the rule is applied from `frogdb-commands`, so the fix spans three crates. Flagging for whoever owns the timeseries command surface.
- **`Value` encode exhaustiveness (F19)** is a `frogdb-types` ↔ `frogdb-persistence` contract. If the types agent proposes a derive or registry macro for value variants, wiring encode exhaustiveness into it closes F19 structurally rather than by test.
- **Issue-file hygiene**: two files under `.scratch/testing-improvements/issues/` are both numbered 66; issues 65 and 66 are still open/`needs-triage`. Coordinator's call.
