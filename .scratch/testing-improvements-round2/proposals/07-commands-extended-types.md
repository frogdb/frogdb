# frogdb-commands, extended / probabilistic / structured types — testing gap audit (round 2)

## Scope

Audited `frogdb-server/crates/commands/src/`: `stream/` (basic, read, pending, consumer_groups,
info, mod), `json/`, `vectorset/`, `event_sourcing/`, `bitmap.rs`, `geo.rs`, `timeseries.rs`,
`hyperloglog.rs`, `bloom.rs`, `cms.rs`, `cuckoo.rs`, `topk.rs`, `tdigest.rs`. Backing types were
read where the command is a thin shell: `types/src/{json,geo,vectorset,hyperloglog,topk,tdigest,
timeseries/*}.rs`, `persistence/src/serialization/{probabilistic,search,timeseries}.rs`.

Out of scope (sibling agent): `basic.rs`, `string.rs`, `hash.rs`, `list.rs`, `set.rs`,
`sorted_set/`, `generic.rs`, `expiry.rs`, `scan.rs`, `sort.rs`, `blocking.rs`.

**Coverage (from `target/llvm-cov/depth/depth.json`, 2026-07-28):** 6431 lines in area, 5161
covered = **80.25%** (workspace 85.0%). Depth classes, deduped by `(file, fn)` over 639 functions:

| class | count |
|---|---|
| well-covered | 281 |
| **untested** | **159** |
| **monoculture** | **73** |
| **single-test** | **63** |
| covered | 63 |

Worst files by line coverage: `event_sourcing/all.rs` 25.0, `vectorset/vrange.rs` 52.6,
`hyperloglog.rs` 56.2, `vectorset/vlinks.rs` 57.7, `vectorset/vsim.rs` 60.5,
`timeseries.rs` 66.2, `vectorset/vadd.rs` 68.6, `json/numeric.rs` 70.4, `tdigest.rs` 73.2,
`event_sourcing/replay.rs` 74.6, `cuckoo.rs` 75.4, `cms.rs` 75.5, `topk.rs` 76.2, `bloom.rs` 77.1,
`stream/consumer_groups.rs` 78.2.

**Crate shape.** The crate has **no `tests/` directory** and only **22 inline `#[test]`
functions in the whole audited area** — 6 in `geo.rs` (`dynamic_keys` STORE-destination
extraction), 8 in `bloom.rs` and 8 in `cuckoo.rs` (`flag_value_pin_tests`, spec-flag pins).
Every other file in scope — all of `stream/`, `json/`, `vectorset/`, `event_sourcing/`,
`bitmap.rs`, `timeseries.rs`, `hyperloglog.rs`, `cms.rs`, `topk.rs`, `tdigest.rs` — has **zero**
inline tests. Consequently ~100% of the behavioural coverage of these ~2.5k commands-LOC comes
from boundary 4 (`redis-regression/tests/*` and `server/tests/integration_*.rs`), through a real
socket, a real connection, RESP encode/decode and routing. That is the single biggest structural
problem in this area and it is why the negative/error-path coverage is so thin: every negative
case costs a server boot.

## Summary

The extended-type surface is broad, shallowly tested, and tested at the wrong level. Three risk
shapes dominate. **(1) Mutate-then-error.** Several commands mutate the store and *then* return an
error: `JsonValue::num_incr_by`/`num_mult_by`/`set` walk multi-match paths mutating in place and
`?`-return on the first bad match, and `JSON.SET`'s rollback snapshot (`json/basic.rs:109`) is
dropped without restoring on exactly that path — the client sees an error while half the document
already changed, the version bumps and the write propagates. **(2) Silent lossy conversions on
write paths.** `ES.SNAPSHOT` replaces a non-UTF-8 state with `""` and returns `OK`; timeseries
compaction rules are never serialized at all, so `TS.CREATERULE` is permanently lost on restart;
`ES.APPEND`'s `__frogdb:es:all` stream is written outside the command's WAL record. All three are
"returns success, data is gone" and none has a test. **(3) Nondeterminism under verbatim
propagation.** `VADD` seeds a vector set's REDUCE projection matrix from `rand::random()`
(`types/src/vectorset.rs:163`) and `TOPK.ADD` calls `rand::random::<f64>()` per decay attempt, so a
replica that receives the propagated command builds *different* state than the primary. On top of
that, an entire tier of primary operational paths has literally zero executions across the 7258-test
suite: `XGROUP CREATE <id>`, `XGROUP SETID`, `XSETID ENTRIESADDED/MAXDELETEDID`,
`XAUTOCLAIM ... JUSTID`, `XCLAIM TIME`, and `PFSELFTEST` (64 regions, `untested`). A consumer-group
bug here loses messages silently.

**Recommended shape change**: give the crate a boundary-3 home. Command *semantics* for these types
(PEL state machine, JSONPath mutation atomicity, sketch merge error ordering, TS retention/compaction)
need real dispatch, a real store and a real WAL seam — which is exactly `core/tests/shard_driver/`
(`ShardDriver::new(n).execute(shard, "XAUTOCLAIM", &args)`, plus `capture_keyspace`,
`memory_check`, `expiry_index_check`) — and need nothing from the socket. Pure algorithmic edges
(geohash boundaries, JSONPath parsing, sketch codecs) belong in inline `#[cfg(test)]` at boundary 1.

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| inline `#[cfg(test)]` in area | 22 tests total: geo `dynamic_keys` (6), bloom/cuckoo spec-flag pins (16) | geo tests are correctly boundary 1 | no inline tests at all for stream/json/vectorset/ES/timeseries/hll/cms/topk/tdigest |
| `core/tests/shard_driver/` | real `ShardWorker` + `register_all`, no socket | ideal home for command semantics; has `capture_keyspace`, `memory_check`, `expiry_index_check` probes | **not used by a single test in this area** |
| `core/tests/proptest_json.rs` | JSON parse/get/set/delete | 1000 cases, never-panic guarantees | liveness only — almost every assertion is `is_ok()`/`!panic`. No correctness oracle for JSONPath |
| `redis-regression/tests/stream_tcl.rs`, `stream_cgroups_tcl.rs` | Redis parity for streams | broad XADD/XREADGROUP/XCLAIM parity | XDELEX/XACKDEL **excluded as "not implemented"** though both are implemented; upstream lag tests (1319-1519) excluded |
| `redis-regression/tests/{hyperloglog,cms_topk,tdigest,geo,bitops}_*.rs` | probabilistic + geo parity | good error-path coverage for PFMERGE wrongtype | assertions are lower-bound-only (`>= N`); 7 corrupted-HLL tests dropped; `geo_tcl` is the *sole* test for `GEORADIUSBYMEMBER` (128 regions, `monoculture`) |
| `server/tests/integration_{streams,json,cms,topk,event_sourcing}.rs`, `timeseries.rs` | happy paths over RESP | end-to-end confidence | many assertions live inside `if let Response::Array(..)` with no `else` → vacuous on shape regression; `timeseries.rs` pins `num_shards: Some(1)` so MRANGE fan-out is never exercised |
| `server/tests/integration_dump_restore.rs` | DUMP/RESTORE + corruption matrix | includes `hll` in `CORRUPT_KINDS` (:630-641) | **cms, topk, tdigest, vectorset, timeseries-rules absent from the matrix and from any round-trip test** |
| `persistence/src/serialization/` unit tests | HLL merge operand, truncation rejection | genuinely good (`hll_merge_promotes_to_dense_like_in_memory:739`) | `copy_codec_round_trips_all_value_variants:257` asserts only `key_type()` equality — an empty CMS would pass |

## Findings

### F1: `ES.SNAPSHOT` silently discards a non-UTF-8 snapshot state and replies OK
- **Severity** 5 — the aggregate state is destroyed in place and the reply says success; a later
  `ES.REPLAY` returns `(version=N, state="")`, so the consumer skips every event `<= N` *and* has
  no state. Unrecoverable silent data loss.
- **Likelihood** 3 — binary-serialized aggregate state (protobuf/msgpack/bincode/CBOR) is the
  natural choice for an event-sourcing snapshot; nothing in the API says "text only".
- **Effort** 1 — one `shard_driver` (or even direct handler) call with a `\xff` byte.
- **Priority** 20
- **Evidence**: `commands/src/event_sourcing/snapshot.rs:46` —
  `let stored = format!("{}:{}", version, std::str::from_utf8(state).unwrap_or(""));` then
  `ctx.store.set(...)` and `Ok(Response::ok())`. Read side
  `commands/src/event_sourcing/replay.rs:52-80` splits on the first `':'`; its whole error family
  (lines 60-61, 64-67, 71-73, 76) is zero-exec. `event_sourcing/replay.rs` is 74.6% line coverage.
- **Proposed test**: `ES.SNAPSHOT agg 5 <0xff 0xfe binary>` then `ES.REPLAY agg` — assert either
  the returned state is byte-identical, or the SNAPSHOT is rejected with an error and the stored
  snapshot is unchanged. Pin the chosen semantics; today it is neither.
- **Boundary** 3 — needs the real store and command dispatch, nothing from the socket.

### F2: Timeseries compaction rules are never serialized — `TS.CREATERULE` is lost on restart
- **Severity** 5 — a downsampling rule silently disappears at restore/AOF replay/DUMP-RESTORE.
  Source keys keep growing, the destination series silently stops receiving buckets, and
  `TS.INFO` reports no rule. Nothing errors.
- **Likelihood** 4 — any restart, any replica full-sync, any `MIGRATE` of the source key.
- **Effort** 3 — server integration restart, or boundary 1 codec assertion plus a restore test.
- **Priority** 20
- **Evidence**: `persistence/src/serialization/timeseries.rs:26` `serialize_timeseries` writes no
  rules field; `types/src/timeseries/value.rs:160` `TimeSeriesValue::from_raw` hardcodes
  `rules: Vec::new()`. `TS.CREATERULE` declares `WalStrategy::PersistFirstKey`
  (`commands/src/timeseries.rs:1208`), so the WAL record is the serialized source value — i.e. the
  rule-free one. `integration_dump_restore.rs:229-237` round-trips only `TS.ADD`/`TS.GET`.
- **Proposed test**: `TS.CREATERULE src dst AGGREGATION avg 60000`, restart, assert
  `TS.INFO src` still lists the rule **and** a subsequent `TS.ADD src` crossing a bucket boundary
  still writes to `dst`. Plus a boundary-1 codec assertion that `serialize→deserialize` preserves
  `rules`.
- **Boundary** 4 for the restart half (process lifecycle), 1 for the codec half.

### F3: JSON multi-path mutations are non-atomic and the rollback snapshot is not restored on error
- **Severity** 4 — `JSON.NUMINCRBY doc '$.*' 1` on `{"a":1,"b":"x"}` returns
  `ERR ... not a number` **after** having already incremented `a`. The client reads an error and
  reasonably assumes no change. The write still gets a `WriteCommandMeta`, so the version bumps,
  the WAL persists the partially-mutated document and the command propagates verbatim.
- **Likelihood** 4 — wildcard/multi-match paths over heterogeneous documents are the normal
  RedisJSON usage; RedisJSON itself validates all matches before mutating.
- **Effort** 2 — pure `JsonValue` unit test plus one `shard_driver` assertion on stored bytes.
- **Priority** 18
- **Evidence**: `types/src/json.rs:362-386` — the loop does `*value = new_val` per matched path and
  `_ => return Err(JsonError::NotANumber)` mid-iteration; note `update_cached_size()` (line 388) is
  also skipped on that path, so the cached memory size goes stale too. Same shape in `num_mult_by`
  (`:393`) and `set` (`:306-311`, `set_at_path` can return `Err(NotAnObject)` after earlier paths
  mutated). At the command layer `commands/src/json/basic.rs:109-112` deliberately takes
  `let snapshot = json.clone();` for the growth-limit rollback, but the very next line
  `json.set(...).map_err(...)?` returns **without** restoring it — the rollback only covers
  `enforce_growth_limits`. `json/numeric.rs` is 70.4% covered and the multi-result branch
  (`:112-125`) is `untested`.
- **Proposed test**: on `{"a":1,"b":"x"}`, `JSON.NUMINCRBY doc '$.*' 1` → assert error **and**
  `JSON.GET doc '$'` byte-identical to the pre-command document; assert the key version did not
  change. Same for `NUMMULTBY`, `STRAPPEND`, and a nested `JSON.SET` that trips `NotAnObject`.
- **Boundary** 2 for the type-level atomicity (crate API on `JsonValue`), 3 for the
  version/propagation half.

### F4: `VADD` seeds the REDUCE projection matrix from `rand::random()` — primary and replica build different vector sets
- **Severity** 5 — the replica projects the *same* input vector through a *different* matrix, so
  its stored vectors are unrelated to the primary's. `VSIM` returns different neighbours on the
  replica, and after a failover every previously-inserted REDUCE'd vector is effectively garbage.
  A consistency violation with no error anywhere.
- **Likelihood** 3 — requires `VADD ... REDUCE <n>` on a key created while a replica is attached.
  Full-sync is safe (the codec does persist `uid`), so only live propagation diverges — which is
  the common case for keys created after the replica attached.
- **Effort** 3 — `TestServer::start_primary`/`start_replica` already exist.
- **Priority** 18
- **Evidence**: `types/src/vectorset.rs:163` `vs.uid = rand::random();`; `:563`
  `self.projection_matrix = generate_projection_matrix(self.uid, original_dim, self.dim)`; `:688`
  `StdRng::seed_from_u64(uid)`. `persistence/src/serialization/search.rs:42,127` persists `uid` and
  the matrix (so RDB/DUMP is fine), but `VADD` has `repl_override: None` → verbatim propagation.
  `vectorset/vadd.rs` is 68.6% covered; `vsim.rs` 60.5%.
- **Proposed test**: primary+replica, `VADD k REDUCE 4 VALUES 8 ... elem`, wait for sync, assert
  `VEMB k elem` and `VSIM k VALUES 8 ...` are identical on both. A cheaper companion at boundary 1:
  assert `uid` is derived deterministically from the key (once the fix makes it so).
- **Boundary** 5 — genuinely a replication-divergence property. See OPTIONS.
- **OPTIONS**:
  - *Boundary 1* — assert `uid` derivation is a pure function of the key name. Cheap, fast, but
    only tests the fix, not the property; a future refactor could reintroduce randomness elsewhere
    (e.g. `VectorSetValue::new`).
  - *Boundary 4/5, primary+replica* — asserts the real property (`VEMB` equality after sync).
    ~2s of harness time, catches any future source of nondeterminism.
  - **Recommendation**: both. Boundary 5 as the property test, boundary 1 as the fast regression
    pin. The boundary-5 test should be generalised into a "verbatim-propagated write is
    deterministic" table covering `VADD` and `TOPK.ADD` (F13).

### F5: `XAUTOCLAIM ... JUSTID` and `XCLAIM ... TIME/LASTID` have zero executions in the entire suite
- **Severity** 4 — `JUSTID` is the documented way to reclaim without bumping the delivery counter;
  if it bumps it anyway, entries cross `max-deliveries` early and get routed to a dead-letter
  stream. If it drops entries from the reply, a consumer silently never processes them.
- **Likelihood** 4 — `XAUTOCLAIM ... JUSTID` is the standard reclaim loop in every consumer-group
  worker template.
- **Effort** 2 — pure command semantics, no socket needed.
- **Priority** 18
- **Evidence**: `commands/src/stream/pending.rs` zero-exec lines **428** and **439-442** (the whole
  XAUTOCLAIM JUSTID branch), **175** (XCLAIM `TIME`), **181-183** (XCLAIM `LASTID`), plus 86,
  114-116, 187, 193, 202-203, 221-223, 340-342, 380-382. File is 89.6% line-covered, which is
  exactly why percentage is not the signal here.
- **Proposed test**: build a group with 3 pending entries at known idle times; `XAUTOCLAIM g c
  0 0-0 JUSTID` → assert the reply is IDs only, that `XPENDING` retry-count is **unchanged** for
  the claimed entries (vs incremented without JUSTID), and that the returned cursor advances.
  Separately `XCLAIM ... TIME <ms>` → assert `XPENDING` reports the injected delivery time.
- **Boundary** 3 — needs the real PEL and a controllable `ClaimClock`; needs nothing from RESP.

### F6: `ES.APPEND`'s `__frogdb:es:all` stream is written outside the command's WAL record
- **Severity** 4 — `ES.ALL` is the global event log. `WalStrategy::PersistFirstKey` persists only
  `args[0]` (the aggregate stream), so the mutation to `__frogdb:es:all` is not in the command's
  WAL record. Divergence between the aggregate streams and the global log after WAL replay, with
  no error. `append_to_all_stream` additionally swallows every failure.
- **Likelihood** 4 — any restart.
- **Effort** 3 — restart round-trip.
- **Priority** 17
- **Evidence**: `commands/src/event_sourcing/append.rs` — `ES_ALL_KEY = b"__frogdb:es:all"`,
  `ES_ALL_MAXLEN = 100_000`; `append_to_all_stream` does `Err(_) => return` and
  `let _ = all_stream.add(...)`, discarding both the error and the assigned ID. Spec declares
  `WalStrategy::PersistFirstKey`. `event_sourcing/all.rs` is 25.0% covered (its `execute` is a
  `ServerWideOp::EsAll` stub, lines 34-44 zero-exec).
- **Proposed test**: append 5 events across 2 aggregates, restart, assert `ES.ALL` still returns 5
  events in order. Also assert `__frogdb:es:all` is not visible to `KEYS *`/`DBSIZE`/`SCAN`, and
  that an `ES.APPEND` whose `$all` write fails does not silently report success.
- **Boundary** 4 — process restart is the behaviour under test.

### F7: `XSETID ENTRIESADDED` / `MAXDELETEDID` — the whole stream-metadata restore surface is untested
- **Severity** 4 — these fields drive `XINFO STREAM entries-added`, `max-deleted-entry-id` and every
  consumer-group **lag** computation. A restore tool (or a replica rebuild) that sets them wrong
  produces silently wrong lag, which drives consumer autoscaling and alerting.
- **Likelihood** 3 — used by RDB restore paths and by operators repairing a stream.
- **Effort** 2.
- **Priority** 16
- **Evidence**: `commands/src/stream/basic.rs` zero-exec **404-410** (`ENTRIESADDED` parse),
  **412** (`MAXDELETEDID`), **420, 422-426** (the `max_deleted_id > new_last_id` validation),
  **442, 445** (`set_entries_added` / `set_max_deleted_id`). Also in `stream/consumer_groups.rs`:
  **136** (`XGROUP CREATE` with an explicit ID — `StreamId::parse(id_arg)?`), **276-279** and
  **286** (`XGROUP SETID` `$` and explicit-ID branches) are all zero-exec.
- **Proposed test**: `XSETID s 5-0 ENTRIESADDED 100 MAXDELETEDID 3-0` → assert `XINFO STREAM`
  reflects all three, assert `XSETID s 5-0 MAXDELETEDID 9-0` is rejected, and assert a group's
  reported lag matches the hand-computed value after the metadata is restored.
- **Boundary** 3.

### F8: `XCLAIM FORCE` creates a PEL entry for an ID that is not in the stream, and never evicts PEL entries for deleted messages
- **Severity** 4 — a phantom pending entry that `XREADGROUP <consumer> 0` will hand to a consumer
  as a message that does not exist, and that inflates the group's pending count forever. Redis's
  `XCLAIM FORCE` checks entry existence first.
- **Likelihood** 3 — `FORCE` is used exactly in the recovery paths where entries have been trimmed
  away.
- **Effort** 2.
- **Priority** 16
- **Evidence**: `commands/src/stream/pending.rs` — the FORCE filter is
  `group.pending_idle(id).map_or(force, |idle| idle >= min_idle_time)`, with no lookup of `id` in
  the stream. XAUTOCLAIM has the corresponding eviction (`drop_missing_pending` against
  `deleted_ids` from `autoclaim_scan`); XCLAIM has no equivalent.
- **Proposed test**: `XADD`, `XREADGROUP`, `XDEL` the entry, then `XCLAIM g c2 0 <id> FORCE` →
  assert the entry is **not** added to the PEL (or is added and immediately evicted, matching
  XAUTOCLAIM), and that `XPENDING` counts agree between the XCLAIM and XAUTOCLAIM routes.
- **Boundary** 3.

### F9: Timeseries duplicate timestamp landing in an already-compressed chunk inserts a second copy
- **Severity** 4 — `TS.GET`/`TS.RANGE` return two samples with the same timestamp,
  `total_samples` over-counts, and every aggregation double-counts. No error is raised. The
  DUPLICATE_POLICY the user configured is silently not applied.
- **Likelihood** 3 — needs `> chunk_size` (default 256) samples before the duplicate, i.e. any
  long-lived series with a retrying writer.
- **Effort** 2.
- **Priority** 16
- **Evidence**: `types/src/timeseries/value.rs:169` checks only `active_samples`; only the `BLOCK`
  policy consults chunks (`:192-201`). FIRST/MIN/MAX/SUM/LAST fall through to `:203-204` and insert
  a duplicate. `range()` (`:266-284`) returns both. Same shape in `incrby` (`:418-427`): it reads
  the chunked value, calls `active_samples.remove()` (a no-op for chunk data), then re-adds. Every
  existing duplicate-policy test uses ≤3 samples (`timeseries_regression.rs:238-303,526-581`,
  `server/tests/timeseries.rs:487-545`), so the compressed path is never reached.
- **Proposed test**: add 300 samples, then re-add timestamp 10 under each policy → assert exactly
  one sample at that timestamp, the policy-correct value, and `TS.INFO totalSamples == 300`.
- **Boundary** 1 for the `TimeSeriesValue` half (pure), 3 for the command-level policy assertion.

### F10: `XDELEX` / `XACKDEL` are implemented but excluded from the parity suites as "not implemented"
- **Severity** 4 — these commands own the `KEEPREF`/`DELREF`/`ACKED` reference semantics across
  *multiple* consumer groups. Getting `ACKED` wrong deletes an entry another group still had
  pending → silent message loss for that group.
- **Likelihood** 3 — Redis 8.x ack-and-delete is the recommended trimming pattern for
  multi-group streams.
- **Effort** 3 — un-skipping in the parity suite is cheap; the multi-group matrix is the work.
- **Priority** 15
- **Evidence**: `redis-regression/tests/stream_tcl.rs` and `stream_cgroups_tcl.rs` header
  exclusion lists mark XACKDEL and XDELEX "not implemented", but `commands/src/stream/basic.rs:464-519`
  implements XDELEX and `consumer_groups.rs` implements XACKDEL (its missing-key branch, line 403,
  is zero-exec). `stream/mod.rs::parse_delete_ref_strategy` silently returns the default on an
  unknown token rather than erroring.
- **Proposed test**: 2 groups, 1 entry pending in both; `XDELEX s ACKED IDS 1 <id>` → assert the
  entry survives; ack in group A only → still survives; ack in B → now deleted, and both PELs are
  clean. Plus `XDELEX s BOGUSREF ...` must error, not silently default.
- **Boundary** 3 for the semantics matrix; keep the parity un-skip at boundary 4.

### F11: Consumer-group **lag** / `entries-read` accounting has no coverage at all
- **Severity** 3 — wrong lag is the metric operators autoscale consumers on, and it is the only
  signal that a group is falling behind. Wrong-but-plausible is worse than absent.
- **Likelihood** 4 — reported on every `XINFO GROUPS`, which every stream dashboard polls.
- **Effort** 2.
- **Priority** 15
- **Evidence**: the upstream lag tests (`stream_cgroups_tcl.rs` upstream lines 1319-1519) are in
  the exclusion list. `compute_lag`, `entries_read`, `entries_added`, `max_deleted_id` have no
  direct test; `stream/info.rs` is 89.4% covered with `xinfo_stream::{closure#0}` (62 regions)
  classed `monoculture` — reached only by `stream_cgroups_tcl`.
- **Proposed test**: table-driven — add N, read M, XDEL K entries from the middle, XTRIM, then
  assert `lag` against a hand-computed expected value including the "lag becomes NULL when
  `max_deleted_id` makes it uncomputable" case.
- **Boundary** 3.

### F12: CMS, TopK and TDigest have **no** DUMP/RESTORE, RDB or WAL round-trip test
- **Severity** 4 — `RESTORE` (and the cross-shard `MIGRATE`→`RESTORE` path) is the one ingress for
  externally-supplied sketch bytes. The hardened bounds checks in
  `persistence/src/serialization/probabilistic.rs:587-680` (`deserialize_topk`, `deserialize_cms`,
  width/depth overflow, `safe_capacity`) are never driven by a real restore. A sketch that
  deserializes into garbage answers every future query wrongly, silently.
- **Likelihood** 3 — slot migration, DUMP/RESTORE-based backup tooling, replica full-sync.
- **Effort** 3.
- **Priority** 15
- **Evidence**: `server/tests/integration_dump_restore.rs:630-641` `CORRUPT_KINDS` lists
  string/list/hash/set/zset/stream/hll/json — **cms, topk, tdigest, vectorset are absent**; grep
  for `CMS.`/`TOPK.`/`TDIGEST.` across `integration_dump_restore.rs`, `integration_persistence.rs`,
  `dump_regression.rs`, `dump_tcl.rs` returns zero hits. The one cross-type round-trip,
  `persistence/src/serialization/mod.rs:257-368`, asserts only
  `value.key_type() == back.key_type()` (`:359-364`) — a codec returning an empty CMS passes.
- **Proposed test**: (a) extend `CORRUPT_KINDS` with cms/topk/tdigest/vectorset so the byte-flip
  matrix runs over them; (b) a content-preserving round-trip: build each sketch, serialize,
  deserialize, assert **query answers** match (`CMS.QUERY` counts, `TOPK.LIST WITHCOUNT` order and
  counts, `TDIGEST.QUANTILE` at 0/0.5/1 and `TDIGEST.INFO` weight) — not `key_type()`.
- **Boundary** 1 for the content round-trip (pure codec), 4 for the corruption matrix (RESTORE is
  the real ingress). See OPTIONS.
- **OPTIONS**:
  - *Boundary 1 only* — codec round-trip in `persistence`. Fast, catches content loss, but does
    not exercise `RESTORE`'s framing/CRC/version prologue.
  - *Boundary 4 only* — extend the existing `CORRUPT_KINDS` matrix. Reuses working
    infrastructure, catches the real ingress, but is slow and a poor place to assert content.
  - **Recommendation**: both, split by purpose — content assertions at boundary 1, hostile-bytes at
    boundary 4 by adding the four kinds to the existing matrix (near-zero marginal cost).

### F13: `TOPK.ADD` is nondeterministic — primary and replica diverge under verbatim propagation
- **Severity** 4 — the replica's sketch differs from the primary's, so `TOPK.LIST`/`TOPK.COUNT`
  answer differently depending on which node you read, and a failover changes the answers.
- **Likelihood** 3 — any replicated deployment using TOPK.
- **Effort** 3.
- **Priority** 15
- **Evidence**: `types/src/topk.rs:120` calls `rand::random::<f64>()` inside the decay loop
  (`:118-124`, up to 100000 iterations per item per row). `integration_replication.rs:6995` lists
  `TOPK.ADD` only in a smoke matrix; no test asserts primary/replica convergence.
- **Proposed test**: fold into the same "verbatim-propagated writes are deterministic" table as F4
  — drive N adds on the primary, wait for sync, assert `TOPK.LIST WITHCOUNT` is byte-identical on
  both nodes.
- **Boundary** 5.

### F14: `BF.LOADCHUNK` / `CF.LOADCHUNK` parse attacker-supplied headers with no semantic validation
- **Severity** 4 — `CF.LOADCHUNK`'s `let fp_bytes = num_buckets * layer_bucket_size as usize * 2;`
  can wrap on the `usize` multiply, so the subsequent `offset + fp_bytes > data.len()` check
  passes and the per-bucket indexing panics (crash-loop, since the filter is
  `WalStrategy::PersistFirstKey` and survives restart). `Vec::with_capacity(num_buckets)` with
  `num_buckets ≈ 2^63` is an OOM abort. No semantic check on `k == 0`, `capacity == 0`,
  `count > capacity`, or a NaN/negative `error_rate`.
- **Likelihood** 2 — requires a client sending crafted chunks, but SCANDUMP/LOADCHUNK exists
  precisely so untrusted-ish backup blobs get replayed.
- **Effort** 2.
- **Priority** 14
- **Evidence**: `commands/src/bloom.rs:495-668` and `commands/src/cuckoo.rs:559-748`. Coverage:
  `CfLoadchunk::execute` 131 regions and `BfLoadchunk::execute` 112 regions are both `single-test`,
  reached only by `bloom_regression::{cf,bf}_scandump_loadchunk_roundtrip`;
  `CfScandump` 69 and `BfScandump` 65 regions likewise. `bloom.rs` 77.1%, `cuckoo.rs` 75.4%.
- **Proposed test**: a table of malformed chunks — truncated header, `num_layers = u32::MAX`,
  `num_buckets` chosen to wrap `fp_bytes`, `k = 0`, `count > capacity`, `error_rate = NaN` — each
  asserting a clean `CommandError`, no panic, and **the pre-existing key unchanged**.
- **Boundary** 1 — the parser is pure over `&[u8]`; today it can only be reached through a socket,
  which is the anti-pattern the brief calls out. Extracting the header parse into a testable
  function is part of the work.

### F15: Every probabilistic-sketch assertion is a lower bound (`>= N`) — a sketch returning garbage passes
- **Severity** 3 — wrong-but-large answers from CMS/HLL/TopK are exactly the failure mode these
  tests exist to catch, and none of them can.
- **Likelihood** 3 — any regression in a counter width, a hash seed, or a merge weight.
- **Effort** 2 — property tests with bounded relative error.
- **Priority** 13
- **Evidence**: `cms_topk_regression.rs:100,123,168,212,579` and `integration_cms.rs:359` are all
  `>=`; `hyperloglog_regression.rs:12,17,35` assert `>= 3`, `>= 5`, `>= 9` — an HLL returning
  `i64::MAX` passes all three. `tdigest_regression.rs:353,470` use ±5.0 on a uniform 1..100.
  `cms_topk_regression.rs:606` asserts only `items.len() == 5` for eviction and never inspects an
  expelled name — no test anywhere observes a non-nil expelled value from `commands/src/topk.rs:153`.
- **Proposed test**: proptest over insert multisets — assert HLL cardinality within the
  structure's stated error bound *in both directions*, CMS `query(x) >= true_count(x)` **and**
  `<= true_count(x) + eps * total`, TopK returns the true top-k for a sufficiently separated
  distribution.
- **Boundary** 1 — pure over `types/src/{hyperloglog,cms,topk}.rs`. Property/fuzz, not example
  tests: the invariant is statistical, so enumerating examples cannot express it.

### F16: JSONPath supports a narrow subset with silent-acceptance edges, and `proptest_json` has no correctness oracle
- **Severity** 3 — `$..name` (recursive descent), filters `?(@.x>1)`, slices `[0:2]` and unions
  `[0,1]` are all rejected or mis-parsed, so a RedisJSON client gets an error or a wrong answer.
  Worse, a trailing dot silently normalizes away: `JSON.SET k '$.' <v>` produces an empty segment
  list, so `set_at_path` hits its `path.is_empty()` branch and **replaces the entire document**.
- **Likelihood** 3 — `$.` is one typo away from `$.a`; recursive descent is common in RedisJSON docs.
- **Effort** 2.
- **Priority** 13
- **Evidence**: `types/src/json.rs:887-891` — recursive descent returns
  `InvalidPath("recursive descent not supported")`; `:928-935` only accepts a bare `i64` in
  brackets, so slices and unions error; `:900-904` drops an empty key segment; `:1060-1063`
  `set_at_path` replaces `*data` on an empty path; `:846-848` `normalize_path` prepends `$.` to
  anything not starting with `$`. `core/tests/proptest_json.rs` is 1000-case but almost every
  property is `prop_assert!(result.is_ok())` or "did not panic" — no oracle.
- **Proposed test**: (a) a boundary-1 table pinning, for each unsupported syntax, that it produces a
  *specific* error rather than a wrong answer — including `$.`, `$.a.`, `$[`, `$..a`, `$[0:2]`,
  `$[?(@.x>1)]`; (b) turn `proptest_json` into a differential test against a reference JSONPath
  implementation, or at minimum add algebraic properties (`set(p,v)` then `get(p)` yields `v`;
  `delete(p)` then `get(p)` is empty; `get` never returns a value not reachable by manual
  navigation).
- **Boundary** 1 — pure path parsing/navigation. Property, not example, for (b).

### F17: `TS.MADD` partial-failure and auto-create semantics are 0% covered
- **Severity** 3 — `TS.MADD` returns a per-element array; a caller cannot tell which elements
  failed if the error shape is wrong, and silently-dropped samples look like a healthy write.
- **Likelihood** 3 — MADD with a mixed batch is its whole purpose.
- **Effort** 2.
- **Priority** 13
- **Evidence**: `commands/src/timeseries.rs` — arity check `:455-459` and **every** per-element
  error arm (`:468` bad timestamp, `:475` bad value, `:486` add error, `:489` WRONGTYPE, `:500`
  auto-create add error) are untested; both MADD tests
  (`timeseries_regression.rs:310-332`, `server/tests/timeseries.rs:308-345`) pre-create every key
  and pass only valid triples, so the auto-create branch (`:491-503`) never runs.
- **Proposed test**: one MADD mixing a valid triple, a bad timestamp, a WRONGTYPE key and a
  not-yet-existing key → assert the reply array positionally pairs successes with errors, and that
  the valid samples landed while nothing else did.
- **Boundary** 3.

### F18: `TS.RANGE` and `TS.MRANGE` disagree on unimplemented options — one errors, the other silently ignores
- **Severity** 3 — `TS.MRANGE ... ALIGN 0` returns results computed with *no* alignment and no
  warning; unknown tokens after `FILTER` are swallowed as filter expressions and dropped. The
  caller gets a plausible, wrong time series.
- **Likelihood** 3 — `ALIGN`/`BUCKETTIMESTAMP`/`EMPTY`/`GROUPBY`/`REDUCE`/`LATEST` are all
  standard RedisTimeSeries options and none is implemented.
- **Effort** 2.
- **Priority** 13
- **Evidence**: `commands/src/timeseries.rs:876` errors on `ALIGN` for `TS.RANGE`;
  `core/src/shard/timeseries_execution.rs:294` has a catch-all `_ => {}` for MRANGE options, and
  `:197-218` treats unrecognised post-`FILTER` tokens as filter expressions which `:132-136`
  silently `filter_map`s away. Also `:340` returns `""` instead of nil for a `SELECTED_LABELS`
  label the series lacks.
- **Proposed test**: a table asserting that every unimplemented option produces the **same**
  error from `TS.RANGE` and `TS.MRANGE`, and that an unparseable filter expression errors rather
  than being dropped.
- **Boundary** 3.

### F19: `VADD` accepts `NaN`/`inf` vector components
- **Severity** 3 — a NaN component makes cosine distance NaN, which makes the HNSW graph's
  comparisons meaningless from that insert onward; `VSIM` returns silently wrong (and
  order-unstable) neighbours for the whole set, permanently.
- **Likelihood** 2 — requires a client serializing a NaN, which happens when an upstream embedding
  model emits one.
- **Effort** 1.
- **Priority** 12
- **Evidence**: `commands/src/vectorset/vadd.rs:276-284` `parse_f32` is a bare `str::parse::<f32>()`,
  which accepts `"nan"`, `"inf"`, `"-inf"`. `vadd.rs` guards `dim > MAX_DIM` (`:184`) but nothing
  else about the values. `vadd.rs` 68.6%, `vsim.rs` 60.5%, `vrange.rs` 52.6%, `vlinks.rs` 57.7%.
- **Proposed test**: `VADD k VALUES 3 nan 1 2 elem` → assert an error and that the key is
  unchanged/absent; same for `inf`. Add a proptest that `VSIM` results are a stable permutation
  under repeated calls.
- **Boundary** 1 for the parse rejection; 3 for the "key unchanged after rejection" half.

### F20: Geo edge cases are tested only through a full RESP client — the anti-pattern the brief names
- **Severity** 2 — mostly a test-design cost, but it is why the geo error paths are uncovered:
  every negative case costs a server boot, so none were written.
- **Likelihood** 3 — the uncovered branches include real ones (STORE with an empty result must
  delete the destination and fire `del`).
- **Effort** 1 — the pure functions already exist and are directly callable.
- **Priority** 11
- **Evidence**: `GeoradiusbymemberCommand` is 128 regions classed `monoculture`, reached only by
  `redis-regression/tests/geo_tcl.rs` (8 tests). `BitposCommand` (64 regions) likewise, only
  `bitops_tcl`. Zero-exec in `commands/src/geo.rs`: 86-88/91-93 (NX+XX conflict), 152-153 (delete
  emptied key), 271/278/333 (GEOHASH/GEOPOS null results), 422-424 (GEOSEARCHSTORE missing source
  deletes dest + `del`), 511-512 (invalid lon/lat), 642-644 (STORE + WITH* incompatibility),
  669-675, **703-706 (GEORADIUS STORE with an empty result)**, 916-982 (the whole
  `parse_geosearch_options` error family). Meanwhile `types/src/geo.rs` has 7 inline tests at
  93.4% and `geohash_range_for_bbox` (`:335-352`) is entirely zero-exec **with no callers** — dead
  code. The 6 inline tests in `commands/src/geo.rs:1304-1366` (`dynamic_keys` STORE-destination
  extraction) are correctly at boundary 1 and are the model to follow.
- **Proposed test**: move the pure cases down — a boundary-1 table over
  `geohash_encode`/`geohash_decode`/`haversine_distance`/`is_within_radius` at the poles
  (±90), the antimeridian (±180), lat exactly ±85.05112878 (the Mercator clamp), and
  `(0,0)`; assert encode→decode round-trips within the documented precision. Keep only the
  STORE-destination and RESP-shape cases at boundary 4. Separately: delete
  `geohash_range_for_bbox` or test it.
- **Boundary** 1 — pure functions over floats; the connection adds nothing.

## Deprioritised

- **`PFDEBUG TODENSE` is a no-op** (`commands/src/hyperloglog.rs:278-281`) whose comment claims
  "FrogDB HLL is always dense", contradicted by `SPARSE_TO_DENSE_THRESHOLD = 3000`
  (`types/src/hyperloglog.rs:25`) — and `hyperloglog_regression.rs:38-47` *asserts the wrong
  behaviour*, pinning the bug. Real, but PFDEBUG is a debug command; fix the code, then the test.
  (S2/L2/E1 → 9.)
- **`PFSELFTEST` is 64 regions, `untested`, zero tests** and its internal `90..=110` estimate bound
  is a hardcoded flake risk. Low value to test a self-test; worth deleting or making it validate
  real encoding as Redis's does.
- **HLL sparse→dense WAL merge-delta**: investigated and dropped. `persistence/src/serialization/
  probabilistic.rs:739,761,801` and `persistence/src/rocks/tests.rs:73` already cover promotion,
  full-value-operand rejection and truncated operands well. Only residue: no end-to-end
  "PFADD past 3000, restart, PFCOUNT equal" assertion — cheap, but genuinely low risk.
- **HLL wire format is not Redis-compatible** (no `HYLL` magic anywhere; `Value::HyperLogLog` is a
  first-class variant, not a string). This is a design decision, not a test gap — but it does mean
  `GET`/`STRLEN`/`SETRANGE` on an HLL key can never match Redis, and the 7 corrupted-HLL parity
  tests are permanently unskippable. Worth a docs note, not a test.
- **`HyperLogLog::from_sparse` does not validate `index < 16384`** (`types/src/hyperloglog.rs:63`),
  and `promote_to_dense` (`:244`) would index out of bounds on an oversized index. Unreachable
  today (no codec path admits one) — but it is one bounds check away from being a
  RESTORE-reachable panic. Flag to the persistence agent rather than testing it here.
- **`ES.ALL` server-wide stub** (`event_sourcing/all.rs:34-44`, 25% coverage) — the uncovered lines
  are the unreachable `CommandError::Internal` guard. Correct as-is.
- **`bitmap.rs` zero-exec lines** (86, 128, 251-252, 333-335, 430, 446-447, 510, 559, 578, 591-592,
  609, 623, 637-639, 642, 656-658, 663) are all cheap negative paths — BITOP invalid op, BITPOS
  `bit > 1`, GETBIT/BITFIELD WRONGTYPE, BITFIELD parse errors. Genuinely worth adding as a single
  boundary-1/3 negative table, but individually too low-severity to score. Bundle them.
- **`CMS.MERGE` does not validate the destination's own dimensions** (`commands/src/cms.rs:384`
  `from_raw` silently replaces them) and **`TDIGEST.MERGE` floors compression at 100**
  (`tdigest.rs:256-268`, so merging two compression-50 digests yields 100). Both are Redis
  divergences worth one test each, folded into F12's content round-trip rather than scored
  separately.
- **`TDIGEST.BYRANK`/`BYREVRANK` are not implemented** at all. Missing feature, not a test gap.
- **`TS.CREATERULE` accepts a self-referencing rule** (source == dest → infinite feedback) and
  `parse_labels` silently drops a trailing odd label. Real bugs; cheap tests; but subsumed by the
  broader "timeseries option parsing has no negative coverage" theme in F17/F18.
- **`XREADGROUP` supports only a single stream** (`stream/read.rs:150-349`, `num_streams != 1`
  errors) and returns `InvalidArgument{"No such key"}` where Redis returns a `NOGROUP`-prefixed
  error. Divergence worth pinning, but it is loud, not silent.
- **Round 1 issue 27** already fixed 11 stream keyspace-notification bugs. Not re-proposing
  notification coverage; the residue is the metadata/JUSTID/DELREF surface in F5/F7/F10.

## Cross-area notes

1. **A boundary-3 test home for the commands crate is the highest-leverage shared investment.**
   `core/tests/shard_driver/harness.rs` already builds a real `ShardWorker` via
   `ShardWorkerBuilder` + `frogdb_commands::register_all` and exposes `execute`, `execute_conn`,
   `tick_expiry`, `capture_keyspace`, `memory_check` and `expiry_index_check` — everything F1, F5,
   F7, F8, F10, F11, F17, F18 need, with none of the socket cost. It is used by **zero** tests in
   this area. Recommend a `core/tests/shard_driver/commands_extended.rs` (or a per-type module
   set) as the default home for command-semantics tests, and treat "needs a `TestServer`" as
   requiring justification. Sibling agents on `basic/string/hash/list/set/sorted_set` almost
   certainly have the same finding — this should be decided once, globally.

2. **"Mutate-then-error" is a cross-cutting invariant, not a JSON quirk.** `core/src/shard/
   execution.rs:240-243` turns a handler `Err` into a response but still builds a
   `WriteCommandMeta` (only `write_was_noop` suppresses it), so a *failed* command still bumps the
   version, persists to the WAL and propagates. `core/src/shard/rollback.rs` exists but only
   covers WAL-persistence failure, not handler failure. Recommend a shared harness assertion —
   "after any command that returns an error, the keyspace version and serialized value of every
   key in its `WalStrategy::actions()` are unchanged" — driven over a large command table. That
   belongs to the core-engine agent; F3 here is one instance.

3. **"Verbatim-propagated writes must be deterministic" needs one shared replication test table.**
   F4 (`VADD` random `uid`) and F13 (`TOPK.ADD` random decay) are the same bug class. The
   replication agent should own a table-driven primary/replica convergence test; this area
   contributes the two known entries and probably is not the only source.

4. **`CORRUPT_KINDS` in `server/tests/integration_dump_restore.rs:630-641` should be exhaustive
   over `TypeMarker`.** It currently omits cms, topk, tdigest and vectorset (F12). A compile-time
   exhaustiveness link between `TypeMarker` (`persistence/src/serialization/marker.rs:36`) and the
   corruption matrix would prevent the next type from being silently omitted. Belongs to the
   persistence/serialization agent.

5. **`persistence/src/serialization/mod.rs:257-368`'s `copy_codec_round_trips_all_value_variants`
   asserts only `key_type()` equality** and `collection_contents_survive_round_trip` deliberately
   covers only zset/hash/list/set. Extending the content assertions to the probabilistic and
   structured types is shared work with the serialization agent (F12).

6. **Dead code to delete**: `types/src/geo.rs:335-352` `geohash_range_for_bbox` is zero-exec with
   no callers anywhere in the workspace.
