# commands — extended types — residual test gaps (12 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/07 — residual findings after promotion to issues 19–76
Score: 12 findings, priority range 11–18
Area: `frogdb-server/crates/commands/src/` — `stream/`, `json/`, `vectorset/`, `event_sourcing/`, `bitmap.rs`, `geo.rs`, `timeseries.rs`, `hyperloglog.rs`, `bloom.rs`, `cms.rs`, `cuckoo.rs`, `topk.rs`, `tdigest.rs` (plus the backing `types/` and `persistence/src/serialization/` files read where the command is a thin shell)

## Context

This is the extended / probabilistic / structured-type command surface: streams, JSON,
vector sets, event sourcing, bitmaps, geo, timeseries and the five sketch families.
**6431 lines in area, 5161 covered = 80.25%** (workspace 85.0%); depth classes deduped by
`(file, fn)` over 639 functions are 281 `well-covered`, **159 `untested`**, **73
`monoculture`**, **63 `single-test`**, 63 `covered`. Structurally the crate has **no
`tests/` directory** and only **22 inline `#[test]` functions in the whole audited area**
(6 in `geo.rs`, 8 each in `bloom.rs`/`cuckoo.rs`), so ~100% of the behavioural coverage of
these ~2.5k commands-LOC arrives at boundary 4 through a real socket. The proposal's verdict
on the shape of that coverage: *"The extended-type surface is broad, shallowly tested, and
tested at the wrong level."*

## Promoted elsewhere

- F1 → issue 43, `.scratch/testing-improvements-round2/issues/` (`ES.SNAPSHOT` silently discards a non-UTF-8 snapshot state and replies OK)
- F2 → issue 44, `.scratch/testing-improvements-round2/issues/` (timeseries compaction rules are never serialized — `TS.CREATERULE` is lost on restart)
- F3 → issue 24, `.scratch/testing-improvements-round2/issues/` (theme T6 — errors do not roll back their effects; JSON multi-path mutation drops its rollback snapshot on the error path)
- F4 → issue 56, `.scratch/testing-improvements-round2/issues/` (non-deterministic writes propagated verbatim — `VADD` REDUCE) **and** issue 25, `.scratch/testing-improvements-round2/issues/` (theme T7 — determinism of propagated writes is unverified)
- F13 → issue 56, `.scratch/testing-improvements-round2/issues/` (non-deterministic writes propagated verbatim — `TOPK.ADD` decay uses `rand`) **and** issue 25, `.scratch/testing-improvements-round2/issues/` (theme T7)
- F14 → issue 70, `.scratch/testing-improvements-round2/issues/` (unbounded allocations — `BF/CF.LOADCHUNK` `usize` wrap)
- F15 → issue 33, `.scratch/testing-improvements-round2/issues/` (§4 tests that cannot fail — all sketch assertions are lower-bound-only)
- F17 → issue 23, `.scratch/testing-improvements-round2/issues/` (theme T5 — partial failure reported as total success; `TS.MADD` partial failure)

## Residual findings

### F5 — `XAUTOCLAIM ... JUSTID` and `XCLAIM ... TIME/LASTID` have zero executions in the entire suite

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

### F6 — `ES.APPEND`'s `__frogdb:es:all` stream is written outside the command's WAL record

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

### F7 — `XSETID ENTRIESADDED` / `MAXDELETEDID` — the whole stream-metadata restore surface is untested

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

### F8 — `XCLAIM FORCE` creates a PEL entry for an ID that is not in the stream, and never evicts PEL entries for deleted messages

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

### F9 — Timeseries duplicate timestamp landing in an already-compressed chunk inserts a second copy

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

### F10 — `XDELEX` / `XACKDEL` are implemented but excluded from the parity suites as "not implemented"

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

### F11 — Consumer-group **lag** / `entries-read` accounting has no coverage at all

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

### F12 — CMS, TopK and TDigest have **no** DUMP/RESTORE, RDB or WAL round-trip test

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
- **Folded in from the proposal's Deprioritised section**: *"`CMS.MERGE` does not validate the
  destination's own dimensions (`commands/src/cms.rs:384` `from_raw` silently replaces them) and
  `TDIGEST.MERGE` floors compression at 100 (`tdigest.rs:256-268`, so merging two compression-50
  digests yields 100). Both are Redis divergences worth one test each, folded into F12's content
  round-trip rather than scored separately."*

### F16 — JSONPath supports a narrow subset with silent-acceptance edges, and `proptest_json` has no correctness oracle

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

### F18 — `TS.RANGE` and `TS.MRANGE` disagree on unimplemented options — one errors, the other silently ignores

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
- **Folded in from the proposal's Deprioritised section**: *"`TS.CREATERULE` accepts a
  self-referencing rule (source == dest → infinite feedback) and `parse_labels` silently drops a
  trailing odd label. Real bugs; cheap tests; but subsumed by the broader 'timeseries option
  parsing has no negative coverage' theme in F17/F18."* (F17 is promoted to issue 23,
  `.scratch/testing-improvements-round2/issues/`; the residue lands here.)

### F19 — `VADD` accepts `NaN`/`inf` vector components

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

### F20 — Geo edge cases are tested only through a full RESP client — the anti-pattern the brief names

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`).
`MASTER.md` §5 names `types/src/geo.rs:335-352 geohash_range_for_bbox` among the dead code to
delete, but cites no finding numbers, so it claims nothing on its own — the deletion half should
land with that sweep, and the boundary-1 geo table stays here.

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

### Deprioritised in the proposal (unnumbered — not counted as findings)

Carried verbatim so nothing in `proposals/07-commands-extended-types.md` is lost. The proposal
gave these no `F<n>` numbers, so they are outside the 20-finding arithmetic and carry no
acceptance criterion. Two further bullets — the `CMS.MERGE`/`TDIGEST.MERGE` divergences and the
`TS.CREATERULE` self-reference / `parse_labels` bullet — are reproduced above inside F12 and F18
respectively, because the proposal itself folded them there.

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
- **`TDIGEST.BYRANK`/`BYREVRANK` are not implemented** at all. Missing feature, not a test gap.
- **`XREADGROUP` supports only a single stream** (`stream/read.rs:150-349`, `num_streams != 1`
  errors) and returns `InvalidArgument{"No such key"}` where Redis returns a `NOGROUP`-prefixed
  error. Divergence worth pinning, but it is loud, not silent.
- **Round 1 issue 27** already fixed 11 stream keyspace-notification bugs. Not re-proposing
  notification coverage; the residue is the metadata/JUSTID/DELREF surface in F5/F7/F10.

## Acceptance criteria

- [ ] F5: a test asserts that `XAUTOCLAIM g c 0 0-0 JUSTID` over a group with 3 pending entries returns IDs only, leaves the claimed entries' `XPENDING` retry-count **unchanged** (versus incremented without `JUSTID`), and advances the returned cursor; and that `XCLAIM ... TIME <ms>` makes `XPENDING` report the injected delivery time.
- [ ] F6: a test asserts that after appending 5 events across 2 aggregates and restarting, `ES.ALL` still returns all 5 in order; that `__frogdb:es:all` is invisible to `KEYS *`/`DBSIZE`/`SCAN`; and that an `ES.APPEND` whose `$all` write fails does not report success.
- [ ] F7: a test asserts `XSETID s 5-0 ENTRIESADDED 100 MAXDELETEDID 3-0` is reflected in all three `XINFO STREAM` fields, that `XSETID s 5-0 MAXDELETEDID 9-0` is rejected, and that a group's reported lag equals the hand-computed value after the metadata is restored.
- [ ] F8: a test asserts that `XCLAIM g c2 0 <id> FORCE` for an `XDEL`-ed entry does not leave a phantom PEL entry (or evicts it immediately, matching XAUTOCLAIM), and that `XPENDING` counts agree between the XCLAIM and XAUTOCLAIM routes.
- [ ] F9: a test asserts that re-adding timestamp 10 after 300 samples yields exactly one sample at that timestamp with the policy-correct value under **each** DUPLICATE_POLICY, and `TS.INFO totalSamples == 300`.
- [ ] F10: a test asserts, for one entry pending in two groups, that `XDELEX s ACKED IDS 1 <id>` leaves the entry alive until both groups have acked, that both PELs are clean afterwards, and that `XDELEX s BOGUSREF ...` errors rather than silently defaulting; and the XDELEX/XACKDEL exclusion bullets are removed from the `stream_tcl.rs`/`stream_cgroups_tcl.rs` headers.
- [ ] F11: a table-driven test asserts consumer-group `lag` against hand-computed expected values after add-N / read-M / XDEL-from-the-middle / XTRIM, including the case where `max_deleted_id` makes lag NULL.
- [ ] F12: `CORRUPT_KINDS` in `integration_dump_restore.rs` includes cms, topk, tdigest and vectorset, **and** a codec round-trip asserts query answers survive (`CMS.QUERY` counts, `TOPK.LIST WITHCOUNT` order and counts, `TDIGEST.QUANTILE` at 0/0.5/1, `TDIGEST.INFO` weight) rather than only `key_type()`; plus one assertion each that `CMS.MERGE` validates the destination's dimensions and that `TDIGEST.MERGE` does not floor compression at 100.
- [ ] F16: a table asserts a *specific* error (not a wrong answer) for each of `$.`, `$.a.`, `$[`, `$..a`, `$[0:2]`, `$[?(@.x>1)]`; and `proptest_json` asserts a correctness oracle — either a differential comparison against a reference JSONPath implementation, or the algebraic properties `set(p,v)`→`get(p) == v`, `delete(p)`→`get(p)` empty, and `get` never returning an unreachable value.
- [ ] F18: a table asserts that every unimplemented timeseries option (`ALIGN`, `BUCKETTIMESTAMP`, `EMPTY`, `GROUPBY`, `REDUCE`, `LATEST`) produces the **same** error from `TS.RANGE` and `TS.MRANGE`, and that an unparseable post-`FILTER` expression errors instead of being dropped; plus one assertion that a self-referencing `TS.CREATERULE` is rejected and one that `parse_labels` rejects a trailing odd label.
- [ ] F19: a test asserts `VADD k VALUES 3 nan 1 2 elem` errors and leaves the key unchanged/absent, likewise for `inf`/`-inf`; plus a proptest that repeated `VSIM` calls return a stable permutation.
- [ ] F20: a boundary-1 table asserts `geohash_encode`/`geohash_decode`/`haversine_distance`/`is_within_radius` at ±90 latitude, ±180 longitude, ±85.05112878 and `(0,0)`, with encode→decode round-tripping within the documented precision; and `geohash_range_for_bbox` is either deleted or has a test.

## Depends on

- issue 01, `.scratch/testing-improvements-round2/issues/` (I1 — `shard_driver` harness extension; the proposal's cross-area note 1 names `core/tests/shard_driver/harness.rs` as the boundary-3 home that F5, F7, F8, F10, F11 and F18 all need — real `ShardWorker` + `register_all` with `execute`, `capture_keyspace`, `memory_check` and `expiry_index_check`, and **zero** tests in this area use it today)
- issue 17, `.scratch/testing-improvements-round2/issues/` (I17 — `CORRUPT_KINDS` exhaustive over `TypeMarker`; F12(a) is exactly this, and the compile-time exhaustiveness link to `persistence/src/serialization/marker.rs:36` is what stops the next type being silently omitted)
