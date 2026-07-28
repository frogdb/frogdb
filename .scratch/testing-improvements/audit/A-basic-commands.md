# Area A: Basic commands / Protocol / Expiry / Introspection (agent report, verified evidence)

Well-covered (filtered out): EXPIRE NX/XX/GT/LT + overflow/negative, maxmemory 7-policy matrix, WRONGTYPE across families, CLIENT NO-EVICT/UNBLOCK, OBJECT ENCODING/IDLETIME/FREQ, protocol multibulk/bulk-len limits, HSCAN NOVALUES, RESP3 Map/Set/Double/Push basics.

### 1. `errorstats-info-untested-end-to-end`
INFO errorstats/rejected_calls/failed_calls/total_error_replies fully implemented (counters+128-cap core/src/client_registry/mod.rs:37-88; prefix :96-99; dispatch wiring record_error_response dispatch.rs:614-622 + connection.rs:375-379; rendering info/sections.rs:532-542) but only unit-tested in isolation (fabricated counts sections.rs:1006-1012; counter mod.rs:1472+; prefix :1449). integration_info.rs:82-83 asserts headers only. NO test drives real error through server asserting errorstat_<PREFIX>:count / rejected vs failed split. info_tcl.rs:42-56 STALE: claims feature unimplemented, excludes all 10 upstream errorstats tests.
- expected: WRONGTYPE → errorstat_WRONGTYPE + failed_calls; arity/unknown → rejected_calls. Rejected-vs-failed = real contract.
- tests: re-port reclassifiable upstream errorstats tests + integration asserting exact deltas; fix stale doc.
- **L2 / C1** (observability accuracy — user-flagged priority).

### 2. `scan-full-iteration-guarantee-weak-test`
Content-hash cursor guards Redis full-iteration guarantee (core/src/store/hashmap.rs:1044-1085, comment :1044-1052). Only stress test tcl_scan_guarantees_under_write_load (redis-regression/tests/scan_tcl.rs:507-538) writes SAME 10 keys idempotently — no growth, no rehash, ~110-key table. Naive positional cursor regression would pass. No unit test forcing resizes mid-iteration; collision-at-COUNT-boundary "vanishingly rare skip" unpinned.
- tests: hashmap.rs unit — 5000 keys, iterate, insert 50k distinct between batches (multiple resizes), assert original N all returned; proptest random interleavings (present-throughout ⊆ returned). Strengthen TCL port w/ distinct keys.
- **L2 / C2** (SCAN-based migration/backup silently misses keys).

### 3. `resp3-double-non-finite-wire-format-untested`
RESP2 doubles via format_float (protocol/src/response.rs:239) special-cases inf/-inf/nan (:876-885), tested (zset_tcl.rs:2767-2777). RESP3 passes raw f64 into Resp3BytesFrame::Double (response.rs:320-323) — external redis-protocol encoder, format_float bypassed. All RESP3 Double tests finite (resp3.rs:370,:401; zset_tcl RESP3 finite only); no inf/nan anywhere in resp3.rs. zset exclusion :21 dropped ±inf tests under "internal-encoding" label but reply formatting is portable protocol behavior.
- expected: RESP3 ZSCORE of +inf member = exact wire `,inf\r\n` (and `,-inf\r\n`).
- tests: HELLO 3; ZADD k inf m; ZSCORE raw-read assert exact bytes; also ZINCRBY/GEODIST/ZADD INCR paths.
- **L2 / C2** (RESP3 clients break parsing; localized).

### 4. `restore-corrupt-payload-contract-untested`
DUMP/RESTORE frame (persistence/src/serialization/mod.rs:84-110 build_frame) = type byte/flags/expiry/LFU/padding/len — NO CRC64, NO version footer (Redis validates + rejects). deserialize (:113-160) validates header size + length bounds only. All RESTORE tests happy-path (integration_dump_restore.rs:57-384 round-trips/REPLACE/BUSYKEY/TTL; dump_tcl.rs existing-key/syntax/TTL). None feed corrupted/truncated/bit-flipped/type-mismatched payloads. deserialize.rs fuzz target = memory safety of low-level fn, not RESTORE command error contract. Upstream corrupt-dump-fuzzer.tcl excluded as "RDB" — correct for format, dropped FrogDB's own robustness coverage.
- expected: RESTORE of undeserializable payload → clean error, never materialize garbage/crash. No checksum → per-type validation is only defense; must fail closed per type.
- tests: DUMP each type; RESTORE w/ (a) truncation (b) flipped type byte (c) corrupted length (d) garbage; assert error + no key. Add restore_payload fuzz target driving actual command path.
- **L2 / C2.**

## Noted, not filed
- info_tcl.rs:42-56 stale doc (fix with gap 1).
- RESP3 Big-Number/Verbatim/Boolean no server→client coverage — no common command emits them.
- INCR i64::MAX overflow has error path (types/src/types/string_value.rs:168) but no explicit regression — low value (checked_add).
