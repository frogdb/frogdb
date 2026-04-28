# Redis 8.6.0 Compatibility — Action Items

271 tests across 22 files. After accounting for already-implemented features (MONITOR, RESET,
CLIENT REPLY, OBJECT IDLETIME, RESP3/HELLO, latency stats), the remaining work breaks into
**14 discrete feature areas** below. Each is an independent unit suitable for parallel planning.

**Expansion format**: Each item will be expanded into an architecture outline — key design
decisions, data flow, module boundaries — not function-level specifics. Adaptation tests
(replication via Raft, RocksDB persistence, tokio metrics) are included as sub-items within
their parent feature area.

52 tests permanently excluded (encoding, RDB/AOF, CLI, single-DB) — see
[excluded.md](excluded.md).

---

## Action Items

| # | Feature | Tests | File | Status |
|---|---------|-------|------|--------|
| 1 | Keyspace Notifications | 25 | `pubsub_tcl.rs` | Done |
| 2 | HOTKEYS Subsystem | 43 | `hotkeys_tcl.rs` | Done (38 pass, 6 cluster-excluded) |
| 3 | INFO Keysizes & Key-Memory-Stats | 38 | `info_keysizes_tcl.rs` | Done (34 pass, 20 repl-excluded) |
| 4 | Error Statistics | 12 | `info_tcl.rs` | Done |
| 5 | Latency Histograms for INFO | 6 | `info_tcl.rs` | Done |
| 6 | Client Eviction (`maxmemory-clients`) | 22 | `client_eviction_tcl.rs` + `maxmemory_tcl.rs` | Done (22 pass) |
| 7 | RESP3 Protocol Completion | 22 | mixed | Done |
| 8 | CONFIG REWRITE | 5 | `introspection_tcl.rs` | Done |
| 9 | MULTI/EXEC Enhancements | 17 | `multi_tcl.rs` | Done |
| 10 | OBJECT IDLETIME & Access Tracking | 6 | `introspection2_tcl.rs` + `maxmemory_tcl.rs` | Done |
| 11 | Command Statistics & GETKEYSANDFLAGS | 11 | `introspection2_tcl.rs` | Done (11/11 pass) |
| 12 | Lazyfree & Async Deletion | 7 | `lazyfree_tcl.rs` | Done (9/9 pass) |
| 13 | FUNCTION Enhancements | 6 | `functions_tcl.rs` | Done |
| 14 | Miscellaneous Smaller Items (index) | ~22 | mixed | See sub-items |
| 14a | Error Message Fixes | 3 | `set_tcl.rs`, `sort_tcl.rs` | Done |
| 14b | Hash & Set Fuzzing | 3 | `set_tcl.rs`, `hash_tcl.rs` | Done |
| 14c | Debug Commands & Expired Key Scanning | 3 | `hyperloglog_tcl.rs`, `scan_tcl.rs` | Done |
| 14d | Query Buffer Observability | 3 | `querybuf_tcl.rs` | Deferred |
| 14e | Runtime Metrics Adaptation | 10 | mixed | Deferred |
| 14f | Replication Verification | 4 | mixed | Done |
| 14g | Introspection Command Gaps | 5+ | `introspection_tcl.rs` | Done |

---

## Priority Ordering (suggested)

**High value / many tests unlocked:**
1. Keyspace Notifications (25 tests) — enables pub/sub notification ecosystem
2. RESP3 Protocol Completion (22 tests) — unblocks many scattered tests
3. Client Eviction (22 tests combined) — important for production safety
4. Error Statistics (12 tests) — straightforward observability improvement

**Medium value:**
5. HOTKEYS (43 tests but entirely new subsystem with uncertain value)
6. INFO Keysizes (38 tests but complex tracking infrastructure)
7. MULTI/EXEC Enhancements (17 tests, mostly verification)
8. Latency Histograms (6 tests, builds on existing infrastructure)
9. Command Statistics & GETKEYSANDFLAGS (11 tests)

**Lower priority / adaptation work:**
10. CONFIG REWRITE (5 tests)
11. Lazyfree Counters (7 tests)
12. FUNCTION Enhancements (6 tests)
13. OBJECT IDLETIME verification (6 tests)
14. Miscellaneous (assorted small fixes)

---

## Verification

After implementing each feature area:
- Run `just test redis-regression <test_file_name>` for the relevant test file
- Verify all previously-skipped tests now pass
- Run full regression suite to confirm no regressions
