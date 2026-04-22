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
| 1 | [Keyspace Notifications](01-keyspace-notifications.md) | 25 | `pubsub_tcl.rs` | Implemented (tests need porting) |
| 2 | [HOTKEYS Subsystem](02-hotkeys.md) | 43 | `hotkeys_tcl.rs` | Not implemented |
| 3 | [INFO Keysizes & Key-Memory-Stats](03-info-keysizes.md) | 38 | `info_keysizes_tcl.rs` | Not implemented |
| 4 | [Error Statistics](04-error-statistics.md) | 12 | `info_tcl.rs` | Implemented (tests need porting) |
| 5 | [Latency Histograms for INFO](05-latency-histograms.md) | 6 | `info_tcl.rs` | Implemented (tests need porting) |
| 6 | [Client Eviction (`maxmemory-clients`)](06-client-eviction.md) | 22 | `client_eviction_tcl.rs` + `maxmemory_tcl.rs` | Not implemented |
| 7 | [RESP3 Protocol Completion](07-resp3-completion.md) | 22 | mixed | Mostly implemented |
| 8 | CONFIG REWRITE | 5 | `introspection_tcl.rs` | Done |
| 9 | [MULTI/EXEC Enhancements](09-multi-exec.md) | 17 | `multi_tcl.rs` | Partial |
| 10 | [OBJECT IDLETIME & Access Tracking](10-object-idletime.md) | 6 | `introspection2_tcl.rs` + `maxmemory_tcl.rs` | Partial |
| 11 | [Command Statistics & GETKEYSANDFLAGS](11-command-stats.md) | 11 | `introspection2_tcl.rs` | Done (11/11 pass) |
| 12 | Lazyfree & Async Deletion | 7 | `lazyfree_tcl.rs` | Done (9/9 pass) |
| 13 | [FUNCTION Enhancements](13-function-enhancements.md) | 6 | `functions_tcl.rs` | Partial |
| 14 | [Miscellaneous Smaller Items](14-miscellaneous.md) (index) | ~22 | mixed | Mixed |
| 14a | [Error Message Fixes](14a-error-message-fixes.md) | 3 | `set_tcl.rs`, `sort_tcl.rs` | Partially implemented |
| 14b | [Hash & Set Fuzzing](14b-hash-set-fuzzing.md) | 3 | `set_tcl.rs`, `hash_tcl.rs` | Not implemented |
| 14c | [Debug Commands & Expired Key Scanning](14c-debug-commands.md) | 3 | `hyperloglog_tcl.rs`, `scan_tcl.rs` | Mostly implemented (1 fail: scan_with_expired_keys) |
| 14d | [Query Buffer Observability](14d-query-buffer-observability.md) | 3 | `querybuf_tcl.rs` | Not implemented |
| 14e | [Runtime Metrics Adaptation](14e-runtime-metrics-adaptation.md) | 10 | mixed | Not implemented |
| 14f | [Replication Verification](14f-replication-verification.md) | 4 | mixed | Done |
| 14g | [Introspection Command Gaps](14g-introspection-gaps.md) | 5+ | `introspection_tcl.rs` | Mixed |

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
