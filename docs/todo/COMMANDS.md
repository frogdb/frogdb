# Redis 8 Command Gap Analysis

Audit of Redis commands available in Redis 7.4 through 8.4 that are not yet implemented in FrogDB,
separated into commands that should be implemented for compatibility vs those intentionally omitted.

## Commands That Should Be Implemented

### Probabilistic Data Structures (Redis Stack)

All commonly expected Redis Stack probabilistic data structures are now implemented: Bloom Filters,
Cuckoo Filters, Count-Min Sketch, T-Digest, and Top-K.

## Stubs (recognized but return "not implemented")

| Command | Notes                                                                    |
| ------- | ------------------------------------------------------------------------ |
| WAITAOF | Could be adapted for per-write durability confirmation in FrogDB's model |

## Intentionally Not Implemented

| Command                                                             | Reason                                                   |
| ------------------------------------------------------------------- | -------------------------------------------------------- |
| SELECT (non-zero)                                                   | Single database per instance; SELECT 0 accepted as no-op |
| SWAPDB                                                              | Single database architecture                             |
| MOVE                                                                | Single database architecture                             |
| MODULE                                                              | No modular architecture — features are native            |
| SAVE                                                                | Continuous WAL persistence; use BGSAVE for snapshots     |
| BGREWRITEAOF                                                        | No AOF; RocksDB manages WAL lifecycle                    |
| SYNC                                                                | Legacy replication protocol; PSYNC is implemented        |
| MONITOR                                                             | Not planned (diagnostic command)                         |
| CONFIG REWRITE                                                      | Changes are transient; use TOML config directly          |
| DEBUG SEGFAULT/RELOAD/CRASH-AND-RECOVER/SET-ACTIVE-EXPIRE/OOM/PANIC | Unsafe — explicitly rejected                             |
