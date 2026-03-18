# Redis 8 Command Gap Analysis

Audit of Redis commands available in Redis 7.4 through 8.4 that are not yet implemented in FrogDB,
separated into commands that should be implemented for compatibility vs those intentionally omitted.

## Commands That Should Be Implemented

### Vector Set Commands (Redis 8.0) — new data type, ~12 commands

A new native data type for HNSW-based vector similarity search.

| Command     | Description               |
| ----------- | ------------------------- |
| VADD        | Add element to vector set |
| VCARD       | Get cardinality           |
| VDIM        | Get dimensionality        |
| VEMB        | Get vector embedding      |
| VGETATTR    | Get element attributes    |
| VINFO       | Get vector set info       |
| VLINKS      | Get HNSW graph links      |
| VRANDMEMBER | Get random element(s)     |
| VREM        | Remove element            |
| VSETATTR    | Set element attributes    |
| VSIM        | Similarity search         |
| VRANGE      | Range query               |

### Search Commands — 1 command

| Command   | Description                                       |
| --------- | ------------------------------------------------- |
| FT.HYBRID | Hybrid vector+text search with RRF/linear ranking |

### Probabilistic Data Structures (Redis Stack)

Commonly expected by Redis Stack clients. FrogDB has Bloom Filters, Cuckoo Filters, T-Digest,
and Top-K but not these other types.

**Count-Min Sketch** (~6 commands): CMS.INCRBY, CMS.INFO, CMS.INITBYDIM, CMS.INITBYPROB,
CMS.MERGE, CMS.QUERY

## Stubs (recognized but return "not implemented")

| Command      | Notes                                          |
| ------------ | ---------------------------------------------- |
| WAITAOF      | Should eventually be implemented               |
| BGREWRITEAOF | FrogDB uses different persistence model        |
| SAVE         | FrogDB uses different persistence model        |
| SYNC         | Old replication protocol; PSYNC is implemented |

## Intentionally Not Implemented

| Command                                                             | Reason                                                   |
| ------------------------------------------------------------------- | -------------------------------------------------------- |
| SELECT (non-zero)                                                   | Single database per instance; SELECT 0 accepted as no-op |
| SWAPDB                                                              | Single database architecture                             |
| MOVE                                                                | Single database architecture                             |
| MODULE                                                              | No modular architecture — features are native            |
| MONITOR                                                             | Not planned (diagnostic command)                         |
| CONFIG REWRITE                                                      | Changes are transient; use TOML config directly          |
| DEBUG SEGFAULT/RELOAD/CRASH-AND-RECOVER/SET-ACTIVE-EXPIRE/OOM/PANIC | Unsafe — explicitly rejected                             |

## Priority

1. **Medium**: Vector sets (12 commands) — new data type, growing adoption
2. **Medium**: Probabilistic data structures (CMS) — common in Redis Stack
   deployments
3. **Lower**: FT.HYBRID
