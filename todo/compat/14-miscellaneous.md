# 14. Miscellaneous Smaller Items (~22 tests — mixed files)

Smaller items that don't warrant their own feature area but still need implementation.

## Items

| Item | Tests | File | Work |
|------|-------|------|------|
| `SMISMEMBER` arity error | 1 | `set_tcl.rs` | Match Redis error message format |
| `SORT` error messages | 2 | `sort_tcl.rs` | Match Redis numeric sort error format |
| `SORT BY` in cluster | 1 | `sort_tcl.rs` | Verify/fix SORT behavior with external keys in cluster |
| `SDIFF` fuzz test | 1 | `set_tcl.rs` | Port randomized set diff correctness test |
| `Hash fuzzing` | 2 | `hash_tcl.rs` | Port randomized hash correctness tests |
| `PFDEBUG GETREG` | 1 | `hyperloglog_tcl.rs` | Implement HLL register inspection command |
| `SCAN with expired keys` | 1 | `scan_tcl.rs` | Implement DEBUG SET-ACTIVE-EXPIRE equivalent |
| Query buffer observability | 3 | `querybuf_tcl.rs` | Add `qbuf=` field to CLIENT LIST, DEBUG PAUSE-CRON equiv |
| IO thread distribution | 2 | `introspection_tcl.rs` | Adapt to tokio task distribution metrics |
| Eventloop/memory metrics | 5 | `info_tcl.rs` | *Adapt*: tokio task/poll metrics, RocksDB memory stats |
| Dict/storage introspection | 3 | `other_tcl.rs` | *Adapt*: map rehash tests to RocksDB compaction behavior |

## Tests by Source File

### set_tcl.rs

- `SMOVE only notify dstset when the addition is successful` — requires keyspace notifications (see [01-keyspace-notifications.md](01-keyspace-notifications.md))
- `SPOP new implementation: code path #1 propagate as DEL or UNLINK` — **adapt**: verify SPOP replication via Raft
- `SMISMEMBER requires one or more members` — implement Redis-compatible arity error message
- `SDIFF fuzzing` — port fuzz test

### sort_tcl.rs

- `sort by in cluster mode` — **adapt**: test SORT behavior in FrogDB cluster mode
- `SORT will complain with numerical sorting and bad doubles (1)` — match Redis error format
- `SORT will complain with numerical sorting and bad doubles (2)` — match Redis error format
- `SETRANGE with huge offset` — large-memory stress test

### hash_tcl.rs

- `KEYS command return expired keys when allow_access_expired is 1` — **adapt**: implement equivalent config flag
- `HRANDFIELD with RESP3` — requires RESP3 protocol support (see [07-resp3-completion.md](07-resp3-completion.md))
- `HGETDEL propagated as HDEL command to replica` — **adapt**: verify HGETDEL replication via Raft
- `Hash fuzzing #1 - $size fields` — port fuzz test
- `Hash fuzzing #2 - $size fields` — port fuzz test

### hyperloglog_tcl.rs

- `PFDEBUG GETREG returns the HyperLogLog raw registers` — implement `PFDEBUG GETREG`

### scan_tcl.rs

- `{$type} SCAN with expired keys` — implement `DEBUG SET-ACTIVE-EXPIRE` or equivalent

### querybuf_tcl.rs

- `query buffer resized correctly`
- `query buffer resized correctly when not idle`
- `query buffer resized correctly with fat argv`

### introspection_tcl.rs

- `IO threads client number`
- `Clients are evenly distributed among io threads`

### info_tcl.rs

- `stats: eventloop metrics` — adapt: tokio task/poll cycle metrics
- `stats: instantaneous metrics` — adapt: equivalent sampling for FrogDB's async runtime
- `stats: client input and output buffer limit disconnections`
- `memory: database and pubsub overhead and rehashing dict count` — adapt: RocksDB memory stats
- `memory: used_memory_peak_time is updated when used_memory_peak is updated`

### other_tcl.rs

- `Don't rehash if redis has child process` — adapt: test compaction behavior under memory pressure
- `Redis can trigger resizing` — adapt: test storage engine resize/compaction
- `Redis can rewind and trigger smaller slot resizing` — adapt: test storage engine downsizing

### zset_tcl.rs

- `ZSCORE after a DEBUG RELOAD - $encoding` — **adapt**: test against server restart

### list_tcl.rs

- `Check if list is still ok after a DEBUG RELOAD - $type` — **adapt**: test against server restart

### introspection_tcl.rs (additional)

- `CONFIG sanity` — adapt: validate FrogDB config params
- `config during loading` — adapt: test config behavior during RocksDB recovery
- `CLIENT REPLY OFF/ON: disable all commands reply` — implement `CLIENT REPLY OFF`
- `CLIENT command unhappy path coverage` — implement `CLIENT CACHING`/`TRACKING` unhappy paths
- `RESET does NOT clean library name` — requires `RESET` command

### protocol_tcl.rs

- `test argument rewriting - issue 9598`
