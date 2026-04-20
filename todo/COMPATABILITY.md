# Redis 8.6.0 Compatibility — Tests to Implement

271 tests to implement or adapt across 22 files. 52 tests permanently excluded (encoding,
RDB/AOF, CLI, single-DB).

Tests marked **adapt** require translation from Redis-specific mechanisms to FrogDB equivalents
(e.g. replication → Raft, DEBUG RELOAD → server restart, RDB persistence → RocksDB).

---

## hotkeys_tcl.rs — 43 tests

Implement `HOTKEYS START/STOP/RESET/GET` subcommands under `CLIENT TRACKING`.

- `HOTKEYS START - METRICS required`
- `HOTKEYS START - METRICS with CPU only`
- `HOTKEYS START - METRICS with NET only`
- `HOTKEYS START - METRICS with both CPU and NET`
- `HOTKEYS START - with COUNT parameter`
- `HOTKEYS START - with DURATION parameter`
- `HOTKEYS START - with SAMPLE parameter`
- `HOTKEYS START - with SLOTS parameter in cluster mode`
- `HOTKEYS START - Error: session already started`
- `HOTKEYS START - Error: invalid METRICS count`
- `HOTKEYS START - Error: METRICS count mismatch`
- `HOTKEYS START - Error: METRICS invalid metrics`
- `HOTKEYS START - Error: METRICS same parameter`
- `HOTKEYS START - Error: COUNT out of range`
- `HOTKEYS START - Error: SAMPLE ratio invalid`
- `HOTKEYS START - Error: SLOTS not allowed in non-cluster mode`
- `HOTKEYS START - Error: SLOTS count mismatch`
- `HOTKEYS START - Error: SLOTS already specified`
- `HOTKEYS START - Error: duplicate slots`
- `HOTKEYS START - Error: invalid slot - negative value`
- `HOTKEYS START - Error: invalid slot - out of range`
- `HOTKEYS START - Error: invalid slot - non-integer`
- `HOTKEYS START - Error: slot not handled by this node`
- `HOTKEYS STOP - basic functionality`
- `HOTKEYS RESET - basic functionality`
- `HOTKEYS RESET - Error: session in progress`
- `HOTKEYS GET - returns nil when not started`
- `HOTKEYS GET - sample-ratio field`
- `HOTKEYS GET - no conditional fields without selected slots`
- `HOTKEYS GET - no conditional fields with sample_ratio = 1`
- `HOTKEYS GET - conditional fields with sample_ratio > 1 and selected slots`
- `HOTKEYS GET - selected-slots field with individual slots`
- `HOTKEYS GET - selected-slots with unordered input slots are sorted`
- `HOTKEYS GET - selected-slots returns full range in non-cluster mode`
- `HOTKEYS GET - selected-slots returns node's slot ranges when no SLOTS specified in cluster mode`
- `HOTKEYS GET - selected-slots returns each node's slot ranges in multi-node cluster`
- `HOTKEYS GET - RESP3 returns map with flat array values for hotkeys`
- `HOTKEYS - nested commands`
- `HOTKEYS - commands inside MULTI/EXEC`
- `HOTKEYS - EVAL inside MULTI/EXEC with nested calls`
- `HOTKEYS - tracks only keys in selected slots`
- `HOTKEYS - multiple selected slots`
- `HOTKEYS detection with biased key access, sample ratio = $sample_ratio`

## info_keysizes_tcl.rs — 38 tests

Implement `INFO keysizes`, key-memory-histograms, `DEBUG KEYSIZES-HIST-ASSERT`,
`DEBUG ALLOCSIZE-SLOTS-ASSERT`.

### KEYSIZES (16 tests)

- `KEYSIZES - Test i'th bin counts keysizes between (2^i) and (2^(i+1)-1) as expected $suffixRepl`
- `KEYSIZES - Histogram values of Bytes, Kilo and Mega $suffixRepl`
- `KEYSIZES - Test hyperloglog $suffixRepl`
- `KEYSIZES - Test List $suffixRepl`
- `KEYSIZES - Test SET $suffixRepl`
- `KEYSIZES - Test ZSET $suffixRepl`
- `KEYSIZES - Test STRING $suffixRepl`
- `KEYSIZES - Test STRING BITS $suffixRepl`
- `KEYSIZES - Test complex dataset $suffixRepl`
- `KEYSIZES - Test HASH ($type) $suffixRepl`
- `KEYSIZES - Test Hash field lazy expiration ($type) $suffixRepl`
- `KEYSIZES - Test RESTORE $suffixRepl`
- `KEYSIZES - Test RENAME $suffixRepl`
- `KEYSIZES - Test DEBUG KEYSIZES-HIST-ASSERT command`
- `KEYSIZES - Test RDB $suffixRepl` — **adapt**: test against RocksDB persistence
- `KEYSIZES - DEBUG RELOAD reset keysizes $suffixRepl` — **adapt**: test against server restart

### KEY-MEMORY-STATS (21 tests)

- `KEY-MEMORY-STATS - Empty database should have empty key memory histogram`
- `KEY-MEMORY-STATS - key memory histogram should appear`
- `KEY-MEMORY-STATS - List keys should appear in key memory histogram`
- `KEY-MEMORY-STATS - All data types should appear in key memory histogram`
- `KEY-MEMORY-STATS - Histogram bins should use power-of-2 labels`
- `KEY-MEMORY-STATS - DEL should remove key from key memory histogram`
- `KEY-MEMORY-STATS - Modifying a list should update key memory histogram`
- `KEY-MEMORY-STATS - FLUSHALL clears key memory histogram`
- `KEY-MEMORY-STATS - Larger allocations go to higher bins`
- `KEY-MEMORY-STATS - EXPIRE eventually removes from histogram`
- `KEY-MEMORY-STATS - Test RESTORE adds to histogram`
- `KEY-MEMORY-STATS - RENAME should preserve key memory histogram`
- `KEY-MEMORY-STATS - Hash field lazy expiration ($type)`
- `KEY-MEMORY-STATS - Test DEBUG KEYSIZES-HIST-ASSERT command`
- `KEY-MEMORY-STATS disabled - key memory histogram should not appear`
- `KEY-MEMORY-STATS - cannot enable key-memory-histograms at runtime when disabled at startup`
- `KEY-MEMORY-STATS - can disable key-memory-histograms at runtime and distrib_*_sizes disappear`
- `KEY-MEMORY-STATS - cannot re-enable key-memory-histograms at runtime after disabling`
- `KEY-MEMORY-STATS - DEBUG RELOAD preserves key memory histogram` — **adapt**: test against server restart
- `KEY-MEMORY-STATS - Replication updates key memory stats on replica` — **adapt**: test against FrogDB replication
- `KEY-MEMORY-STATS - DEL on primary updates key memory stats on replica` — **adapt**: test against FrogDB replication

### SLOT-ALLOCSIZE (1 test)

- `SLOT-ALLOCSIZE - Test DEBUG ALLOCSIZE-SLOTS-ASSERT command`

## pubsub_tcl.rs — 30 tests

### Keyspace notifications (25 tests)

Implement `CONFIG SET notify-keyspace-events` and the keyspace notification pub/sub mechanism.

- `Keyspace notifications: we receive keyspace notifications`
- `Keyspace notifications: we receive keyevent notifications`
- `Keyspace notifications: we can receive both kind of events`
- `Keyspace notifications: we are able to mask events`
- `Keyspace notifications: general events test`
- `Keyspace notifications: list events test`
- `Keyspace notifications: set events test`
- `Keyspace notifications: zset events test`
- `Keyspace notifications: hash events test ($type)`
- `Keyspace notifications: stream events test`
- `Keyspace notifications:FXX/FNX with HSETEX cmd`
- `Keyspace notifications: expired events (triggered expire)`
- `Keyspace notifications: expired events (background expire)`
- `Keyspace notifications: evicted events`
- `Keyspace notifications: test CONFIG GET/SET of event flags`
- `Keyspace notifications: new key test`
- `Keyspace notifications: overwritten events - string to string`
- `Keyspace notifications: type_changed events - hash to string`
- `Keyspace notifications: both overwritten and type_changed events`
- `Keyspace notifications: configuration flags work correctly`
- `Keyspace notifications: RESTORE REPLACE different type - restore, overwritten and type_changed events`
- `Keyspace notifications: SET on existing string key - overwritten event`
- `Keyspace notifications: setKey on existing different type key - overwritten and type_changed events`
- `Keyspace notifications: overwritten and type_changed events for RENAME and COPY commands`
- `Keyspace notifications: overwritten and type_changed for *STORE* commands`

### RESP3 pub/sub (5 tests)

Requires RESP3 protocol support (`HELLO 3`).

- `Pub/Sub PING on RESP$resp`
- `PubSub messages with CLIENT REPLY OFF`
- `publish to self inside multi`
- `publish to self inside script`
- `unsubscribe inside multi, and publish to self`

## info_tcl.rs — 26 tests

### Error statistics (12 tests)

Implement per-error-type tracking in `INFO stats` (`errorstat_*` fields, `rejected_calls`,
`failed_calls`).

- `errorstats: failed call authentication error`
- `errorstats: failed call NOSCRIPT error`
- `errorstats: failed call NOGROUP error`
- `errorstats: failed call within LUA`
- `errorstats: failed call within MULTI/EXEC`
- `errorstats: rejected call unknown command`
- `errorstats: rejected call within MULTI/EXEC`
- `errorstats: rejected call due to wrong arity`
- `errorstats: rejected call by OOM error`
- `errorstats: rejected call by authorization error`
- `errorstats: blocking commands`
- `errorstats: limit errors will not increase indefinitely`

### Latency statistics (6 tests)

Implement per-command latency histograms (`latencystats` in `INFO`), configurable percentiles.

- `latencystats: disable/enable`
- `latencystats: configure percentiles`
- `latencystats: bad configure percentiles`
- `latencystats: blocking commands`
- `latencystats: subcommands`
- `latencystats: measure latency`

### Client info fields (2 tests)

Add `pubsub_clients`, `watching_clients`, `total_watched_keys` to `INFO clients` and `watch=N`
to `CLIENT INFO`.

- `clients: pubsub clients`
- `clients: watching clients`

### Eventloop / memory metrics (5 tests)

**Adapt** to FrogDB's async runtime (tokio) and RocksDB memory model.

- `stats: eventloop metrics` — adapt: tokio task/poll cycle metrics instead of Redis event loop
- `stats: instantaneous metrics` — adapt: equivalent sampling for FrogDB's async runtime
- `stats: client input and output buffer limit disconnections`
- `memory: database and pubsub overhead and rehashing dict count` — adapt: RocksDB memory stats
- `memory: used_memory_peak_time is updated when used_memory_peak is updated`

### Cluster (1 test)

- `Verify that LUT overhead is properly updated when dicts are emptied or reused` — **adapt**: test equivalent memory accounting in FrogDB's storage layer

## client_eviction_tcl.rs — 15 tests

Implement `maxmemory-clients` configuration and per-client memory tracking with eviction.

- `client evicted due to large argv`
- `client evicted due to large query buf`
- `client evicted due to large multi buf`
- `client evicted due to percentage of maxmemory`
- `client evicted due to watched key list`
- `client evicted due to pubsub subscriptions`
- `client evicted due to tracking redirection`
- `client evicted due to client tracking prefixes`
- `client evicted due to output buf`
- `client no-evict $no_evict`
- `avoid client eviction when client is freed by output buffer limit`
- `decrease maxmemory-clients causes client eviction`
- `evict clients only until below limit`
- `evict clients in right order (large to small)`
- `client total memory grows during $type`

## introspection_tcl.rs — 18 tests

### MONITOR command (6 tests)

Implement the `MONITOR` command for real-time command logging.

- `MONITOR can log executed commands`
- `MONITOR can log commands issued by the scripting engine`
- `MONITOR can log commands issued by functions`
- `MONITOR supports redacting command arguments`
- `MONITOR correctly handles multi-exec cases`
- `MONITOR log blocked command only once`

### CONFIG REWRITE (5 tests)

**Adapt** to FrogDB's TOML-based config persistence.

- `CONFIG REWRITE sanity`
- `CONFIG REWRITE handles save and shutdown properly`
- `CONFIG REWRITE handles rename-command properly`
- `CONFIG REWRITE handles alias config properly`
- `CONFIG save params special case handled properly`

### Protocol (3 tests)

- `CLIENT REPLY OFF/ON: disable all commands reply` — implement `CLIENT REPLY OFF`
- `CLIENT command unhappy path coverage` — implement `CLIENT CACHING`/`TRACKING` unhappy paths
- `RESET does NOT clean library name` — requires `RESET` command (see [other_tcl.rs](#other_tclrs--8-tests))

### Config adaptation (2 tests)

**Adapt** to FrogDB's config format.

- `CONFIG sanity` — adapt: validate FrogDB config params instead of Redis ones
- `config during loading` — adapt: test config behavior during RocksDB recovery

### IO thread distribution (2 tests)

**Adapt** to FrogDB's tokio-based threading model.

- `IO threads client number`
- `Clients are evenly distributed among io threads`

## multi_tcl.rs — 17 tests

### OOM handling (2 tests)

Implement `maxmemory` enforcement in transaction context.

- `EXEC with at least one use-memory command should fail`
- `EXEC with only read commands should not be rejected when OOM`

### Replication adaptation (8 tests)

**Adapt** to FrogDB's Raft-based replication. Verify that commands within MULTI/EXEC are
correctly replicated to followers.

- `MULTI propagation of EVAL`
- `MULTI propagation of PUBLISH`
- `MULTI propagation of SCRIPT LOAD`
- `MULTI propagation of SCRIPT FLUSH`
- `MULTI propagation of XREADGROUP`
- `MULTI with $cmd` — inner-command propagation matrix
- `exec with write commands and state change`
- `exec with read commands and stale replica state change`

### DEBUG: WATCH with stale keys (3 tests)

Implement `DEBUG SET-ACTIVE-EXPIRE` or equivalent mechanism to test stale-key behavior.

- `WATCH stale keys should not fail EXEC`
- `Delete WATCHed stale keys should not fail EXEC`
- `FLUSHDB while watching stale keys should not fail EXEC`

### Script timeout (3 tests)

Implement configurable Lua script execution timeout.

- `MULTI and script timeout`
- `EXEC and script timeout`
- `just EXEC and script timeout`

### Config error handling (1 test)

- `MULTI with config error` — **adapt**: test against FrogDB config error behavior

## introspection2_tcl.rs — 16 tests

### OBJECT IDLETIME / access tracking (5 tests)

Implement `OBJECT IDLETIME` (LRU access time tracking) and no-touch mode
(`CLIENT NO-TOUCH ON`).

- `TTL, TYPE and EXISTS do not alter the last access time of a key`
- `TOUCH alters the last access time of a key`
- `Operations in no-touch mode do not alter the last access time of a key`
- `Operations in no-touch mode TOUCH alters the last access time of a key`
- `Operations in no-touch mode TOUCH from script alters the last access time of a key`

### Command statistics (6 tests)

Implement per-command call/usec statistics in `INFO commandstats`.

- `command stats for GEOADD`
- `command stats for EXPIRE`
- `command stats for BRPOP`
- `command stats for MULTI`
- `command stats for scripts`
- `errors stats for GEOADD`

### COMMAND GETKEYSANDFLAGS (5 tests)

Implement `COMMAND GETKEYSANDFLAGS` and movablekeys flag.

- `COMMAND GETKEYSANDFLAGS`
- `COMMAND GETKEYSANDFLAGS invalid args`
- `COMMAND GETKEYSANDFLAGS MSETEX`
- `$cmd command will not be marked with movablekeys`
- `$cmd command is marked with movablekeys`

## zset_tcl.rs — 10 tests

Requires RESP3 protocol support and `DEBUG RELOAD` adaptation.

### RESP3 (9 tests)

- `ZINTER RESP3 - $encoding`
- `Basic $popmin/$popmax - $encoding RESP3`
- `$popmin/$popmax with count - $encoding RESP3`
- `$popmin/$popmax - $encoding RESP3`
- `BZPOPMIN/BZPOPMAX readraw in RESP$resp`
- `ZMPOP readraw in RESP$resp`
- `BZMPOP readraw in RESP$resp`
- `ZRANGESTORE RESP3`
- `ZRANDMEMBER with RESP3`

### DEBUG RELOAD (1 test)

- `ZSCORE after a DEBUG RELOAD - $encoding` — **adapt**: test against server restart

## maxmemory_tcl.rs — 9 tests

### Client eviction scenarios (7 tests)

Requires `maxmemory-clients` implementation (see [client_eviction_tcl.rs](#client_eviction_tclrs--15-tests)).

- `eviction due to output buffers of many MGET clients, client eviction: false`
- `eviction due to output buffers of many MGET clients, client eviction: true`
- `eviction due to input buffer of a dead client, client eviction: false`
- `eviction due to input buffer of a dead client, client eviction: true`
- `eviction due to output buffers of pubsub, client eviction: false`
- `eviction due to output buffers of pubsub, client eviction: true`
- `client tracking don't cause eviction feedback loop` — also requires RESP3 (`HELLO 3`)

### Access tracking (1 test)

- `lru/lfu value of the key just added` — implement `OBJECT IDLETIME`/`FREQ` tracking

### Dict rehashing under memory pressure (1 test)

- `Don't rehash if used memory exceeds maxmemory after rehash` — **adapt**: test equivalent
  memory-pressure behavior in FrogDB (e.g. RocksDB compaction deferral)

## other_tcl.rs — 8 tests

### RESET command (5 tests)

Implement the `RESET` command to clear client state.

- `RESET clears client state`
- `RESET clears MONITOR state`
- `RESET clears and discards MULTI state`
- `RESET clears Pub/Sub state`
- `RESET clears authenticated state`

### Dict introspection (3 tests)

**Adapt** to FrogDB's storage engine. Test equivalent behaviors if applicable (e.g. hash table
resizing under memory constraints).

- `Don't rehash if redis has child process` — adapt: test compaction behavior under memory pressure
- `Redis can trigger resizing` — adapt: test storage engine resize/compaction
- `Redis can rewind and trigger smaller slot resizing` — adapt: test storage engine downsizing

## protocol_tcl.rs — 7 tests

### RESP3 protocol (6 tests)

- `RESP3 attributes`
- `RESP3 attributes readraw`
- `RESP3 attributes on RESP2`
- `test big number parsing` — also needs DEBUG
- `test bool parsing` — also needs DEBUG
- `test verbatim str parsing` — also needs DEBUG

### DEBUG (1 test)

- `test argument rewriting - issue 9598`

## lazyfree_tcl.rs — 7 tests

Implement `lazyfreed_objects` and `lazyfree_pending_objects` counters, `CONFIG RESETSTAT`.

- `lazy free a stream with all types of metadata`
- `lazy free a stream with deleted cgroup`
- `FLUSHALL SYNC optimized to run in bg as blocking FLUSHALL ASYNC`
- `Run consecutive blocking FLUSHALL ASYNC successfully`
- `FLUSHALL SYNC in MULTI not optimized to run as blocking FLUSHALL ASYNC`
- `Client closed in the middle of blocking FLUSHALL ASYNC`
- `Pending commands in querybuf processed once unblocking FLUSHALL ASYNC`

## functions_tcl.rs — 6 tests

- `FUNCTION - deny oom` — implement maxmemory enforcement for functions
- `FUNCTION - deny oom on no-writes function` — OOM should not block read-only functions
- `FUNCTION - test debug reload different options` — **adapt**: test against server restart
- `FUNCTION - test debug reload with nosave and noflush` — **adapt**: test against server restart
- `FUNCTION - allow stale` — **adapt**: test against FrogDB Raft replication (stale replica reads)
- `FUNCTION - test function dump and restore` — **adapt**: implement `FUNCTION DUMP`/`RESTORE` with FrogDB serialization format

## hash_tcl.rs — 5 tests

- `KEYS command return expired keys when allow_access_expired is 1` — **adapt**: implement equivalent config flag
- `HRANDFIELD with RESP3` — requires RESP3 protocol support
- `HGETDEL propagated as HDEL command to replica` — **adapt**: verify HGETDEL replication via Raft
- `Hash fuzzing #1 - $size fields` — port fuzz test
- `Hash fuzzing #2 - $size fields` — port fuzz test

## set_tcl.rs — 4 tests

- `SMOVE only notify dstset when the addition is successful` — requires keyspace notifications
- `SPOP new implementation: code path #1 propagate as DEL or UNLINK` — **adapt**: verify SPOP replication via Raft
- `SMISMEMBER requires one or more members` — implement Redis-compatible arity error message
- `SDIFF fuzzing` — port fuzz test

## sort_tcl.rs — 4 tests

- `sort by in cluster mode` — **adapt**: test SORT behavior in FrogDB cluster mode
- `SORT will complain with numerical sorting and bad doubles (1)` — match Redis error format
- `SORT will complain with numerical sorting and bad doubles (2)` — match Redis error format
- `SETRANGE with huge offset` — large-memory stress test

## querybuf_tcl.rs — 3 tests

**Adapt** to FrogDB's config format. Implement observable `qbuf=` field in `CLIENT LIST` and
equivalent of `DEBUG PAUSE-CRON`.

- `query buffer resized correctly`
- `query buffer resized correctly when not idle`
- `query buffer resized correctly with fat argv`

## scripting_tcl.rs — 2 tests

Requires RESP3 protocol support.

- `Script with RESP3 map`
- `Script return recursive object`

## hyperloglog_tcl.rs — 1 test

- `PFDEBUG GETREG returns the HyperLogLog raw registers` — implement `PFDEBUG GETREG`

## list_tcl.rs — 1 test

- `Check if list is still ok after a DEBUG RELOAD - $type` — **adapt**: test against server restart

## scan_tcl.rs — 1 test

- `{$type} SCAN with expired keys` — implement `DEBUG SET-ACTIVE-EXPIRE` or equivalent

---

## Permanently Excluded (52 tests)

### Encoding assertions — 34 tests

Tests that assert Redis-internal encoding types (ziplist, listpack, intset, hashtable) which
FrogDB does not use. These cannot be meaningfully adapted.

**hash_tcl.rs** (21): `Is the small hash encoded with a listpack?`, `Is the big hash encoded
with an hash table?`, `Is a ziplist encoded Hash promoted on big payload?`, `HGET against the
small/big hash`, `HMSET - small/big hash`, `HMGET - small/big hash`, `HKEYS - small/big hash`,
`HVALS - small/big hash`, `HGETALL - small/big hash`, `HSTRLEN against the small/big hash`,
`HRANDFIELD - $type`, `Stress test the hash ziplist -> hashtable encoding conversion`,
`Hash ziplist of various encodings`, `Hash ziplist of various encodings - sanitize dump`

**set_tcl.rs** (12): `SADD overflows the maximum allowed elements in a listpack - $type`,
`Set encoding after DEBUG RELOAD`, `Generated sets must be encoded correctly - $type`,
`SDIFFSTORE with three sets - $type`, `SUNION hashtable and listpack`,
`SRANDMEMBER - $type`, `SPOP integer from listpack set`,
`SPOP new implementation: code path #1/2/3 $type`,
`SRANDMEMBER histogram distribution - $type`,
`SRANDMEMBER with a dict containing long chain`

**sort_tcl.rs** (1): `$command GET <const>` (encoding-loop variant)

### RDB/AOF-specific — 7 tests

**info_keysizes_tcl.rs** (1): `KEY-MEMORY-STATS - RDB save and restart preserves key memory
histogram`

**info_tcl.rs** (1): `stats: debug metrics` (AOF/cron duration sums)

**lazyfree_tcl.rs** (1): `Unblocks client blocked on lazyfree via REPLICAOF command`

**multi_tcl.rs** (2): `MULTI with BGREWRITEAOF`, `MULTI with config set appendonly`

**other_tcl.rs** (2): `Same dataset digest if saving/reloading as AOF?`,
`EXPIRES after AOF reload (without rewrite)`

### Single-DB — 6 tests

**info_keysizes_tcl.rs** (2): `KEYSIZES - Test MOVE $suffixRepl`,
`KEYSIZES - Test SWAPDB $suffixRepl`

**multi_tcl.rs** (4): `SWAPDB does not touch non-existing key replaced with stale key`,
`SWAPDB does not touch stale key replaced with another stale key`,
`WATCH is able to remember the DB a key belongs to`,
`SWAPDB does not touch watched stale keys`

### CLI arguments — 5 tests

**introspection_tcl.rs** (5): `redis-server command line arguments - error cases`,
`...allow passing option name and option value in the same arg`,
`...wrong usage that we support anyway`, `...save with empty input`,
`...take one bulk string with spaces for MULTI_ARG configs parsing`
