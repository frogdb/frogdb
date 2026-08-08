# Permanently Excluded (52 tests)

These tests cannot be meaningfully adapted to FrogDB.

## Encoding assertions — 34 tests

Tests that assert Redis-internal encoding types (ziplist, listpack, intset, hashtable) which
FrogDB does not use.

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

## RDB/AOF-specific — 7 tests

**info_keysizes_tcl.rs** (1): `KEY-MEMORY-STATS - RDB save and restart preserves key memory histogram`

**info_tcl.rs** (1): `stats: debug metrics` (AOF/cron duration sums)

**lazyfree_tcl.rs** (1): `Unblocks client blocked on lazyfree via REPLICAOF command`

**multi_tcl.rs** (2): `MULTI with BGREWRITEAOF`, `MULTI with config set appendonly`

**other_tcl.rs** (2): `Same dataset digest if saving/reloading as AOF?`,
`EXPIRES after AOF reload (without rewrite)`

## Single-DB — 6 tests

**info_keysizes_tcl.rs** (2): `KEYSIZES - Test MOVE $suffixRepl`,
`KEYSIZES - Test SWAPDB $suffixRepl`

**multi_tcl.rs** (4): `SWAPDB does not touch non-existing key replaced with stale key`,
`SWAPDB does not touch stale key replaced with another stale key`,
`WATCH is able to remember the DB a key belongs to`,
`SWAPDB does not touch watched stale keys`

## CLI arguments — 5 tests

**introspection_tcl.rs** (5): `redis-server command line arguments - error cases`,
`...allow passing option name and option value in the same arg`,
`...wrong usage that we support anyway`, `...save with empty input`,
`...take one bulk string with spaces for MULTI_ARG configs parsing`
