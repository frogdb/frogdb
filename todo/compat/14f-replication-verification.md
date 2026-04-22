# 14f. Replication Verification (4 tests — `set_tcl.rs`, `hash_tcl.rs`, `zset_tcl.rs`, `list_tcl.rs`)

**Status**: Done
**Scope**: Verify that SPOP and HGETDEL replicate correctly via Raft WAL, and that data survives
server restart (DEBUG RELOAD equivalent).

## Architecture

### SPOP Replication Propagation

Redis's `SPOP new implementation: code path #1 propagate as DEL or UNLINK` test verifies that
when SPOP removes the last member of a set, the replication stream contains a `DEL` (or `UNLINK`)
command rather than individual `SREM` commands. This ensures replicas clean up the key properly.

FrogDB uses Raft-based WAL replication. The SPOP command
(`frogdb-server/crates/commands/src/set.rs`) uses `WalStrategy::PersistOrDeleteFirstKey` which
means the WAL entry either persists the modified set or deletes the key if empty. This is the
correct behavior — the test should verify the WAL entry type when the set becomes empty.

### HGETDEL Replication Propagation

Redis's `HGETDEL propagated as HDEL command to replica` test verifies that HGETDEL's write
effect is replicated as an HDEL command (the canonical delete operation for hash fields).

FrogDB's HGETDEL (`frogdb-server/crates/commands/src/hash.rs`) uses WAL strategy for
persistence. The test should verify that after HGETDEL, the affected fields are deleted on
replicas — regardless of whether the WAL representation is HDEL or a state-based update.

### DEBUG RELOAD (Persistence Survival)

Redis's `ZSCORE after a DEBUG RELOAD` and `Check if list is still ok after a DEBUG RELOAD` tests
verify data survives a full reload of the dataset from disk. In FrogDB, the equivalent is a
server restart (since RocksDB persistence is always active, not snapshot-based).

**FrogDB equivalent**: Stop and restart the server, then verify data integrity.

## Implementation Steps

1. **SPOP replication test**:
   - Create a set, SPOP all members
   - Verify the key no longer exists
   - In a cluster setup, verify the replica sees the key deleted
   - Alternatively: inspect WAL entries to confirm delete propagation

2. **HGETDEL replication test**:
   - Create a hash, use HGETDEL to remove fields
   - Verify fields are gone on both primary and replica
   - Verify key deletion when all fields are removed

3. **Persistence survival tests**:
   - Implement a test helper that restarts the test server
   - After restart, verify ZSCORE returns correct values
   - After restart, verify list content and ordering is preserved

## Integration Points

- `frogdb-server/crates/commands/src/set.rs` — SPOP, WalStrategy
- `frogdb-server/crates/commands/src/hash.rs` — HGETDEL, WalStrategy
- Raft WAL infrastructure — entry types for delete vs. persist
- Test harness — server restart helper
- `frogdb-persistence` — RocksDB recovery path

## FrogDB Adaptations

| Redis Concept | FrogDB Equivalent |
|---------------|-------------------|
| Replication stream (DEL/SREM) | Raft WAL entry (PersistOrDelete) |
| Replica receiving HDEL | Raft log application on followers |
| DEBUG RELOAD (RDB load) | Server restart with RocksDB recovery |
| AOF rewrite verification | Not applicable (WAL-based) |

**Key difference**: Redis tests inspect the replication stream format. FrogDB tests should
verify the *effect* (data state on replicas) rather than the wire format, since Raft replication
uses state-based entries, not command replay.

## Tests

- `SPOP new implementation: code path #1 propagate as DEL or UNLINK`
- `HGETDEL propagated as HDEL command to replica`
- `ZSCORE after a DEBUG RELOAD - $encoding`
- `Check if list is still ok after a DEBUG RELOAD - $type`

## Verification

```bash
just test frogdb-server-redis-regression "SPOP.*propagate"
just test frogdb-server-redis-regression "HGETDEL propagated"
just test frogdb-server-redis-regression "ZSCORE after"
just test frogdb-server-redis-regression "list.*DEBUG RELOAD"
```
