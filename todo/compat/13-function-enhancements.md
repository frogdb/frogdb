# 13. FUNCTION Enhancements (6 tests — `functions_tcl.rs`)

**Status**: FUNCTION LOAD/DELETE/FLUSH/LIST/STATS/DUMP/RESTORE implemented
**Scope**: Add OOM enforcement, stale-replica behavior, and verify DUMP/RESTORE format.

## Work

### OOM enforcement (2 tests)

Reject function execution when OOM (except read-only functions).

- `FUNCTION - deny oom`
- `FUNCTION - deny oom on no-writes function`

### Stale replica (1 test — *adapt*)

Allow `allow-stale` flagged functions on stale Raft followers.

- `FUNCTION - allow stale`

### DUMP/RESTORE (1 test — *adapt*)

Verify FrogDB serialization format round-trips correctly.

- `FUNCTION - test function dump and restore`

### Server restart (2 tests — *adapt*)

Verify functions persist across RocksDB restart.

- `FUNCTION - test debug reload different options`
- `FUNCTION - test debug reload with nosave and noflush`

**Key files to modify**: `connection/handlers/scripting/function.rs`, memory check integration
