# 9. MULTI/EXEC Enhancements (17 tests — `multi_tcl.rs`)

**Status**: MULTI/EXEC core implemented; missing OOM handling, replication verification, script
timeout interaction, stale-key WATCH behavior
**Scope**: Fill gaps in transaction behavior.

## Work

### OOM handling (2 tests)

Reject write commands in EXEC when OOM, allow read-only.

- `EXEC with at least one use-memory command should fail`
- `EXEC with only read commands should not be rejected when OOM`

### Replication verification (8 tests — *adapt*)

Verify EVAL, PUBLISH, SCRIPT LOAD/FLUSH, XREADGROUP propagate correctly through Raft log;
test on primary+follower topology.

- `MULTI propagation of EVAL`
- `MULTI propagation of PUBLISH`
- `MULTI propagation of SCRIPT LOAD`
- `MULTI propagation of SCRIPT FLUSH`
- `MULTI propagation of XREADGROUP`
- `MULTI with $cmd` — inner-command propagation matrix
- `exec with write commands and state change`
- `exec with read commands and stale replica state change`

### WATCH with stale keys (3 tests)

Implement `DEBUG SET-ACTIVE-EXPIRE` equivalent; ensure expired watched keys don't fail EXEC.

- `WATCH stale keys should not fail EXEC`
- `Delete WATCHed stale keys should not fail EXEC`
- `FLUSHDB while watching stale keys should not fail EXEC`

### Script timeout (3 tests)

Implement configurable lua-time-limit; MULTI interaction with script timeout state.

- `MULTI and script timeout`
- `EXEC and script timeout`
- `just EXEC and script timeout`

### Config error handling (1 test — *adapt*)

- `MULTI with config error` — test against FrogDB config error behavior
