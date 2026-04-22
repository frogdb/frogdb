# 14g. Introspection Command Gaps (5+ tests — `introspection_tcl.rs`, `protocol_tcl.rs`)

**Status**: Complete (6 tests implemented)
**Scope**: Fill gaps in CONFIG validation, CLIENT subcommands, RESET behavior, and protocol
argument rewriting.

## Architecture

### CONFIG Sanity

Redis's `CONFIG sanity` test iterates over all CONFIG parameters, verifying GET/SET roundtrip
works correctly. FrogDB's config handler
(`frogdb-server/crates/server/src/connection/handlers/config.rs`) supports a subset of Redis
config parameters.

**Decision**: Adapt to validate FrogDB's own config parameters roundtrip correctly. This is
marked `intentional-incompatibility:config` in the regression suite — FrogDB has different
config parameters than Redis.

### Config During Loading

Redis's `config during loading` tests CONFIG behavior while RDB is being loaded. FrogDB's
equivalent is behavior during RocksDB recovery/startup.

**Decision**: Also marked `intentional-incompatibility:config`. Adapt to test config access
during FrogDB's startup sequence.

### CLIENT REPLY OFF/ON

FrogDB already has `ReplyMode` infrastructure in
`frogdb-server/crates/server/src/connection/state.rs` with `ReplyMode::On`, `Off`, and support
for `skip_next_reply`. The CLIENT REPLY command is handled in the client handler.

**Current state**: Likely implemented. Verify the test passes.

### CLIENT Command Unhappy Paths

Tests for `CLIENT CACHING` and `CLIENT TRACKING` error cases (calling without proper state,
invalid arguments). These require the CLIENT TRACKING infrastructure.

**Decision**: Depends on CLIENT TRACKING implementation status. The "unhappy path" tests verify
proper error responses for invalid usage.

### RESET Does Not Clean Library Name

The `RESET` command resets connection state (auth, selected DB, subscriptions, etc.) but should
NOT clear the library name set by `CLIENT SETNAME` or `CLIENT SET-INFO LIB-NAME`. This tests a
specific preservation behavior.

**Current state**: Check if RESET command exists and whether it preserves lib-name/lib-ver.

### Argument Rewriting (Issue 9598)

Redis issue 9598 relates to how arguments are rewritten in the command log/slowlog when commands
are modified internally (e.g., `OBJECT HELP` being rewritten). The test verifies that argument
rewriting doesn't corrupt output.

## Implementation Steps

1. **CONFIG sanity adaptation**:
   - Create a test that iterates FrogDB's CONFIG parameters
   - Verify each supports GET and SET (where applicable)
   - Verify SET changes are reflected in subsequent GET

2. **CLIENT REPLY verification**:
   - Run existing test — likely already passes
   - Verify OFF suppresses all replies, ON resumes, SKIP suppresses one

3. **CLIENT unhappy paths**:
   - Depends on TRACKING implementation
   - Implement error responses for CACHING without TRACKING active
   - Implement error responses for invalid TRACKING arguments

4. **RESET lib-name preservation**:
   - Verify RESET implementation preserves `lib_name` and `lib_ver` fields
   - If not, adjust RESET to skip those fields during state clear

5. **Argument rewriting**:
   - Verify SLOWLOG/command logging handles subcommand rewriting correctly
   - Test with commands that have internal argument transformation

## Integration Points

- `frogdb-server/crates/server/src/connection/handlers/config.rs` — CONFIG GET/SET
- `frogdb-server/crates/server/src/connection/handlers/client.rs` — CLIENT subcommands
- `frogdb-server/crates/server/src/connection/state.rs` — ReplyMode, connection reset
- `frogdb-server/crates/server/src/connection/` — RESET command handler
- Slowlog infrastructure — argument representation

## FrogDB Adaptations

- **CONFIG**: FrogDB has different configuration parameters (RocksDB settings, Raft timeouts,
  shard counts). The sanity test must validate FrogDB's parameter set, not Redis's.
- **CLIENT TRACKING**: If not yet implemented, the unhappy-path tests are blocked on that
  feature. Mark as dependent on tracking implementation.
- **RESET**: Behavior should match Redis semantics for the fields that exist in FrogDB. Fields
  that don't exist (DB selection in multi-DB sense) can be skipped.

## Tests

- `CONFIG sanity`
- `config during loading`
- `CLIENT REPLY OFF/ON: disable all commands reply`
- `CLIENT command unhappy path coverage`
- `RESET does NOT clean library name`
- `test argument rewriting - issue 9598`

## Verification

```bash
just test frogdb-server-redis-regression "CONFIG sanity"
just test frogdb-server-redis-regression "CLIENT REPLY"
just test frogdb-server-redis-regression "RESET.*library"
just test frogdb-server-redis-regression "argument rewriting"
```
