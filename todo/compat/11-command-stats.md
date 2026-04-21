# 11. Command Statistics & COMMAND GETKEYSANDFLAGS

**Tests**: 11 (6 commandstats, 5 GETKEYSANDFLAGS)
**Source**: `frogdb-server/crates/redis-regression/tests/introspection2_tcl.rs`
**Status**: `INFO commandstats` exists with `calls` only; `usec`/`usec_per_call` hardcoded to 0; `rejected_calls`/`failed_calls` hardcoded to 0; `COMMAND GETKEYSANDFLAGS` not implemented.

---

## Architecture

### commandstats collection

The current stats pipeline:

1. **Per-connection local accumulator** (`ConnectionState.local_stats: ClientStatsDelta`) records `(command_name, latency_us)` pairs on every command completion in `connection.rs` line 526.
2. **Periodic sync** to `ClientRegistry.command_call_counts` (a `RwLock<HashMap<String, u64>>`) happens every 100 commands or 1000ms, plus immediately for blocking commands.
3. **Render in INFO**: `connection/handlers/scatter.rs` line 490 replaces the `# Commandstats\r\n` placeholder from `commands/info.rs:build_commandstats_info()` with `cmdstat_<name>:calls=N,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0` lines.

**Gap**: Only `calls` is real. The `usec`, `usec_per_call`, `rejected_calls`, and `failed_calls` fields are hardcoded to 0. The latency data exists in `CommandTypeStats.latency_total_us` at the per-client level but is not aggregated into the server-wide counter.

### GETKEYSANDFLAGS key position resolution

Redis's `COMMAND GETKEYSANDFLAGS` returns for each key: `[key_name, [flags...]]` where flags are strings like `"R"`, `"W"`, `"OW"`, `"RW"`. FrogDB already has:

- `CommandEntry.keys(args)` for extracting key positions from args.
- `CommandEntry.flags()` with `WRITE`/`READONLY` bits for determining access type.

**Gap**: There is no per-key flag annotation. Redis 7+ uses command-spec key specifications with per-key flags (read, write, overwrite, insert, delete). FrogDB's `Command::keys()` returns positions but no per-key access flags. A new trait method or annotation system is needed to provide per-key flags alongside positions.

---

## Implementation Steps

### Phase 1: Complete commandstats counters (usec + failures)

**Goal**: Make `INFO commandstats` emit accurate `usec`, `usec_per_call`, `rejected_calls`, `failed_calls`.

1. **Extend server-wide stats structure** in `frogdb-server/crates/core/src/client_registry/mod.rs`:
   - Replace `command_call_counts: RwLock<HashMap<String, u64>>` with a struct:
     ```rust
     pub struct ServerCommandStats {
         pub calls: u64,
         pub usec: u64,
         pub rejected_calls: u64,
         pub failed_calls: u64,
     }
     ```
   - New field: `command_stats: RwLock<HashMap<String, ServerCommandStats>>`

2. **Propagate latency through the delta sync** in `frogdb-server/crates/core/src/client_registry/mod.rs` (`update_stats`):
   - Currently iterates `delta.command_latencies` and increments only `calls`.
   - Also accumulate `usec` from the latency value already present in the tuple.

3. **Track failures**: In `frogdb-server/crates/server/src/connection.rs` around line 516:
   - After checking `has_error` (line 516), record the error in `local_stats`.
   - Add a `command_failures: Vec<String>` to `ClientStatsDelta` (or extend `command_latencies` to a triple: `(name, latency_us, failed: bool)`).
   - On sync, increment `failed_calls` in the server-wide map.

4. **Track rejected calls**: Rejected means the command was refused before execution (wrong arity, auth failure, OOM, read-only replica refusal). These are checked in `connection.rs` guards before `execute`. Add a `record_rejected_command(cmd_name)` path.

5. **Update render in scatter.rs** (line 508-511):
   - Read from the new `ServerCommandStats` map.
   - Compute `usec_per_call = usec as f64 / calls as f64` (or 0.00 if calls==0).
   - Format: `cmdstat_{cmd}:calls={calls},usec={usec},usec_per_call={usec_per_call:.2},rejected_calls={rejected},failed_calls={failed}`

6. **Reset support**: `reset_command_call_counts()` already clears the map; rename/extend to `reset_command_stats()`.

### Phase 2: Implement COMMAND GETKEYSANDFLAGS

**Goal**: Add the `GETKEYSANDFLAGS` subcommand to the COMMAND handler.

1. **Add a `keys_with_flags` method** to the `Command` and `CommandMetadata` traits in `frogdb-server/crates/core/src/command.rs`:
   ```rust
   /// Per-key access flag for COMMAND GETKEYSANDFLAGS.
   pub enum KeyAccessFlag {
       R,   // Read
       W,   // Write (insert or overwrite)
       OW,  // Overwrite only (key must exist)
       RW,  // Read + Write
   }

   /// Default implementation derives flags from CommandFlags.
   fn keys_with_flags<'a>(&self, args: &'a [Bytes]) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
       let keys = self.keys(args);
       let flag = if self.flags().contains(CommandFlags::WRITE) {
           KeyAccessFlag::OW  // most write commands overwrite
       } else {
           KeyAccessFlag::R
       };
       keys.into_iter().map(|k| (k, vec![flag])).collect()
   }
   ```

2. **Override for commands with mixed access patterns**:
   - `SORT ... STORE dest`: source key is `R`, dest key is `OW`
   - `RENAME src dest`: src is `RW`, dest is `OW`
   - `COPY src dest`: src is `R`, dest is `OW`
   - `LMOVE/RPOPLPUSH src dest`: src is `RW`, dest is `W`
   - `MSETEX`: all keys are `OW`

3. **Add GETKEYSANDFLAGS handler** in `frogdb-server/crates/commands/src/basic.rs` (inside `CommandCommand::execute`, after the `b"GETKEYS"` arm):
   ```rust
   b"GETKEYSANDFLAGS" => {
       if args.len() < 2 {
           return Err(CommandError::WrongArity { command: "command|getkeysandflags" });
       }
       let cmd_name = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
       let cmd_args = &args[2..];
       // Look up command, call keys_with_flags, format response
       // Response format: [[key1, [flag1, flag2]], [key2, [flag3]], ...]
   }
   ```

4. **Wire up `CommandRegistry::get_entry()` access** to support both full commands and metadata-only commands.

### Phase 3: Movablekeys introspection

**Goal**: `COMMAND INFO` reports the `movablekeys` flag for commands whose key positions depend on argument values (SORT, EVAL, EVALSHA, MSETEX, XREAD, GEORADIUS with STORE, MIGRATE).

1. **Add `MOVABLEKEYS` to `CommandFlags`** in `frogdb-server/crates/core/src/command.rs`:
   ```rust
   const MOVABLEKEYS = 0b10_0000_0000_0000_0000;
   ```

2. **Apply the flag** to commands with argument-dependent key positions:
   - `SortCommand` / `SortStoreCommand` (STORE changes key count)
   - `MsetexCommand` (numkeys prefix determines key positions)
   - `EvalCommand` / `EvalshaCommand` (numkeys arg)
   - `XreadCommand` (STREAMS keyword determines key positions)
   - Any command whose `keys()` implementation parses args to determine positions

3. **Emit in COMMAND INFO** response: include `"movablekeys"` in the flags array when the flag is set.

---

## Integration Points

| What | Where |
|------|-------|
| Command execution timing | `connection.rs` line 513 (`elapsed_us`) |
| Stats recording | `connection.rs` line 526 (`local_stats.record_command`) |
| Error detection | `connection.rs` line 516 (`has_error`) |
| Periodic sync to registry | `connection.rs` line 561 (`maybe_sync_stats`) |
| Blocking command force-sync | `connection.rs` line 531 |
| Server-wide counter storage | `client_registry/mod.rs` line 234 (`command_call_counts`) |
| INFO commandstats render | `connection/handlers/scatter.rs` line 490-517 |
| CONFIG RESETSTAT clear | `connection/handlers/config.rs` line 122 |
| COMMAND handler | `commands/src/basic.rs` line 93-335 (CommandCommand) |
| Command trait (keys) | `core/src/command.rs` line 335 (`fn keys`) |
| CommandRegistry entry lookup | `core/src/registry.rs` line 115 (`get_entry`) |

---

## FrogDB Adaptations

1. **MSETEX** (FrogDB-specific command): Has variable key positions via `numkeys` arg. Its `keys()` already parses the arg at `string.rs:1663`. Needs `MOVABLEKEYS` flag and a `keys_with_flags` override returning `OW` for all keys.

2. **FT.* search commands**: These are scatter-gather and don't have traditional key positions. They can return empty keys from `GETKEYSANDFLAGS` (no slot routing needed).

3. **TS.* time series commands**: Similar to search, handled as server-wide ops.

4. **ES.* event stream commands**: Keyless or scatter-gather; return empty from GETKEYSANDFLAGS.

5. **Latency tracking per-shard vs. per-command**: FrogDB's multi-shard architecture means command latency in `commandstats` includes routing overhead (shard message round-trip). This is acceptable since it measures wall-clock time from client perspective.

---

## Test List

### commandstats (6 tests)

Verify per-command call/usec/failures counting. Require CONFIG RESETSTAT and accurate format:
`cmdstat_<cmd>:calls=N,usec=N,usec_per_call=N.NN,rejected_calls=N,failed_calls=N`

- `command stats for GEOADD`
- `command stats for EXPIRE`
- `command stats for BRPOP`
- `command stats for MULTI`
- `command stats for scripts`
- `errors stats for GEOADD`

### COMMAND GETKEYSANDFLAGS (5 tests)

Implement subcommand returning key positions and per-key flags (R/W/OW/RW); handle movablekeys flag.

- `COMMAND GETKEYSANDFLAGS`
- `COMMAND GETKEYSANDFLAGS invalid args`
- `COMMAND GETKEYSANDFLAGS MSETEX`
- `$cmd command will not be marked with movablekeys`
- `$cmd command is marked with movablekeys`

---

## Verification

```bash
# Phase 1: commandstats accuracy
just test frogdb-server "command stats for GEOADD"
just test frogdb-server "command stats for EXPIRE"
just test frogdb-server "command stats for BRPOP"
just test frogdb-server "command stats for MULTI"
just test frogdb-server "command stats for scripts"
just test frogdb-server "errors stats for GEOADD"

# Phase 2: GETKEYSANDFLAGS
just test frogdb-server "COMMAND GETKEYSANDFLAGS"
just test frogdb-server "COMMAND GETKEYSANDFLAGS invalid args"
just test frogdb-server "COMMAND GETKEYSANDFLAGS MSETEX"

# Phase 3: movablekeys
just test frogdb-server "movablekeys"

# Full regression
just test frogdb-server introspection2_tcl
just test frogdb-server info_command_tcl
just test frogdb-server info_tcl
```
