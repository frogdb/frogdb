# 11. Command Statistics & COMMAND GETKEYSANDFLAGS (11 tests — `introspection2_tcl.rs`)

**Status**: `INFO commandstats` exists; COMMAND GETKEYSANDFLAGS not implemented
**Scope**: Verify commandstats accuracy and implement GETKEYSANDFLAGS.

## Work

### commandstats (6 tests)

Verify per-command call/usec/failures counting.

- `command stats for GEOADD`
- `command stats for EXPIRE`
- `command stats for BRPOP`
- `command stats for MULTI`
- `command stats for scripts`
- `errors stats for GEOADD`

### COMMAND GETKEYSANDFLAGS (5 tests)

Implement subcommand returning key positions and flags (R/W/OW/RW) for any command;
handle movablekeys flag.

- `COMMAND GETKEYSANDFLAGS`
- `COMMAND GETKEYSANDFLAGS invalid args`
- `COMMAND GETKEYSANDFLAGS MSETEX`
- `$cmd command will not be marked with movablekeys`
- `$cmd command is marked with movablekeys`

**Key files to modify**: `commands/metadata.rs`, command registry, new handler for GETKEYSANDFLAGS
