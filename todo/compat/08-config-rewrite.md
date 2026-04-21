# 8. CONFIG REWRITE (5 tests — `introspection_tcl.rs`)

**Status**: Not implemented
**Scope**: Implement `CONFIG REWRITE` to persist runtime config changes to FrogDB's TOML config file.

## Work

- Read current TOML config file
- Merge runtime-modified parameters into TOML structure
- Write back preserving comments and formatting where possible
- Handle special cases (save params, shutdown, rename-command, alias)
- Add safety: atomic write with temp file + rename

**Key files to modify**: Config crate, `connection/handlers/` (new handler or extend config handler)

## Tests

- `CONFIG REWRITE sanity`
- `CONFIG REWRITE handles save and shutdown properly`
- `CONFIG REWRITE handles rename-command properly`
- `CONFIG REWRITE handles alias config properly`
- `CONFIG save params special case handled properly`
