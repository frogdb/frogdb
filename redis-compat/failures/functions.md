# unit/functions — 1 remaining error

## Fixed

The following failures have been resolved:

### Library name missing quotes in error message
- **Test:** `FUNCTION - test function stats on loading failure`
- **Fix:** Added quotes around library name in `LibraryAlreadyExists` error (`crates/scripting/src/error.rs`)

### Global table not protected with readonly metatable
- **Test:** `FUNCTION - trick global protection 1`
- **Fix:** Added `__newindex` / `__index` metatable on `_G` in `apply_sandbox()` (`crates/core/src/scripting/lua_vm.rs`)

### getmetatable available during FUNCTION LOAD
- **Test:** `FUNCTION - test getmetatable on script load`
- **Fix:** Removed `getmetatable` and added strict `__index` in loader sandbox (`crates/scripting/src/loader.rs`)

## Remaining Failure

### Engine name is case-sensitive
- **Expected:** `#!LUA name=mylib` and `#!lua name=mylib` both accepted as valid shebang lines
- **Actual:** Only `#!lua` (lowercase) is accepted; `#!LUA` returns `ERR Unsupported engine: LUA`
- **Root cause:** `crates/scripting/src/parser.rs:42` performs exact string comparison:
  ```rust
  if engine != "lua" {
      return Err(FunctionError::UnsupportedEngine { engine });
  }
  ```
- **Fix:** Use case-insensitive comparison:
  ```rust
  if !engine.eq_ignore_ascii_case("lua") {
      return Err(FunctionError::UnsupportedEngine { engine });
  }
  ```

## Source Files

| File | What to change |
|------|----------------|
| `crates/scripting/src/parser.rs:42` | Change `engine != "lua"` to case-insensitive comparison |

## Cross-Suite Dependencies
None.
