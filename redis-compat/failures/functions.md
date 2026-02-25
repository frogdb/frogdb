# unit/functions — 1 error

## Category
FUNCTION engine name case-sensitivity.

## Failure

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

## Recommended Fix
One-line change. This is the lowest-effort fix across all failing suites.

## Cross-Suite Dependencies
None.
