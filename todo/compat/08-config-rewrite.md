# 8. CONFIG REWRITE (5 tests -- `introspection_tcl.rs`)

**Status**: Not implemented
**Source files**:
- Config loading: `frogdb-server/crates/server/src/config/loader.rs` (Figment + TOML)
- Config types: `frogdb-server/crates/config/src/lib.rs` (serde `Config` struct)
- Param registry: `frogdb-server/crates/config/src/params.rs` (`ConfigParamInfo` with section/field mapping)
- Runtime config: `frogdb-server/crates/server/src/runtime_config.rs` (`ConfigManager`)
- CONFIG handler: `frogdb-server/crates/server/src/connection/handlers/config.rs`
- Connection deps: `frogdb-server/crates/server/src/connection/deps.rs` (`AdminDeps`)

**Current behavior**: Returns `ERR CONFIG REWRITE is not supported. Use the configuration file to persist changes.`

---

## Architecture

### TOML Read-Modify-Write Strategy

FrogDB uses TOML config (not Redis `.conf`). The rewrite must:

1. **Read** the original TOML file from disk (the path used at startup)
2. **Parse** it as a `toml_edit::DocumentMut` (preserves comments, formatting, ordering)
3. **Merge** current runtime values from `ConfigManager` into the document
4. **Write** the modified document atomically (temp file + rename)

The `toml_edit` crate (already in `Cargo.lock` as a transitive dep of `toml` 1.x) provides
lossless TOML parsing that preserves comments and whitespace. Add it as a direct dependency.

### Comment Preservation

Using `toml_edit::DocumentMut` instead of `toml::Value` means:
- User comments above/beside keys are preserved
- Key ordering within sections is preserved
- Only values that differ from the file are overwritten
- New keys (runtime-only params with no file entry) are appended to the correct `[section]`

### Atomic Write Safety

```
1. Read original file into memory
2. Parse as toml_edit::DocumentMut
3. Merge runtime config changes
4. Write to temp file: <config_path>.tmp.<pid>
5. fsync the temp file
6. Rename temp file over original (atomic on POSIX)
```

If any step fails, the original file is untouched.

---

## Implementation Steps

### Step 1: Store config file path at startup

The config file path is currently consumed in `Config::load()` and discarded. It must be
stored so CONFIG REWRITE can find the file later.

**File**: `frogdb-server/crates/server/src/runtime_config.rs`

Add a `config_file_path: Option<PathBuf>` field to `ConfigManager`:

```rust
pub struct ConfigManager {
    // ... existing fields ...
    /// Path to the TOML config file (None if using defaults only).
    config_file_path: RwLock<Option<PathBuf>>,
}
```

Add setter/getter:
```rust
pub fn set_config_file_path(&self, path: PathBuf) { ... }
pub fn config_file_path(&self) -> Option<PathBuf> { ... }
```

**File**: `frogdb-server/crates/server/src/server/init.rs`

After creating `ConfigManager`, call `config_manager.set_config_file_path(path)` if a config
file was used at startup. The path comes from the CLI `--config` argument or the detected
default `frogdb.toml`.

**File**: `frogdb-server/crates/server/src/config/loader.rs`

Return the resolved config file path alongside the `Config` from `Config::load()` (either as
a tuple or by adding a field to Config). Simplest: add `pub config_source_path: Option<PathBuf>`
to the `Config` struct (skipped from serde with `#[serde(skip)]`).

### Step 2: Add `toml_edit` dependency

**File**: `Cargo.toml` (workspace root, `[workspace.dependencies]`)

```toml
toml_edit = "0.25"
```

**File**: `frogdb-server/crates/server/Cargo.toml`

```toml
toml_edit.workspace = true
```

### Step 3: Implement the rewrite logic

**File**: `frogdb-server/crates/server/src/runtime_config.rs` (or a new submodule `rewrite.rs`)

```rust
use std::path::Path;
use toml_edit::DocumentMut;

impl ConfigManager {
    /// Rewrite the config file, merging current runtime values into the TOML document.
    ///
    /// Returns Ok(()) on success, or an error string suitable for Redis protocol.
    pub fn rewrite_config(&self) -> Result<(), String> { ... }
}
```

Core logic:
1. Get `config_file_path` -- if None, return `"ERR The server is running without a config file"`
2. Read file contents (`std::fs::read_to_string`)
3. Parse: `contents.parse::<DocumentMut>()` -- on error, return parse error
4. For each param in `config_param_registry()` where `section.is_some() && field.is_some()`:
   - Get current runtime value via `self.get(param.name)`
   - Navigate to `doc[section][field]` in the TOML document
   - If the section doesn't exist, create it
   - Update the value (converting types appropriately: integers, strings, booleans)
5. Handle special value conversions:
   - `min-replicas-max-lag`: runtime is in seconds, TOML field is `min-replicas-timeout-ms` (multiply by 1000)
   - Boolean config params stored as `"yes"/"no"` in CONFIG GET need conversion to TOML `true/false`
6. Write to temp file, fsync, rename

### Step 4: Wire up the CONFIG REWRITE handler

**File**: `frogdb-server/crates/server/src/connection/handlers/config.rs`

Replace the current error stub:
```rust
"REWRITE" => self.handle_config_rewrite(),
```

New method:
```rust
fn handle_config_rewrite(&self) -> Response {
    match self.admin.config_manager.rewrite_config() {
        Ok(()) => Response::ok(),
        Err(e) => Response::error(e),
    }
}
```

### Step 5: Handle no-op and runtime-only params

Params where `section: None` (e.g., `save`, `lua-time-limit`, listpack thresholds) are
**not** written to the TOML file since they have no config file representation. This matches
Redis behavior where CONFIG REWRITE only persists params that have a config file equivalent.

The `noop: true` params (like `save`, `hz`, `activedefrag`) are skipped entirely -- they
exist only for Redis test suite compatibility and don't affect FrogDB behavior.

---

## Integration Points

### ConfigManager Access

The handler already accesses `self.admin.config_manager` (an `Arc<ConfigManager>`). The
rewrite method is synchronous (file I/O is brief) so no async needed.

### Param Registry as Source of Truth

`config_param_registry()` in `frogdb-server/crates/config/src/params.rs` already maps each
CONFIG param to its TOML `section` and `field`. This is the iteration source for rewrite:

```rust
for param in config_param_registry() {
    if let (Some(section), Some(field)) = (param.section, param.field) {
        let values = self.get(param.name);
        if let Some((_, value)) = values.first() {
            update_toml_doc(&mut doc, section, field, &value, param.name);
        }
    }
}
```

### Value Type Conversion

The CONFIG GET values are always strings. When writing back to TOML, types must be inferred:
- Integer params (port, maxmemory, timeouts): parse as `i64` and write as TOML integer
- Boolean params (enabled, per-request-spans): `"yes"/"true"` -> `true`, `"no"/"false"` -> `false`
- String params (bind, durability-mode, loglevel): write as TOML string
- Path params (data-dir, cert-file): write as TOML string

A helper function or metadata on `ConfigParamInfo` can drive this. Simplest approach: try
parsing as integer first, then boolean, fall back to string.

---

## FrogDB Adaptations

### TOML vs Redis .conf Format

| Aspect | Redis .conf | FrogDB TOML |
|--------|-------------|-------------|
| Format | `key value` flat | `[section]\nkey = value` hierarchical |
| Comments | `#` at line start | `#` anywhere (preserved by `toml_edit`) |
| Types | Everything is a string | Native integers, booleans, strings, arrays |
| Special syntax | `save 900 1\nsave 300 10` | N/A (no save param in TOML) |

### FrogDB-Specific Params (no Redis equivalent)

These are persisted via their `section`/`field` mapping:
- **Persistence**: `durability-mode`, `sync-interval-ms`, `batch-timeout-ms`, `wal-failure-policy`
- **Server**: `scatter-gather-timeout-ms`, `num-shards` (immutable, written as-is)
- **Logging**: `per-request-spans`
- **Replication**: `min-replicas-to-write`, `min-replicas-timeout-ms`

### No-op/Missing Redis Concepts

These Redis CONFIG REWRITE behaviors don't apply to FrogDB:
- **`save` params**: Redis rewrites `save 900 1` lines. FrogDB has no save directive -- skip.
- **`rename-command`**: FrogDB has no command renaming feature -- skip.
- **`alias`**: FrogDB has no command alias feature -- skip.
- **`shutdown-on-sigterm`**: Not a FrogDB concept -- skip.

The tests for these (see below) will pass by verifying CONFIG REWRITE succeeds without error
and that the file is valid TOML afterward, even though these params produce no file changes.

---

## Tests

- `CONFIG REWRITE sanity`
- `CONFIG REWRITE handles save and shutdown properly`
- `CONFIG REWRITE handles rename-command properly`
- `CONFIG REWRITE handles alias config properly`
- `CONFIG save params special case handled properly`

### Test Strategy

These tests are currently excluded in `introspection_tcl.rs` with tag `external:skip`. To pass them:

1. **Sanity test**: CONFIG SET a mutable param, CONFIG REWRITE, verify the file was updated
   and the value persists across a conceptual restart (re-parse the file).
2. **Save/shutdown test**: CONFIG SET save (no-op in FrogDB), CONFIG REWRITE should succeed
   without error. The test likely verifies the operation doesn't crash.
3. **Rename-command test**: FrogDB has no rename-command. CONFIG REWRITE should still succeed.
   May need to stub or adapt the test expectations.
4. **Alias test**: Same as rename-command -- CONFIG REWRITE succeeds, no alias in file.
5. **Save params special case**: CONFIG SET save to various values, CONFIG REWRITE. Since
   `save` is a no-op param, this should succeed silently.

Some tests may need Redis-specific assertions adapted (checking for `save ""` lines in the
config file won't apply to TOML). Review each test's expectations against TOML output.

---

## Verification

```bash
# Unit tests for the rewrite logic
just test frogdb-server rewrite

# Integration test: start server with config, CONFIG SET, CONFIG REWRITE, verify file
just test frogdb-server config_rewrite

# Run the specific Redis regression tests
just test redis-regression "CONFIG REWRITE"

# Verify no regressions in other CONFIG tests
just test redis-regression "CONFIG"
```
