# Checkable Assertions

Machine-verifiable claims in FrogDB documentation. Each entry has:
- The claim (what the doc says)
- Where to verify (source of truth in code)
- Which doc file contains the claim

Use this as a checklist during audit mode.

## Structural Assertions

### Workspace Configuration

| Claim | Doc File | Verification |
|-------|----------|-------------|
| `edition = "2024"` | REPO.md (line ~95) | `Cargo.toml` → `[workspace.package] edition` |
| `resolver = "2"` | REPO.md (line ~83) | `Cargo.toml` → `[workspace] resolver` |
| Workspace has 5 members | REPO.md (lines ~84-90) | `Cargo.toml` → `[workspace] members` (actual: 28) |
| Members: server, core, protocol, lua, persistence | REPO.md | `Cargo.toml` → `[workspace] members` |
| `publish = false` | REPO.md (line ~96) | `Cargo.toml` → `[workspace.package]` (check if field exists) |

### Crate Names

| Canonical Name | Docs That Reference | Verification |
|---------------|-------------------|-------------|
| `frogdb-protocol` | REPO.md, ARCHITECTURE.md, INDEX.md | `Cargo.toml` internal crates |
| `frogdb-types` | ARCHITECTURE.md, STORAGE.md | `Cargo.toml` internal crates |
| `frogdb-core` | REPO.md, ARCHITECTURE.md, INDEX.md | `Cargo.toml` internal crates |
| `frogdb-scripting` | (should replace `frogdb-lua`) | `Cargo.toml` internal crates |
| `frogdb-persistence` | REPO.md, ARCHITECTURE.md | `Cargo.toml` internal crates |
| `frogdb-server` | REPO.md, ARCHITECTURE.md | `Cargo.toml` internal crates |
| `frogdb-acl` | AUTH.md, ARCHITECTURE.md | `Cargo.toml` internal crates |
| `frogdb-cluster` | CLUSTER.md | `Cargo.toml` internal crates |
| `frogdb-replication` | REPLICATION.md | `Cargo.toml` internal crates |
| `frogdb-vll` | VLL.md | `Cargo.toml` internal crates |
| `frogdb-commands` | COMMANDS.md, EXECUTION.md | `Cargo.toml` internal crates |
| `frogdb-telemetry` | OBSERVABILITY.md | `Cargo.toml` internal crates |
| `frogdb-debug` | DEBUGGING.md | `Cargo.toml` internal crates |
| `frogdb-search` | — | `Cargo.toml` internal crates |
| `frogdb-macros` | — | `Cargo.toml` internal crates |
| `frogdb-metrics-derive` | — | `Cargo.toml` internal crates |
| `frogdb-test-harness` | TESTING.md | `Cargo.toml` internal crates |
| `frogdb-testing` | TESTING.md | `Cargo.toml` internal crates |

**Known drift**: Docs reference `frogdb-lua` — should be `frogdb-scripting`.

### Crate Count

| Claim | Doc File | Verification |
|-------|----------|-------------|
| "5 crates" | ARCHITECTURE.md (line ~98) | Count `[workspace] members` in Cargo.toml |
| "Cargo workspace with 5 crates" | ARCHITECTURE.md | Same |

**Known drift**: Actual count is 28 workspace members.

### Dependency Versions

| Dep | Claimed Version | Doc File | Actual (Cargo.toml) |
|-----|----------------|----------|---------------------|
| `thiserror` | `"1"` | REPO.md (line ~104) | `"2"` |
| `mlua` | `"0.9"` | REPO.md (line ~214) | `"0.10"` |
| `rocksdb` | `"0.21"` | REPO.md (line ~232) | `"0.24"` |
| `tokio` | `"1"` | REPO.md (line ~100) | `"1"` (ok) |
| `bytes` | `"1"` | REPO.md (line ~101) | `"1"` (ok) |
| `redis` (dev) | `"0.24"` | REPO.md (line ~109) | Check Cargo.toml dev-deps |
| `griddle` | `"0.5"` | REPO.md (line ~178) | `"0.5"` (ok) |

### File Paths

| Claimed Path | Doc File | Verification |
|-------------|----------|-------------|
| `crates/lua/` | REPO.md (lines ~48-54), ARCHITECTURE.md | Actual: `frogdb-server/crates/scripting/` |
| `crates/server/` | REPO.md (lines ~22-28) | Actual: `frogdb-server/crates/server/` |
| `crates/core/` | REPO.md (lines ~29-38) | Actual: `frogdb-server/crates/core/` |
| `crates/protocol/` | REPO.md (lines ~41-47) | Actual: `frogdb-server/crates/protocol/` |
| `crates/persistence/` | REPO.md (lines ~55-61) | Actual: `frogdb-server/crates/persistence/` |
| `tests/` | REPO.md (lines ~62-65) | Integration tests are in `frogdb-server/crates/server/tests/` |
| `benches/` | REPO.md (line ~67) | Actual: `frogdb-server/benchmarks/` |
| `spec/` | REPO.md (line ~68) | Actual: `docs/spec/` |

### Cross-Doc Links

All `](*.md)` links in spec docs should resolve. Check by:
1. Grep for markdown link patterns: `\]\([^)]*\.md[^)]*\)`
2. Resolve relative paths from the doc's directory
3. Verify the target file exists

**Known items to check:**
- INDEX.md: link to `../todo/ROLLING_UPGRADE.md` — exists ✓
- INDEX.md: link to `../todo/INDEX.md` — verify exists
- INDEX.md: link to `../todo/optimizations/INDEX.md` — verify exists

## API Surface Assertions

### Struct/Trait Definitions in Code Blocks

| Type | Doc File | Source File |
|------|----------|-------------|
| `CommandContext` struct | ARCHITECTURE.md (lines ~219-250) | `crates/core/src/command.rs` or similar |
| `AclChecker` trait | ARCHITECTURE.md (lines ~523-527) | `crates/acl/src/` |
| `WalWriter` trait | ARCHITECTURE.md (lines ~535-539) | `crates/persistence/src/wal.rs` |
| `ParsedCommand` struct | ARCHITECTURE.md, PROTOCOL.md | `crates/protocol/src/` |
| `Store` trait | STORAGE.md | `crates/core/src/store/` |
| `Command` trait | EXECUTION.md | `crates/core/src/command.rs` |
| `Value` enum | STORAGE.md | `crates/types/src/types.rs` |
| `CommandError` enum | EXECUTION.md | `crates/core/src/` or `crates/types/src/` |

### Public API Re-exports

REPO.md quotes `pub use` statements from `lib.rs` files. These should match the actual re-exports.

| Crate | Doc Section | Source |
|-------|-------------|--------|
| `frogdb-protocol` | REPO.md (lines ~262-264) | `crates/protocol/src/lib.rs` |
| `frogdb-core` | REPO.md (lines ~285-299) | `crates/core/src/lib.rs` |
| `frogdb-lua` (stale name) | REPO.md (lines ~330-333) | `crates/scripting/src/lib.rs` |
| `frogdb-persistence` | REPO.md (lines ~352-357) | `crates/persistence/src/lib.rs` |

## Configuration Assertions

Config keys and default values referenced in CONFIGURATION.md should match the actual
config structs in `crates/server/src/config/`.

**Spot-check list:**
- `num_shards` default
- `bind` address default
- `port` default (6379)
- `wal_mode` default
- `max_memory` default
- `eviction_policy` default

## Behavioral Assertions

### Justfile Recipes

CLAUDE.md and REPO.md reference Justfile recipes. Verify recipe names exist in `Justfile`.

**Recipes referenced in CLAUDE.md:**
- `just check`, `just test`, `just lint`, `just fmt`, `just build`
- `just test <crate>`, `just test <crate> <pattern>`
- `just lint <crate>`, `just fmt <crate>`
- `just concurrency`, `just clean-stale`
- `just lint-py`, `just fmt-py`, `just fmt-check`

**Recipes referenced in REPO.md:**
- `just build`, `just release`, `just test`, `just fmt`, `just fmt-check`
- `just lint`, `just deny`, `just check`, `just run`, `just run-release`
- `just clean`, `just watch`, `just doc`

### Rustfmt Edition

REPO.md (line ~414) shows `edition = "2021"` in `rustfmt.toml`. Check actual `rustfmt.toml`.

### Profile Configuration

REPO.md quotes `[profile.release]` with `lto = "thin"`. Actual Cargo.toml has `lto = true`.
This is a known drift point.
