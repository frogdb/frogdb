# Checkable Assertions

Machine-verifiable claims in FrogDB documentation. Each entry has:
- The claim (what the doc says)
- Where to verify (source of truth in code)
- Which doc file contains the claim

Use this as a checklist during audit mode.

Documentation lives in `website/src/content/docs/` (abbreviated as `docs/` below for brevity).

## Structural Assertions

### Crate Names

| Canonical Name | Docs That Reference | Verification |
|---------------|-------------------|-------------|
| `frogdb-protocol` | `docs/architecture/architecture.md` | `Cargo.toml` internal crates |
| `frogdb-types` | `docs/architecture/architecture.md`, `docs/architecture/storage.md` | `Cargo.toml` internal crates |
| `frogdb-core` | `docs/architecture/architecture.md` | `Cargo.toml` internal crates |
| `frogdb-scripting` | `docs/guides/lua-scripting.md` | `Cargo.toml` internal crates |
| `frogdb-persistence` | `docs/architecture/persistence.md` | `Cargo.toml` internal crates |
| `frogdb-server` | `docs/architecture/architecture.md` | `Cargo.toml` internal crates |
| `frogdb-acl` | `docs/architecture/architecture.md` | `Cargo.toml` internal crates |
| `frogdb-cluster` | `docs/architecture/clustering.md` | `Cargo.toml` internal crates |
| `frogdb-replication` | `docs/architecture/replication.md` | `Cargo.toml` internal crates |
| `frogdb-vll` | `docs/architecture/vll.md` | `Cargo.toml` internal crates |
| `frogdb-commands` | `docs/guides/commands.md`, `docs/architecture/execution.md` | `Cargo.toml` internal crates |
| `frogdb-telemetry` | `docs/operations/monitoring.md` | `Cargo.toml` internal crates |
| `frogdb-debug` | `docs/architecture/debugging.md` | `Cargo.toml` internal crates |
| `frogdb-search` | — | `Cargo.toml` internal crates |
| `frogdb-macros` | — | `Cargo.toml` internal crates |
| `frogdb-metrics-derive` | — | `Cargo.toml` internal crates |
| `frogdb-test-harness` | `docs/architecture/testing.md` | `Cargo.toml` internal crates |
| `frogdb-testing` | `docs/architecture/testing.md` | `Cargo.toml` internal crates |

### Cross-Doc Links

All markdown links in website docs should resolve. Check by:
1. Grep for markdown link patterns: `\]\([^)]*\)`
2. Verify internal links point to existing pages
3. Run `just docs-build` — Astro build will warn about broken links

### File Paths in Docs

| Claimed Path | Doc File | Verification |
|-------------|----------|-------------|
| `crates/server/` | `docs/architecture/architecture.md` | Actual: `frogdb-server/crates/server/` |
| `crates/core/` | `docs/architecture/architecture.md` | Actual: `frogdb-server/crates/core/` |
| `crates/protocol/` | `docs/architecture/protocol.md` | Actual: `frogdb-server/crates/protocol/` |
| `crates/persistence/` | `docs/architecture/persistence.md` | Actual: `frogdb-server/crates/persistence/` |
| `crates/scripting/` | `docs/guides/lua-scripting.md` | Actual: `frogdb-server/crates/scripting/` |

## API Surface Assertions

### Struct/Trait Definitions in Code Blocks

| Type | Doc File | Source File |
|------|----------|-------------|
| `CommandContext` struct | `docs/architecture/architecture.md` | `crates/core/src/command.rs` or similar |
| `WalWriter` trait | `docs/architecture/persistence.md` | `crates/persistence/src/wal.rs` |
| `ParsedCommand` struct | `docs/architecture/protocol.md` | `crates/protocol/src/` |
| `Store` trait | `docs/architecture/storage.md` | `crates/core/src/store/` |
| `Command` trait | `docs/architecture/execution.md` | `crates/core/src/command.rs` |
| `Value` enum | `docs/architecture/storage.md` | `crates/types/src/types.rs` |

## Configuration Assertions

Config keys and default values referenced in `docs/operations/configuration.md` should match
the actual config structs in `crates/server/src/config/`.

**Spot-check list:**
- `num_shards` default
- `bind` address default
- `port` default (6379)
- `wal_mode` default
- `max_memory` default
- `eviction_policy` default

## Behavioral Assertions

### Justfile Recipes

CLAUDE.md references Justfile recipes. Verify recipe names exist in `Justfile`.

**Recipes referenced in CLAUDE.md:**
- `just check`, `just test`, `just lint`, `just fmt`, `just build`
- `just test <crate>`, `just test <crate> <pattern>`
- `just lint <crate>`, `just fmt <crate>`
- `just concurrency`, `just clean-stale`
- `just lint-py`, `just fmt-py`, `just fmt-check`
