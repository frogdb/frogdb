# `INFO memory` allocator fields and `MEMORY PURGE` still say "no jemalloc" — the binary ships jemalloc

Status: needs-triage
Type: bug (introspection accuracy) / ruling needed
Area: info / memory

## Problem

The server sets jemalloc as its global allocator:

```rust
// frogdb-server/crates/server/src/main.rs:7
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

but the memory surface is written as if it did not:

- `MEMORY PURGE` → `Response::ok()` with `// Without jemalloc, this is a no-op`
  (`connection/observability_conn_command.rs:404-407`). It claims to have purged and does nothing.
- `MEMORY MALLOC-SIZE` echoes its input with `// Without jemalloc, just return the input size`
  (`:387-401`).
- `INFO memory`: `allocator_allocated:0`, `allocator_active:0`, `allocator_resident:0`,
  `allocator_frag_ratio:1.00`, `allocator_frag_bytes:0`, `allocator_rss_ratio:1.00`,
  `rss_overhead_*`, `mem_fragmentation_ratio:1.00`, `mem_fragmentation_bytes:0`
  (`info/sections.rs:190-217`, `commands/info.rs:268-288`).
- `mem_allocator:rust` — the allocator is jemalloc; Redis reports `jemalloc-5.x.y`.
- `total_system_memory:0` — readable from the OS.
- `used_memory_rss` is set equal to `used_memory` (`sections.rs:182`), so
  `mem_fragmentation_ratio` is structurally `1.00` and can never signal anything.
- `mem_replication_backlog:0`, `mem_clients_slaves:0`, `mem_clients_normal:0` — the backlog
  geometry and per-client memory are both tracked elsewhere in the same file.

A `1.00` fragmentation ratio and a `0` allocator footprint are the fields an operator reads to
decide whether to restart a node. They are currently fabricated, which ADR-0005 ruling 3 forbids.

## Why this is `needs-triage` and not `ready-for-agent`

Three judgment calls that should not be guessed:

1. **Dependency**: real values need `tikv-jemalloc-ctl` (sibling of the existing
   `tikv-jemallocator` dep) and `--features stats`/`profiling` on the allocator, plus a decision
   about the non-jemalloc build (`cfg(target_env = "msvc")` currently has no jemalloc at all —
   those builds must degrade to something truthful, likely omitting the fields).
2. **`mem_allocator` string**: report `jemalloc-<version>` (matches Redis, and tooling parses it)
   vs. something FrogDB-specific. Ruling needed on whether matching Redis's spelling here is
   truthful or misleading.
3. **Scope**: `used_memory_rss` from the OS (`task_info`/`/proc/self/statm`) makes
   `mem_fragmentation_ratio` meaningful, but is a bigger change than reading jemalloc counters.
   May want to split.

`MEMORY PURGE` returning `+OK` without purging is the one part with no ruling needed — it is
either wired to `arenas.purge` or it must stop claiming success.

## Acceptance criteria (draft, pending ruling)

- [ ] `MEMORY PURGE` actually purges, or is reclassified as `NotSupported` with a reason
- [ ] `allocator_allocated`/`active`/`resident` are non-zero and move with load
- [ ] `mem_allocator` names the allocator actually in use
- [ ] `total_system_memory` matches the host
- [ ] Non-jemalloc builds omit the fields rather than emitting zeros
- [ ] `mem_replication_backlog`/`mem_clients_*` read from the trackers that already hold them

Size: M
