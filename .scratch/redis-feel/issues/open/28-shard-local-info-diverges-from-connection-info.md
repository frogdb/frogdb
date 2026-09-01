# Shard-local `INFO` hardcodes `maxmemory:0`/`noeviction` while connection-level `INFO` reports the real config

Status: ready-for-agent
Type: bug (introspection accuracy)
Area: info

## Problem

FrogDB has two INFO renderers and they disagree about live configuration.

Connection-level (`frogdb-server/crates/server/src/info/sections.rs:201-208`) reads the real
config:

```rust
.field("maxmemory", cfg.maxmemory)
.field("maxmemory_policy", &cfg.policy)
```

Shard-local (`frogdb-server/crates/server/src/commands/info.rs:278-280`, the registered
`InfoCommand` — `server/register.rs:84`) emits literals:

```
maxmemory:0
maxmemory_human:0B
maxmemory_policy:noeviction
```

So an instance configured with `maxmemory 4gb` / `allkeys-lru` reports its policy correctly on one
path and reports `noeviction` on the other. `OBJECT FREQ` already errors correctly based on the
*real* policy (`commands/src/generic.rs:425`), so a client can see `maxmemory_policy:noeviction`
and simultaneously be told an LFU policy is active.

This is the duplication class the repo has already been bitten by — the regression tests at
`info/sections.rs:1413` and `:1507` exist precisely because "both renderers independently
hardcoded zeros", and they pin `stats`/`replication` fields against the shard-local builders for
that reason. `maxmemory` was never pinned that way.

## Ruling

Make the shard-local renderer read the same config source, then pin it: extend the existing
"one feed, both renderers" test pattern (`sections.rs:1413`, `:1496`, `:1707`) to cover the memory
block, so a literal reintroduced on either side fails a test.

Preferred: delete the duplicated block outright and have the shard-local path call the same
builder, the way `build_stats_info`/`build_backlog_info`/`build_replica_link_info` are already
shared. A second renderer that only *happens* to agree is the bug generator.

## Acceptance criteria

- [ ] With `maxmemory 4gb` + `allkeys-lru` configured, both INFO paths report the same
      `maxmemory`/`maxmemory_policy`
- [ ] A test compares the memory block of both renderers against one source, in the style of the
      existing `sync_lines`/`net_byte_lines` cross-checks
- [ ] The `maxmemory:0` / `noeviction` literals are gone from `commands/info.rs`
- [ ] An audit note in `## Comments` lists any other field still duplicated between the two
      renderers (candidates: the `# CPU` block, `db0:` keyspace line, `master_failover_state`)

Size: S
