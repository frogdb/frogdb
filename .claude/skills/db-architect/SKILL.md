---
name: db-architect
description: >
  Architectural design and implementation planning for FrogDB. Produces a design
  rationale and implementation plan written to the plan mode plan file. Invoke for
  any non-trivial change: new commands, new subsystems, cross-crate feature additions,
  concurrency changes, persistence changes, protocol changes, replication impacts,
  or any work touching more than one crate. Also use when the user asks about
  architecture, component design, request flows, dispatch mechanisms, or tradeoffs.
  Even for simpler changes like adding a single command, use this skill to ensure
  the right execution strategy, WAL strategy, and flags are chosen.
---

# FrogDB DB Architect

This skill guides architectural design for FrogDB changes. It produces a design rationale and
implementation plan written to the plan mode plan file.

## Trigger Tiers

Calibrate the depth of design work to the change:

| Tier | When | What to produce |
|------|------|-----------------|
| **Skip** | Typo fixes, single-line bug fixes, mechanical renames, test-only additions | No design needed |
| **Lightweight** | Single new command, adding a flag to an existing enum, small 2-3 file changes within one crate | Lightweight design template |
| **Full design** | New subsystem/crate, cross-crate changes, data flow changes, persistence format changes, new execution strategy, concurrency model changes, cluster/replication behavior changes | Full design template |

## Design Workflow

Follow these steps in order. Write results to the plan mode plan file.

### Step 1: Understand the Request

- Read the user's request carefully
- Ask clarifying questions (databases require extreme attention to detail)
- Identify affected subsystems
- Read relevant architecture/operations docs in `website/src/content/docs/` for the affected subsystems
- Research Redis/Valkey/DragonflyDB precedent for the feature or behavior

### Step 2: Map Affected Components

- Read `references/crate-map.md` to identify all crates and files affected
- Trace request flow paths (see `website/src/content/docs/architecture/request-flows.md`)
- Identify the correct `ExecutionStrategy` and `WalStrategy`
- For new commands, read `references/command-guide.md`

### Step 3: Evaluate Alternatives

- Generate 2+ approaches (even if one is obvious)
- Evaluate each against the architectural rules below
- Consider performance, persistence, cluster, and replication implications
- **Always prefer fan-out/fan-in via shard message passing** over global tasks or shared state.
  If a viable message-passing approach exists, use it. If there is a significant tradeoff
  (e.g., latency, complexity, correctness), present the tradeoff and prompt for input.

### Step 4: Design the Solution

- Select best approach with rationale
- Define new types, traits, enums
- Define data flow — show how requests and responses move through the system
- Identify configuration and observability additions
- Ensure each new component has a single, clear responsibility

### Step 5: Write the Implementation Plan

- Files to create/modify in dependency order
- Interface-level changes (new traits, enum variants, function signatures)
- Test strategy
- Verification sequence (`just` commands)
- Doc updates needed (`website/src/content/docs/`)

### Step 6: Risk Assessment

- Edge cases and failure modes
- Backward compatibility (breaking changes are acceptable, but note them)
- Performance risks
- Concurrency testing needs (Shuttle/Turmoil)

---

## Architectural Rules

Read `~/.claude/skills/shared/frogdb-rules.md` for the full set of non-negotiable architectural
rules (shared-nothing, thread-per-core, message passing, shard-local, flow clarity, design
principles, anti-patterns). All rules there apply to every change. Violations must be explicitly
justified.

The sections below cover db-architect-specific guidance that supplements the shared rules.

### ExecutionStrategy Selection

Every command declares how it should be executed:

| Strategy | When to Use | Examples |
|----------|-------------|----------|
| `Standard` | Single-key, route by hash (default) | GET, SET, LPUSH, ZADD |
| `ConnectionLevel(op)` | Handled by ConnectionHandler, not routed to shards | pub/sub, transactions, auth, scripting, admin, connection state, replication, persistence |
| `Blocking { default_timeout }` | May suspend waiting for data | BLPOP, BRPOP, BZPOPMIN |
| `ScatterGather { merge }` | Multi-key spanning shards | MGET, DEL, EXISTS |
| `ServerWide(op)` | All-shard execution | SCAN, KEYS, DBSIZE, FLUSHDB, RANDOMKEY |
| `RaftConsensus` | Cluster topology mutations | CLUSTER ADDSLOTS, CLUSTER MEET |
| `AsyncExternal` | Async I/O outside shard path | MIGRATE, DEBUG SLEEP |

**MergeStrategy** for ScatterGather:

| Strategy | When | Examples |
|----------|------|----------|
| `OrderedArray` | Preserve key order | MGET |
| `SumIntegers` | Sum results | DEL, EXISTS, TOUCH, UNLINK |
| `CollectKeys` | Collect all keys | KEYS |
| `CursoredScan` | Merge cursor pagination | SCAN |
| `AllOk` | All must succeed | MSET, FLUSHDB |
| `Custom` | Custom merge logic | Complex multi-shard ops |

### WalStrategy Selection

Every write command declares how its effects are persisted:

| Strategy | When | Examples |
|----------|------|----------|
| `PersistFirstKey` | Persist key's new value | SET, APPEND, INCR, LPUSH, SADD, ZADD, HSET |
| `DeleteKeys` | Persist deletion | DEL, UNLINK, GETDEL |
| `PersistOrDeleteFirstKey` | Persist if exists, else delete | LPOP, RPOP, SPOP, SREM, HDEL, LTRIM |
| `RenameKeys` | Delete old, persist new | RENAME, RENAMENX |
| `MoveKeys` | Persist-or-delete source, persist dest | RPOPLPUSH, LMOVE |
| `PersistDestination(idx)` | Persist destination at arg index | SINTERSTORE, COPY, ZRANGESTORE |
| `NoOp` | No WAL needed | FLUSHDB, FLUSHALL |
| `Infer` | **Legacy fallback — never use for new commands** | — |

### Command Trait Contract

Every command must declare:

- `name()` — uppercase command name (e.g., `"GET"`)
- `arity()` — `Fixed(n)`, `AtLeast(n)`, or `Range { min, max }`
- `flags()` — `CommandFlags` bitflags (WRITE, READONLY, FAST, BLOCKING, etc.)
- `execution_strategy()` — defaults to `Standard`
- `wal_strategy()` — defaults to `Infer` (override for all new commands)
- `execute()` — command logic, receives `CommandContext` and args
- `keys()` — extract key(s) from args for routing

Optional overrides:

- `wakes_waiters()` — return `Some(WaiterKind::List|SortedSet|Stream)` if this write unblocks waiters
- `requires_same_slot()` — return `true` if all keys must hash to same slot

### Key Routing Rules

- Internal: `xxhash64(extract_hash_tag(key)) % num_shards`
- Cluster: `CRC16(extract_hash_tag(key)) % 16384`
- Hash tags `{tag}` colocate at both levels
- Multi-key commands must validate CROSSSLOT unless `allow_cross_slot_standalone`

### Crate Boundary Rules

| Crate | Owns | Does NOT Own |
|-------|------|-------------|
| `frogdb-protocol` | Wire format (RESP2/RESP3) | Business logic, routing |
| `frogdb-types` | Value types, data structures | Storage, persistence |
| `frogdb-persistence` | RocksDB, WAL, snapshots, serialization | Command logic, routing |
| `frogdb-vll` | Multi-shard lock coordination | Data storage, commands |
| `frogdb-replication` | Primary/replica streaming | Command execution |
| `frogdb-cluster` | Raft consensus, slot management | Connection handling |
| `frogdb-acl` | Users, permissions, ACL log | Command execution |
| `frogdb-scripting` | Lua function registry, loading | VM execution (in core) |
| `frogdb-search` | FT index, query parsing | Storage of indexed data |
| `frogdb-core` | Command trait, Store, shard worker, pub/sub, eviction, scripting VM | Server binary, config, network |
| `frogdb-commands` | Data-structure command impls | Server commands, routing |
| `frogdb-server` | Connection handler, dispatch, routing, config, admin API, TLS | Data structure operations |
| `frogdb-telemetry` | Metrics, tracing, health | Data operations |

---

## Output Templates

### Full Design (for full-tier changes)

Write this to the plan mode plan file:

```
## Context
[Why this change is being made]

## Prior Art
[Redis/Valkey/DragonflyDB precedent]

## Alternatives Considered
### Option A: [Name]
[Description, pros, cons]
### Option B: [Name]
[Description, pros, cons]

## Decision
[Selected option and rationale]

## Data Flow
[Step-by-step: how requests and responses move through the system for this feature]

## Affected Components
| Crate | File | Change Type | Description |
|-------|------|-------------|-------------|

## New Types / Interfaces
[Code blocks for new definitions]

## Implementation Steps
[Ordered, grouped by crate, dependency order]

## Test Strategy
[Unit, integration, concurrency, property tests]

## Verification
[Ordered just commands]

## Doc Updates
[Which website/src/content/docs/ files need updating]

## Risks and Edge Cases
[Numbered list]
```

### Lightweight Design (for lightweight-tier changes)

```
## Context
[Problem + decision in 2-3 sentences]

## Implementation Steps
[Ordered list with file paths]

## Test Strategy
[What to test]

## Verification
[just commands]
```

---

## Reference File Pointers

Read these when working on designs:

| When | Read |
|------|------|
| Architectural rules and principles | `~/.claude/skills/shared/frogdb-rules.md` |
| Mapping affected components | `references/crate-map.md` |
| Full designs — completeness check | `references/design-checklist.md` |
| Implementing new commands | `references/command-guide.md` |
| Touching a specific subsystem | `website/src/content/docs/architecture/{subsystem}.md` |
| Understanding request flows | `website/src/content/docs/architecture/request-flows.md` |
| Architecture overview | `website/src/content/docs/architecture/architecture.md` |
| Concurrency model | `website/src/content/docs/architecture/concurrency.md` |

---

## When to Defer

| Situation | What to do |
|-----------|------------|
| Restructuring or decomposing existing code | Use `/refactorer` skill |
| Running build/lint/test | Use `/check` skill |
| Benchmarks/load testing | Use `/benchmark` skill |
| CPU/memory profiling | Use `/profile` skill |
| Jepsen distributed tests | Use `/jepsen-testing` skill |
