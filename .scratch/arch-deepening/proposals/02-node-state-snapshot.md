# Proposal 02 — One NodeStateSnapshot; INFO / JSON status / debug UI become thin renderers

## Summary

Node-observable state — memory, keyspace, per-shard stats, persistence/WAL lag,
replication identity, clients, and cluster/slot topology — is modeled **three
times** across three surfaces, plus a **fourth conversion shim** that re-derives
cluster topology for the debug UI:

1. **INFO wire text** (`server/src/info/`) — a family of `*Snapshot` structs
   gathered via one shard scatter and rendered by an `InfoSection` registry.
2. **JSON `/status`** (`telemetry/src/status.rs`) — a parallel family of
   `*Status` structs gathered via its *own* two shard scatters and serialized
   by serde.
3. **Debug web UI** (`debug/src/web_ui/state.rs`) — a third family of
   `*Snapshot` / `*Data` / `*Stats` structs fed through provider traits.
4. **The shim** (`server/src/debug_providers.rs`) — `ClusterStateProvider`
   converts `frogdb-core`/`frogdb-cluster` types into the debug crate's cluster
   snapshot structs, re-implementing slot-range compaction that already exists
   in `frogdb-cluster`.

Each surface is a *shallow* module: its interface (the struct shape) is nearly
identical to its implementation (copy fields off shards), and the same
observable concept is re-declared, re-aggregated, and re-rendered in each. There
is no shared upstream interface. This proposal introduces a single **deep**
`NodeStateSnapshot`, populated once per request by the subsystems that own the
data, with the three surfaces reduced to pure rendering functions over that
snapshot and the `debug_providers.rs` conversion shim deleted.

## Files involved (verified paths + line counts)

| File | Lines | Role |
|------|-------|------|
| `frogdb-server/crates/server/src/info/mod.rs` | 976 | INFO snapshot structs + `gather_shard_snapshot` scatter |
| `frogdb-server/crates/server/src/info/sections.rs` | 1034 | `InfoSection` renderers (one per section) |
| `frogdb-server/crates/telemetry/src/status.rs` | 852 | `StatusCollector` + `*Status` structs → `to_json` |
| `frogdb-server/crates/debug/src/web_ui/state.rs` | 541 | Debug UI snapshot structs + provider traits |
| `frogdb-server/crates/server/src/debug_providers.rs` | 191 | Cluster→debug conversion shim |
| `frogdb-server/crates/cluster/src/types.rs` | (`get_node_slots` @ 513–540) | Canonical slot-range compaction |

Total in the three-surface + shim core: **3594 lines**.

## Problem (the parallel struct families, side by side)

Below, three concrete observables — **WAL lag**, a **slot-range list**, and
**per-shard stats** — are traced through every place they are modeled.

### Field A: WAL durability lag — modeled and aggregated in 2 places (rendered in a 3rd)

**INFO** — `WalAggregate` (`info/mod.rs:305–348`), with its own fold logic:

```rust
// info/mod.rs:305
pub struct WalAggregate {
    pub pending_ops: usize,
    pub pending_bytes: usize,
    pub durability_lag_ms: u64,   // worst (max) shard
    pub flush_failures: u64,
    pub lost_ops: u64,
    pub last_flush_ok: bool,
    pub last_flush_time_ms: u64,  // oldest (min) shard
}
// absorb(): max on lag, min on timestamp, sum on the rest, AND on last_flush_ok
```

**Telemetry** — `WalLagStatus` + `ShardLagStatus` (`status.rs:180–220`), a
*different* shape aggregated *differently* (keeps a per-shard `Vec`, computes
**both** max and average lag):

```rust
// status.rs:182
pub struct WalLagStatus {
    pub pending_ops_total: usize,
    pub pending_bytes_total: usize,
    pub max_durability_lag_ms: u64,
    pub avg_durability_lag_ms: f64,   // <- INFO does not compute this
    pub flush_failures_total: u64,
    pub lost_ops_total: u64,
    pub last_flush_ok: bool,
    pub shards: Vec<ShardLagStatus>,  // <- INFO does not keep per-shard detail
}
```

Both fold the *same* upstream `WalLagStats` shard reply, but through two
hand-written aggregation loops: `ShardInfoSnapshot::absorb` (`mod.rs:285`) and
`StatusCollector::gather_wal_lag_stats` (`status.rs:421–494`). The two are
already **inconsistent** — telemetry surfaces an average and per-shard rows;
INFO surfaces neither. The debug UI models WAL a third notional way (its
`LatencyData`/bundle path) but currently stubs it.

### Field B: slot-range list — compaction algorithm duplicated verbatim

The exact same "compact sorted slots into `(start,end)` ranges" loop
(`if slot == end + 1 { end = slot } else { push; start=end=slot }`) exists twice:

**Canonical, in `frogdb-cluster`** (`cluster/src/types.rs:513–540`,
`get_node_slots` → `Vec<SlotRange>`), consumed by the `CLUSTER NODES` /
`CLUSTER SHARDS` commands (`server/src/commands/cluster/mod.rs:343`, `:498`):

```rust
// cluster/src/types.rs:528
for slot in iter {
    if slot == end + 1 { end = slot; }
    else { ranges.push(SlotRange::new(start, end)); start = slot; end = slot; }
}
ranges.push(SlotRange::new(start, end));
```

**Re-implemented in the shim** (`debug_providers.rs:113–131`,
`compact_slot_ranges` → `Vec<(u16,u16)>`) purely so the debug crate's
`ClusterNodeSnapshot.slot_ranges` gets a shape it likes:

```rust
// debug_providers.rs:120
for &slot in &slots[1..] {
    if slot == end + 1 { end = slot; }
    else { ranges.push((start, end)); start = slot; end = slot; }
}
ranges.push((start, end));
```

Same algorithm, two return types (`SlotRange` vs `(u16,u16)`), two test suites
(`cluster` + `debug_providers.rs:146–167`). Add a rendering surface for slot
ranges and you write this loop a third time.

### Field C: per-shard stats — three struct shapes, three scatters

| Concept | INFO | Telemetry | Debug UI |
|---------|------|-----------|----------|
| struct | folds into `ShardInfoSnapshot` (aggregate only) `mod.rs:242` | `ShardStatus { id, keys, memory_bytes, peak_memory_bytes }` `status.rs:223` | `ShardStats { shard_id, keys, memory_bytes, queue_depth }` `state.rs:121` |
| shard message | `ShardMessage::InfoSnapshot` `mod.rs:636` | `ShardMessage::MemoryStats` `status.rs:402` **and** `ShardMessage::WalLagStats` `status.rs:427` | `DebugQuery::GetShardStats` `state.rs:72` (stubbed, `state.rs:376`) |

Three surfaces, **four** distinct shard round-trip families for overlapping
data, and the debug path returns `Vec::new()` because wiring a fourth scatter
was never finished (`state.rs:370–379`).

### The same story repeats for every observable

| Concept | INFO | Telemetry | Debug UI |
|---------|------|-----------|----------|
| memory | `ShardInfoSnapshot.used/peak_memory` + `MemoryConfigSnapshot` | `MemoryStatus` `status.rs:128` | (via `ShardStats`) |
| clients | `ClientsSnapshot` `mod.rs:352` | `ClientsStatus` `status.rs:117` | `ClientSnapshot` (per-client) `state.rs:135` |
| persistence | `PersistenceSnapshot` `mod.rs:447` | `PersistenceStatus`+`SnapshotStatus` `status.rs:143` | — |
| replication | `ReplicationSnapshot`/`PrimarySnapshot`/`ReplicaLine` `mod.rs:381–433` | `ClusterStatus.mode` only | `ReplicationInfoProvider` trait `state.rs:51` |
| keyspace | `ShardInfoSnapshot.keys` + `KeyspaceStats` | `KeyspaceStatus` `status.rs:237` | — |
| commands/latency | `LatencySnapshot` `mod.rs:457` | `CommandsStatus` `status.rs:245` | `LatencyData` `state.rs:104` |
| cluster/slots | `CLUSTER` cmd via `get_node_slots` | — | `ClusterOverviewSnapshot`/`ClusterNodeSnapshot`/`MigrationSnapshot` via shim |

## Why it is shallow / fragmented (architecture vocabulary)

- **Three adapters at three seams for one concept, with no shared upstream
  interface.** Each surface is its own adapter from raw subsystem state to a
  rendering-specific struct. Ousterhout: a real seam needs *two* adapters over
  *one* interface. Here we have three adapters and **no** interface — each
  re-declares the concept from scratch, so the "seam" hides nothing and buys no
  substitutability. The modules are shallow: `MemoryStatus`, `ShardStatus`,
  `ClientsStatus` are interfaces almost identical to their one-line
  implementations (`used_bytes: shard_stats.sum()`).

- **No locality / no leverage.** WAL-lag aggregation lives in two functions
  (`ShardInfoSnapshot::absorb`, `gather_wal_lag_stats`) that already disagree.
  The slot-range compaction lives in two functions that agree only because
  someone copied it correctly. There is no single place that owns "the node's
  observable WAL state," so no fix or feature has leverage across surfaces.

- **The deletion test fails.** Deleting `debug_providers.rs::compact_slot_ranges`
  does not simplify the system, because `get_node_slots` in `frogdb-cluster`
  already does the job — the shim exists only to change the return type. That is
  the signature of a shallow adapter that should not exist.

- **The "add one field" test fails hard.** To add a single new observable —
  say `wal_recovery_lag_ms` — an author must edit: (1) `WalAggregate` + its
  `absorb`/`from_shard` (`info/mod.rs`), (2) `PersistenceSection::render`
  (`info/sections.rs`), (3) `WalLagStatus` + `gather_wal_lag_stats` +
  `ShardLagStatus` (`status.rs`), and possibly (4) the debug bundle path — four
  trees, two aggregation loops, plus new tests in each. Nothing forces them to
  stay consistent; they already aren't.

## Proposed change

Introduce **one deep module** — a `NodeStateSnapshot` — that is the single
source of truth for node-observable state. It should live in **`telemetry`**
(or a new `observability` crate) because that crate already depends on
`frogdb-core` shard types and is downstream of both `server` renderers and the
`debug` UI. The design:

1. **Subsystems populate once.** A single `collect()` performs **one** shard
   scatter (the union of today's `InfoSnapshot` + `MemoryStats` + `WalLagStats`
   requests — `InfoSnapshot` already carries most of it) and folds it into
   `NodeStateSnapshot`. Aggregation logic (WAL max/avg/min, slot-range
   compaction via `frogdb-cluster::get_node_slots`) lives here, **once**.

2. **The three renderers consume it.**
   - `InfoBuilder`/`InfoSection` render INFO text from `&NodeStateSnapshot`.
   - `StatusCollector::to_json` serializes a view of `&NodeStateSnapshot`.
   - Debug UI handlers read `&NodeStateSnapshot` fields directly.

3. **`debug_providers.rs` provider traits collapse to reads off the snapshot.**
   `ClusterInfoProvider`/`ReplicationInfoProvider`/`ClientInfoProvider` and
   `convert_node`/`convert_migration`/`compact_slot_ranges` are deleted; the
   debug UI reads `snapshot.cluster` (which already used
   `frogdb-cluster::get_node_slots`).

The snapshot is a plain data value with no I/O, so every renderer becomes a pure
function `fn(&NodeStateSnapshot) -> Bytes|Json|Html`.

## Before / After (real code)

### Before — persistence/WAL modeled in two files, aggregated twice

INFO side (`info/mod.rs:305` + `info/sections.rs:232`):

```rust
// info/mod.rs
pub struct WalAggregate { pub pending_ops: usize, pub durability_lag_ms: u64, /* … */ }
impl WalAggregate {
    fn absorb(&mut self, lag: &WalLagStats) {
        self.pending_ops += lag.pending_ops;
        self.durability_lag_ms = self.durability_lag_ms.max(lag.durability_lag_ms);
        self.last_flush_time_ms = self.last_flush_time_ms.min(lag.last_flush_timestamp_ms);
        // …
    }
}

// info/sections.rs:232  (PersistenceSection::render)
if let Some(wal) = &sh.wal {
    w.field("wal_pending_ops", wal.pending_ops)
     .field("wal_durability_lag_ms", wal.durability_lag_ms)
     .field("wal_last_flush_status", if wal.last_flush_ok {"ok"} else {"err"})
     // …
}
```

Telemetry side (`status.rs:421`), a second aggregation loop and a second struct:

```rust
// status.rs:452  (gather_wal_lag_stats)
for response in &responses {
    if let Some(ref lag_stats) = response.lag_stats {
        pending_ops_total += lag_stats.pending_ops;
        total_durability_lag_ms += lag_stats.durability_lag_ms;   // for the average
        if lag_stats.durability_lag_ms > max_durability_lag_ms {
            max_durability_lag_ms = lag_stats.durability_lag_ms;
        }
        shards.push(ShardLagStatus { /* per-shard copy */ });
    }
}
WalLagStatus { pending_ops_total, max_durability_lag_ms, avg_durability_lag_ms, shards, /* … */ }
```

### After — one aggregate, three thin renderers

```rust
// telemetry/observability/wal.rs  (owns aggregation ONCE)
pub struct WalState {
    pub pending_ops: usize,
    pub pending_bytes: usize,
    pub max_durability_lag_ms: u64,
    pub avg_durability_lag_ms: f64,
    pub flush_failures: u64,
    pub lost_ops: u64,
    pub last_flush_ok: bool,
    pub last_flush_time_ms: u64,
    pub per_shard: Vec<ShardWalState>,   // superset both surfaces can read
}
impl WalState {
    pub fn fold(shards: &[WalLagStats]) -> Option<Self> { /* the ONE loop */ }
}

// info/sections.rs  (pure render)
fn render_persistence(w: &mut SectionWriter, wal: &WalState) {
    w.field("wal_pending_ops", wal.pending_ops)
     .field("wal_durability_lag_ms", wal.max_durability_lag_ms)
     .field("wal_last_flush_status", if wal.last_flush_ok {"ok"} else {"err"});
}

// telemetry/status.rs  (pure serialize — a view struct or #[derive(Serialize)] on WalState)
fn wal_json(wal: &WalState) -> WalLagStatus { WalLagStatus::from(wal) }
```

### "Add one new WAL-lag field" (`wal_recovery_lag_ms`)

**Before:** edit `WalAggregate` + `from_shard` + `absorb` (`info/mod.rs`),
`PersistenceSection::render` (`sections.rs`), `WalLagStatus` +
`gather_wal_lag_stats` + `ShardLagStatus` (`status.rs`) — **3 files, 2
aggregation loops, ~6 sites**, plus tests in each, with nothing enforcing
agreement.

**After:** add one field to `WalState` + one line in `WalState::fold`, then one
line in each renderer that chooses to surface it — **1 aggregation site**, and
the surfaces opt in independently without re-deriving the value.

## Testability improvement

Today, exercising any surface end-to-end requires a near-fully-booted server:
`StatusCollector::collect` (`status.rs:301`) awaits real `ShardSender`
channels; `gather_shard_snapshot` (`info/mod.rs:627`) scatters to live shards;
debug `get_shard_stats`/`get_latency` are stubbed precisely *because* wiring a
real scatter under test is expensive. The existing unit tests only cover the
pieces that were already pure: `SectionWriter` formatting, `compact_slot_ranges`
in isolation, serde round-trips of hand-built `*Status` structs.

With a single `NodeStateSnapshot` value:

- **INFO text** becomes `assert_eq!(render(&snapshot), expected_bytes)` over a
  fixture snapshot — no shards, no scatter. (The `test_support::sources()`
  helper at `info/mod.rs:668` already hints at this pattern; it would become
  *the* seam for all three surfaces.)
- **JSON status** becomes `assert_json!(to_json(&snapshot))` over the same
  fixture.
- **Debug UI HTML** becomes a pure render test over the same fixture.

One fixture builder feeds golden tests for all three surfaces, guaranteeing they
report consistent values — impossible today because each computes its own.

## Migration path (incremental — avoid a big-bang rewrite)

This is large; do it surface-by-surface behind the new type, never rewriting all
three at once.

1. **Land `NodeStateSnapshot` + `collect()` alongside the existing code.** Build
   it from the *same* shard replies INFO already gathers (`InfoSnapshot` is the
   richest scatter). No renderer changes yet; add unit tests over fixtures.
2. **Adopt it in the JSON `/status` surface first.** It is the smallest
   (`status.rs`, 852 L), has the weakest existing tests, and several of its
   fields are already stubbed to `0` (`total_writes`, `expired_keys_total`,
   `ops_per_sec` — `status.rs:376,386,390`), so switching to real snapshot data
   is a net *improvement*, not just a refactor. Delete `gather_shard_stats` /
   `gather_wal_lag_stats` in favor of the shared `collect()`.
3. **Delete the `debug_providers.rs` shim + collapse the provider traits.**
   Point the debug UI at `snapshot.cluster` (populated via
   `frogdb-cluster::get_node_slots`). This removes the duplicated
   `compact_slot_ranges` and the `convert_*` functions and unstubs
   `get_shard_stats`/`get_latency`.
4. **Adopt it in INFO last.** INFO has the strongest existing tests and the
   strictest wire-format contract, so migrate it once the snapshot shape has
   settled. Keep `SectionWriter`/`InfoSection` (they are already deep and
   worth keeping); only swap `InfoSources` for `&NodeStateSnapshot`.
5. **Remove the redundant shard round-trips.** Once all three read the snapshot,
   collapse `MemoryStats` + `WalLagStats` scatters into the single
   `InfoSnapshot`-style gather.

Each step is independently shippable and keeps the wire/JSON output
byte-identical (guarded by golden tests captured before step 1).

## Risks / open questions

- **Crate layering / cycles.** `NodeStateSnapshot` must sit where `server`
  (INFO), `telemetry` (JSON), and `debug` (UI) can all depend on it without a
  cycle. `telemetry` is the natural home, but if `server` types
  (`ReplicationSnapshot`, cluster identity) must be represented, some of them
  may need to move down to `frogdb-core`/`frogdb-cluster`. Needs a dependency
  audit before step 1.
- **Wire-format drift.** INFO's Redis-compatibility contract is strict
  (`master_replid2`, `second_repl_offset` sentinels, `field_opt` honesty about
  disabled subsystems). The snapshot must preserve `Option`-ness so a renderer
  can still *omit* a field rather than emit a placeholder `0`. Golden tests
  before migrating are mandatory.
- **Divergent aggregation is a feature in one spot?** Telemetry computes
  `avg_durability_lag_ms` and keeps per-shard rows; INFO deliberately does not.
  The unified `WalState` must be a *superset* so each renderer picks what it
  surfaces — confirm no surface actually needs a *different* aggregation (e.g.
  min vs max) of the same field.
- **Snapshot cost.** A single richer scatter per request is cheaper than today's
  up-to-three, but the snapshot value is larger; confirm it is built on demand
  per request (as INFO already does) and not cached stale.
- **Debug UI per-client detail.** `ClientSnapshot` (`state.rs:135`) is
  per-connection, not aggregated like `ClientsStatus`. The snapshot should hold
  the registry handle / per-client list, with the aggregate counts derived —
  confirm this is the right granularity for the shared type.

## Effort estimate

**L (Large).** Three surfaces + a shim across four crates (`server`,
`telemetry`, `debug`, and likely `core`/`cluster` for relocated types),
totalling ~3600 lines of directly affected code, plus a strict Redis wire-format
contract that demands golden tests before any change. The work is
*decomposable* (the five-step migration ships incrementally and the JSON surface
alone delivers value by unstubbing fields), but the full consolidation —
including deleting the `debug_providers.rs` shim and collapsing the redundant
shard scatters — is unavoidably a multi-PR effort with cross-crate layering
decisions.
