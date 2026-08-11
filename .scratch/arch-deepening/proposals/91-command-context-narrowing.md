# Proposal 91 — `CommandContext` is 25 fields wide, 296 handlers read 6 of them, and the wide half is copied field-by-field at three sites — the third one drops seven

Round 38 · lane: commands + core types · candidate **CT4** · effort **M** (the narrowing)
+ **S** ×3 (three independently-landable hotfixes, one of them a LIVE user-visible bug) ·
**no locked crate edited** (`frogdb-core`, `frogdb-commands`, `frogdb-server`) · **zero
`FM-` tags in any edited region** — but four `FM-` *prose citations* sit in the regions I
move, and §Risks says how they travel.

**Rev 2** — revised against an adversarial review that returned **AMEND** with three design
flaws (all three real; all three applied) and nine corrections (all nine re-verified against
the tree and applied). The design changed materially: see §Review dispositions at the end
for what moved and why, and §The shape / §Risks for the new design. The review's
VERIFIED-GOOD findings (the LIVE chain, the field tally, the dead-tranche ranges, the H3
arithmetic) are unchanged.

**Verified at HEAD `dd840ca3`** (was `8a170652` at rev 1). `git diff --stat
8a170652..dd840ca3` touches **eight files, all `.scratch/arch-deepening/proposals/*.md`** —
no code file moved between rev 1 and rev 2, so every `file:line` below was re-checked
against the working tree at `dd840ca3` and is exact. Dirty in the shared tree right now:
proposals 81 / 82 / 84 (modified). **No code file is dirty**; concurrent authors are in
`.scratch/` only.

---

## Corrections to the lane brief

The brief was directionally right and numerically loose. Four claims are adjusted, one is
confirmed exactly, and **one adjustment changes the proposal's status from "latent" to
LIVE**.

| Brief claim | Verified at HEAD |
|---|---|
| "`CommandContextCore` + `as_core()` are dead (`command.rs:1057-1088`, `:1537`); only other ref is a `lib.rs` re-export — pure delete" | **Confirmed, with one extra reference the brief missed.** Struct + impl = `:1035-1088` (banner `:1035-1037`, doc `:1039-1056`, `pub struct` `:1057-1075`, `impl` `:1077-1088`). `as_core` = `:1517-1546` (doc `:1517-1535`, fn `:1536-1546`). Re-export `lib.rs:72`. **Fourth site: `store/typed.rs:107`**, a doc comment reading ``/// store is used as `&mut dyn Store` (see `CommandContextCore::store`), and`` — a dangling intra-doc reference the moment the type dies. Total ≈ **85 lines**, 4 files. |
| "`CommandContext` has ~25 fields; `frogdb-commands` uses 6; the other ~18 have 0 uses" | **Adjusted to exact: 25 / 6 / 19.** Field list is `command.rs:1260-1377`; I counted the declarations, not the doc lines. The used 6 are `store`, `protocol_version`, `effects`, `num_shards`, `json_limits`, `command_registry`. Per-field tally in §Problem 2. The brief said 18 because 6+18=24 ≠ 25. |
| "29/40 command files have zero tests" | **Adjusted: 296 `impl Command for` blocks live in `frogdb-commands`; only 7 files in the crate construct a `CommandContext` at all.** The file-count framing does not survive — the crate is not 40 files and the handlers are not one-per-file. The load-bearing number is different and worse: **`CommandContext::new` appears at exactly 14 call sites workspace-wide, 13 of them test/fuzz scaffolding and 1 of them production** (§Problem 3). |
| "tests need `Box::leak`" | **Confirmed and quantified: 14 `Box::leak` calls in `frogdb-commands`, exactly 2 per test-context helper × 7 helpers** (`basic.rs:905-906`, `bloom.rs:681-682`, `cuckoo.rs:759-760`, `generic.rs:679-680`, `hash.rs:2105-2106`, `sort.rs:561-562`, `string.rs:1575-1576`). All seven are byte-identical apart from the file they sit in. One of the two leaks exists **only** to satisfy a constructor parameter no handler in the crate ever reads. |
| — *(not in the brief)* | **The wide half is dead-or-dying in a second, larger tranche.** `ReplicationContextRef` (`:1030-1033`), `replication_context()` (`:1574-1589`), `require_replication()` (`:1612-1626`), `has_replication()` (`:1590-1595`), `require_cluster()` (`:1597-1610`) and `CommandContext::is_cluster_mode()` (`:1511-1515`) have **zero call sites anywhere in the workspace**. `ClusterContextRef` + `cluster_context()` have **exactly one** (`server/src/commands/version.rs:45`). §Problem 1. |
| — *(not in the brief)* | **LIVE defect, not latent.** `scripting/gate.rs:454-506` (`run_local`) rebuilds a `CommandContext` by hand and propagates **11 of the 18** fields it would need; **7 are silently dropped**. Two of the seven are read by registered, script-callable commands, so `redis.call('COMMAND','COUNT')` returns `0` and `redis.call('CLUSTER','INFO')` reports standalone on a cluster node. §Problem 4. |

---

## Summary

`CommandContext` is a single 25-field value that serves three audiences with almost no
overlap: the **296 data-structure handlers** in `frogdb-commands`, which read 6 fields; the
**50 admin/introspection handlers** in `frogdb-server`, which read most of the other 19; and
the **shard runtime**, which populates all 25. Because there is one struct and no grouping,
"hand a command its context" is spelled as *25 individual field assignments*, and that
spelling is repeated at three sites. Two of the three are correct. The third —
`scripting/gate.rs`, the Lua `redis.call` path — is a hand-maintained list that has been
patched **three separate times, one field per patch** (`FM-REPLICATION-059`,
`issue 10 / FM-PERSISTENCE-022`, `issue 42`) and is **still missing seven fields today**.

The tree has already tried to narrow this seam once, additively: `CommandContextCore`,
`ClusterContextRef`, `ReplicationContextRef`, `as_core()`, `require_cluster()`,
`require_replication()`, `has_replication()`, `is_cluster_mode()` are all *views* onto
subsets of the 25. **That attempt failed completely and measurably**: of those eight, six
have zero callers, one has one caller, and `server/src/commands/cluster/admin.rs` — the
single densest consumer of cluster fields, **18 `ClusterDisabled` sites, 10 of them a direct
`ctx.<field>.ok_or(CommandError::ClusterDisabled)?`** — reaches past all of them straight
into `ctx.node_id` / `ctx.cluster_state` / `ctx.raft` (`admin.rs:132`, `:248`, `:394`).
Adding a narrow view next to a wide field never narrows anything, because the wide field is
still there and still shorter to type.

**The proposal is to narrow by subtraction instead.** Move the 19 fields no data-structure
handler reads out of `CommandContext` and into one owned, `Clone` + `Default`
`NodeContext`, held as a single field. The handler-facing interface keeps the 6 fields it
actually uses, so **`frogdb-commands` changes by zero lines of handler code**. And the
constructor stops being a partial one: every value that cannot be safely defaulted becomes a
parameter, so **propagating a context becomes exhaustive by construction** — there is no
longer a post-construction assignment list that can be incomplete. That is what makes the
`gate.rs` defect class *unrepresentable* rather than fixed for the fourth time. The
`shard_senders` parameter that forces a second `Box::leak` in every test helper disappears
into `NodeContext`.

---

## Files involved

Verified paths, line counts at HEAD `8a170652`.

| File | Lines | Role in this proposal |
|---|---:|---|
| `frogdb-server/crates/core/src/command.rs` | 2014 | **Primary.** `Command` trait `:714`, `execute` `:720`, `CommandContext` `:1260-1377`, `new` `:1381-1416`, the eight view/accessor items `:1011-1088` + `:1511-1626`. |
| `frogdb-server/crates/core/src/lib.rs` | 166 | Re-export list `:72` (`CommandContextCore`), `:74` (`ReplicationContextRef`). |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | The **only production construction site**: `command_context` `:333-382`, 25-field struct literal `:355`. FM prose `:349`. Also edited by 81 and 88 — §Risks. |
| `frogdb-server/crates/core/src/scripting/gate.rs` | 1244 | **The defect site.** `ScriptInvoker` re-declares 17 context fields `:295-348`; `from_context` copies them one by one `:350-383`; `classify` `:220-262`; `reject_server_wide` `:441-451`; `run_local` `:454-506`, whose assignment block `:484-504` re-assembles 11 of 18. FM prose `:316`, `:329`, `:336`, `:501`; FM-tagged tests `:1095`, `:1142`; 3 test-only `ScriptInvoker { … }` literals at `:817` (helper `live_invoker:809-838`), `:1058`, `:1106`. |
| `frogdb-server/crates/core/src/shard/scripting.rs` | 261 | The **already-fixed sibling path** — `execute_script_sub_command:196-225` routes through the shared builder, with a comment at `:217-220` saying exactly why. The asymmetry with `run_local` is the argument. |
| `frogdb-server/crates/core/src/shard/types.rs` | 1498 | Feasibility evidence: `ShardCluster:582-607` already holds the same handles as owned `Option<Arc<…>>`; accessors `:612-638`. |
| `frogdb-server/crates/core/src/store/typed.rs` | 584 | Doc reference to `CommandContextCore` at `:107` — must be reworded, not just deleted. |
| `frogdb-server/crates/commands/src/basic.rs` | 1054 | `CommandCommand` `:113`, spec `:117-133`, degraded branches `:147-151`, `:193`, `:267`, `:287`, `:303`, `:334`; the 5 `ctx.command_registry` read sites are `:147`/`:267`/`:303`/`:334` (`if let Some(registry) = …`) and `:193` (`.and_then`); test helper `Box::leak` `:905-906`, `new` `:907`. |
| `frogdb-server/crates/commands/src/{bloom,cuckoo,generic,hash,sort,string}.rs` | — | Six more `Box::leak` context helpers; identical shape. Leak pairs at `bloom:681-682`, `cuckoo:759-760`, `generic:679-680`, `hash:2105-2106`, `sort:561-562`, `string:1575-1576`. |
| `testing/fuzz/fuzz_targets/{resp_pipeline,restore_payload,cmd_dispatch}.rs` | — | **Four more `CommandContext::new` sites** (`resp_pipeline:50`, `:72`, `restore_payload:82`, `cmd_dispatch:67`) in a crate with its own `[workspace]` (`testing/fuzz/Cargo.toml:10-11`) — **invisible to `just check` / `just lint` / `just test`**. Any constructor-signature change must be applied here by hand and verified with `just fuzz-build` (or `cargo +nightly fuzz build` in that directory). |
| `frogdb-server/crates/server/src/commands/cluster/mod.rs` | 1211 | `CLUSTER` spec `:88-107`, `execute:110`, `cluster_info:278-280` (the standalone fallback the script path wrongly hits). |
| `frogdb-server/crates/server/src/commands/cluster/admin.rs` | — | The evidence that the accessor seam lost: 18 `ClusterDisabled` sites, 10 of which are a direct `ctx.<field>.ok_or(…)` (`:132`, `:194`, `:247`, `:248`, `:356`, `:394`, `:395`, `:434`, `:452`, `:486`), plus 10 further direct reads (`ctx.raft.is_none()` `:23`/`:101`/`:135`/`:197`/`:251`/`:359`/`:455`/`:489`, `ctx.cluster_state` `:164`/`:628`) — **20 direct node-field reads in one file, zero accessor uses**. |
| `frogdb-server/crates/server/src/commands/version.rs` | 139 | The **only** `cluster_context()` caller, `:45`. |
| `frogdb-server/crates/core/src/scripting/bindings.rs` | — | `is_forbidden_in_script:10-25`, `is_forbidden_subcommand:28-41` — reachability proof for §Problem 4. |
| `Justfile` | 1387 | `lint-gates:329`; `lint-script-gate:1080-1107` is the template for the optional construction gate. |

---

## Problem

### 1. Six of the eight "narrow view" items are dead; the seventh has one caller

The Context Helper Structs section (`command.rs:1011-1088`) plus the accessor block
(`:1511-1626`) is an entire narrowing layer with essentially no consumers:

| Item | Lines | Call sites outside `command.rs` |
|---|---|---:|
| `CommandContextCore` struct + `get_or_create` impl | `:1035-1088` (54) | **0** |
| `CommandContext::as_core` | `:1517-1546` (30) | **0** |
| `ReplicationContextRef` | `:1029-1033` (5) | **0** |
| `CommandContext::replication_context` | `:1574-1589` (16) | **0** |
| `CommandContext::require_replication` | `:1612-1626` (15) | **0** |
| `CommandContext::has_replication` | `:1590-1595` (6) | **0** |
| `CommandContext::require_cluster` | `:1597-1610` (14) | **0** |
| `CommandContext::is_cluster_mode` | `:1511-1515` (5) | **0** |
| `ClusterContextRef` + `cluster_context` | `:1015-1027`, `:1548-1572` (38) | **1** — `version.rs:45` |

The table sums to **183 lines of interface with one live consumer**. Of those, **145 are
deletable today with zero call-site churn** — the eight zero-caller items, i.e. H1 (85) +
H2 (60); the remaining 38 are `ClusterContextRef` + `cluster_context`, which `version.rs:45`
still uses and which H1/H2 therefore leave alone. Both numbers appear below and they mean
different things: 183 = the size of the failed layer, 145 = the free-deletion subset.
`require_replication` is doubly dead: it is the *only* producer of
`CommandError::ReplicationDisabled` (`types/src/error.rs:150`), which therefore has no
reachable path to the wire.

The interesting part is *why* it is dead. These items were added to let a command say "I
only need the core" or "I require cluster mode". Nothing adopted them, because the wide
fields stayed public next to the views. `admin.rs` — which would be the flagship
`require_cluster()` client, with **18 `ClusterDisabled` returns** — instead writes
`ctx.<field>.ok_or(CommandError::ClusterDisabled)?` **ten** times (`ctx.node_id` at `:132`,
`:194`, `:247`, `:356`, `:395`, `:452`, `:486`; `ctx.cluster_state` at `:248`; `ctx.raft` at
`:394`, `:434`) and reaches the same fields directly ten more times for the other eight
returns (`ctx.raft.is_none()` ×8, `ctx.cluster_state` ×2). **This is the deletion test applied to a narrowing
strategy, and it comes back negative: delete the entire views layer and nothing outside
`version.rs` notices.** Additive narrowing does not narrow. That is the architectural
lesson this proposal is built on, and it is why the design below *removes* fields from the
handler interface rather than adding another view beside them.

### 2. The 25/6/19 split, counted

`CommandContext` declares 25 fields (`command.rs:1260-1377`). Every `ctx.<field>` and
`ctx.<method>` occurrence in `frogdb-commands`, tallied:

```
515 ctx.store            51 ctx.notify_event      15 ctx.get_or_create
 24 ctx.protocol_version 22 ctx.effects           21 ctx.num_shards
  6 ctx.json_limits       5 ctx.command_registry   3 ctx.record_lookup
                                                   2 ctx.rewrite_propagation
```

Six distinct fields (`store`, `protocol_version`, `effects`, `num_shards`, `json_limits`,
`command_registry`) and four inherent methods. Nothing else. I checked the obvious escape
hatches: alternate bindings (`context.` / `c.` tallies show only `c.effects` ×9 and
`c.store` ×4, both `CommandEffects`/store locals), and the three "session" fields —
`shard_senders`, `shard_id`, `conn_id` appear in `frogdb-commands` **only inside the seven
`Box::leak` test helpers** and in `scan.rs:29-37`, where `shard_id` is a local variable in
cursor encoding, not a context field.

**The other 19 have zero reads *of any kind* — production or test — in the crate that owns
296 of the workspace's 346 production `Command` impls:** `shard_senders`, `shard_id`,
`conn_id`, `replication_tracker`, `cluster_state`, `node_id`, `raft`, `network_factory`,
`quorum_checker`, `is_replica`, `is_replica_flag`, `role_controller`, `master_host`,
`master_port`, `master_link_up`, `master_sync_error`, `snapshot_stats`,
`bgsave_in_progress`, `recovery_stats`. (Verified as a single alternation grep for
`ctx\.<field>\b` over `commands/src/` — **zero hits**. The three "session" fields reach the
crate only as *positional arguments* to `CommandContext::new` inside the seven `Box::leak`
helpers, never as reads.)

**Implementor counts, stated precisely.** `impl Command for` appears **371** times
workspace-wide. **346 are production**: 296 in `frogdb-commands` (208 `src/*.rs` + 32
`sorted_set/` + 21 `json/` + 17 `stream/` + 12 `vectorset/` + 6 `event_sourcing/`) and 50 in
`frogdb-server` (49 `server/src/commands/` + 1 `commands/cluster/`). The remaining **25 are
`frogdb-core` test mocks**, all inside `#[cfg(test)]` modules — `registry.rs:281`/`:315`,
`shard/rollback.rs` ×4, `shard/execution.rs` ×4, `shard/post_execution.rs` ×6,
`shard/event_loop.rs` ×3, `shard/eviction.rs` ×2, `shard/panic_guard.rs` ×2,
`shard/persistence.rs`, and the `wal_mock!` macro at `command.rs:1796` (inside the
`#[cfg(test)]` module opening at `:1652`). Use **346** when the sentence is about interface
width in production; use **371** when it is about what a signature change would have to
recompile.

The split is clean rather than accidental: those 19 are exactly the ones the *server*
crate's admin surface reads (`shard_senders` ×22, `node_id` ×17, `raft` ×14, `is_replica`
×10, `cluster_state` ×10, …; 98 reads in total, per the tally in §Risks). Two audiences,
one struct, no boundary between them. **The locality is wrong**: fields that only the
node-management module cares about are declared in the interface every data-structure
command implements against.

### 3. The constructor forces `Box::leak` in every test, for a field no test reads

`CommandContext::new(store, shard_senders, shard_id, num_shards, conn_id, protocol_version)`
(`:1381-1416`) takes two borrows: `&'a mut dyn Store` and `&'a Arc<Vec<ShardSender>>`.
A test that wants a helper returning `CommandContext<'static>` must therefore leak both:

```rust
fn ctx() -> CommandContext<'static> {
    let store = Box::leak(Box::new(HashMapStore::new()));
    let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
    CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
}
```
— `basic.rs:905-907`, and six byte-identical copies. **The second leak exists purely to
satisfy a parameter that no handler in the crate reads** (§Problem 2). The first is a
consequence of the `'static` return, which is itself a consequence of the constructor being
too awkward to inline into each test.

The knock-on: `CommandContext::new` has **14 call sites in the entire workspace**, and
**thirteen of them are test or fuzz scaffolding**:

| Where | Sites |
|---|---|
| `frogdb-commands` `#[cfg(test)]` helpers | `basic:907`, `bloom:683`, `cuckoo:761`, `generic:681`, `hash:2107`, `sort:563`, `string:1577` (7) |
| `frogdb-core` `#[cfg(test)]` helpers | `command.rs:1982`, `scripting/executor.rs:757` (2) |
| `testing/fuzz` targets — **separate `[workspace]`** | `resp_pipeline.rs:50`, `:72`, `restore_payload.rs:82`, `cmd_dispatch.rs:67` (4) |
| **production** | `scripting/gate.rs:472` (1) |

A constructor that is 93% scaffolding, used once for real, in the exact place where the bug
is, is a seam telling you where it broke. **The four fuzz sites are a blast-radius item, not
a footnote**: `testing/fuzz/Cargo.toml` declares its own `[workspace]`, so those files do
not compile under `just check`, `just lint`, or `just test`. A constructor-signature change
that is green on the whole workspace can still leave the fuzz corpus unbuildable, and the
break surfaces only in the nightly fuzz job.

### 4. LIVE: `run_local` drops seven fields, and two of them are user-visible

The Lua bridge does not receive a `CommandContext`. `ScriptInvoker` (`gate.rs:295-348`)
**re-declares 17 of the 25 fields as its own struct fields**; `from_context`
(`:350-383`) copies them across one at a time (`shard_senders: Arc::clone(ctx.shard_senders)`
`:360` … `store: RefCell::new(&mut *ctx.store)` `:381`); then, when `classify` routes a
keyless command to `Plan::Local` (`:220-262`), `run_local` (`:454-506`) builds a **fresh**
`CommandContext` and hand-assigns fields back onto it:

```rust
let mut ctx = CommandContext::new(              // :472-479
    &mut **store, &self.shard_senders, self.shard_id,
    self.num_shards, self.conn_id, self.protocol_version,
);
ctx.is_replica = …;  ctx.is_replica_flag = …;  ctx.master_host = …;   // :484-504
ctx.master_port = …; ctx.master_link_up = …;   ctx.master_sync_error = …;
ctx.replication_tracker = …; ctx.json_limits = …; ctx.snapshot_stats = …;
ctx.bgsave_in_progress = …;  ctx.recovery_stats = …;
handler.execute(&mut ctx, args)                                       // :506
```

Eleven assignments. **Seven fields are never propagated: `cluster_state`, `node_id`,
`raft`, `network_factory`, `quorum_checker`, `command_registry`, `role_controller`** — each
silently `None` inside every script-invoked command.

This is reachable, not theoretical:

- **`redis.call('COMMAND','COUNT')` returns `0`.** `CommandCommand`'s spec is `Standard`
  with `KeySpec::None` (`basic.rs:117-133`) → keyless → `Plan::Local`. It is not on
  `is_forbidden_in_script`'s list (`bindings.rs:10-25`, which covers MULTI/EXEC/DISCARD/
  WATCH/EVAL/EVALSHA/SCRIPT/SUBSCRIBE only) and is not `ServerWide`, so
  `reject_server_wide` (`:441-451`) lets it through. With `ctx.command_registry == None`
  the handler takes its degraded branch: `Response::Integer(0)` at `basic.rs:147-151`,
  empty arrays at `:267` / `:287` / `:303`, degraded `INFO`/`LIST` at `:193` / `:334`.
- **`redis.call('CLUSTER','INFO')` reports standalone on a cluster node.** Same shape —
  spec is `Standard` / `KeySpec::None` (`server/src/commands/cluster/mod.rs:88-107`),
  registered at `server/src/server/register.rs:142`, and `is_forbidden_subcommand`
  (`bindings.rs:28-41`) blocks only `CLUSTER RESET` and `CLUSTER FLUSHSLOTS`. With
  `ctx.cluster_state == None`, `cluster_info` (`:278-280`) falls through to its
  standalone-mode reply.

**This is a known bug class that the tree has already patched three times, one field per
patch, always after a user hit it:**

| Fix | Field added to `run_local` | Evidence |
|---|---|---|
| `FM-REPLICATION-059` | `replication_tracker` | `gate.rs:316` doc; forcing tests `:1095`, `:1142` |
| issue 10 / `FM-PERSISTENCE-022` | `bgsave_in_progress` / `snapshot_stats` | `gate.rs:327-329`, `:501`; `command.rs:1349`; `worker.rs:349` |
| issue 42 / `FM-PERSISTENCE-022` | `recovery_stats` | `gate.rs:336`; `command.rs:1368` |

And the *cross-shard* script path was fixed properly — by routing through the shared
builder instead of hand-assembling. `shard/scripting.rs:217-222` says so in as many words:

> Route through the shared builder so a cross-shard script sub-command sees the same
> cluster + replica identity as any other command on this shard (previously it used the
> bare `new` constructor)

So the tree already knows the correct answer, applied it to the remote path, and left the
local path on the hand-assembled constructor. **The two paths through the same gate now
disagree about what a command's context contains.** Seven fields is the current gap; the
number only ever grows, because every new `CommandContext` field starts life un-propagated
and stays that way until someone files an issue.

---

## Proposed change

**Group the node-wide half into one value; keep the handler interface at the six fields
handlers use; make every value that cannot be safely defaulted a constructor parameter, so
propagation is exhaustive by construction instead of a transcription.**

### The shape

Field types below are the ones the tree actually declares (`command.rs:1260-1377`), not
idealised ones: `shard_id` / `num_shards` are `usize`, `is_replica_flag` is
`Option<Arc<AtomicBool>>`, `snapshot_stats` is a plain `SnapshotStats` value,
`bgsave_in_progress` is a plain `bool`, `recovery_stats` is a non-optional
`Arc<RecoveryStats>`.

```rust
/// The execution environment a data-structure handler never reads: node-wide
/// handles plus the shard/connection coordinates of this particular execution.
/// Owned; `Clone` + `Default` derive cleanly (every member is `Arc`, `Option`,
/// `bool`, `usize`, `String`, or a `Default`-deriving value type).
///
/// NOT cacheable across commands — see §Risks. Eight of these nineteen are
/// re-read per call by `worker.rs:333-382`.
#[derive(Clone, Default)]
pub struct NodeContext {
    // routing / session coordinates
    pub shard_senders: Arc<Vec<ShardSender>>,
    pub shard_id: usize,
    pub conn_id: u64,
    // cluster
    pub cluster_state: Option<Arc<ClusterState>>,
    pub node_id: Option<u64>,
    pub raft: Option<Arc<ClusterRaft>>,
    pub network_factory: Option<Arc<ClusterNetworkFactory>>,
    pub quorum_checker: Option<Arc<dyn QuorumChecker>>,
    // replication / identity
    pub replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    pub role_controller: Option<Arc<dyn RoleController>>,
    pub is_replica: bool,
    pub is_replica_flag: Option<Arc<AtomicBool>>,
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub master_link_up: bool,
    pub master_sync_error: Option<String>,
    // persistence
    pub snapshot_stats: SnapshotStats,
    pub bgsave_in_progress: bool,
    pub recovery_stats: Arc<RecoveryStats>,
}

pub struct CommandContext<'a> {
    pub store: &'a mut dyn Store,          // 515 reads in frogdb-commands
    pub effects: CommandEffects,           //  22   — the only Default-safe field
    pub protocol_version: ProtocolVersion, //  24
    pub num_shards: usize,                 //  21
    pub json_limits: JsonLimits,           //   6
    pub command_registry: Option<&'a Arc<CommandRegistry>>, // 5 — stays BORROWED
    pub node: NodeContext,                 // everything else, as one value
}

impl<'a> CommandContext<'a> {
    /// Every value that cannot be safely defaulted is a parameter. `effects` is
    /// the sole `Default::default()`, and it is genuinely empty-at-start.
    pub fn new(
        store: &'a mut dyn Store,
        protocol_version: ProtocolVersion,
        num_shards: usize,
        json_limits: JsonLimits,
        command_registry: Option<&'a Arc<CommandRegistry>>,
        node: NodeContext,
    ) -> Self { … }
}
```

Six of the seven fields are constructor parameters, so `CommandContext` has **no partial
constructor at all** and no post-construction assignment is needed anywhere. That is the
mechanism; four properties follow.

**(a) `frogdb-commands` does not change.** The six fields handlers read stay spelled
`ctx.store`, `ctx.num_shards`, … exactly as today, **with the same types** — in particular
`command_registry` stays `Option<&'a Arc<CommandRegistry>>` rather than becoming owned,
because owning it would break all five read sites (`basic.rs:147`/`:267`/`:303`/`:334` bind
by value in `if let Some(registry) = ctx.command_registry`, and `:193` does
`ctx.command_registry.and_then(…)`; both move out of a borrow of `ctx` once the field is
`Option<Arc<_>>`, forcing `.as_ref()` / `.clone()`). 515 + 24 + 22 + 21 + 6 + 5 = **593 read
sites and 296 `Command` impls are untouched, zero lines**. This is deliberate and it is also
the scope-boundary argument against proposal 90 (§Risks).

**(b) The server's admin handlers pay a mechanical rename — and about 30 of them are not
purely mechanical.** `ctx.node_id` → `ctx.node.node_id`, and so on, at **98 sites in
`frogdb-server` plus 35 in `frogdb-core`** (tally in §Risks). ~102 of those are pure path
edits, `sed`-able and reviewable as a diff of field paths. The other **31 change ownership
as well as path**, because the field goes from `Option<&'a Arc<T>>` / `Option<&'a dyn T>` to
`Option<Arc<T>>`: `ctx.cluster_state` ×10, `ctx.raft` ×14, `ctx.replication_tracker` ×4,
`ctx.network_factory` ×2, `ctx.quorum_checker` ×1 in the server crate. Those need
`.as_ref()` / `.clone()` / `.as_deref()` by hand — e.g. `admin.rs:248`
`let cluster_state = ctx.cluster_state.ok_or(CommandError::ClusterDisabled)?` becomes
`ctx.node.cluster_state.as_ref().ok_or(…)?`, which is a move out of a borrow today and a
borrow tomorrow. **A blind `sed` will not compile**; the honest description is "~102
sed-able + 31 hand edits", and the 31 are exactly the ones a reviewer should read.

**(c) Propagation stops being a list.** `ScriptInvoker` drops 16 mirrored field declarations
(`gate.rs:295-348`) and keeps `node: NodeContext` plus the handler-facing values it must
also carry (`num_shards`, `protocol_version`, `json_limits`, `command_registry` as an owned
`Option<Arc<CommandRegistry>>`, cloned from `ctx.command_registry` in `from_context`);
`from_context` becomes one clone plus four scalar copies; **`run_local`'s eleven-line
assignment block (`:484-504`) disappears entirely** and is replaced by passing the values to
the total constructor:

```rust
let mut ctx = CommandContext::new(
    &mut **store,
    self.protocol_version,
    self.num_shards,
    self.json_limits,
    self.command_registry.as_ref(),
    self.node.clone(),
);
```

**The seven-dropped-fields defect stops being a bug you fix and becomes a state you cannot
express**: there is no post-construction assignment list to be incomplete, and every
non-defaultable value is a parameter the compiler demands. Note what rev 1 got wrong here
and what this fixes: `command_registry` is *both* one of the six handler-facing fields *and*
one of the seven `run_local` drops *and* the direct cause of
`redis.call('COMMAND','COUNT') → 0`. A design that moved only the 19 into `NodeContext` and
left `ctx.node = self.node.clone()` as the fix would have left `command_registry`,
`num_shards` and `json_limits` as loose assignments — **the flagship symptom would have
survived the refactor**. Making them parameters is what closes it.

**(d) The test constructor loses one leak, and gains explicitness.** `shard_senders` moves
into `NodeContext`, so the parameter that forces the second `Box::leak` disappears —
**7 of the 14 `Box::leak` calls in `frogdb-commands` die immediately**. The trade is honest
and worth stating: the constructor goes from 6 parameters to 6 parameters, but they are
*different* parameters, and a test helper now reads

```rust
CommandContext::new(store, ProtocolVersion::Resp2, 1,
                    JsonLimits::default(), None, NodeContext::default())
```

which is more verbose than today's `CommandContext::new(store, shard_senders, 0, 1, 0, Resp2)`
— and is *supposed* to be, because each default is now written down rather than assumed. If
that verbosity annoys, the follow-up is a `#[cfg(test)]`-free `CommandContext::for_test(store)`
in `frogdb-core` behind a `test-util` feature; I do **not** propose it here, because a
convenience constructor with silent defaults is how this seam got into trouble in the first
place, and because the four fuzz targets live outside the workspace and could not use a
`#[cfg(test)]` one anyway.

### Why an owned `NodeContext` is feasible

`ShardCluster` (`shard/types.rs:582-589`, constructor `:593-608`) **already stores exactly
these handles as owned `Option<Arc<…>>`** — `raft`, `cluster_state`, `node_id`,
`network_factory`, `quorum_checker`, `replication_tracker` — with accessors at
`:612-638` that *narrow* them to `&Arc` / `&dyn`
purely to satisfy `CommandContext`'s borrowed fields. Only 6 of the 19 `CommandContext`
fields are borrows today (`shard_senders`, `replication_tracker`, `cluster_state`, `raft`,
`network_factory`, `quorum_checker`); the other 13 are already owned or `Copy`. So the owned
`NodeContext` is not a new ownership model — it is the model `ShardCluster` already uses,
with the borrow-narrowing step removed.

**What it is not:** a value that can be built once and reused. `worker.rs:333-382` must
rebuild it per command; see §Risks, which withdraws rev 1's shard-lifetime recommendation
and explains exactly which fields forbid it.

### Vocabulary

- **Interface**: `Command::execute(&mut CommandContext, &[Bytes])` (`command.rs:720`) is
  the workspace's single widest interface — **346 production implementors** (371 counting
  `frogdb-core`'s 25 `#[cfg(test)]` mocks). Its parameter currently publishes 25 fields to
  implementors that need 6. Narrowing the *parameter* narrows the *interface*, and does so
  without touching the signature (§Risks, boundary with 80).
- **Module / locality**: cluster and replication handles belong to the node-management
  module, not to the data-structure command module. Today they are declared in the type
  that every data-structure command programs against.
- **Seam**: there are three places where a context crosses into a handler
  (`worker.rs:355`, `gate.rs:472`, `shard/scripting.rs:222`). Two go through the shared
  builder; one transcribes fields. Grouping the wide half *and* making the constructor total
  makes all three exhaustive by construction — the compiler, not a reviewer, checks that
  none of them omits anything.
- **Adapter**: `ScriptInvoker` is an adapter that currently re-declares its adaptee's
  fields instead of holding it. Holding one `NodeContext` plus the four handler-facing
  values, and feeding them to a *total* constructor, is the whole fix.
- **Deletion test**: applied twice. To the *views layer* — deleting it costs one call site
  (§Problem 1), so it is not carrying weight and should go. To the *proposal* — if the
  narrowing is deleted, `gate.rs` keeps a hand-maintained **18-item** list (25 fields − 6
  constructor arguments − `effects`) that has been wrong four times, and the absence of a
  total constructor is what makes the fifth time possible.

---

## Testability improvement

1. **7 `Box::leak`s die on the constructor change alone** (one per helper, the
   `shard_senders` leak) — a certain, compiler-forced result. The remaining 7 are the
   `store` leak, which exists only because the helper returns `CommandContext<'static>`;
   they die when the helpers are inlined into their tests, which this proposal does **not**
   claim as a consequence. Rev 1 claimed a two-argument constructor made inlining obvious;
   with the total six-argument constructor it is a judgement call, so the second 7 are
   listed as an **optional follow-up**, not a benefit of this change.
2. **A default-constructible `NodeContext` makes "run a handler in isolation" a two-line
   setup.** Today, testing a handler that reads *any* of the 19 fields means spelling a
   25-field struct literal by hand; that is why `command.rs:1982` and `executor.rs:757`
   exist as bespoke helpers. With `NodeContext::default()` plus targeted overrides, a test
   for e.g. `INFO replication` under a replica role is
   `NodeContext { is_replica: true, ..Default::default() }` passed to the constructor — an
   override written at construction, not assigned afterwards.
3. **The `gate.rs` propagation becomes assertable in one test instead of one per field.**
   Today's `FM-REPLICATION-059` tests (`gate.rs:1095`, `:1142`) each pin exactly one field,
   which is why three fields got three separate regression tests and seven fields got none.
   With a single value, `assert!(ctx_in_script.node.same_handles_as(&invoker.node))` covers
   all 19 at once.

   **`#[derive(PartialEq)]` will not compile, and rev 1 was wrong to claim it would.**
   `Arc<T>: PartialEq` requires `T: PartialEq` — it compares *values* through `Deref`, not
   pointers. Four independent blockers: `Arc<dyn QuorumChecker>` and `Arc<dyn RoleController>`
   are bare `Send + Sync` traits (`command.rs:317`, `:255`) and can never be `PartialEq`;
   `RecoveryStats` (`persistence/src/recovery.rs:21`) and `ClusterState`
   (`cluster/src/state.rs:23`) derive only `Debug, Clone, Default`; `Arc<AtomicBool>` has no
   `PartialEq` either. (`SnapshotStats` does derive `PartialEq, Eq` —
   `persistence/src/snapshot/mod.rs:75` — and is a plain value field, so it is the one
   member that compares by value for free.)

   The replacement is a hand-written, `#[cfg(test)]`-optional
   `fn same_handles_as(&self, other: &Self) -> bool` on `NodeContext`: `Arc::ptr_eq` for the
   handle fields, `==` for the scalars and `SnapshotStats`. This is the right semantics
   anyway — the assertion we want is "the script's context points at the *same*
   coordinator/tracker/state", not "at an equal one".

   **This partially undercuts the "proof against future fields" claim, and I am not going to
   pretend otherwise.** A hand-written comparison has to gain an arm when a field is added,
   exactly like today's per-field tests. What survives is the important half: *propagation*
   is exhaustive by construction (a struct literal / total constructor cannot omit a field),
   so a new field cannot be silently dropped even if the test is not updated. The comparison
   helper is a second-line check, not the mechanism. A cheap belt-and-braces addition:
   `#[deny(non_exhaustive_omitted_patterns)]`-style discipline is unavailable for structs,
   so instead destructure exhaustively inside `same_handles_as`
   (`let Self { shard_senders, shard_id, … } = self;`) — then adding a field to
   `NodeContext` **fails to compile** until the helper is extended.

   `Clone` and `Default` do derive cleanly: `Arc<dyn Trait>: Clone` always,
   `Arc<Vec<T>>: Default` via `Vec::default`, `Arc<RecoveryStats>: Default` because
   `RecoveryStats: Default`, `SnapshotStats: Default`.
4. **Optional chokepoint gate.** `lint-script-gate` (`Justfile:1080-1107`) is an existing
   compile-free grep gate over `crates/core/src/scripting`. The same pattern supports
   "`CommandContext` is constructed only in `shard/worker.rs`" — i.e. `CommandContext::new`
   and `CommandContext {` outside the builder and `#[cfg(test)]` is a lint failure. That
   turns "the script path must not hand-assemble a context" from a comment
   (`shard/scripting.rs:217-220`) into an enforced invariant. I list this as optional
   because it adds a sixteenth gate to a family the round is already growing.
5. **The three test-only `ScriptInvoker { … }` literals** (at `gate.rs:817`, inside the
   `live_invoker` helper `:809-838`; `:1058`; `:1106`) each spell 19 fields; they collapse to
   `node: NodeContext::default()` plus four handler-facing values plus the field under test.

---

## Risks / scope boundaries vs sibling proposals

### Sibling edges

| Proposal | Overlap | Edge |
|---|---|---|
| **90 — CommandSpec default** | Declares itself the **solo, last** commands-crate writer at `90:526` ("a mechanical sweep should be the round's last commands-crate writer … commit 3 lands after 67/70/80/89 are merged") and `90:580` ("**solo** in the commands crate, **after** 67 / 70 / 80 / 89"). **91 is not named in either list.** | **No conflict, by design.** Property (a) of the design is that `frogdb-commands` handler code changes by **zero lines** — verified as zero `ctx.<node-field>` hits in `commands/src/`. My only footprint in that crate is the 7 test-helper `ctx()` functions (7 files × 3 lines: two `Box::leak` lines and the `new` call), which 90's spec sweep does not touch (it rewrites `static SPEC` blocks and `use` lists). **If the reviewer prefers zero overlap, 91 can drop the test-helper cleanup entirely and land it as a follow-up after 90** — that costs nothing but the leak removal. Ordering preference: 91 before 90 (91's diff is 3 lines/file in 7 files; 90's is crate-wide and wants a clean base), but either order works. |
| **80 — response wire fold** | Explicitly **rejects** changing `Command::execute`'s signature (its §Risks, `:440-470`); touches `commands/src/blocking.rs`, `stream/read.rs`. | **Compatible and mutually reinforcing.** 91 also does not change the signature — it changes what the parameter *contains*. No file overlap. |
| **81 — core dead seams** | Edits `shard/worker.rs` (constructors) and `shard/*` + `server/*`; **does not touch `command.rs`**. | **File-level overlap at `worker.rs`, and the regions are verified disjoint.** 91 rewrites `command_context:333-382`; 81's `worker.rs` edits are `use :24`, field `:117`, the four constructors at `:385-399` / `:403-425` / `:435-459` / `:464-495`, and test `:988` (81:69, 81:145) — **none inside `:333-382`**. `git` may still conflict on hunk context; semantic conflict cannot arise. **Order: 81 first** (it is a deletion; rebasing 91's single-function rewrite onto it is trivial, the reverse is not). Note both proposals move around the same `FM-PERSISTENCE-022` prose citation at `worker.rs:349`, which sits **inside** 91's region and ~36 lines above 81's first edit — 91 owns carrying it. |
| **88 — served-wake effects** | Edits `shard/worker.rs` among 8 files. | Same as 81, and likewise **verified disjoint**: 88's `worker.rs` regions are `SlotVersions :56-96`, field `:175`, `:613`, `:618`, `:632`, `:648-664`, `:902` (88:97) — none inside `:333-382`. **Order: 88 before or after, but not concurrently with 91's `worker.rs` edit.** |
| **89 — probabilistic chunk codec** | `commands/src/bloom.rs`, `cuckoo.rs`. | **Same files, fully disjoint regions — rev 1 overstated this as "direct overlap".** 89 edits `bloom.rs:532-548`, `:593`, `:612-663`, `:657` and `cuckoo.rs:591-611`, `:672-743` (89:355, 89:365); my helpers are at `bloom.rs:681-683` and `cuckoo.rs:759-761`, **below every one of them**. The only real interaction is renumbering: 89 deletes ~124 lines above my helpers, so my line cites shift by that much after it lands. **91 still yields** (drop those two files from the leak cleanup if 89 is in flight) — the yield is harmless and removes even the renumbering question. |
| **67 / 70** | Other `frogdb-commands` files. | No overlap under property (a). |

### Risks

- **`NodeContext` MUST be rebuilt per command. Rev 1's recommended mitigation is withdrawn
  as unsound.** Rev 1 preferred "(i) build the `NodeContext` once per shard and store
  `node: &'a NodeContext`". The *lifetime* argument for (i) is fine — `store` and `node`
  would be disjoint field borrows of `ShardWorker`, and the shard outlives every command.
  **The semantics are not.** `worker.rs:333-382` does not read nineteen stable handles; it
  computes eight of them per call, and caching any of them re-introduces exactly the
  static-placeholder bug class this proposal exists to retire:

  | Field | Why it cannot be cached | Site |
  |---|---|---|
  | `conn_id` | It is a **parameter of `command_context`**, different for every connection. A shard-lifetime struct cannot hold it at all. | `worker.rs:335` |
  | `node_id` | Re-derived per call as `cluster_state().and_then(self_node_id).or(cluster.node_id())` — the comment says why: the dynamic id is **updated by `CLUSTER RESET HARD`** and must win over the static one captured at connection creation. | `:340-344` |
  | `is_replica` | Live read of `identity.is_replica()`; flips on `REPLICAOF` / promotion mid-shard-lifetime. | `:345` |
  | `snapshot_stats`, `bgsave_in_progress` | Live coordinator reads, **added by issue 10 / FM-PERSISTENCE-022 precisely because a stale value rendered a static `rdb_last_bgsave_status:ok`**. The in-tree comment at `:346-349` and the field doc at `command.rs:1345-1351` both say this in as many words. Caching them reverts that fix. | `:350-352` |
  | `master_host`, `master_port`, `master_link_up`, `master_sync_error` | Live `identity` reads; `master_link_up` / `master_sync_error` change on every link transition and are what INFO's `master_link_status` / `master_sync_error` render. | `:372-375` |

  Only `recovery_stats` is documented as a legitimately-once snapshot
  (`command.rs:1358-1371`: "a one-time event rather than evolving state … copied once at
  `ShardWorker` construction"), and `shard_senders` / `shard_id` / the five cluster handles
  are stable. That is 11 cacheable, 8 not — so **(i) is off the table**, and with it the
  "single pointer copy per command" performance story.

  **Recommended: (iii) owned + clone per command**, i.e. rebuild `NodeContext` inside
  `command_context` exactly as the struct literal is built today, with (ii) `Arc<NodeContext>`
  available only if a *future* refactor first makes the eight live fields lazy. (ii) is not
  a drop-in today: an `Arc<NodeContext>` still has to be **allocated** per command to carry
  the live values, which is strictly worse than cloning refcounts. `conn_id`'s
  per-call-ness, incidentally, is the cleanest single proof that this value is
  command-scoped, and it is why `conn_id` stays *inside* `NodeContext` rather than being
  hoisted to a seventh inline `CommandContext` field: there is no shard-lifetime variant
  for it to obstruct. (If a reviewer finds `conn_id` under a type named `NodeContext`
  misleading, the alternative is a 7th inline field and a 7th constructor parameter; the
  type's doc comment names it as "node-wide handles **plus the shard/connection
  coordinates of this execution**" to keep the name honest.)

- **The Arc-churn measurement is a GATE, not a contingency.** Under (iii) each command pays
  roughly **6 additional `Arc` clone/drop pairs** — `shard_senders`, `cluster_state`, `raft`,
  `network_factory`, `quorum_checker`, `replication_tracker` move from `&Arc` / `&dyn` to
  owned `Arc`. That is *on top of* what `command_context` already pays today:
  `is_replica_flag` (`Arc` clone, `:370`), `role_controller` (`:371`), `recovery_stats`
  (`:353`) = 3 `Arc` clones, plus `master_host` / `master_sync_error` (2 `String`
  allocations, `:372`/`:375`) and a `SnapshotStats` clone (`:351`). So the change roughly
  **triples** the refcount traffic on a path that runs once per command.

  **This must be measured before the refactor lands, and a regression is a blocker, not a
  note.** Concretely: run the standard `just bench` command-dispatch benchmarks (and a
  memtier smoke) against a branch that only changes the six fields from borrowed to owned
  — a ~30-line change, no restructuring — and compare. If p99 dispatch latency or
  throughput regresses beyond noise, the proposal must fall back to keeping the six cluster
  handles *borrowed* inside `NodeContext<'a>` (a lifetime-parameterised `NodeContext`, which
  costs the script path — `ScriptInvoker` owns its copies precisely because it outlives the
  borrow — and would mean `ScriptInvoker` keeps an owned mirror struct after all). **That
  fallback loses most of the proposal's value**, so the measurement decides whether 91's
  refactor half is worth doing at all. It does not affect the three hotfixes, which are
  independent and carry the user-visible fix.
- **Field-grouping judgment call.** I keep `num_shards`, `json_limits` and
  `command_registry` **inline** on `CommandContext` even though they are node-scoped,
  because handlers read them (21 / 6 / 5) and moving them would put ~32 field-path edits
  into `frogdb-commands` — precisely the crate 90 wants solo. Rev 1 left them inline *and*
  defaulted them in the constructor, which was the flaw (§Proposed change (c)); rev 2 keeps
  them inline **and makes them constructor parameters**, which gets the compiler-forcing
  without the commands-crate churn. The stricter split (all node-scoped fields in
  `NodeContext`) is cleaner on paper and costs ~32 edits in the crate 90 wants solo; it
  should be a **follow-up after 90 lands**, or never. Stated here so a reviewer does not
  read the inline three as an oversight.
- **`FM-` prose citations travel.** Four sit in regions I move — `gate.rs:316`
  (FM-REPLICATION-059), `gate.rs:329` and `:501` (FM-PERSISTENCE-022 / issue 10),
  `gate.rs:336` (issue 42) — plus `command.rs:1349`, `:1368` and `worker.rs:349`. **None
  is an `FM-` *tag* on a test**, so `just lint-failure-modes` is not affected; the two
  tagged tests (`gate.rs:1095`, `:1142`) keep their tags and must keep passing unchanged.
  The doc comments must be **carried onto the corresponding `NodeContext` fields**, not
  dropped — they are the institutional memory of the three prior fixes, and the whole
  argument for this proposal is written in them.
- **No locked crate is edited.** `frogdb-core`, `frogdb-commands` and `frogdb-server` are
  outside the four locked areas (txn / persistence / replication / cluster crates). The
  *behavior* touched by hotfix H3 is script-visible replication and cluster reporting,
  which is why H3 wants regression tests in `gate.rs` next to the existing FM-tagged ones.
- **`CommandError::ReplicationDisabled` becomes unreachable** once `require_replication`
  is deleted (it is already unreachable in practice — the only producer is the dead
  accessor). Recommend **leaving the variant** in `types/src/error.rs:150`: it is one line
  of Redis error vocabulary, and deleting it is a separate wire-surface question.
- **Blast-radius honesty.** 346 production `Command` impls (371 with `frogdb-core`'s test
  mocks), 593 handler read sites, 1 production construction site, **14 `CommandContext::new`
  sites — 4 of them in `testing/fuzz`, a crate with its own `[workspace]` that `just check`,
  `just lint` and `just test` never compile.** Any constructor change must be applied to
  those four by hand and verified with a fuzz build; a green workspace does not prove them.
  The field-path churn, tallied per field:

  | Field | `frogdb-server` | `frogdb-core` | Ownership change? |
  |---|---:|---:|---|
  | `shard_senders` | 22 | 1 | yes (`&Arc` → `Arc`) |
  | `node_id` | 17 | 0 | no |
  | `raft` | 14 | 0 | **yes** |
  | `is_replica` | 10 | 5 | no |
  | `cluster_state` | 10 | 0 | **yes** |
  | `replication_tracker` | 4 | 3 | **yes** |
  | `recovery_stats` | 4 | 2 | no |
  | `role_controller` | 3 | 0 | no |
  | `is_replica_flag` | 3 | 3 | no |
  | `network_factory` | 2 | 0 | **yes** |
  | `master_port` | 2 | 4 | no |
  | `master_host` | 2 | 4 | no |
  | `master_link_up` | 1 | 4 | no |
  | `master_sync_error` | 1 | 2 | no |
  | `snapshot_stats` | 1 | 2 | no |
  | `bgsave_in_progress` | 1 | 2 | no |
  | `quorum_checker` | 1 | 0 | **yes** (`&dyn` → `Arc<dyn>`) |
  | `conn_id` | 0 | 2 | no |
  | `shard_id` | 0 | 1 | no |
  | **total** | **98** | **35** | **31 of 133** |

  `frogdb-commands`: **0**. The design deliberately routes the churn to these 133 sites and
  away from the 296-impl crate. Rev 1 called all of it "~100 sed-able renames"; that was
  wrong twice — the count is ~133, and 31 of them are hand edits (§Proposed change (b)). If
  a reviewer rejects the asymmetry, the alternative is a genuinely large sweep and the
  proposal should be re-costed as L.

---

## Effort

**M, at the top of the band** for the narrowing: one new struct, one rewritten builder
(`worker.rs:333-382`), one rewritten adapter (`gate.rs:295-506`, which *shrinks*), **~102
mechanical + 31 hand field-path edits** across `frogdb-server` (98) and `frogdb-core` (35),
7 test-helper edits in `frogdb-commands`, 4 hand edits in `testing/fuzz` (outside the
workspace), and a hand-written `same_handles_as` on `NodeContext`. No signature change to
`Command::execute`, no new dependency, no locked crate. The optional construction gate adds
**S** on top. **Gated on the Arc-churn benchmark** (§Risks): a measured dispatch regression
sends the refactor half back for redesign, without affecting H1/H2/H3.

### Independently landable hotfixes

Three, in recommended landing order. Each is a standalone commit that stands without the
refactor, and each shrinks the refactor's diff.

**H3 — `run_local` drops seven fields (S, LIVE, land first).** Add the seven missing
assignments to the block at `gate.rs:484-504`: `cluster_state`, `node_id`, `raft`,
`network_factory`, `quorum_checker`, `command_registry`, `role_controller` — the fields must
also be added to `ScriptInvoker` (`:295-348`) and `from_context` (`:350-383`), and the three
test-only literals at `:817` / `:1058` / `:1106` updated, which is why this is S and not XS.

The one non-obvious mechanic: `ScriptInvoker` holds `registry: &'a CommandRegistry` (a bare
reference, passed separately at `executor.rs:328` / `:524`), which cannot fill
`CommandContext.command_registry: Option<&'a Arc<CommandRegistry>>`. The propagation must
come from the *outer* context instead — `command_registry: ctx.command_registry.cloned()`
in `from_context`, stored as `Option<Arc<CommandRegistry>>`, then `.as_ref()` at the
`run_local` assignment. That works because the EVAL entry context is built by
`worker.rs:333-382`, which sets `command_registry: Some(&self.registry)` (`:367`). Same
pattern for `cluster_state` / `raft` / `network_factory` / `replication_tracker`
(`.cloned()`) and `quorum_checker` (needs an `Arc<dyn QuorumChecker>`, so this one field
must be sourced as an owned `Arc` — `ShardCluster` already holds it that way at
`types.rs:587`, and `ShardCluster::quorum_checker()` at `:632` is the accessor that narrows
it to `&dyn`; either widen that accessor or add an `Arc`-returning sibling).

Ships the user-visible fix (`redis.call('COMMAND','COUNT')` → real count;
`redis.call('CLUSTER','INFO')` → real cluster info) **now**, with regression tests beside
the existing FM-tagged ones at `gate.rs:1095`/`:1142`. **State plainly in the commit
message that this is the fourth one-field-at-a-time patch to the same list** — H3 is the
symptom fix; the narrowing is what stops the fifth. If only one thing from this proposal
lands, it should be H3.

Arithmetic check for a reviewer: `CommandContext` has 25 fields; `run_local` passes 6 to
`new` (`store`, `shard_senders`, `shard_id`, `num_shards`, `conn_id`, `protocol_version`),
assigns 11 (`:484-504`), and leaves `effects` at its genuine default. 6 + 11 + 1 = 18, so
**exactly 7 are unaccounted for** — the seven named above. There is no eighth.

**H1 — delete `CommandContextCore` (S, zero-behavior).** `command.rs:1035-1088` (54 lines)
+ `as_core` `:1517-1546` (30 lines) + the `lib.rs:72` re-export + reword the dangling doc at
`store/typed.rs:107`. ≈85 lines, 4 files, **zero call sites**, compiles-or-it-doesn't. The
brief's "pure delete, S effort" is **confirmed** — with the caveat that `typed.rs:107` must
be reworded rather than left pointing at a deleted type.

**H2 — delete the second dead tranche (S, zero-behavior).** `ReplicationContextRef`
(`:1029-1033`), `replication_context` (`:1585` in `:1574-1589`), `require_replication`
(`:1623` in `:1612-1626`), `has_replication` (`:1593` in `:1590-1595`), `require_cluster`
(`:1608` in `:1597-1610`), `is_cluster_mode` (`:1513` in `:1511-1515`) + the `lib.rs:74`
re-export (`ReplicationContextRef` in the `pub use command::{…}` block at `:71-76`). ≈60
lines, **zero call sites**. Leave `ClusterContextRef` / `cluster_context` alive for
`version.rs:45`, or inline that one use and delete those too (a further ~38 lines) —
reviewer's call. Leave `CommandError::ReplicationDisabled` in place.

**H2 has one name trap: `is_cluster_mode` is not a unique identifier.** The name occurs on
at least six unrelated types — `ConnCtx`-style connection deps (`server/src/connection/
deps.rs:164`, called at `hotkeys.rs:239`, `:326`), the `ConnectionCommand` trait
(`core/src/conn_command.rs:69`, impl `:994`), `ShardCluster`
(`core/src/shard/types.rs:612`, called from `shard/scripting.rs:27`/`:57`,
`shard/functions.rs:55`), `ShardWorker` (`types.rs:728`), the hotkeys trait impl
(`hotkeys.rs:463`), and a plain local/parameter binding in `scripting/executor.rs` (`:111`,
`:156`, `:182`, `:231`, `:261`, and 8 more). **Only `command.rs:1513` is the
`CommandContext` method.** Delete by line, not by `sed 's/is_cluster_mode//'`; a name-based
sweep breaks the scripting executor and the connection layer.

H1 and H2 together remove **145 lines** of the failed additive-narrowing layer — the
zero-caller subset of the 183-line views layer (§Problem 1) — which is both free value and
the clearest possible statement of why the narrowing must proceed by subtraction.

### Final hotfix list and landing order

| # | What | Effort | Depends on | Lands |
|---|---|---|---|---|
| **H3** | `run_local` propagates all 25 fields | S | nothing | **first** — LIVE user-visible fix |
| **H1** | delete `CommandContextCore` + `as_core` + `lib.rs:72` + reword `typed.rs:107` | S | nothing | any time |
| **H2** | delete the 6 zero-caller view items + `lib.rs:74` | S | nothing (name-careful, see above) | any time |
| — | the narrowing (`NodeContext` + total constructor) | M | Arc-churn benchmark; after 81 and 88 touch `worker.rs`; before or after 90 | last |

All three hotfixes are shippable immediately at the line numbers above; none depends on the
refactor, and each shrinks the refactor's diff.

---

## Review dispositions

Rev 1 was reviewed adversarially; verdict **AMEND**. Every finding was independently
re-verified against the tree at `dd840ca3` before being applied. **All twelve stand; none is
refuted.** Two are recorded with a refinement rather than a straight application, marked ✎.

### Design flaws (all three real, all three applied)

| # | Finding | Verified how | Disposition |
|---|---|---|---|
| **F1** | Claim (c) "19 assignments → 1 move" fails for the flagship symptom: `command_registry` is simultaneously one of the six kept-inline fields, one of the seven `run_local` drops, and the direct cause of `COMMAND COUNT → 0`. Post-refactor `run_local` still had ~4 loose assignments, and `new(store, protocol_version)` left `num_shards` — read at 21 handler sites — with no compiler-forced value. | Read `command.rs:1260-1377`, `gate.rs:454-506`, `basic.rs:147-334`; `grep -c "ctx\.num_shards" commands/src` = 21. The flaw is exact: rev 1's design would have shipped the refactor with the flagship bug intact. | **Applied — adopted the reviewer's suggested fix.** `CommandContext::new` becomes total: `new(store, protocol_version, num_shards, json_limits, command_registry, node)`. Claim (c) reframed from "19 → 1 assignment" to **"exhaustive by construction"**, which is both true and a stronger property. **Design choice justified below.** |
| **F2** | Recommended mitigation (i) — per-shard `&'a NodeContext` — is unsound: 8 of the 19 fields are computed per call at `worker.rs:333-382`, and caching `snapshot_stats`/`bgsave_in_progress` re-introduces the exact static-placeholder bug issue 10 / FM-PERSISTENCE-022 fixed. | Read `worker.rs:333-382` line by line. Every claim checks out: `conn_id` is the fn parameter (`:335`); `node_id` re-derived `:340-344` with an in-code comment naming HARD reset; `is_replica` `:345`; coordinator reads `:350-352` under the FM comment at `:346-349`; `master_*` `:372-375`. Also confirmed `recovery_stats` (`:353`) is the one documented once-only snapshot (`command.rs:1358-1371`). | **Applied.** §Risks rewritten: (i) **withdrawn**, per-command rebuild (iii) recommended, (ii) demoted to "not a drop-in — an `Arc<NodeContext>` still allocates per command". Added a table naming the 8 must-stay-live fields with their sites. **The Arc-churn measurement is now a GATE** with a stated fallback and a stated consequence (the fallback loses most of the value). |
| **F3** | `#[derive(PartialEq)]` on `NodeContext` will not compile. | `Arc<T>: PartialEq` needs `T: PartialEq`. Confirmed `QuorumChecker` (`command.rs:317`) and `RoleController` (`:255`) are bare `Send + Sync`; `RecoveryStats` (`persistence/src/recovery.rs:21`) and `ClusterState` (`cluster/src/state.rs:23`) derive only `Debug, Clone, Default`; `Arc<AtomicBool>` likewise. | **Applied ✎.** Replaced with a hand-written `same_handles_as` using `Arc::ptr_eq`, and the "future-field-proof" claim explicitly walked back. **Refinement:** the helper destructures `Self` exhaustively, so adding a `NodeContext` field breaks its compilation — recovering most of the future-proofing the derive was claimed to give. Also verified and stated that `Clone` + `Default` **do** derive cleanly (the reviewer's parenthetical, confirmed: `SnapshotStats` derives `PartialEq, Eq` at `persistence/src/snapshot/mod.rs:75`). |

### Corrections (all nine verified and applied)

| # | Finding | Verified | Disposition |
|---|---|---|---|
| **C4** | "286" → 296; 346 implementors is production-only, 371 total incl. 25 core test mocks; `CommandContext::new` 10 → 14 sites, +4 fuzz targets in a separate `[workspace]`. | `impl Command for` = 371 total / 296 commands / 50 server / 25 core (all under `#[cfg(test)]`, incl. the `wal_mock!` macro at `command.rs:1796` inside the module opening at `:1652`). `CommandContext::new` = 14 sites; `testing/fuzz/Cargo.toml:10-11` declares `[workspace]`. | Applied in title, Summary, §Problem 2, §Problem 3 (new table), §Vocabulary, blast radius, Files table (new fuzz row). |
| **C5** | Six sketch field types wrong. | All six confirmed against `command.rs:1268/1271/1310/1352/1357/1371`: `shard_id: usize`, `num_shards: usize`, `is_replica_flag: Option<Arc<AtomicBool>>`, `snapshot_stats: SnapshotStats`, `bgsave_in_progress: bool`, `recovery_stats: Arc<RecoveryStats>`. | Applied — sketch rewritten with real types plus a note that they are the tree's, not idealised. |
| **C6** | Property (a) "zero lines" false if `command_registry` becomes owned: 5 read sites break. | Confirmed at `basic.rs:147`, `:193`, `:267`, `:303`, `:334`. | **Applied — kept it borrowed**, which preserves "zero lines" exactly and interacts correctly with the F1 fix (it is passed to the total constructor as `Option<&'a Arc<_>>`). |
| **C7** | "~100 sed-able renames" understates: ~30 change ownership. | Counted: `cluster_state` 10 + `raft` 14 + `replication_tracker` 4 + `network_factory` 2 + `quorum_checker` 1 = **31** in the server crate. `admin.rs:248` verified as the named example (a move out of a borrow today). | Applied — property (b) rewritten as "~102 sed-able + 31 hand edits", with `admin.rs:248` shown. |
| **C8** | Nine numeric/line fixes. | Each re-checked: `admin.rs` **18** `ClusterDisabled` and **10** `ok_or` direct field reads (plus 10 further direct reads = 20 total); `ctx.cluster_state` server-side **10**; views table sums **183**, of which 145 = H1+H2; `reject_server_wide` **:441-451**; `classify` ends **:262**; `run_local` block **:484-504**; all 7 `Box::leak` pairs **one line lower** than rev 1 said; `ScriptInvoker` literals at **:817 / :1058 / :1106**; 90's solo declaration at **90:526 / :580** (rev 1 cited 90:344-401, a different section); 89's regions **fully disjoint** from my two helpers; 81's and 88's `worker.rs` regions **verified disjoint** from `:333-382`. | All applied. |
| **C9** | `is_cluster_mode` delete must be name-careful. | Confirmed 30+ same-name hits on `ConnCtx`-style deps (`deps.rs:164`), `ConnectionCommand` (`conn_command.rs:69`), `ShardCluster` (`types.rs:612`), `ShardWorker` (`:728`), `hotkeys.rs:463`, and 14 local bindings in `scripting/executor.rs`. | Applied — explicit trap note in H2. |

### The F1 design choice, and why

Two options were on the table.

**Option A (chosen)** — make the non-`Default`-safe inline fields constructor parameters:
`new(store, protocol_version, num_shards, json_limits, command_registry, node)`.

**Option B** — move `num_shards`, `json_limits`, `command_registry` into `NodeContext` too,
leaving a 3-field `CommandContext`.

Option A wins on four counts.

1. **It preserves property (a) exactly.** Option B puts ~32 field-path edits into
   `frogdb-commands` (21 `num_shards` + 6 `json_limits` + 5 `command_registry`), the one
   crate proposal 90 declares itself the **solo, last** writer of (`90:526`, `90:580`).
   Rev 1's central scope argument was "91 does not write handler code"; Option B discards it
   and creates a real ordering conflict where none exists.
2. **It fixes the flagship symptom either way, but A does it without the conflict.** Both
   options make `command_registry` compiler-forced at `run_local`. A gets there for free.
3. **The mechanism generalises; B's does not.** The defect class is *post-construction
   assignment of fields the constructor silently defaulted*. Option B shrinks the number of
   such fields to zero *for the fields it moves*, but leaves the pattern legal for any field
   added later to `CommandContext`. Option A removes the pattern: after it, `effects` is the
   only `Default::default()` in the constructor, and it is genuinely empty-at-start. A new
   field added to `CommandContext` must be added to `new`'s signature, which breaks all 14
   call sites — loudly, which is the point.
4. **B is not blocked, only deferred.** With A landed, moving the three into `NodeContext`
   later is a pure field-path sweep in `frogdb-commands` with no semantic content — exactly
   the kind of change that should follow 90 rather than fight it.

The cost of A is honestly stated in §Proposed change (d): the constructor is more verbose at
the 13 scaffolding call sites. That is the correct direction of travel for a seam whose
entire failure history is silent defaults.
