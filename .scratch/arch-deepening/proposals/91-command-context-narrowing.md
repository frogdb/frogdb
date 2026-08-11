# Proposal 91 — `CommandContext` is 25 fields wide, 286 handlers read 6 of them, and the wide half is copied field-by-field at three sites — the third one drops seven

Round 38 · lane: commands + core types · candidate **CT4** · effort **M** (the narrowing)
+ **S** ×3 (three independently-landable hotfixes, one of them a LIVE user-visible bug) ·
**no locked crate edited** (`frogdb-core`, `frogdb-commands`, `frogdb-server`) · **zero
`FM-` tags in any edited region** — but four `FM-` *prose citations* sit in the regions I
move, and §Risks says how they travel.

**Verified at HEAD `8a17065247c8935e6476fbffc92a5e94a1b20854`.** Re-verified from
`175a997d`: `git diff --stat 175a997d..8a170652` touches **one file**,
`.scratch/arch-deepening/proposals/88-served-wake-effects.md` — no code file moved, so
every `file:line` below is exact at HEAD. Dirty in the shared tree right now: proposal 89
(modified) and proposal 92 (untracked). **No code file is dirty**; concurrent authors are
in `.scratch/` only.

---

## Corrections to the lane brief

The brief was directionally right and numerically loose. Four claims are adjusted, one is
confirmed exactly, and **one adjustment changes the proposal's status from "latent" to
LIVE**.

| Brief claim | Verified at HEAD |
|---|---|
| "`CommandContextCore` + `as_core()` are dead (`command.rs:1057-1088`, `:1537`); only other ref is a `lib.rs` re-export — pure delete" | **Confirmed, with one extra reference the brief missed.** Struct + impl = `:1035-1088` (banner `:1035-1037`, doc `:1039-1056`, `pub struct` `:1057-1075`, `impl` `:1077-1088`). `as_core` = `:1517-1546` (doc `:1517-1535`, fn `:1536-1546`). Re-export `lib.rs:72`. **Fourth site: `store/typed.rs:107`**, a doc comment reading ``/// store is used as `&mut dyn Store` (see `CommandContextCore::store`), and`` — a dangling intra-doc reference the moment the type dies. Total ≈ **85 lines**, 4 files. |
| "`CommandContext` has ~25 fields; `frogdb-commands` uses 6; the other ~18 have 0 uses" | **Adjusted to exact: 25 / 6 / 19.** Field list is `command.rs:1260-1377`; I counted the declarations, not the doc lines. The used 6 are `store`, `protocol_version`, `effects`, `num_shards`, `json_limits`, `command_registry`. Per-field tally in §Problem 2. The brief said 18 because 6+18=24 ≠ 25. |
| "29/40 command files have zero tests" | **Adjusted: 296 `impl Command for` blocks live in `frogdb-commands`; only 7 files in the crate construct a `CommandContext` at all.** The file-count framing does not survive — the crate is not 40 files and the handlers are not one-per-file. The load-bearing number is different and worse: **`CommandContext::new` appears at exactly 10 call sites workspace-wide, 9 of them `#[cfg(test)]` helpers and 1 of them production** (§Problem 3). |
| "tests need `Box::leak`" | **Confirmed and quantified: 14 `Box::leak` calls in `frogdb-commands`, exactly 2 per test-context helper × 7 helpers** (`basic.rs:906-907`, `bloom.rs:682-683`, `cuckoo.rs:760-761`, `generic.rs:680-681`, `hash.rs:2106-2107`, `sort.rs:562-563`, `string.rs:1576-1577`). All seven are byte-identical apart from the file they sit in. One of the two leaks exists **only** to satisfy a constructor parameter no handler in the crate ever reads. |
| — *(not in the brief)* | **The wide half is dead-or-dying in a second, larger tranche.** `ReplicationContextRef` (`:1030-1033`), `replication_context()` (`:1574-1589`), `require_replication()` (`:1612-1626`), `has_replication()` (`:1590-1595`), `require_cluster()` (`:1597-1610`) and `CommandContext::is_cluster_mode()` (`:1511-1515`) have **zero call sites anywhere in the workspace**. `ClusterContextRef` + `cluster_context()` have **exactly one** (`server/src/commands/version.rs:45`). §Problem 1. |
| — *(not in the brief)* | **LIVE defect, not latent.** `scripting/gate.rs:454-506` (`run_local`) rebuilds a `CommandContext` by hand and propagates **11 of the 18** fields it would need; **7 are silently dropped**. Two of the seven are read by registered, script-callable commands, so `redis.call('COMMAND','COUNT')` returns `0` and `redis.call('CLUSTER','INFO')` reports standalone on a cluster node. §Problem 4. |

---

## Summary

`CommandContext` is a single 25-field value that serves three audiences with almost no
overlap: the **286 data-structure handlers** in `frogdb-commands`, which read 6 fields; the
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
single densest consumer of cluster fields, 12 `ClusterDisabled` sites — reaches past all of
them straight into `ctx.node_id` / `ctx.cluster_state` / `ctx.raft` (`admin.rs:132`, `:248`,
`:394`). Adding a narrow view next to a wide field never narrows anything, because the wide
field is still there and still shorter to type.

**The proposal is to narrow by subtraction instead.** Move the 19 fields no data-structure
handler reads out of `CommandContext` and into one owned, `Clone` + `Default`
`NodeContext`, held as a single field. The handler-facing interface keeps the 6 fields it
actually uses, so **`frogdb-commands` changes by zero lines of handler code**. Propagating
the node-wide half becomes one value move instead of 19 assignments — which makes the
`gate.rs` defect class *structurally unrepresentable*, not merely fixed for the fourth
time. And `CommandContext::new` loses the parameter that forces `Box::leak` in every test.

---

## Files involved

Verified paths, line counts at HEAD `8a170652`.

| File | Lines | Role in this proposal |
|---|---:|---|
| `frogdb-server/crates/core/src/command.rs` | 2014 | **Primary.** `Command` trait `:714`, `execute` `:720`, `CommandContext` `:1260-1377`, `new` `:1381-1416`, the eight view/accessor items `:1011-1088` + `:1511-1626`. |
| `frogdb-server/crates/core/src/lib.rs` | 166 | Re-export list `:72` (`CommandContextCore`), `:74` (`ReplicationContextRef`). |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | The **only production construction site**: `command_context` `:333-382`, 25-field struct literal `:355`. FM prose `:349`. Also edited by 81 and 88 — §Risks. |
| `frogdb-server/crates/core/src/scripting/gate.rs` | 1244 | **The defect site.** `ScriptInvoker` re-declares 17 context fields `:295-348`; `from_context` copies them one by one `:350-383`; `run_local` re-assembles 11 of 18 `:454-506`. FM prose `:316`, `:329`, `:336`, `:501`; FM-tagged tests `:1095`, `:1142`; 3 struct literals in test helpers `:809-838`. |
| `frogdb-server/crates/core/src/shard/scripting.rs` | 261 | The **already-fixed sibling path** — `execute_script_sub_command:196-225` routes through the shared builder, with a comment at `:217-220` saying exactly why. The asymmetry with `run_local` is the argument. |
| `frogdb-server/crates/core/src/shard/types.rs` | 1498 | Feasibility evidence: `ShardCluster:582-607` already holds the same handles as owned `Option<Arc<…>>`; accessors `:612-638`. |
| `frogdb-server/crates/core/src/store/typed.rs` | 584 | Doc reference to `CommandContextCore` at `:107` — must be reworded, not just deleted. |
| `frogdb-server/crates/commands/src/basic.rs` | 1054 | `CommandCommand` `:113`, spec `:117-133`, degraded branches `:147-151`, `:193`, `:267`, `:287`, `:303`, `:334`; test helper `Box::leak` `:906-907`. |
| `frogdb-server/crates/commands/src/{bloom,cuckoo,generic,hash,sort,string}.rs` | — | Six more `Box::leak` context helpers; identical shape. |
| `frogdb-server/crates/server/src/commands/cluster/mod.rs` | 1211 | `CLUSTER` spec `:88-107`, `execute:110`, `cluster_info:278-280` (the standalone fallback the script path wrongly hits). |
| `frogdb-server/crates/server/src/commands/cluster/admin.rs` | — | The evidence that the accessor seam lost: `:132`, `:248`, `:394` read fields directly. |
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

That is roughly **145 lines of interface with one live consumer.** `require_replication`
is doubly dead: it is the *only* producer of `CommandError::ReplicationDisabled`
(`types/src/error.rs:150`), which therefore has no reachable path to the wire.

The interesting part is *why* it is dead. These items were added to let a command say "I
only need the core" or "I require cluster mode". Nothing adopted them, because the wide
fields stayed public next to the views. `admin.rs` — which would be the flagship
`require_cluster()` client, with twelve `ClusterDisabled` returns — instead writes
`ctx.node_id.ok_or(CommandError::ClusterDisabled)?` twelve times (`:132`, `:194`, `:247`,
`:356`, `:395`, `:452`, `:486`, …). **This is the deletion test applied to a narrowing
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

**The other 19 have zero production reads in the crate that owns 296 of the workspace's 346
`Command` impls:** `shard_senders`, `shard_id`, `conn_id`, `replication_tracker`,
`cluster_state`, `node_id`, `raft`, `network_factory`, `quorum_checker`, `is_replica`,
`is_replica_flag`, `role_controller`, `master_host`, `master_port`, `master_link_up`,
`master_sync_error`, `snapshot_stats`, `bgsave_in_progress`, `recovery_stats`.

The split is clean rather than accidental: those 19 are exactly the ones the *server*
crate's admin surface reads (`node_id` ×17, `cluster_state` ×15, `raft` ×14, `is_replica`
×10, …). Two audiences, one struct, no boundary between them. **The locality is wrong**:
fields that only the node-management module cares about are declared in the interface every
data-structure command implements against.

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
— `basic.rs:905-908`, and six byte-identical copies. **The second leak exists purely to
satisfy a parameter that no handler in the crate reads** (§Problem 2). The first is a
consequence of the `'static` return, which is itself a consequence of the constructor being
too awkward to inline into each test.

The knock-on: `CommandContext::new` has **10 call sites in the entire workspace**, and
**nine of them are `#[cfg(test)]`** (`commands/src/{basic:907, bloom:683, cuckoo:761,
generic:681, hash:2107, sort:563, string:1577}.rs`, `core/src/command.rs:1982`,
`core/src/scripting/executor.rs:757`). The tenth is `scripting/gate.rs:472` — and that one
is production. A constructor that is 90% test scaffolding, used once for real, in the exact
place where the bug is, is a seam telling you where it broke.

### 4. LIVE: `run_local` drops seven fields, and two of them are user-visible

The Lua bridge does not receive a `CommandContext`. `ScriptInvoker` (`gate.rs:295-348`)
**re-declares 17 of the 25 fields as its own struct fields**; `from_context`
(`:350-383`) copies them across one at a time (`shard_senders: Arc::clone(ctx.shard_senders)`
`:360` … `store: RefCell::new(&mut *ctx.store)` `:381`); then, when `classify` routes a
keyless command to `Plan::Local` (`:220-261`), `run_local` (`:454-506`) builds a **fresh**
`CommandContext` and hand-assigns fields back onto it:

```rust
let mut ctx = CommandContext::new(              // :472-478
    &mut **store, &self.shard_senders, self.shard_id,
    self.num_shards, self.conn_id, self.protocol_version,
);
ctx.is_replica = …;  ctx.is_replica_flag = …;  ctx.master_host = …;   // :485-504
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
  `reject_server_wide` (`:432-450`) lets it through. With `ctx.command_registry == None`
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
handlers use; make propagation a move instead of a transcription.**

### The shape

```rust
/// Node-scoped handles: identical for every command on this shard, for the
/// lifetime of the shard. Owned, Clone, Default.
#[derive(Clone, Default)]
pub struct NodeContext {
    shard_senders: Arc<Vec<ShardSender>>,
    shard_id: u16,
    conn_id: u64,
    cluster_state: Option<Arc<ClusterState>>,
    node_id: Option<u64>,
    raft: Option<Arc<ClusterRaft>>,
    network_factory: Option<Arc<ClusterNetworkFactory>>,
    quorum_checker: Option<Arc<dyn QuorumChecker>>,
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    role_controller: Option<Arc<dyn RoleController>>,
    is_replica: bool,
    is_replica_flag: bool,
    master_host: Option<String>,
    master_port: Option<u16>,
    master_link_up: bool,
    master_sync_error: Option<String>,
    snapshot_stats: Option<Arc<SnapshotStats>>,
    bgsave_in_progress: Option<Arc<AtomicBool>>,
    recovery_stats: Option<Arc<RecoveryStats>>,
}

pub struct CommandContext<'a> {
    pub store: &'a mut dyn Store,          // 515 reads in frogdb-commands
    pub effects: CommandEffects,           //  22
    pub protocol_version: ProtocolVersion, //  24
    pub num_shards: u16,                   //  21
    pub json_limits: JsonLimits,           //   6
    pub command_registry: Option<Arc<CommandRegistry>>, // 5
    pub node: NodeContext,                 // everything else, as one value
}
```

Four properties follow, and each one is the point:

**(a) `frogdb-commands` does not change.** The six fields handlers read stay spelled
`ctx.store`, `ctx.num_shards`, … exactly as today. 515 + 24 + 22 + 21 + 6 + 5 = **593 read
sites and 296 `Command` impls are untouched**. This is deliberate and it is also the
scope-boundary argument against proposal 90 (§Risks).

**(b) The server's admin handlers pay a mechanical rename.** `ctx.node_id` →
`ctx.node.node_id`, and so on, at roughly 100 sites concentrated in
`server/src/commands/cluster/*` and the info/replication handlers — `sed`-able, no logic
change, reviewable as a diff of pure field paths.

**(c) Propagation collapses from 19 assignments to 1.** `ScriptInvoker` drops 17 field
declarations (`gate.rs:295-348`) and keeps `node: NodeContext`; `from_context` becomes a
clone; `run_local`'s eleven-line assignment block (`:485-504`) becomes `ctx.node =
self.node.clone();`. **The seven-dropped-fields defect stops being a bug you fix and
becomes a state you cannot express** — there is no longer a per-field list to be
incomplete. That is the leverage: the same edit that fixes today's four-fields-plus-three
also retires the recurrence.

**(d) The test constructor loses its leak.** `CommandContext::new` becomes
`new(store, protocol_version)` with `node: NodeContext::default()`, killing the
`shard_senders` parameter — **7 of the 14 `Box::leak` calls in `frogdb-commands` disappear
immediately**. With a two-argument constructor, tests can hold a local store and build the
context inline instead of returning `CommandContext<'static>` from a helper, which retires
the other 7 as well; I claim the first 7 as certain and the second 7 as a likely follow-on
(§Testability).

### Why an owned `NodeContext` is feasible

`ShardCluster` (`shard/types.rs:582-607`) **already stores exactly these handles as owned
`Option<Arc<…>>`**, with accessors at `:612-638` handing out `&Arc`/`&dyn`. Only 6 of the
19 fields are borrows in `CommandContext` today; the rest are already owned `Option<Arc<…>>`
or `Copy` scalars. So `worker.rs:333-382` can build a `NodeContext` once per shard, cache
it, and hand each command a clone — or, if refcount churn measures badly, hold
`node: &'a NodeContext` (see §Risks for the trade).

### Vocabulary

- **Interface**: `Command::execute(&mut CommandContext, &[Bytes])` (`command.rs:720`) is
  the workspace's single widest interface — 346 implementors. Its parameter currently
  publishes 25 fields to implementors that need 6. Narrowing the *parameter* narrows the
  *interface*, and does so without touching the signature (§Risks, boundary with 80).
- **Module / locality**: cluster and replication handles belong to the node-management
  module, not to the data-structure command module. Today they are declared in the type
  that every data-structure command programs against.
- **Seam**: there are three places where a context crosses into a handler
  (`worker.rs:355`, `gate.rs:472`, `shard/scripting.rs:222`). Two go through the shared
  builder; one transcribes fields. Making the wide half a single value makes all three
  identical by construction.
- **Adapter**: `ScriptInvoker` is an adapter that currently re-declares its adaptee's
  fields instead of holding it. Holding one `NodeContext` is the whole fix.
- **Deletion test**: applied twice. To the *views layer* — deleting it costs one call site
  (§Problem 1), so it is not carrying weight and should go. To the *proposal* — if the
  narrowing is deleted, `gate.rs` keeps a hand-maintained 19-item list that has been wrong
  four times, and `NodeContext`'s absence is what makes the fifth time possible.

---

## Testability improvement

1. **7 `Box::leak`s die on the constructor change alone** (one per helper, the
   `shard_senders` leak), and the remaining 7 die if the helpers are inlined —
   `frogdb-commands` would then contain zero `Box::leak`.
2. **A default-constructible `NodeContext` makes "run a handler in isolation" a two-line
   setup.** Today, testing a handler that reads *any* of the 19 fields means spelling a
   25-field struct literal by hand; that is why `command.rs:1982` and `executor.rs:757`
   exist as bespoke helpers. With `NodeContext::default()` plus targeted overrides, a test
   for e.g. `INFO replication` under a replica role is `ctx.node.is_replica = true;`.
3. **The `gate.rs` propagation becomes assertable in one test instead of one per field.**
   Today's `FM-REPLICATION-059` tests (`gate.rs:1095`, `:1142`) each pin exactly one field,
   which is why three fields got three separate regression tests and seven fields got none.
   With a single value, `assert_eq!(ctx_in_script.node, invoker.node)` covers all 19 —
   including fields added years from now. `NodeContext` deriving `PartialEq` for tests is
   cheap (all members are `Arc`/scalar; pointer equality is the right semantics here).
4. **Optional chokepoint gate.** `lint-script-gate` (`Justfile:1080-1107`) is an existing
   compile-free grep gate over `crates/core/src/scripting`. The same pattern supports
   "`CommandContext` is constructed only in `shard/worker.rs`" — i.e. `CommandContext::new`
   and `CommandContext {` outside the builder and `#[cfg(test)]` is a lint failure. That
   turns "the script path must not hand-assemble a context" from a comment
   (`shard/scripting.rs:217-220`) into an enforced invariant. I list this as optional
   because it adds a sixteenth gate to a family the round is already growing.
5. **The three test-only `ScriptInvoker { … }` literals** (`gate.rs:809-838`) each spell 19
   fields; they collapse to `node: NodeContext::default()` plus the field under test.

---

## Risks / scope boundaries vs sibling proposals

### Sibling edges

| Proposal | Overlap | Edge |
|---|---|---|
| **90 — CommandSpec default** | Declares itself the **solo, last** sweep of `frogdb-commands` (its §Risks, `:344-401`). | **No conflict, by design.** Property (a) of the design is that `frogdb-commands` handler code changes by **zero lines**. My only footprint in that crate is the 7 test-helper `ctx()` functions (7 files × 4 lines), which 90's spec sweep does not touch. **If the reviewer prefers zero overlap, 91 can drop the test-helper cleanup entirely and land it as a follow-up after 90** — that costs nothing but the leak removal. Ordering preference: 91 before 90 (91's diff is 4 lines/file in 7 files; 90's is crate-wide and wants a clean base), but either order works. |
| **80 — response wire fold** | Explicitly **rejects** changing `Command::execute`'s signature (its §Risks, `:440-470`); touches `commands/src/blocking.rs`, `stream/read.rs`. | **Compatible and mutually reinforcing.** 91 also does not change the signature — it changes what the parameter *contains*. No file overlap. |
| **81 — core dead seams** | Edits `shard/worker.rs` (constructors) and `shard/*` + `server/*`; **does not touch `command.rs`**. | **File-level overlap at `worker.rs` only.** 91 rewrites `command_context:333-382`; 81 edits other constructors in the same file. Textual conflict is likely, semantic conflict is not. **Order: 81 first** (it is a deletion; rebasing 91's single-function rewrite onto it is trivial, the reverse is not). |
| **88 — served-wake effects** | Edits `shard/worker.rs` among 8 files. | Same as 81: `worker.rs` textual overlap, different functions. **Order: 88 before or after, but not concurrently with 91's `worker.rs` edit.** |
| **89 — probabilistic chunk codec** | `commands/src/bloom.rs`, `cuckoo.rs`. | **Direct overlap on 2 of my 7 test helpers** (`bloom.rs:682-683`, `cuckoo.rs:760-761`). Trivially small; **91 yields** — drop those two from the leak cleanup if 89 is in flight. |
| **67 / 70** | Other `frogdb-commands` files. | No overlap under property (a). |

### Risks

- **Arc refcount churn (the real one).** An owned `NodeContext` cloned per command means
  up to 8 `Arc` clone/drop pairs per command on the hot path. This is the one claim in the
  proposal that needs a measurement rather than an argument. Mitigations, in order of
  preference: (i) build the `NodeContext` **once per shard** in `worker.rs` and store
  `node: &'a NodeContext` in `CommandContext` — a single pointer copy per command, and the
  shard already outlives every command it runs; (ii) keep it owned but `Arc<NodeContext>`,
  one refcount per command; (iii) owned + clone, only if (i) and (ii) prove awkward for the
  script path (which needs to *hold* a context across a Lua callback). **Recommended:
  (i)**, with (ii) as the fallback if the script path's lifetimes fight back —
  `ScriptInvoker` currently owns its copies precisely because it outlives the borrow.
  Whichever is chosen, `run_local`'s 11-assignment block still collapses to one line, so
  the defect fix does not depend on which variant wins.
- **Field-grouping judgment call.** I keep `num_shards`, `json_limits` and
  `command_registry` **inline** on `CommandContext` even though they are node-scoped,
  because handlers read them (21 / 6 / 5) and moving them would put ~32 field-path edits
  into `frogdb-commands` — precisely the crate 90 wants solo. The stricter split (all
  node-scoped fields in `NodeContext`) is cleaner on paper; it should be a **follow-up
  after 90 lands**, or never. Stated here so a reviewer does not read the inline three as
  an oversight.
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
- **Blast-radius honesty.** 346 `Command` impls, 593 handler read sites, 1 production
  construction site, 10 `CommandContext::new` sites. The design deliberately routes the
  churn to the ~100 field-path renames in the server crate and away from the 296-impl
  crate. If a reviewer rejects that asymmetry, the alternative is a genuinely large sweep
  and the proposal should be re-costed as L.

---

## Effort

**M** for the narrowing: one new struct, one rewritten builder (`worker.rs:333-382`), one
rewritten adapter (`gate.rs:295-506`, which *shrinks*), ~100 mechanical field-path renames
in `frogdb-server`, 7 test-helper edits in `frogdb-commands`. No signature change to
`Command::execute`, no new dependency, no locked crate. The optional construction gate adds
**S** on top.

### Independently landable hotfixes

Three, in recommended landing order. Each is a standalone commit that stands without the
refactor, and each shrinks the refactor's diff.

**H3 — `run_local` drops seven fields (S, LIVE, land first).** Add the seven missing
assignments at `gate.rs:485-504`: `cluster_state`, `node_id`, `raft`, `network_factory`,
`quorum_checker`, `command_registry`, `role_controller` — the fields must also be added to
`ScriptInvoker` (`:295-348`) and `from_context` (`:350-383`), which is why this is S and not
XS. Ships the user-visible fix (`redis.call('COMMAND','COUNT')` → real count;
`redis.call('CLUSTER','INFO')` → real cluster info) **now**, with regression tests beside
the existing FM-tagged ones at `gate.rs:1095`/`:1142`. **State plainly in the commit
message that this is the fourth one-field-at-a-time patch to the same list** — H3 is the
symptom fix; the narrowing is what stops the fifth. If only one thing from this proposal
lands, it should be H3.

**H1 — delete `CommandContextCore` (S, zero-behavior).** `command.rs:1035-1088` (54 lines)
+ `as_core` `:1517-1546` (30 lines) + the `lib.rs:72` re-export + reword the dangling doc at
`store/typed.rs:107`. ≈85 lines, 4 files, **zero call sites**, compiles-or-it-doesn't. The
brief's "pure delete, S effort" is **confirmed** — with the caveat that `typed.rs:107` must
be reworded rather than left pointing at a deleted type.

**H2 — delete the second dead tranche (S, zero-behavior).** `ReplicationContextRef`
(`:1029-1033`), `replication_context` (`:1574-1589`), `require_replication` (`:1612-1626`),
`has_replication` (`:1590-1595`), `require_cluster` (`:1597-1610`), `is_cluster_mode`
(`:1511-1515`) + the `lib.rs:74` re-export. ≈60 lines, **zero call sites**. Leave
`ClusterContextRef` / `cluster_context` alive for `version.rs:45`, or inline that one use
and delete those too (a further ~38 lines) — reviewer's call. Leave
`CommandError::ReplicationDisabled` in place.

H1 and H2 together remove ~145 lines of the failed additive-narrowing layer, which is both
free value and the clearest possible statement of why the narrowing must proceed by
subtraction.
