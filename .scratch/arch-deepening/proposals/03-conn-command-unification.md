# Proposal 03 — Unify connection-command dispatch; declare mutation capability in the spec

> Status: proposal / not yet scheduled. All code references below were verified against the
> tree at branch `workspace-1` (2026-07-20). Line numbers are approximate anchors.

## Summary

Connection-command execution lives in **two parallel worlds**. The migrated world routes a command
through a narrow `ConnCtx` view (`frogdb_core::conn_command::ConnCtx`) and a `ConnectionCommand`
trait executor; the legacy world routes it to a `handle_*` method hanging off the whole
`&mut ConnectionHandler` god object under `connection/handlers/`. To understand *any* connection
command you must first know which world it lives in — and for several groups (scripting,
transaction, pub/sub) the answer is "both."

Inside the migrated world, the `ConnCtx` seam is **shallow**: its interface (25 public fields, 5 of
them `Option<&mut dyn …>` capability slots) is nearly as large and as intricate as its
implementations, and *which* slots are `Some` is an undocumented protocol split across **five**
build pathways (`conn_ctx`, `conn_ctx_authmut`, `conn_ctx_clientmut`, plus inline builders in
`execute_monitor` and `execute_pubsub`). The dispatcher then picks the pathway partly from data
(the command's `ExecutionStrategy`) and partly from **hardcoded string matches** —
`cmd_name == "CLIENT"` and `cmd_name == "MONITOR"` — leaking the builder requirement back into the
driver. The seam therefore does not actually abstract "connection command": the caller must still
know each command's name to construct the right context for it.

This proposal (a) finishes the migration so `handlers/` can be retired, and (b) replaces the ad-hoc
builders + `cmd_name ==` special-cases with a `ConnMutation` capability that each command
**declares in its `CommandSpec`**, so the dispatcher selects the builder from declared data, never
from the command name.

## Files involved (verified paths + counts)

Core seam (trait + context, one file, 1046 lines):

- `frogdb-server/crates/core/src/conn_command.rs`
  - `pub struct ConnCtx<'a>` — line ~590 (25 fields; see below)
  - `impl ConnCtx` builder chain: `new` (~691), `with_conn_state` (~747), `with_tracking` (~754),
    `with_pubsub` (~761), `with_monitor` (~767), `with_username` (~774), `with_full_reads` (~784)
  - `pub trait ConnectionCommand` — line ~809 (`spec` / `execute` / `execute_multi`)

Migrated world — **13** files (`connection/*conn_command*.rs`): 12 command-group executor files
plus the one seam-glue file that authors the builders.

```
connection/conn_command.rs                 (seam glue: base_ctx + the 3 named builders; CONFIG/FT.CURSOR executors)
connection/acl_conn_command.rs             connection/monitor_conn_command.rs
connection/auth_conn_command.rs            connection/observability_conn_command.rs
connection/client_conn_command.rs          connection/persistence_conn_command.rs
connection/connection_state_conn_command.rs connection/pubsub_conn_command.rs
connection/debug_conn_command.rs           connection/scripting_conn_command.rs
connection/info_conn_command.rs            connection/transaction_conn_command.rs
```

Legacy world — **35** files under `connection/handlers/` (34 handler modules + `mod.rs`),
`handle_*` methods on `&self` / `&mut ConnectionHandler`:

```
handlers/{admin,blocking,cluster,debug,hotkeys,info,persistence,pubsub,scatter,
          slowlog,timeseries_scatter,transaction}.rs
handlers/blocking/coordinator.rs
handlers/scripting/{mod,eval,function,script}.rs
handlers/search/{mod,aggregate,aliases,config,create,dict,es,explain,helpers,hybrid,
                 index_mgmt,merge,profile,query,spellcheck,synonyms,tagvals}.rs
```

Dispatcher (777 lines):

- `frogdb-server/crates/server/src/connection/dispatch.rs`
  - `dispatch_connection_command` — line ~116 (registry-union path; the two `cmd_name ==`
    special-cases at ~139 and ~149)
  - `dispatch_connection_state_command` — line ~245 (strategy-scoped → `conn_ctx_authmut`)
  - `DispatchStage::ConnectionCommand` arm — line ~467
  - `DispatchStage::ServerWide` arm — line ~514, `dispatch_server_wide` — line ~167 (routes
    `ServerWideOp` to legacy `handle_*`)
- `frogdb-server/crates/server/src/connection/routing.rs` — `route_and_execute` (line ~29): the
  shard-routing terminal that the legacy world funnels through.
- Inline builders outside the seam file: `execute_monitor`
  (`connection/monitor_conn_command.rs` ~144, `.with_monitor`), `execute_pubsub`
  (`connection/pubsub_conn_command.rs` ~895, `.with_pubsub`).

> Doc drift, worth noting: `connection/conn_command.rs` (top, ~lines 10-14) and the
> `DispatchStage::ConnectionCommand` comment (~465) both assert the legacy path is *gone* and the
> `ConnCtx` union is "the sole connection-command dispatch path." But `dispatch_connection_command`'s
> own doc (~108-112) still says "unmigrated groups fall through to the legacy router→handler path,"
> and the `handlers/` methods are demonstrably live (e.g. `handlers/transaction.rs::execute_transaction`
> is invoked from EXEC; `handlers/scripting/` and `handlers/search/` back the server-wide ops). The
> `router.rs` *name→op lookup table* was deleted; the `&mut ConnectionHandler` handler bodies were
> not. The two worlds still coexist — this proposal is what makes the optimistic comments true.

## Problem (two-worlds friction)

**You must first learn which world a command lives in.** A reader chasing "what does CLIENT do"
lands in `client_conn_command.rs` behind a `ConnCtx`; "what does EXEC do" lands in
`handlers/transaction.rs` on `&mut ConnectionHandler`; "what does EVAL do" is split — a
`scripting_conn_command.rs` executor *and* `handlers/scripting/eval.rs` helpers, because the seam
reaches part of the work and the handler reaches the rest (the shard transaction, the socket
teardown). The mental model is not "connection commands"; it is "connection commands, minus the
ones that still need the god object, plus the parts of migrated ones that leaked back out."

**The `ConnCtx` interface is wide.** 25 public fields, every one of which a caller of `ConnCtx::new`
must supply or default. Twenty are always-present shared borrows / scalars; five are `Option<&mut
dyn …>` capability slots. The struct doc calls it "a narrow, per-command view," but for any *given*
command 20+ of those fields are dead weight — CONFIG touches `config`/`shard_senders`/a few stats
handles and ignores the other ~20.

**The which-builder-populates-which protocol is implicit.** Nothing in the type system records that
`conn_state` is `Some` iff the command mutates auth/connection state, that `tracking` is `Some` iff
the command is CLIENT, that `monitor`/`pubsub` are `Some` only under their bespoke inline builders.
The knowledge lives in prose scattered across five build sites and five field doc-comments. Add a
sixth capability and you must find and update every builder that "should" leave it `None`.

**The `cmd_name ==` leak means the seam does not abstract "connection command."** In
`dispatch_connection_command`, after the registry hands back a `&'static dyn ConnectionCommand`, the
dispatcher **still branches on the command's string name** to decide how to build its context:

- `ExecutionStrategy::ConnectionLevel(PubSub)` → `execute_pubsub` (data-driven — good)
- `cmd_name == "CLIENT"` → `conn_ctx_clientmut` (string special-case)
- `cmd_name == "MONITOR"` → `execute_monitor` (string special-case)
- else → `conn_ctx` (read-only)

Two of the four branches are string equality on the driver's hot path. The trait object was
supposed to be enough to execute a command; it is not, because it cannot tell the dispatcher which
mutable capabilities to wire.

## Why it is shallow / fragmented (architecture vocab)

In Ousterhout's terms, a *deep* module hides a large implementation behind a small interface. The
`ConnCtx` seam inverts this: its **interface is nearly as complex as its implementation.** The 25
fields, the 5 `Option` slots, the 7 `with_*`/`new` constructors, and the 5 undocumented
"which-slots-are-Some" conventions are all things a caller must know — and by the definition in the
project's architecture vocabulary, an interface *includes* "which `Option` fields are `Some` in
which builder." That information is currently carried in comments, not types, so it is interface
that the compiler does not enforce.

The seam is also **fragmented**: one logical decision ("build the context this command needs") is
spread across `dispatch_connection_command`, `dispatch_connection_state_command`, the
`PreAuthIntercept` arm, `execute_monitor`, and `execute_pubsub` — five sites, three of them keyed by
a `cmd_name` string. A seam that leaks its *builder requirement* back into the driver via string
matching has poor **locality**: changing how CLIENT is dispatched, or migrating a new mutating
command, means editing the driver, not just adding a command. The **leverage** a good seam should
give — "register a command, and dispatch just works" — is absent for any command that mutates.

The **deletion test**: if the migration were finished and the capability declared in data, the
entire `handlers/` tree and both `cmd_name ==` branches could be deleted, and the driver would not
need to change. Today neither can be deleted, because the driver still names the commands.

## Proposed change

**Part (a) — finish the migration.** Move each remaining legacy `handle_*` group behind a
`ConnectionCommand` executor + `ConnCtx` (or, for genuinely shard-routed work, behind the existing
`ServerWideOp`/`route_and_execute` seam, which is already data-driven). When a group is fully
migrated, delete its `handlers/*.rs`. When `handlers/` is empty, delete the module. This makes the
two optimistic comments true and collapses the "which world" question to a single world.

**Part (b) — declare mutation capability in the spec.** Add a `ConnMutation` enum to
`frogdb_core` and a `mutation: ConnMutation` field to `CommandSpec`. Each connection command
declares its mutation capability alongside its other mechanical facts (arity, keys, WAL, strategy):

```rust
/// Which connection-local mutable capabilities a connection command needs wired
/// into its `ConnCtx`. Declared in the command's `CommandSpec`; the dispatcher
/// selects the builder from this, never from the command name.
pub enum ConnMutation {
    /// Pure reads. Dispatched through the read-only `conn_ctx` view.
    None,
    /// Mutates per-connection auth/protocol state (AUTH, HELLO, RESET,
    /// ASKING/READONLY/READWRITE). Wires `conn_state = Some`.
    Auth,
    /// CLIENT: mutates connection state *and* drives tracking IO. Wires
    /// `conn_state = Some` and `tracking = Some`.
    Client,
    /// MONITOR: registers the connection on the executed-command feed. Wires
    /// `monitor = Some`.
    Monitor,
    /// Pub/sub family: multi-response, needs the pub/sub machinery. Wires
    /// `pubsub = Some` and dispatches via `execute_multi`.
    PubSub,
}
```

The dispatcher then reduces to a single **data-driven** `match` over `command.spec().mutation` — no
`cmd_name` anywhere — and there is exactly one builder, parameterized by the declared capability set.
`CommandSpec::validate` gains a cross-field check (mirroring the existing `ConnectionLevelWithWal`
check) that the declared `mutation` agrees with the `ExecutionStrategy::ConnectionLevel(op)` variant.

## Before / After

### Before — the `ConnCtx` field block (`core/src/conn_command.rs` ~590)

```rust
pub struct ConnCtx<'a> {
    pub config: &'a dyn ConfigProvider,
    pub client_registry: &'a ClientRegistry,
    pub latency_histograms: &'a CommandLatencyHistograms,
    pub keyspace_stats: &'a KeyspaceStats,
    pub shard_senders: &'a [ShardSender],
    pub snapshot_coordinator: &'a dyn crate::persistence::SnapshotCoordinator,
    pub hotkey_session: &'a SharedHotkeySession,
    pub hotkey_cluster: &'a dyn HotkeyClusterProvider,
    pub protocol_version: ProtocolVersion,
    pub cursor_store: &'a dyn CursorStoreProvider,
    pub metrics_recorder: &'a dyn crate::MetricsRecorder,
    pub memory_diag: &'a dyn MemoryDiagProvider,
    pub num_shards: usize,
    pub max_clients: u64,
    pub cluster_enabled: bool,
    pub acl_manager: &'a AclManager,
    pub command_registry: &'a CommandRegistry,
    pub username: &'a str,
    pub info: &'a dyn InfoProvider,
    pub scripting: &'a dyn ScriptingProvider,
    // ---- the five capability slots, populated by an implicit builder protocol ----
    pub conn_state: Option<&'a mut dyn ConnStateMut>,       // Some iff AUTH/HELLO/RESET/ASKING/…
    pub tracking:   Option<&'a mut dyn ClientTrackingProvider>, // Some iff CLIENT
    pub pubsub:     Option<&'a mut dyn PubSubProvider>,     // Some iff pub/sub family
    pub debug:      Option<&'a dyn DebugProvider>,          // Some iff DEBUG (via read-only builder)
    pub monitor:    Option<&'a mut dyn MonitorProvider>,    // Some iff MONITOR
}
```

25 fields; the `Some`/`None` discipline of the last five is documented only in prose.

### Before — the three named builders (`server/src/connection/conn_command.rs` ~122-184)

```rust
pub(crate) fn conn_ctx(&self) -> ConnCtx<'_> {
    Self::base_ctx(&self.admin, &self.core, &self.observability, &self.cluster,
                   &self.memory_diag, self.num_shards)
        .with_full_reads(self, self, Some(self),
                         self.state.protocol_version, self.state.username())
}

pub(crate) fn conn_ctx_authmut(&mut self) -> ConnCtx<'_> {
    Self::base_ctx(&self.admin, &self.core, &self.observability, &self.cluster,
                   &self.memory_diag, self.num_shards)
        .with_conn_state(&mut self.state)
}

pub(crate) fn conn_ctx_clientmut(&mut self) -> ConnCtx<'_> {
    Self::base_ctx(&self.admin, &self.core, &self.observability, &self.cluster,
                   &self.memory_diag, self.num_shards)
        .with_conn_state(&mut self.state)
        .with_tracking(&mut self.tracking_io)
}
```

…plus two more build sites the driver also reaches: `execute_monitor` (`.with_monitor`) and
`execute_pubsub` (`.with_pubsub`). Five pathways, one per capability shape.

### Before — the `cmd_name ==` dispatch (`server/src/connection/dispatch.rs` ~116-153)

```rust
async fn dispatch_connection_command(&mut self, cmd_name: &str, args: &[Bytes])
    -> Option<Vec<Response>>
{
    let command = self.core.registry.get_entry(cmd_name)?.as_connection()?;
    if matches!(command.spec().strategy,
                ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)) {
        return Some(self.execute_pubsub(command, args).await);
    }
    if cmd_name == "CLIENT" {                                   // <-- string special-case
        return Some(vec![command.execute(&mut self.conn_ctx_clientmut(), args).await]);
    }
    if cmd_name == "MONITOR" {                                  // <-- string special-case
        return Some(vec![self.execute_monitor(command, args).await]);
    }
    Some(vec![command.execute(&mut self.conn_ctx(), args).await])
}
```

### After — capability declared in the spec, builder selected from data

Each command's `CommandSpec` gains `mutation:` (shown for CLIENT):

```rust
static CLIENT_SPEC: CommandSpec = CommandSpec {
    name: "CLIENT",
    // …arity/flags/keys/access/wal/wakes/event/…
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
    mutation: ConnMutation::Client,   // <-- the one new declarative fact
};
```

One builder, parameterized by the declared capability, replaces the five:

```rust
/// Build the ConnCtx a command declared it needs. The single build site; the
/// `ConnMutation` comes from the command's spec, so there is no `cmd_name` here.
fn conn_ctx_for(&mut self, m: ConnMutation) -> ConnCtx<'_> {
    let base = Self::base_ctx(&self.admin, &self.core, &self.observability,
                              &self.cluster, &self.memory_diag, self.num_shards);
    match m {
        ConnMutation::None    => base.with_full_reads(self, self, Some(self),
                                     self.state.protocol_version, self.state.username()),
        ConnMutation::Auth    => base.with_conn_state(&mut self.state),
        ConnMutation::Client  => base.with_conn_state(&mut self.state)
                                     .with_tracking(&mut self.tracking_io),
        ConnMutation::Monitor => base.with_monitor(&mut self.monitor_io()),
        ConnMutation::PubSub  => base.with_pubsub(&mut self.pubsub_io()),
    }
}

async fn dispatch_connection_command(&mut self, cmd_name: &str, args: &[Bytes])
    -> Option<Vec<Response>>
{
    let command = self.core.registry.get_entry(cmd_name)?.as_connection()?;
    let mutation = command.spec().mutation;                 // data, not the name
    let mut ctx = self.conn_ctx_for(mutation);
    Some(command.execute_multi(&mut ctx, args).await)       // uniform multi-response seam
}
```

The dispatcher no longer knows any command's name. `ConnCtx` becomes constructible from a declared
capability set: given a `ConnMutation`, `conn_ctx_for` produces exactly the right slots `Some` —
the "which builder populates which" protocol is now a single exhaustive `match` the compiler checks,
not five prose conventions. (Borrow-disjointness across `self.state` / `self.tracking_io` /
`self.monitor_io` / pub-sub channels still holds — the `match` arms take the same disjoint `&mut`
sub-borrows the five builders take today, just in one place.)

## Testability improvement

Today a unit test that wants CLIENT's context must know to call `conn_ctx_clientmut` (and that it
exists, and that it wires `conn_state` + `tracking`). After the change, a test declares the
capability the same way production does — `ConnCtx` built `for(ConnMutation::Client)` over fixture
deps — with no knowledge of which of five builders is "the right one." Dispatch itself becomes
**table-testable**: a test can assert, for a table of `(command, expected ConnMutation)` rows, that
`spec().mutation` matches, and that `conn_ctx_for(m)` populates exactly the expected `Some` slots —
replacing today's untested prose invariants. The existing `Fixture` in
`server/src/connection/conn_command.rs` (which already builds a `ConnCtx` over stub deps) extends
naturally: add a `mutation` argument and drive both the read and mutate paths through one helper.

## Migration path (incremental)

1. **Add the declarative field, default-preserving.** Introduce `ConnMutation` in
   `core/src/command.rs` (next to `ConnectionLevelOp`) and add `mutation: ConnMutation` to
   `CommandSpec` with a sensible default (`None`). Populate it on the already-migrated command
   specs to match today's behavior (`CLIENT` → `Client`, `MONITOR` → `Monitor`, AUTH/HELLO/RESET/
   connection-state → `Auth`, pub/sub → `PubSub`, everything else → `None`). No dispatch change yet;
   just data that agrees with current routing. Add the `validate` cross-check.
2. **Switch the dispatcher to the data.** Replace the `cmd_name ==` branches and the PubSub
   `matches!` in `dispatch_connection_command` (and fold in `dispatch_connection_state_command`)
   with `conn_ctx_for(command.spec().mutation)`. Delete `conn_ctx_authmut` / `conn_ctx_clientmut`
   as named methods once `conn_ctx_for` subsumes them. This is behavior-preserving; the
   pin/table test from the previous step guards it.
3. **Migrate remaining handler groups, one at a time.** For each `handlers/*` group, stand up a
   `*_conn_command.rs` executor (or route through the existing `ServerWideOp` seam for
   shard-scatter work), declare its `mutation`, register it, and delete the legacy `handle_*`. Do
   this group-by-group with a commit per group (per the subagent-orchestration guidance) so each
   step is independently reviewable and the watchdog never stalls on a mega-diff. Start with the
   groups that already have a migrated sibling (scripting, transaction, pub/sub) to eliminate the
   worst "lives in both worlds" cases first.
4. **Delete the legacy path last.** When `handlers/` holds only shard-routing helpers that
   legitimately belong to `route_and_execute` (or is empty), remove the module and correct the
   now-accurate doc comments. The deletion test passes: no driver edit was needed to remove it.

## Risks / open questions

- **Borrow-checker disjointness.** The five builders exist partly *because* the `&mut` sub-borrows
  of `self` (`self.state`, `self.tracking_io`, monitor/pubsub channels) are delicate — the
  `base_ctx` doc explicitly warns against collapsing to `&self`. The single `conn_ctx_for` `match`
  must reproduce the exact same disjoint sub-borrows per arm; this is mechanical but must be
  verified to compile, not assumed. Low risk (same borrows, relocated), but it is the thing most
  likely to bite.
- **`Monitor`/`PubSub` need extra inputs.** `execute_monitor`/`execute_pubsub` construct a
  `MonitorIo`/`PubSubIo` from *two* handler fields each. `conn_ctx_for` must own those constructions
  (shown as `self.monitor_io()` / `self.pubsub_io()` helpers). Verify no lifetime surprise when the
  `Io` temporary is created inside the builder vs. at the call site.
- **`ConnMutation` vs `ConnectionLevelOp` overlap.** `ConnectionLevelOp` already distinguishes
  `PubSub`/`ConnectionState`/`Auth`/`Admin`. `ConnMutation` is a finer, *capability*-oriented cut
  (CLIENT and CONFIG are both `Admin` but need different mutation wiring). Open question: keep them
  as two orthogonal fields (routing vs. capability) — recommended, since `Admin` maps to both
  `None` (CONFIG) and `Client` (CLIENT) — or derive one from the other where possible. The
  `validate` cross-check documents the legal combinations regardless.
- **Scope of part (a).** Fully retiring `handlers/` is a large surface (34 modules incl. all of
  `search/`). Some of that work is arguably *shard-routed* server-wide command territory, not
  connection-command territory, so "finish the migration" must be scoped as "migrate genuine
  connection commands; route the rest through the existing `ServerWideOp` seam," not "everything
  becomes a `ConnCtx` command."

## Effort estimate

- **Part (b) — the `ConnMutation` spec field + unified builder + dispatcher rewrite: M.** Bounded,
  mostly mechanical, behavior-preserving, and independently landable ahead of part (a). The one
  real risk is borrow-disjointness in `conn_ctx_for`; everything else is data population + a
  `validate` rule + a table test. This part alone kills both `cmd_name ==` special-cases and the
  five-builder protocol.
- **Part (a) — finishing the migration and deleting `handlers/`: L.** Broad surface (34 modules),
  requires per-group judgment about connection-command vs. shard-routed, and touches the hottest
  path. Best done group-by-group over many commits, gated by the pinning tests part (b) introduces.

Recommended sequencing: land **(b) first** (it delivers most of the architectural leverage and de-
risks (a) by making dispatch data-driven), then chip away at **(a)** group-by-group.
