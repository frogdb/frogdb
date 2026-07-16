# Proposal: ConnCtx Builder Collapse

Status: implemented
Date: 2026-07-16

## Problem

`ConnCtx` is the connection-command seam's test surface: a command executes against a narrow view
of the connection (`frogdb-server/crates/core/src/conn_command.rs:578-662`, 25 fields) instead of
`&mut ConnectionHandler`. The seam **earns its keep** — every connection command is exercised
socketless by constructing a `ConnCtx` over fixture deps
(`server/src/connection/conn_command.rs:507-554` and eight sibling fixtures), no handler, no socket.
This proposal *slims* the seam; it does not remove it.

The friction is not the seam's existence — it is that the 25-field list is authored **fourteen
times**. Every one re-lists the same fields; the production builders differ only in a few capability
slots, and the mutable ones fake three fields purely to satisfy the borrow checker:

| Symptom | Where |
|---------|-------|
| Read-only production builder (`info: self`, `debug: Some(self)`, real protocol/username) | `server/src/connection/conn_command.rs:77-108` |
| AUTH/HELLO builder — same 25 fields, but `info: &NOOP_INFO`, `scripting: &NOOP_SCRIPTING`, `protocol_version: default()`, `username: ""`, `conn_state: Some(&mut self.state)` | `server/src/connection/conn_command.rs:124-162` |
| CLIENT builder — identical placeholders again, plus `tracking: Some(&mut self.tracking_io)` | `server/src/connection/conn_command.rs:174-206` |
| Pub/sub dispatch — full 25-field literal inline, `pubsub: Some(&mut pubsub_io)`, same four placeholders | `server/src/connection/pubsub_conn_command.rs:910-936` |
| MONITOR dispatch — full 25-field literal inline, `monitor: Some(&mut monitor_io)`, same four placeholders | `server/src/connection/monitor_conn_command.rs:154-180` |
| Nine test fixtures — each re-lists all 25 fields | `conn_command.rs:526`, `info_conn_command.rs:115`, `acl_conn_command.rs:461`, `auth_conn_command.rs:520`, `debug_conn_command.rs:886`, `persistence_conn_command.rs:202`, `connection_state_conn_command.rs:334`, `observability_conn_command.rs:1047`, `transaction_conn_command.rs:460` |

Two consequences:

1. **Every new subsystem is a fourteen-site edit.** Adding one field to `ConnCtx` (the next command
   group that needs a capability core cannot already name) means editing five production literals and
   nine fixtures, in lockstep, or the crate does not compile. The field list has no single home.

2. **A placeholder-and-Noop dialect no one can read at a glance.** The four mutable dispatch paths
   set `username: ""`, `protocol_version: default()`, `info: &NOOP_INFO`, `scripting:
   &NOOP_SCRIPTING` — not because the command wants those values, but because `info: self` takes a
   *whole-`self`* shared borrow that cannot coexist with the `&mut self.state` the mutable path
   needs. The placeholder is borrow-checker exhaust masquerading as data; two `static NOOP_*`
   declarations are re-pasted into each of the four functions.

The seam is right. The fourteen hand-copied field lists are the debt.

## Design

Two moves: give the field list **one authoring site** in core, and layer capabilities on top so no
call site re-lists ambient fields or fakes placeholders.

### One field-list home — `ConnCtx::new` in core

```rust
impl<'a> ConnCtx<'a> {
    /// The one place the ambient-field list is authored. Capability slots
    /// default to absent; info/scripting default to the no-op providers and
    /// identity to placeholders — a read-only caller overrides them, a mutable
    /// caller leaves them (it reads identity through `conn_state`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &'a dyn ConfigProvider,
        client_registry: &'a ClientRegistry,
        latency_histograms: &'a CommandLatencyHistograms,
        keyspace_stats: &'a KeyspaceStats,
        shard_senders: &'a [ShardSender],
        snapshot_coordinator: &'a dyn SnapshotCoordinator,
        hotkey_session: &'a SharedHotkeySession,
        hotkey_cluster: &'a dyn HotkeyClusterProvider,
        cursor_store: &'a dyn CursorStoreProvider,
        metrics_recorder: &'a dyn MetricsRecorder,
        memory_diag: &'a dyn MemoryDiagProvider,
        acl_manager: &'a AclManager,
        command_registry: &'a CommandRegistry,
        num_shards: usize,
        max_clients: u64,
        cluster_enabled: bool,
    ) -> Self {
        const NOOP_INFO: &NoopInfoProvider = &NoopInfoProvider;
        const NOOP_SCRIPTING: &NoopScriptingProvider = &NoopScriptingProvider;
        ConnCtx {
            config, client_registry, latency_histograms, keyspace_stats, shard_senders,
            snapshot_coordinator, hotkey_session, hotkey_cluster, cursor_store,
            metrics_recorder, memory_diag, acl_manager, command_registry,
            num_shards, max_clients, cluster_enabled,
            protocol_version: ProtocolVersion::default(),  // read-only path overrides
            username: "",                                  // read-only path overrides
            info: NOOP_INFO, scripting: NOOP_SCRIPTING,    // read-only path overrides
            conn_state: None, tracking: None, pubsub: None, debug: None, monitor: None,
        }
    }

    fn with_conn_state(mut self, s: &'a mut dyn ConnStateMut) -> Self { self.conn_state = Some(s); self }
    fn with_tracking(mut self, t: &'a mut dyn ClientTrackingProvider) -> Self { self.tracking = Some(t); self }
    fn with_pubsub(mut self, p: &'a mut dyn PubSubProvider) -> Self { self.pubsub = Some(p); self }
    fn with_monitor(mut self, m: &'a mut dyn MonitorProvider) -> Self { self.monitor = Some(m); self }
    // read-only path only:
    fn with_full_reads(mut self, info, scripting, debug, proto, user) -> Self { .. }
}
```

The placeholders become **defaults**, authored once; the two `NOOP_*` statics live once; the caps
are absent by default. Nothing downstream re-lists fields.

### The production adapter carries the borrow-split — `server/.../conn_command.rs`

The whole reason there are three named builders is a borrow conflict: read-only wants `info: self`
(a whole-`self` shared borrow); the mutable paths want `&mut self.state` / `&mut self.tracking_io`,
which cannot coexist with it. `ConnCtx::new` does not fix that by itself — the fix is that the
handler adapter borrows only the **disjoint sub-fields** (`self.admin`, `self.observability`,
`self.core`, `self.cluster`, `self.memory_diag`) at the call site, leaving `self.state`,
`self.tracking_io`, `self.pubsub_*`, `self.monitor_*` free for the cap slots:

```rust
impl ConnectionHandler {
    /// Ambient view over the handler's disjoint subsystem fields. Borrows exactly
    /// the fields that are NEVER a capability slot, so state/tracking_io/pubsub/
    /// monitor stay available to the `.with_*` layer.
    fn base_ctx(&self) -> ConnCtx<'_> {
        ConnCtx::new(
            self.admin.config_manager.as_ref(), self.admin.client_registry.as_ref(),
            self.observability.latency_histograms.as_ref(), /* … 13 more … */,
            self.num_shards, self.admin.config_manager.max_clients(), self.cluster.is_cluster_mode(),
        )
    }

    fn conn_ctx(&self) -> ConnCtx<'_> {          // read-only: real reads + DEBUG
        self.base_ctx().with_full_reads(self, self, Some(self),
            self.state.protocol_version, self.state.username())
    }
    fn conn_ctx_authmut(&mut self) -> ConnCtx<'_> { let c = self.base_ctx(); c.with_conn_state(&mut self.state) }
    fn conn_ctx_clientmut(&mut self) -> ConnCtx<'_> {
        let c = self.base_ctx(); c.with_conn_state(&mut self.state).with_tracking(&mut self.tracking_io)
    }
}
```

`execute_pubsub` / `execute_monitor` collapse the same way: `self.base_ctx().with_pubsub(&mut
pubsub_io)` / `.with_monitor(&mut monitor_io)`. Five 25-field literals become one `base_ctx` plus
five one-line cap tails. (`base_ctx` must borrow the named sub-fields, not `&self` wholesale, or the
disjointness the borrow checker needs is lost — that constraint is why the ambient list is wired in
the *server* adapter and only the field *identities* live in core's `ConnCtx::new`.)

Test fixtures call `ConnCtx::new(..)` directly over their owned deps, then `.with_*` if the command
under test needs a cap — the nine fixtures stop re-listing fields too.

### Adjudicating the 11 provider traits: fold or keep

The finding asked whether the single-impl providers are hypothetical seams to fold back to concrete
types. The **crate dependency direction settles it, not adapter count**: `frogdb-core` does not
depend on `frogdb-server` (`core/Cargo.toml` has no server dep; server → core). Every provider impl
names a *server* type. Folding any of them into `ConnCtx` as a concrete type would make `core` name
a server type — a dependency cycle. The trait boundary *is* the crate boundary.

| Trait | Sole impl (server) | Verdict | Reason |
|-------|--------------------|---------|--------|
| `ConfigProvider` | `ConfigManager` | keep | Core cannot name `ConfigManager`; tests reuse the *real* one, so it buys no test polymorphism — but it buys the crate seam, which is mandatory. |
| `CursorStoreProvider` | `AggregateCursorStore` | keep | Same: server type, core-uncallable. |
| `HotkeyClusterProvider` | `ClusterDeps` | keep | Server cluster wiring. |
| `MemoryDiagProvider` | `MemoryDiag` wrapper | keep | Wraps `frogdb_debug` collector. |
| `InfoProvider` | `ConnectionHandler` | keep | Whole fleet-aggregation surface. |
| `ScriptingProvider` | `ConnectionHandler` | keep | Lua VM / cross-shard EVAL live server-side. |
| `ClientTrackingProvider` | `tracking_io` | keep | Invalidation channel + shard registration. |
| `ConnStateMut` | `ConnectionState` | keep | Mutable per-connection state (server). |
| `PubSubProvider` | `PubSubIo` | keep | Disjoint handler-borrow bundle. |
| `DebugProvider` | `ConnectionHandler` | keep | Tracer / bundle / per-shard round-trips. |
| `MonitorProvider` | `MonitorIo` | keep | Monitor broadcast channel. |

**All eleven keep.** The deletion test fails in the keep direction: deleting any of these traits does
not delete plumbing, it makes `core` fail to compile. The contrast is the four deps `ConnCtx`
already names *concretely* — `snapshot_coordinator: &dyn SnapshotCoordinator`, `metrics_recorder:
&dyn MetricsRecorder`, `acl_manager: &AclManager`, `command_registry: &CommandRegistry` — all core
types, correctly *not* wrapped in bespoke providers. The design already applies exactly the fold
rule ("concrete when core can name it, provider only when it cannot"); the provider set is already
minimal against the crate boundary. There is nothing to fold — the leverage is entirely in the
builder collapse.

## Why this is the right depth

- **Locality.** The `ConnCtx` field list, its placeholder/Noop defaults, and its cap-absent baseline
  live in one place — `ConnCtx::new` — beside the struct they populate. The borrow-split (which
  handler sub-fields are ambient vs. capability) lives in one place, `base_ctx`, beside the handler
  it splits. Neither concern is smeared across fourteen literals.
- **Leverage.** Adding a subsystem to the seam goes from a fourteen-site lockstep edit to one
  (`ConnCtx::new` + the struct field), with `base_ctx` the only production wiring change. The
  `.with_*` vocabulary makes each dispatch path state *only* its capability, so the difference
  between the AUTH path and the CLIENT path is one method call, not a diff of two 25-line literals.
- **Deletion test.** The change deletes: four `NOOP_INFO`/`NOOP_SCRIPTING` static re-declarations,
  the `username: ""` / `protocol_version: default()` placeholder pairs at four call sites, and ~13
  copies of the 25-field literal — pure boilerplate, zero behavior. Deleting duplicated construction
  while the one seam and its tests stand unchanged is the signature of slimming an interface, not
  removing a module.
- **Deepens, does not fork.** No new trait, no second seam, no change to any executor or to the
  registry `CommandImpl::Connection` dispatch. `ConnCtx` keeps its exact shape and every field; only
  its *construction* is centralized. The mutable-`ConnCtx` mechanism from proposal
  [04-connection-state-encapsulation](04-connection-state-encapsulation.md) (`conn_state: Option<&mut
  dyn ConnStateMut>`) is preserved verbatim — `with_conn_state` is just its typed constructor.

## Testing impact

- **No new behavior to test.** Construction centralization is refactor-only; the existing socketless
  command tests (the nine fixtures) are the regression surface and must pass unchanged after they
  switch to `ConnCtx::new(..).with_*`.
- **Fixture ergonomics improve.** A `ConnCtx::new` + `.with_*` fixture is shorter and states only the
  caps the command reads, which is itself a small correctness win (a fixture can no longer silently
  disagree with production on an ambient field it copied wrong).
- **One targeted assertion worth adding:** a compile-level check (or doc-test) that the read-only
  `conn_ctx` overrides `info`/`scripting`/`debug`/`protocol_version`/`username` away from the
  defaults — the one place where "the default is a placeholder" must not leak into the read path.
- Run `just test frogdb-server` (connection-command suites) and `just check frogdb-core` for the
  new `ConnCtx::new` signature.

## Risks / open questions

- **`ConnCtx::new` arity.** Sixteen ambient parameters is a lot; `#[allow(clippy::too_many_arguments)]`
  is the honest tradeoff versus a second struct. An `AmbientDeps<'a>` bundle struct would cut the
  arity but re-introduces a field list to keep in sync and complicates the disjoint-borrow story at
  `base_ctx` (the caller must still name each sub-field to preserve disjointness). Preference:
  positional `new` now; revisit a bundle only if a *third* ambient field lands.
- **Consuming `.with_*` vs. mutating.** The sketch uses `self`-consuming builders (`mut self ->
  Self`). For the mutable paths this threads the `&mut` borrow through cleanly; verify the borrow
  checker accepts `base_ctx()` (a temporary) being moved into `.with_conn_state(&mut self.state)`
  without the temporary extending the ambient borrow past the `&mut`. If it fights, fall back to
  field assignment on a `let mut ctx` (still one field-list home, slightly less fluent).
- **`base_ctx` must stay a sub-field borrow.** If anyone "simplifies" `base_ctx` to take `&self` and
  read `self.state` inside, the disjointness that frees `self.state`/`self.tracking_io` for the caps
  is lost and the mutable builders stop compiling. Worth a comment pinning the invariant, since it is
  the whole reason the split lives in the server adapter and not in core.
- **`const NOOP_*` promotion.** The sketch promotes the no-op providers to `const` references inside
  `new`; confirm `NoopInfoProvider`/`NoopScriptingProvider` are zero-field (they are) so `const`
  promotion is valid, else keep the existing `static` pattern (once, in `new`, not four times).

## Implementation notes (2026-07-16)

Implemented in `1f2e35dc`, as designed with one anticipated adjustment:

- **`base_ctx` is an associated fn, not a `&self` method.** The "consuming `.with_*` vs. mutating"
  risk resolved one level earlier than the sketch anticipated: a `&self` method borrows the *whole*
  handler for the returned `ConnCtx`'s lifetime, so no builder style downstream can free
  `self.state`/`self.tracking_io` for the `&mut` caps. `ConnectionHandler::base_ctx(admin, core,
  observability, cluster, memory_diag, num_shards)` takes the disjoint dep groups as explicit
  parameters instead; each call site names the sub-field borrows, preserving disjointness exactly as
  the "must stay a sub-field borrow" risk note required (the invariant is pinned in its doc comment).
  The `self`-consuming `.with_*` builders then compose without a fight.
- `ConnCtx::new` (core) is the one field-list home; the `NOOP_*` statics collapsed into two `const`
  promotions inside it (the providers are zero-sized, so promotion is valid). All four
  `username: ""`/`protocol_version: default()` placeholder pairs and both re-pasted static
  declarations are gone from the server crate.
- The five production literals became `base_ctx` + one-line cap tails
  (`with_full_reads` / `with_conn_state` / `with_conn_state().with_tracking()` / `with_pubsub` /
  `with_monitor`); the nine fixtures call `ConnCtx::new(..)` directly, plus `with_username` where
  the command under test reads identity without `conn_state` (ACL) or the fixture previously pinned
  `"default"`. The debug fixture assigns its `Option<&dyn DebugProvider>` slot directly (fields are
  public).
- The "one targeted assertion" landed as a core unit test
  (`conn_command::tests::new_defaults_are_placeholders_and_with_full_reads_overrides_them`): `new`'s
  defaults are the documented placeholders/no-ops/absent caps, and `with_full_reads` overrides every
  one of them.
- All 11 provider traits kept, per the verdict table — no trait was folded.
</content>
</invoke>
