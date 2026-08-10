# Proposal 66 — `ShardWorkerBuilder`: make the *wiring recipe* the interface, not just the assembly

Round 38 · lane: server composition · effort **M** · candidate SV4
Verified against the current tree at `55d73174` (worktree `arch-round-38-99`); every citation
below was read at that SHA, none inherited from the candidate brief. **Re-verified line by line
at HEAD `159cb7a2`** after adversarial review; every correction that pass produced is folded in
below, and the two claims it refuted (§7's universal "asserted by nothing", and the `ShardWiring`
`Default`) are restated rather than repeated.

## Summary

`ShardWorkerBuilder` (`shard/builder.rs`) is two different modules wearing one name.

Its **assembly** half is real and earns its keep: `try_build` (`builder.rs:333-478`) is the only
place in the tree that writes the `ShardWorker { .. }` struct literal — **27 field initializers**
(26 unconditional plus the `cfg(any(test, feature = "shard-driver"))` `driven_ticks`), matching
`ShardWorker`'s declaration one-for-one (`worker.rs:106-209`), nine of them grouped sub-structs —
and every constructor funnels through it. Its **wiring** half is a shell.
Ten `with_*` methods exist for exactly the cluster / shared-flag / registry inputs production
needs, and **not one of them has ever had a caller**: `with_cluster`, `with_quorum_checker`,
`with_function_registry`, `with_recovery_stats`, `with_per_request_spans`, `as_replica`,
`with_core_deps`, `with_persistence_deps`, `with_cluster_deps`, plus the four dependency-group
structs those last three exist to accept (`ShardCoreDeps`, `ShardPersistenceDeps`,
`ShardClusterDeps`, `ShardConfig`) — dead, workspace-wide, verified by grep.

Production wires its shards somewhere else entirely: `server/shards.rs:157-302` applies **25
post-construction setter calls** (24 distinct methods) to a worker the builder already handed
back. Of those 24, the builder can express **10**; the remaining **14** have no builder method
at all. And `frogdb-shard-harness` — the deterministic driver that exists to exercise a real
`ShardWorker` — **cannot reach that recipe at any price**: its `Cargo.toml` depends on
`frogdb-core`, not on the server crate, so the 25-line wiring sequence in `server/shards.rs` is
structurally out of its reach. The harness therefore builds a worker with four setters
(`harness.rs:82-87`) and every scenario runs against a shard whose role controller, replication
tracker, cluster handles, JSON limits, wait-queue limits, keyspace stats, data dir, search
manager, and six shared `CONFIG SET` atomics are all defaults.

The proposal is to move the *recipe* into `frogdb-core` behind the builder — a `ShardWiring`
value object plus the 14 missing `with_*` methods — so the server supplies **values** and the
builder owns **order and completeness**, and the harness can construct the same wiring the
server does. Then delete the ten dead builder methods, the four dead dependency-group structs,
the one dead wrapper constructor (`ShardWorker::new`), and the `pub` on the setters whose sole
production caller was `shards.rs`.

The other three wrapper constructors **stay**. They are not dead and this proposal does not
delete them: `ShardWorker::with_eviction` has **twelve** call sites — eleven core unit tests
(`event_loop.rs:633`, `vll.rs:123`, `blocking.rs:1599`, `panic_guard.rs:346`, `eviction.rs:520`,
`execution.rs:1365`, `dispatch_core.rs:253`, `diagnostics.rs:504`, `post_execution.rs:973` and
`:995`, `rollback.rs:151`) plus the production branch at `shards.rs:143` — and
`with_fake_persistence` / `with_persistence` are the other two production branches
(`shards.rs:107`, `:126`). SV4-c retires only the *production* uses of the three; the eleven
core-test callers keep compiling untouched, and budgeting them is explicitly *not* part of this
change.

**Live-vs-latent ruling.** The candidate brief's headline — *"builder.rs claims 'single
construction path' … (doc false)"* — **does not survive verification, and is withdrawn**. The
doc at `builder.rs:73-79` scopes its claim precisely ("there is exactly one place that
assembles the worker's grouped sub-structs") and that claim is **true at HEAD**: all four
convenience constructors call `build()` → `try_build()`. A *different* doc is provably false —
the module header at `mod.rs:14-25`, whose example calls a four-argument
`ShardWorkerBuilder::new` (the real one takes two, `builder.rs:127`) and whose "Dependency
Groups" section advertises three structs nobody constructs. That is the doc-only hotfix, below.
**No live bug is claimed anywhere in this proposal.** One latent inconsistency is identified
with its named precondition (the recovered store dropped on the no-Rocks branch, §6) and one
proven behavioral delta is flagged for the implementation to decide (the double-built script
executor, §5).

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/core/src/shard/builder.rs` | 523 | **the change.** `ShardWorkerBuilder` struct `:94-123`, `new` `:127-158`, the 24 `with_*`/`as_replica` methods `:161-327`, `try_build` `:333-478`, `build` `:489-492`, `ShardBuilderError` `:41-56`, `WalMode` `:64-71`, doc `:73-93`, fake-WAL unit test `:495-523` |
| `frogdb-server/crates/server/src/server/shards.rs` | 367 | **the change.** `ShardSpawnContext` (30 fields) `:20-56`, `spawn_shard_workers` `:66-315`, the three construction branches `:104-155`, the 25 setter calls `:157-302`, search recovery `:262-293`, tick pump `:332-367` |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | **the change.** Four convenience constructors `:385-493` (`new` `:385`, `with_eviction` `:403`, `with_fake_persistence` `:435`, `with_persistence` `:464`); the **25** `pub` setters/installers at `:231`, `:237`, `:258`, `:272`, `:279`, `:284`, `:289`, `:295`, `:303`, `:310`, `:319`, `:496`, `:510`, `:515`, `:524`, `:531`, `:538`, `:547`, `:566`, `:571`, `:576`, `:581`, `:586`, `:591`, `:596` (of which `shards.rs` drives 23 — all but `set_is_replica` `:237` and the dead `set_replication_broadcaster` `:566`); the struct declaration `:106-209`; `data_dir()` default `:223-228` |
| `frogdb-server/crates/core/src/shard/types.rs` | 1498 | **the change (deletions).** The `// Dependency Groups for ShardWorkerBuilder` banner `:665-667`, `ShardCoreDeps` `:669-686`, `ShardPersistenceDeps` `:688-700`, `ShardClusterDeps` `:702-731` (struct + its `impl`), `ShardConfig` `:733-744` — all dead; contiguous, so the deletion is the single span `:665-744`. Untouched above/below: `ShardIdentity` `:30-…` (`set_data_dir` `:72`, `set_is_replica_flag`, `role_controller`), `ShardPersistence`, and the two `// FM-` tagged tests at `:1268` (FM-PERSISTENCE-005) and `:1421` (FM-REPLICATION-061) |
| `frogdb-server/crates/core/src/shard/mod.rs` | 96 | **the doc hotfix.** Header `:1-25` (false example `:19`, dead "Dependency Groups" `:7-12`); re-export lists `:70`, `:87-94` shrink with the deletions |
| `frogdb-server/crates/shard-harness/src/harness.rs` | 399 | **the payoff.** `ShardDriver::new` `:60-95` — the builder call `:82-87` (four setters); `worker()` `&mut` escape hatch `:104-106` |
| `frogdb-server/crates/shard-harness/Cargo.toml` | 32 | **the constraint.** Dependencies are `frogdb-core` (features `shard-driver`, `fake-wal`), `frogdb-commands`, `frogdb-protocol`, `frogdb-vll` — **no `frogdb-server`**. This is why the recipe must live in core. |
| `frogdb-server/crates/server/src/server/mod.rs` | 598 | **edge, not owned.** The single `ShardSpawnContext` literal `:376-407` — 30 `infra.*`/`cluster.*`/`repl.*` field reads. Proposal 63's surface; see Risks |
| `frogdb-server/crates/server/src/server/init.rs` | 669 | **read-only.** Produces `rocks_store` / `recovered_stores` / `recovery_stats` `:276-281`, `:469-470`; `InitResult` fields `:54-55` |
| `frogdb-server/crates/recovery/src/lib.rs` | 270 | **read-only — the precondition source.** The `rocks_backed` predicate `:188-190` (**not** `persistence_active`; no such identifier exists), the fresh-stores arm `:222`, `fresh_shards` `:268` (LOCKED area: persistence/recovery, gate 0.85) |
| `frogdb-server/crates/recovery/src/shards.rs` | 183 | **read-only.** `restore` `:44-67` — "no existing data ⇒ `num_shards` fresh empty stores" `:60-66` |
| `frogdb-server/crates/core/src/shard/search/lifecycle.rs` | 680 | **read-only.** `IndexLifecycleManager::new` `:80-90` (pure, no I/O), `set_data_dir` `:94-96`, `index_dir` `:101-106` |
| `frogdb-server/crates/core/src/shard/wait_queue.rs` | 931 | **read-only.** `new()` `:116-118` = `with_limits(10000, 50000)`; `with_limits` `:121` |
| `frogdb-server/crates/core/src/scripting/executor.rs` | — | **read-only.** `ScriptExecutor::new` `:128-140` — builds a `LuaVm` (`lua_vm.rs:100`) plus a `ScriptCache` |
| `frogdb-server/crates/server/src/connection/builder.rs` | 267 | **not touched.** Proposal 67's file. Its own `with_cluster` `:104` / `try_build` `:210` are a *different* type — named here only so a grep for those symbols does not confuse the two |

## Problem

### 1. The doc claim, adjudicated

`builder.rs:73-79`:

```rust
/// Builder for creating [`ShardWorker`] instances with a fluent API.
///
/// This is the **single construction path** for [`ShardWorker`]: the
/// [`ShardWorker::new`], [`ShardWorker::with_eviction`], and
/// [`ShardWorker::with_persistence`] convenience constructors all funnel through
/// [`try_build`](Self::try_build), so there is exactly one place that assembles
/// the worker's grouped sub-structs.
```

Traced, all four: `ShardWorker::new` `:393-398`, `with_eviction` `:415-424`,
`with_fake_persistence` `:448-459`, `with_persistence` `:480-492` — each ends `.build()`, which
is `self.try_build().expect(..)` (`builder.rs:489-492`). **The claim holds as written.** The
brief's "doc false" is not sustained; the finding is downgraded and the proposal does not rest
on it.

Two smaller inaccuracies in the same comment are worth recording because they are the tell for
the real problem: `ShardWorker::new` is named as a flagship caller and has **zero call sites in
the workspace** — the doc comment at `builder.rs:76` is the only occurrence of the string
`ShardWorker::new` outside its own definition. And the list omits `with_fake_persistence`
(`worker.rs:435`), which production does use, under `#[cfg(feature = "turmoil")]`.

What *is* false is the module header two files up, `mod.rs:14-25`:

```rust
//! Use [`ShardWorkerBuilder`] for a fluent construction API:
//!
//! ```rust,ignore
//! let worker = ShardWorkerBuilder::new(shard_id, num_shards, message_rx, new_conn_rx)
//!     .with_registry(registry)
//!     .with_shard_senders(shard_senders)
//!     .with_eviction(eviction_config)
//!     .with_persistence(rocks_store, wal_writer)
//!     .build();
```

`ShardWorkerBuilder::new` takes **two** arguments (`builder.rs:127`), and `with_persistence`
takes `(Arc<RocksStore>, WalConfig)` (`builder.rs:227`), not a wal *writer*. `rust,ignore`
means no doctest ever caught it. The header's "Dependency Groups" section above it
(`mod.rs:7-12`) advertises three types with zero constructors anywhere. Doc-only hotfix, below.

### 2. The census: what production builds versus what production wires

`spawn_shard_workers` picks one of three constructors per shard (`shards.rs:104-155`) and then
applies 25 setter calls before `run()` (`shards.rs:157-302`). Every one of them, in order:

| # | `shards.rs` | call | builder method today |
|---:|---:|---|---|
| 1 | `:158` | `set_function_registry` | `with_function_registry` `:248` — **0 callers** |
| 2 | `:161` | `set_keyspace_stats` | — none |
| 3 | `:164` | `set_recovery_stats` | `with_recovery_stats` `:242` — **0 callers** |
| 4 | `:170` | `store.set_warm_store` | — none |
| 5 | `:175` | `set_raft` | `with_cluster` `:254` — **0 callers** |
| 6 | `:178` | `set_cluster_state` | `with_cluster` |
| 7 | `:181` | `set_node_id` | `with_cluster` |
| 8 | `:184` | `set_network_factory` | `with_cluster` |
| 9 | `:187` | `set_quorum_checker` (detector) | `with_quorum_checker` `:269` — **0 callers** |
| 10 | `:189` | `set_quorum_checker` (repl fallback) | `with_quorum_checker` |
| 11 | `:193` | `set_wait_queue_limits` | — none |
| 12 | `:200` | `set_json_limits` | — none |
| 13 | `:203` | `set_is_replica_flag` | — none (`as_replica` `:275` is **different**, see below, and has 0 callers) |
| 14 | `:207` | `set_role_controller` | — none |
| 15 | `:210` | `set_expiry_paused_flag` | — none |
| 16 | `:214` | `set_replication_tracker` | — none |
| 17 | `:225` | `set_per_request_spans` | `with_per_request_spans` `:281` — **0 callers** |
| 18 | `:230` | `set_hotshards_enabled_flag` | — none |
| 19 | `:233` | `set_wal_failure_policy_flag` | `with_wal_failure_policy` `:287` — 1 caller, a core unit test (`rollback.rs:557`) |
| 20 | `:236` | `set_notify_keyspace_events` | — none |
| 21 | `:239` | `set_shard_memory_used` | — none |
| 22 | `:244` | `set_scripting_config` | `with_scripting` `:215` — 1 caller, a harness test (`script_timeout_effects.rs:90`) |
| 23 | `:251` | `set_data_dir` | — none |
| 24 | `:292` | `install_search_manager` | — none |
| 25 | `:302` | `set_driven_ticks` (`cfg(turmoil)`) | — none |

**10 of 24 distinct wirings are expressible by the builder; 14 are not.** (The brief's "builder
covers ~12" is an overcount by two — `as_replica` and `with_store` do not cover
`set_is_replica_flag` and `set_warm_store`.)

`as_replica` versus `set_is_replica_flag` is the sharpest instance of the split. `as_replica`
(`builder.rs:275-278`) sets a `bool` that `ShardIdentity::new` (`types.rs:44-53`) wraps in a
**fresh** `Arc<AtomicBool>`. `set_is_replica_flag` (`worker.rs:272-274` →
`ShardIdentity::set_is_replica_flag`) **replaces** that `Arc` with the server-wide one, which is
the entire point — `worker.rs:266-271` says so: "all shards, the acceptor, and connection
handlers … share a single `Arc<AtomicBool>` so that `REPLICAOF NO ONE` can toggle replica status
server-wide with a single atomic store." A builder-built shard is not merely unconfigured, it is
wired to a flag nobody else can see. The builder offers the version production cannot use, and
not the version it must.

### 3. The half production needs has never had a caller

Grepped workspace-wide (excluding `target/`, `builder.rs`'s own definitions and the
`lib.rs`/`mod.rs` re-export lines), the following have **zero** call sites:

`with_cluster`, `with_quorum_checker`, `with_function_registry`, `with_recovery_stats`,
`with_per_request_spans`, `as_replica`, `with_core_deps` (`:302`), `with_persistence_deps`
(`:312`), `with_cluster_deps` (`:320`), `ShardCoreDeps` (`types.rs:671`), `ShardPersistenceDeps`
(`:690`), `ShardClusterDeps` (`:704`, including `standalone()` and `is_cluster_mode()`),
`ShardConfig` (`:735`), and `ShardWorker::set_replication_broadcaster` (`worker.rs:566`).

That is not a builder anyone stopped using; it is a builder whose production-shaped half was
never adopted. `ShardCoreDeps` and friends were introduced *to make the builder ergonomic for
exactly this call site* — `types.rs:666` literally reads `// Dependency Groups for
ShardWorkerBuilder` — and the call site went the other way.

### 4. The fallible half is a pass-through

`try_build` (`builder.rs:333`) returns `Result<ShardWorker, ShardBuilderError>` and checks four
required fields (`:334-345`). It has **one caller** in the workspace: `build()` at `:490`, which
does `.expect("ShardWorkerBuilder: missing required fields")`. `ShardBuilderError` is never
constructed outside `builder.rs`, never matched, and never printed — its `Display` impl
(`:46-54`) and `std::error::Error` impl (`:56`) have no consumer. So four `Option` fields, four
`ok_or` chains, an error enum with two trait impls, and a re-export exist to convert "missing
field" into a panic that a two-more-arguments `new()` would have made a compile error.

Apply the deletion test to *this* module: delete `ShardBuilderError` + `try_build`'s fallibility
and complexity **vanishes** — nothing reappears at any caller, because there is one caller and
it panics. Apply it to the assembly body of `try_build` (`:347-477`) and complexity **reappears
in five places**: the 27-initializer `ShardWorker { .. }` literal, the `WalMode` match
(`:376-405`), the `KeyspaceNotificationCoordinator` routing decision (`:420-425`), the
`ScriptExecutor` fallible construction (`:412-416`), and the persistence/observability/eviction
sub-struct grouping. The assembly is a deep module. The `Result` around it is a wrapper.

### 5. Ordering *is* behavior — and today the builder does work production discards

Four of the 25 setters are not plain field assignment. Enumerated so the implementation can
prove preservation rather than assume it:

**(a) `set_wait_queue_limits` (`shards.rs:193` → `worker.rs:515-521`) replaces the queue
wholesale:** `self.wait_queue = ShardWaitQueue::with_limits(..)`. Any waiter registered before
it would be silently dropped. Safe today only because the call precedes `run()`
(`shards.rs:307-309`). Moving it into `try_build` — construct `ShardWaitQueue::with_limits`
when set, `::new()` when not — preserves this *and* removes the hazard, since there is then no
window in which a queue can be replaced. Note `new()` **is** `with_limits(10000, 50000)`
(`wait_queue.rs:116-118`), so the unset default is bit-identical.

**(b) `set_scripting_config` (`shards.rs:244-247` → `worker.rs:496-507`) builds a second Lua
VM.** `try_build` already ran `ScriptExecutor::new(self.scripting_config)` (`builder.rs:412`),
defaulting to `ScriptingConfig::default()`; production then throws that executor away and
constructs another with `lua_time_limit_override: Some(..)`. Every shard builds two
`LuaVm`s at boot (`executor.rs:129` → `lua_vm.rs:100`). Routing through `with_scripting` builds
one — **but the two paths differ on the error arm**: `try_build` logs and leaves the executor
`None` (`:413-416`), whereas `set_scripting_config` logs and **keeps the previously built
default executor** (`worker.rs:499-506`). A `ScriptExecutor::new` failure is therefore
"scripting degraded to defaults" today and would become "scripting unavailable" after the move.
This is the one proven behavioral delta in the proposal. The implementation must either
preserve it explicitly (on `Err`, fall back to `ScriptExecutor::new(ScriptingConfig::default())`
inside `try_build`) or land the change as an intentional, documented one. It must not be
discovered during review.

**(c) `set_data_dir` (`:251`) → `install_search_manager` (`:292`) is a build-then-discard.**
`try_build` constructs `IndexLifecycleManager::new(shard_id, PathBuf::from("data"), rocks)` —
a **hardcoded** `"data"` at `builder.rs:409`, a second copy of the same literal that
`worker.rs:227` uses as its fallback. Production then corrects the path (`set_data_dir` sets it
on both the manager and `ShardIdentity`, `worker.rs:231-235`) and, when persistence is enabled,
replaces the whole manager with the one `IndexLifecycleManager::recover` built (`:292`). The
builder's manager is dead on that branch. With `with_data_dir` the hardcode disappears (the
manager is constructed with the real dir), and `with_search_manager` overrides afterwards inside
`try_build` — same end state, provable by inspection because `recover` is handed the same
`data_dir` (`shards.rs:265-269`) that `set_data_dir` sets.

**(d) `install_search_manager` sits behind a `?`.** `IndexLifecycleManager::recover(..)`
(`shards.rs:267-269`) can abort the whole spawn. Under the builder the `recover` call moves
*above* `build()`, so the `?` fires before that shard's worker exists rather than after. The
error propagates identically (`anyhow` out of `spawn_shard_workers`, boot fails). Pre-existing
and unchanged either way: shards `0..N-1` are already **spawned and running** when shard `N`'s
recovery fails, and `spawn_shard_workers` returns `Err` leaving them orphaned. Out of scope,
recorded here so it is not mistaken for a regression introduced by this change.

Everything else in `:157-302` is assignment to disjoint fields and is order-independent. The
`cfg(turmoil)` tick pump (`shards.rs:304`) spawns a task and **stays in the server crate** —
only the `set_driven_ticks(true)` flag moves.

### 6. One latent inconsistency, with its precondition named

The recovered store is passed to two of the three constructors and **dropped on the third**.
`shards.rs:91-93` takes `(store, _expiry_index)` from `recovered_iter`; `with_fake_persistence`
(`:107-119`) and `with_persistence` (`:126-141`) pass it; `with_eviction` (`:143-154`) has no
`store` parameter at all (`worker.rs:403-413`), so on that branch the recovered store is dropped
on the floor and the worker starts on `HashMapStore::default()` (`builder.rs:437`).

**Latent, not live.** Precondition: the `with_eviction` branch is reached only when
`ctx.rocks_store.is_none()`, and `frogdb-recovery` returns `rocks: None` exactly when its
`rocks_backed` predicate is false (`recovery/src/lib.rs:188-190`), in which case `shards` is
`fresh_shards(num_shards)` (`:222`, defined `:268`) — empty stores. `rocks_backed` is
`inputs.persistence.enabled && !inputs.persistence.mode.eq_ignore_ascii_case("fake")`, so it is
false in **two** distinct configurations — persistence disabled, and the `fake` WAL mode that is
enabled-but-RocksDB-less — and *both* take the fresh-stores path. (The proposal previously named
this predicate `persistence_active`, which does not exist in the tree; the correction widens the
precondition rather than narrowing it, so the "unobservable today" conclusion is unchanged and
the case for removing the dependence on it is stronger.) The drop is therefore unobservable
today.
It is a cross-crate precondition, enforced nowhere, in a **LOCKED** area (persistence/recovery,
gate 0.85) that this proposal does not edit. Routing the branch through `with_store(store)`
makes the code state-independent of that precondition without depending on it — a strict
improvement, and behavior-identical under it.

### 7. What the harness can and cannot do

`ShardDriver::new` (`harness.rs:60-95`) builds each worker with four setters:

```rust
let worker = ShardWorkerBuilder::new(shard_id, n)
    .with_message_rx(msg_rx)
    .with_new_conn_rx(conn_rx)
    .with_shard_senders(senders.clone())
    .with_registry(registry.clone())
    .build();
```

The harness *does* expose `worker(shard) -> &mut ShardWorker` (`harness.rs:104-106`), so a test
can call any `pub` setter by hand — exactly one does (`scenario_s6.rs:57`,
`set_wal_failure_policy_flag`). So the accurate claim is not "the harness cannot wire a shard";
it is:

- **The recipe is unreachable.** `frogdb-shard-harness/Cargo.toml` lists `frogdb-core`,
  `frogdb-commands`, `frogdb-protocol`, `frogdb-vll` — **not `frogdb-server`**. The 25-call
  sequence lives in `server/shards.rs`, in a `pub(super)` function
  (`shards.rs:66`) of a crate the harness does not depend on. No harness test can call it, import
  it, or assert against it. Every harness scenario must re-derive prod wiring by hand, from
  reading another crate.
- **Nothing couples the two.** Adding a 26th `worker.set_*` line to `shards.rs` — the routine
  way a new shared handle reaches shards — compiles green and changes no harness scenario. There
  is no count pin, no exhaustive struct, no trait. The twelve harness scenario files
  (`shard-harness/tests/`, plus `main.rs`) drift further from production with each addition,
  silently.
- **The wiring is covered only indirectly and expensively, never at the seam.** The strong form
  of this claim — "no test anywhere fails if you delete a wiring line" — is **false for at least
  two of the 24**, and is withdrawn:
  - `set_notify_keyspace_events` (`shards.rs:236`) *is* covered end to end. `ConfigManager`
    owns the `Arc<AtomicU32>` (`runtime_config.rs:769`), `CONFIG SET notify-keyspace-events`
    stores into it (`:2422`), `notify_keyspace_events_flags()` (`:3588`) hands out that same
    `Arc`, and the worker holds it (`worker.rs:185`, set at `:538-539`) for
    `keyspace_notify.rs` to read. Delete the line and the shard reads a private zeroed atomic
    the `CONFIG SET` never touches, so the booted keyspace-notification tests fail —
    `test_lrem_emits_keyspace_notification` (`integration_pubsub.rs:56`) among them, plus the
    `pubsub_tcl` / `set_tcl` regression suites.
  - `set_json_limits` (`shards.rs:200`) is likewise covered:
    `test_json_set_respects_configured_max_depth` (`integration_json.rs:872`) boots a server at
    `json_max_depth = 4` and asserts a depth-5 document is rejected; without the line every
    shard keeps `JsonLimits::default()` and accepts it.

  The accurate and still-sufficient claim is narrower: **the wiring is asserted only through
  booted-server E2E, never at the seam, and 14 of the 24 have no in-core entry point at all.**
  Coverage that exists is coverage of the *effect*, several crates downstream, at the price of a
  full server boot — it names no wiring line, so a deleted one is diagnosed as "keyspace
  notifications broke" rather than "shard 3 never got the flag". And it is not uniform: the
  chosen example survives intact. `ConfigManager::wal_failure_policy_flag()`
  (`runtime_config.rs:1167`) has exactly one caller in the entire server crate — `shards.rs:233`.
  Delete it and every shard falls back to `WalFailurePolicy::default()` (`builder.rs:372-375`),
  and the FM-PERSISTENCE-005 test at `types.rs:1268-1272` still passes, because it constructs a
  `ShardPersistence` directly. No test in the tree asserts that a *production-spawned* shard
  observes the server-wide handle it is supposed to share.

Concretely, the fields a harness-built worker has today that a production one does not: the
role controller is `None` (`types.rs:40-41`) so `master_host` / `master_port` /
`master_link_up` / `master_sync_error` in `command_context` (`worker.rs:371-375`) are all dead;
`replication_tracker` is `None` (`worker.rs:362`); the cluster quad and quorum checker are
`None`; `is_replica` is a private `Arc` no one else holds; `json_limits` is
`JsonLimits::default()` (`builder.rs:476`); `notify_keyspace_events` is `0`
(`builder.rs:470`); `expiry_paused` is a private flag; the hot-shard counters are unswitchable;
`shard_memory_used` is unset; `keyspace_stats` is unset; `data_dir` is `None` with a `"data"`
fallback; `search` is an empty manager rooted at literal `"data"`; the wait queue is
`(10000, 50000)` regardless of `[blocking]`.

## Why this shape, in the vocabulary

`ShardWorkerBuilder`'s **interface** today is 24 optional setters plus a panicking terminal — a
caller must know which subset production uses, in what order, and which 14 wirings the interface
cannot express at all. That is a **shallow** module: the interface is nearly as complex as the
implementation, and the part that matters most (which fields a *correct* shard must have) is not
in the interface at all — it is in another crate, as a comment-annotated straight line.

The **depth** available here is not "more `with_*` methods". It is that most of the 24 wirings
are **node-wide handles, identical for every shard** — the same `Arc` cloned N times: the role
controller, is-replica flag, expiry-paused flag, per-request-spans flag, hot-shards flag,
wal-failure-policy flag, keyspace-notify flags, keyspace stats, shard-memory vec, recovery
stats, function registry, replication tracker, the cluster quad, the quorum checker, JSON
limits, wait-queue limits, data dir, eviction config, scripting config, registry, senders,
metrics, slowlog id, broadcaster. Only six inputs are genuinely per-shard: `shard_id`,
`message_rx`, `new_conn_rx`, the recovered `store`, the warm-store attachment, and the recovered
search manager.

So the **seam** belongs between *fleet* and *shard*, and it belongs in `frogdb-core`, because
the harness's dependency graph is what decides which crate a shared recipe can live in. Behind
that seam: one value object built once per node, applied N times, with the order written once
inside `try_build`.

**Leverage**: a caller — server or harness — learns one type instead of 24 setter names and
their ordering constraints. **Locality**: a new shared handle is added in one struct; every
adapter that constructs it is forced by the compiler to decide what to pass, instead of silently
inheriting a default. This holds *only* because `ShardWiring` has no `Default` — see SV4-b,
where that is a stated design constraint rather than an afterthought. **Two adapters, not one**:
production (`server/shards.rs`) and the harness
(`shard-harness`) — which is the bar for introducing a seam at all, and is precisely what
`ShardCoreDeps` never cleared.

The deletion test on the proposed `ShardWiring`: delete it and the 24-step ordering reappears at
both adapters, and the harness's copy has to be written by reading the server crate it cannot
depend on. It earns its keep. The deletion test on `ShardCoreDeps`/`ShardPersistenceDeps`/
`ShardClusterDeps`/`ShardConfig` as they exist today: delete them and **nothing reappears
anywhere**. They go.

## Proposed change

Five steps, each independently compilable and reviewable.

### SV4-a — delete what nothing calls (no behavior)

Remove: `with_core_deps`, `with_persistence_deps`, `with_cluster_deps` (`builder.rs:302-327`);
`as_replica` (`:275-278`); the `// Dependency Groups for ShardWorkerBuilder` block —
`ShardCoreDeps`, `ShardPersistenceDeps`, `ShardClusterDeps`, `ShardConfig` and their banner,
contiguous at `types.rs:665-744`; `ShardWorker::new` (`worker.rs:385-399`) **and only that
constructor** — `with_eviction`, `with_fake_persistence` and `with_persistence` all have live
callers (see Summary) and are untouched by SV4-a;
`ShardWorker::set_replication_broadcaster` (`worker.rs:566-568`); the matching re-exports in
`shard/mod.rs:70`, `:89-90` and `core/src/lib.rs:147-149`. Roughly 150 lines, zero call sites
touched. `builder.rs:22-33`'s import of `ShardCoreDeps`/`ShardClusterDeps`/
`ShardPersistenceDeps` goes with them.

### SV4-b — `ShardWiring` in `frogdb-core`, plus the 14 missing `with_*`

A new `shard/wiring.rs` holding one `#[derive(Clone)]` value object of node-wide handles and
`ShardWorkerBuilder::with_wiring(&ShardWiring)` applying it.

**`ShardWiring` does not implement `Default`.** It is constructed either field-by-field (every
field named, so adding one is a compile error at every construction site) or through the single
explicit escape hatch `ShardWiring::none()` — "deliberately unwired", byte-identical to today's
builder defaults, for tests that genuinely want a bare shard. This is the whole Locality claim:
a derived `Default` would let a new shared handle be added and silently inherited as `None` by
every adapter, which is exactly the drift the seam exists to stop, and it would contradict the
compile-error obligation stated under Testability. The cost of dropping `Default` is nil: the
thirteen existing `ShardWorkerBuilder::new` call sites keep compiling because they use the
individual `with_*` methods and never mention `ShardWiring` at all — not because a `Default`
covers them.

Fields, one per
distinct wiring in §2's table, minus the six per-shard ones: `function_registry`,
`keyspace_stats`, `recovery_stats`, `cluster: Option<ClusterHandles>` (the quad, kept together
because production sets them from one `Option` each and they are meaningless apart),
`quorum_checker`, `wait_queue_limits`, `json_limits`, `is_replica_flag`, `role_controller`,
`expiry_paused_flag`, `replication_tracker`, `per_request_spans`, `hotshards_enabled_flag`,
`wal_failure_policy`, `notify_keyspace_events`, `shard_memory_used`, `scripting`, `data_dir`,
`eviction`, `registry`, `shard_senders`, `metrics_recorder`, `slowlog_next_id`,
`replication_broadcaster`, `wal_mode`.

Alongside it, the 14 fluent methods the table says are missing — `with_keyspace_stats`,
`with_warm_store`, `with_wait_queue_limits`, `with_json_limits`, `with_is_replica_flag`,
`with_role_controller`, `with_expiry_paused_flag`, `with_replication_tracker`,
`with_hotshards_enabled_flag`, `with_notify_keyspace_events`, `with_shard_memory_used`,
`with_data_dir`, `with_search_manager`, `with_driven_ticks` (the last under the existing
`#[cfg(any(test, feature = "shard-driver"))]`, matching `worker.rs:257-258`; note the server
calls it under `cfg(feature = "turmoil")`, so that feature must continue to imply core's
`shard-driver` — it does today, since `shards.rs:302` compiles).

`try_build` applies them in the §5 order, with the four ordering-sensitive cases handled as
argued there: wait queue constructed with limits rather than replaced; search manager built
with the real `data_dir` and then overridden by a supplied one; scripting executor built once
with an explicit decision on the `Err` arm; warm store attached to `self.store` after
`unwrap_or_default()`.

### SV4-c — route production through it

`spawn_shard_workers` builds one `ShardWiring` before the loop from `ctx`, then per shard:

```rust
let mut builder = ShardWorkerBuilder::new(shard_id, ctx.num_shards)
    .with_message_rx(msg_rx)
    .with_new_conn_rx(conn_rx)
    .with_store(store)              // now on every branch — see §6
    .with_wiring(&wiring);
// per-shard, branch-dependent:
//   persistence / fake-WAL selection, warm store, recovered search manager
let worker = builder.build();
```

The three-way constructor branch (`shards.rs:104-155`) collapses to selecting `wal_mode` +
`with_persistence(..)` versus neither; the 25 setter calls disappear. The `IndexLifecycleManager::
recover` block (`:262-293`) moves above `build()` and feeds `with_search_manager`. The tick
pump (`:304`) and the `spawn(..)` (`:306-311`) stay exactly where they are.

**`ShardSpawnContext` is unchanged.** Its 30 fields (`shards.rs:20-56`) stay exactly as they
are, and so does its sole literal at `mod.rs:376-407` — thirty `infra.*` / `cluster.*` / `repl.*`
field reads that are **proposal 63's surface**. SV4-c builds the `ShardWiring` *from* `ctx`
inside `spawn_shard_workers`; it does not narrow what `ctx` carries. Shrinking the struct to
"per-shard vectors + `config` + `num_shards` + `shard_monitor` + one `ShardWiring`" is the
obvious follow-up and it is deliberately **out of scope here**, because it is precisely the edit
that would collide with 63 (see Risks).

### SV4-d — narrow the setters

With `shards.rs` no longer calling them, 21 of the 24 setters have no caller outside
`frogdb-core`. Delete the ones with no remaining caller at all; downgrade the rest to
`pub(crate)`, except the handful core's own tests and the harness genuinely drive — the census in
§2 plus the harness grep identifies them precisely: `set_notify_keyspace_events`
(`event_loop.rs:518`, `execution.rs:1380`, `eviction.rs:535`, `event_loop.rs:658`,
`post_execution.rs:1500`), `set_wal_failure_policy_flag` (`rollback.rs:435`,
`scenario_s6.rs:57`), `set_is_replica` (`worker.rs:1004`), and `HashMapStore::set_warm_store`
(defined `store/hashmap.rs:740`; **six** non-production call sites —
`core/tests/tiered_storage.rs:18`, `:318`, `:459`, `store/hashmap.rs:2476`, `eviction.rs:649`,
`shard-harness/tests/eviction_spill_failure.rs:82` — alongside the one production site
`shards.rs:170`; a *store* method, not a worker one, and out of scope). This is where the deletion test pays: `ShardWorker`'s post-construction mutation
surface stops being part of its public interface.

### SV4-e — a count pin so the seam cannot silently re-open

A `lint-shard-wiring-seam` recipe in the family described by `agents/seam-lints.md`
(`Justfile:329` lists the 14 current `lint-gates` members), modelled on `lint-continuation-lock`
(`Justfile:1312`, `scripts/continuation-lock-gate.py`) — the existing precedent for a count pin
rather than a full classification. Invariant: *`server/shards.rs` contains zero
`worker.set_*` / `worker.install_*` calls; every shard wiring goes through `ShardWiring`.* A
compile-free `rg` rule, so it joins `lint-gates` and runs on every commit. Optional and cheap:
pin `ShardWiring`'s field count so adding one is a deliberate two-file change.

**This step is more than one `Justfile` line — budget the doc edits.** Becoming the fifteenth
`lint-gates` member invalidates every hand-maintained count in the family's prose, and the repo
has five of them plus a table:

- `agents/seam-lints.md:4` "Fifteen of these ship today" → sixteen
- `agents/seam-lints.md:9` "runs the compile-free **fourteen** of them" → fifteen
- `agents/seam-lints.md:14` "`just lint` runs the full **fifteen**" → sixteen
- `agents/seam-lints.md:39` "out of scope for … this doc's **\"the 15\"**" → "the 16"
- a new row in the family table (`agents/seam-lints.md:20-37`)
- `CLAUDE.md:243` "**Fifteen** chokepoint gates encode …" → Sixteen

None is hard; all six are easy to miss, and the family's own history is that the two
hand-maintained lists drifted precisely this way. Treat them as part of SV4-e's diff, not as
follow-up.

## Testability improvement

The payoff is specific, not rhetorical: `ShardWiring` lives in `frogdb-core`, which
`frogdb-shard-harness` already depends on, so `ShardDriver` can build the *production* wiring.

1. **The harness gets production-shaped shards.** `ShardDriver::new` (`harness.rs:82-87`) gains
   `.with_wiring(&wiring)` and a `ShardDriver::with_wiring(w)` constructor. Scenarios that are
   currently impossible become ordinary: a `REPLICAOF`-driven role transition (needs
   `role_controller`, unwireable today except by hand-rolling a `RoleController` impl per test),
   `INFO replication`'s `master_sync_error` chain (FM-REPLICATION-061's Invariant runs
   `RoleController::sync_refusal` → `ShardIdentity::master_sync_error` → `CommandContext` —
   every link after the controller is already in-core and driveable), a live `CONFIG SET
   notify-keyspace-events` observed through the *shared* `Arc<AtomicU32>` rather than a
   test-local one, `[blocking]` limits at their configured values instead of `(10000, 50000)`,
   and `[json]` limits enforced instead of defaulted.
2. **One assertion replaces 24 unasserted lines.** A single core-crate test —
   `every_shard_wiring_field_reaches_the_worker` — constructs a `ShardWiring` with distinguishable
   sentinel values, builds, and asserts each landed. Today that test cannot be written: 14 of the
   fields have no builder entry point, and the sequence that sets them is in a crate the test
   cannot import.
3. **Mutation coverage moves to where it counts.** `cargo mutants -p <crate>` runs only that
   package's own tests, so wiring logic sitting in `frogdb-server` contributes to no locked
   crate's score. `try_build` in `frogdb-core` is reachable by core's own tests. `frogdb-core` is
   not itself a gated crate, but the wired state it produces — `ShardPersistence`'s failure-policy
   `Arc` (FM-PERSISTENCE-005), `ShardIdentity`'s role controller (FM-REPLICATION-061) — is the
   input every locked-area assertion downstream depends on.
4. **The drift stops being silent.** SV4-e's count pin plus the non-`Default`-able `ShardWiring`
   of SV4-b (with `ShardWiring::none()` as the one explicit "deliberately unwired" constructor)
   turns "someone added a 26th shared handle and the harness never learned" into a compile error
   or a lint failure.

## Spec / LOCKED impact

**Files edited carry no `// FM-` tag.** Verified by grep: `builder.rs`, `shards.rs`, and
`worker.rs` contain no `// FM-` tag (`worker.rs:349` mentions FM-PERSISTENCE-022 in prose inside
a comment, not as a tag). `scripts/failure-modes.py` binds backticked `Forced by` names to
`// FM-` tags; no binding in either direction is touched, so `just lint-failure-modes` is
unaffected.

**`types.rs` is edited and does carry two tags** — `// FM-PERSISTENCE-005` at `:1268` and
`// FM-REPLICATION-061` at `:1421`, both in its test module. The deletion range for SV4-a is
`:665-744`, entirely above both; neither test, tag, nor the `ShardPersistence` / `ShardIdentity`
types they exercise are touched. The one doc-comment casualty is `ShardPersistenceDeps`'s field
comment at `types.rs:697-698`, which cites "issue 42 / FM-PERSISTENCE-022" in prose — a prose
reference in a dead struct, not a tag, and the same citation survives at `types.rs:409` on the
live `ShardPersistence` field.

**One LOCKED-area Invariant and one forcing test name chains that begin at wiring this proposal
moves.** Neither cites the wiring *site*, but an implementation that drops a wiring would break
them silently:

- **FM-REPLICATION-061** (replication, gate 0.85) — its **Invariant** field
  (`replication-failure-modes.md:1427`) quotes the chain verbatim: "`ReplicaReplicationHandler::
  sync_refusal()` reaches INFO through `ReplicaStream::sync_refusal` → `RoleManager::sync_refusal`
  … → `RoleController::sync_refusal` → `ShardIdentity::master_sync_error()` → `ShardDiagnostics` /
  `CommandContext`". `shards.rs:207` (`set_role_controller`) is the **only production writer** of
  `ShardIdentity::role_controller`. It is unconditional today; it must stay unconditional.
- **FM-PERSISTENCE-005** (persistence, gate 0.85) — weaker, and stated as such. Its Invariant
  (`persistence-failure-modes.md:121`) says nothing about the shared flag or where a shard gets
  it; the dependence appears only in **Forced by** (`:123`), which names
  `should_rollback_follows_shared_flag`. The shared `Arc<AtomicU8>` behind `should_rollback`
  comes from `shards.rs:233`, whose source (`ConfigManager::wal_failure_policy_flag`,
  `runtime_config.rs:1167`) has exactly one caller in the server crate. Dropping it silently
  reverts every shard to `WalFailurePolicy::default()` (`builder.rs:372-375`) — and, as §7
  records, that named test would not notice, because it never spawns a production shard.

**No spec row changes, no failure-mode row is added, and no behavior a spec describes is
altered** — with the single flagged exception of §5(b)'s script-executor error arm, which is
governed by no FM row (scripting is not a locked area) and which the implementation must
resolve explicitly.

**No LOCKED crate is edited.** `frogdb-persistence`, `frogdb-recovery`, `frogdb-replication`,
`frogdb-replication-runtime`, `frogdb-cluster`, `frogdb-cluster-runtime`, `frogdb-txn`,
`frogdb-vll` are all read-only here; `frogdb-recovery` is read for the §6 precondition only.
`frogdb-core` and `frogdb-server` are not gate-bound, so `just mutants-diff` is not required —
but `just lint-gates` must pass, and `lint-continuation-lock`'s per-enum count pins are
untouched because no shard `*Msg` dispatch arm is added or renamed.

## Risks / scope boundaries

### Sibling edges

- **63 (server/mod.rs + init.rs bundles) — sharpest edge, at one expression.** The sole
  `ShardSpawnContext` literal is `mod.rs:376-407`, thirty `infra.*` / `cluster.*` / `repl.*`
  field reads. 63 renames or regroups the *producers*; 66 changes what the *consumer* wants. To
  keep them independent, **66 touches neither `ShardSpawnContext`'s definition (`shards.rs:20-56`)
  nor its producer — it does not edit `mod.rs` or `init.rs` at all.** SV4-c builds the
  `ShardWiring` *inside* `spawn_shard_workers`, from the `ctx` fields as they are named at the
  time of landing. Shrinking `ShardSpawnContext` itself is a follow-up that should land after
  whichever of 63/66 goes second. If 63 lands first, 66's implementation re-derives `ctx` field
  names from the tree. Checked against 63 as it stands on disk: 63 lists `shards.rs` as
  "call-site only … `ShardSpawnContext` `:20-…` fed from bundles" and states explicitly that
  **66 owns `shards.rs` internals** while 63 only feeds the literal at `mod.rs:376-407`. With
  the shrink out of scope, the two are disjoint by both proposals' own accounts.
- **64 (subsystems.rs `Subsystem` trait).** `subsystems.rs` (930 lines) is not read or written
  here. Shard spawning is `mod.rs` Phase 4 and is not currently a `Subsystem`; if 64 makes it
  one, it wraps `spawn_shard_workers`, which this proposal leaves as a function with the same
  signature. No overlap.
- **65 (cluster_init.rs phases).** `cluster_init.rs` (1938 lines) produces `cluster.raft`,
  `cluster.cluster_state`, `cluster.node_id`, `cluster.network_factory`, `cluster.role_controller`,
  `cluster.is_replica_flag` — the values `ShardWiring` consumes. 66 reads them through `ctx`; 65
  changes how they are produced. No shared file.
- **67 (SV5, `ConnectionHandlerBuilder` delete).** `server/src/connection/builder.rs` (267 lines)
  is a *different* builder with confusingly similar members — its own `with_cluster` (`:104`) and
  `try_build` (`:210`). 66 does not touch that file. The two proposals reach **opposite verdicts,
  and that is the point**: 67 deletes a builder with **zero** callers; 66 *deepens* one with
  **thirteen**, because the deletion test says its assembly half earns its keep (§4). What they
  share is only the diagnostic — the same fallible-terminal-with-one-panicking-or-unwrapping-caller
  smell, which in one case means "this thing is unused, delete it" and in the other "the
  `Result` is a wrapper, keep the module and drop the wrapper". A reviewer reading both should
  expect the verdicts to differ and check the *reasoning* is the same, not the outcome.
- **68 (exec-framing), 69 (config combinators), 70 (acl), 71 (search).** 69 touches
  `frogdb-config`, which `ShardWiring` reads *values* from but does not import; 71 touches search
  internals, and 66 only moves *where* `IndexLifecycleManager` is installed, never what it does.
  No file overlap with any of the four.

### Other risks

- **Recreating `ShardCoreDeps`' fate.** The strongest objection to `ShardWiring` is that this
  repo already has four dead dependency-group structs built for this exact builder. The
  difference is enforceable, not aspirational: SV4-c makes production the first adapter in the
  same commit that introduces the type, SV4-e's lint keeps it the only path, and the harness is a
  genuine second adapter. If SV4-c is deferred or dropped, `ShardWiring` must be dropped with it
  — a `ShardWiring` with zero adapters is `ShardCoreDeps` again.
- **A 25-field struct is still a ferry.** Fair. It is a ferry with a *name*, one construction
  site per adapter, an ordering owner, and a compile-time obligation on additions — versus 25
  loose lines with none of those. If a future round finds a natural sub-grouping (the six
  `CONFIG SET` atomics are the obvious candidate), that is a refinement inside this seam, not a
  reason to keep the seam out.
- **`with_wiring` order versus fluent order.** `with_wiring(&w)` followed by an individual
  `with_json_limits(..)` must let the individual call win (harness override), while `with_wiring`
  after individual calls would clobber them. Fluent builders make this ambiguous. Mitigation:
  `with_wiring` populates only fields still unset, or (cleaner) it is a `new`-adjacent
  constructor — `ShardWorkerBuilder::from_wiring(shard_id, num_shards, &w)` — so overrides can
  only follow it. Decide at implementation time and document it in the interface, since it is
  exactly the kind of ordering fact the vocabulary says belongs in the interface.
- **Feature-cfg surface.** Three cfg gates run through this code: `any(test, feature =
  "fake-wal")` (`builder.rs:120-121`, `:295`, `:390`), `any(test, feature = "shard-driver")`
  (`worker.rs:257`, `builder.rs:472`), and the server's `turmoil` (`shards.rs:98-102`, `:301-304`).
  `ShardWiring` must carry the cfg'd fields under the same gates, or the turmoil build breaks in
  a way the default `just check` will not catch. `just lint-turmoil-features` (recipe at
  `Justfile:349`; `Justfile:319` is where `lint` *depends* on it, not where it is defined) is
  the guard.
- **Blast radius.** ~150 lines deleted (SV4-a), ~250 added (SV4-b), ~150 deleted and ~40 added
  (SV4-c), visibility churn across ~21 methods (SV4-d). Every existing
  `ShardWorkerBuilder::new` call site — six in `frogdb-core` (`rollback.rs:548`,
  `dispatch_pubsub.rs:135`, `worker.rs:989`, `eviction.rs:550`, `event_loop.rs:1174`,
  `builder.rs:508`) and seven in the harness (`harness.rs:82`, `scenario_s6.rs:47`,
  `script_timeout_effects.rs:82`, `eviction_spill_failure.rs:93`, `shard_driver.rs:41` and
  `:107`, `rendering_incrbyfloat.rs:35`) — keeps compiling unchanged, because all thirteen use
  the individual `with_*` methods, none of whose signatures change, and none of them names
  `ShardWiring`. (Not because `ShardWiring` has a `Default`; per SV4-b it deliberately does
  not.) Separately, the eleven core unit tests that call `ShardWorker::with_eviction` are
  likewise untouched: SV4-a deletes only `ShardWorker::new`.

## Effort

**M.** Five commits. SV4-a is mechanical deletion, ~30 minutes, reviewable on its own. SV4-b is
the design work: one new file, 14 methods, `try_build` restructured around four ordering-sensitive
cases, and the §5(b) decision. SV4-c rewrites `spawn_shard_workers`' body and leaves
`ShardSpawnContext` alone. SV4-d is visibility churn guided by the census in §2. SV4-e is one
`rg` rule, one `Justfile` recipe, one `agents/seam-lints.md` table row, **and the five
hand-maintained gate counts that a fifteenth `lint-gates` member invalidates** (four in
`agents/seam-lints.md`, one in `CLAUDE.md`) — enumerated under SV4-e so they are not discovered
at review. The full workspace test suite must run (this touches shard
construction on every path, including the turmoil and fake-WAL branches); `just lint-gates`,
`just lint-turmoil-features`, and `just lint-failure-modes` must pass. No wire change, no config
change, no spec row change, and — modulo §5(b) — no behavior change.

## Independently-landable hotfixes

Both are doc-only, both are landable now, and neither depends on any part of SV4.

**H1 — `shard/mod.rs:14-25` documents an API that does not exist.** The example calls
`ShardWorkerBuilder::new(shard_id, num_shards, message_rx, new_conn_rx)`; the real signature is
`new(shard_id: usize, num_shards: usize)` (`builder.rs:127`). It also passes a "wal_writer" to
`with_persistence`, which takes `(Arc<RocksStore>, WalConfig)` (`builder.rs:227`). The
```rust,ignore``` fence means no doctest ever ran it. Fix: replace with the (correct) example
already in `builder.rs:83-93`, or link to it rather than keeping a second copy — the repo's
single-source-of-truth rule applies to doc examples as much as to prose.

**H2 — `shard/mod.rs:7-12` advertises three types nobody constructs.** "Dependencies can be
organized into logical groups for cleaner construction: `ShardCoreDeps` / `ShardPersistenceDeps`
/ `ShardClusterDeps`" — verified zero constructors workspace-wide. As a doc-only hotfix, delete
the section. (SV4-a deletes the types themselves; H2 stands alone if SV4 does not land.)

**Not a hotfix, recorded for the ledger:** `builder.rs:76` names `ShardWorker::new` as a caller
that funnels through `try_build`. The statement is true but vacuous — `ShardWorker::new` has no
call sites, and the doc comment is the only occurrence of that string in the tree outside its own
definition. It is fixed by SV4-a deleting the constructor, not by editing the comment.
