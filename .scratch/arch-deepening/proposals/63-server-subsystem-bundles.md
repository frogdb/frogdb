# Proposal 63 — `Server` adopts the decomposition `ShardWorker` already has

Round 38 · lane: server composition · effort **M** · **not** a LOCKED crate (`frogdb-server` has no
mutation gate) · **not spec-first** (no behavior change proposed; the one live defect found is
carved out as an independently-landable hotfix, not folded in)

Covers exploration-lane candidate **SV1** ("Server god-object + `InitResult` ~40-field
transcription"). Verified against `6e99f567`; **re-verified line-by-line at `159cb7a2`** after the
first adversarial review returned AMEND. Every citation below was re-derived from the tree at that
SHA rather than carried over — where re-derivation contradicted either the original draft or the
review, the contradiction is stated inline rather than silently resolved.

## Summary

`Server` is the last flat god object in the tree. It declares **47 fields** in a 167-line struct
body (`server/mod.rs:62-228`) and constructs itself with a 50-line `Ok(Self { … })` in which
**23 lines are the identical form `field: infra.field`** (`mod.rs:459-508`). The producer of those
fields, `InitResult`, declares **39 fields** (`init.rs:34-104`) whose only job is to survive the
return from `init_infrastructure` to the constructor.

The interesting fact is not that this is verbose. It is that **the grouping already exists, three
times over, and the server composition layer is the one place that throws it away**:

- One layer down in the same crate, `connection/deps.rs` groups the connection's collaborators into
  `CoreDeps`/`AdminDeps`/`ClusterDeps`/`ConnectionConfig`/`ObservabilityDeps`, assembled as the
  five-field `ConnectionDeps` (`deps.rs:303-309`), two of which carry the cheap constructors that
  make them test-assemblable: `ObservabilityDeps` has a **plain `impl Default`**
  (`deps.rs:279-293`, compiled into production), and `ConnectionConfig` has a `#[cfg(test)]`
  `default_for_testing` (`deps.rs:215-233`, gate at `:217`). Two different gating choices for the
  same job — both are available to this proposal, and §Testability picks between them per bundle.
- One crate down, **`ShardWorker` — which does strictly more work than `Server` — is already
  decomposed exactly this way**: 27 fields in a 104-line body (`core/src/shard/worker.rs:106-209`),
  of which **9 are subsystem structs** (`ShardIdentity`, `ShardPersistence`, `ShardObservability`,
  `ShardEviction`, `ShardVll`, `ShardCluster`, `ShardTracking`, `ShardScripting`,
  `ShardSubscriptions`), the rest a small flat remainder (store, receivers, registry).
- In `server/` itself, `bind_listeners` returns a bundle whose doc comment says it is "ready to be
  handed to `Server`" (`listeners.rs:11`) — and `init_infrastructure` un-hands it into five loose
  locals on the next five lines (`init.rs:186-190`).

So this proposal is not "invent a grouping". It is **stop flattening the groupings that already
exist**, using the shard's own vocabulary. `Server` holds `listeners`, `telemetry`,
`persistence`, `cluster`, `replication` plus a nine-field flat remainder; `InitResult` splits along
the retained/consumed line it already has but does not express (27 of its 39 fields are also
declared on `Server`; the other 12 are one-shot ingredients `Server` never stores).

The tax is **live but it is maintenance cost, not a behavior bug**: adding one server-held
subsystem object today costs 4 mechanical edit sites across 2 files, 6 across 3 if it originates in
a later phase. `mod.rs` has taken 76 commits and `init.rs` 32, so that cost is paid continuously.

One genuine **live defect** was found while tracing field ownership, and it is exactly the class of
defect a flat field bag hides: `shared_maxmemory` is an `Arc<AtomicU64>` that is **written once at
boot and never again**, while `CONFIG SET maxmemory` writes a different location — so the
`frogdb_memory_maxmemory_bytes` gauge is permanently stale after any runtime change. It has no
owner, so there is nowhere for the writer to go. It has **two variants**, not one, and the second
is worse; full call path and both variants in §Problem 4. It is offered as a standalone hotfix
that does not wait on the refactor, and the hotfix **is not a one-line addition to the existing
`apply`** — the shape it has to take is dictated by where the atomic is minted, and the tree
already contains the exact pattern to copy (`max_clients`). See §Hotfix.

The `ConfigPlane` bundle named in the lane brief is **rejected here** on the deletion test — see
§"Deletion test, applied honestly".

## Files involved (verified at `6e99f567`, re-verified at `159cb7a2`)

| File | Lines | Role in this proposal |
| --- | --- | --- |
| `frogdb-server/crates/server/src/server/mod.rs` | 598 | **Primary.** `Server` struct :62-228, `with_listeners` :244-509, accessors :543-572 (+ the non-address ones through :597). **Shared with 61** — see [Ordering vs 61](#61-is-a-hard-predecessor-not-a-disjoint-sibling) |
| `frogdb-server/crates/server/src/server/init.rs` | 669 | **Primary.** `InitResult` :34-104, `init_infrastructure` :107-496 |
| `frogdb-server/crates/server/src/server/listeners.rs` | 125 | Read-only reference: `BoundListeners` :12-23 (its doc line is :11) is re-used as-is |
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **Touched mechanically only** — field paths retargeted (`self.x` → `self.telemetry.x` etc.) |
| `frogdb-server/crates/server/src/server/runtime.rs` | 37 | Untouched (reads `self.config` only, :24) |
| `frogdb-server/crates/server/src/connection/deps.rs` | 309 | Precedent, not edited |
| `frogdb-server/crates/core/src/shard/worker.rs` | — | Precedent (`ShardWorker` :106-209), not edited |
| `frogdb-server/crates/core/src/shard/types.rs` | — | Precedent (`ShardObservability` :155-170 and siblings), not edited |
| `frogdb-server/crates/server/src/server/replication_init.rs` | 552 | Return-type site only: `ReplicationInitResult` :22-47 |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | 1938 | Return-type site only: `ClusterInitResult` :43-66 |
| `frogdb-server/crates/server/src/server/shards.rs` | 367 | Call-site only: `ShardSpawnContext` :20-… fed from bundles |

Construction sites that must keep compiling — the entire external surface of `Server`. The draft
listed these with truncated paths and claimed they "all call `Server::with_listeners`", which is
**false**; re-derived at `159cb7a2` from `grep -rn 'Server::new\|Server::with_listeners'`:

| site | entry point |
| --- | --- |
| `frogdb-server/crates/server/src/main.rs:144` | `Server::new(config, log_reload_handle)` |
| `frogdb-server/crates/server/tests/common/sim_helpers.rs:170,235,283,333,472,515` | `Server::new` ×6 |
| `frogdb-server/crates/browser-tests/tests/common/mod.rs:40` | `Server::new` |
| `frogdb-server/crates/test-harness/src/server.rs:697` | `Server::with_listeners(config, listeners, None)` — the **only** external `with_listeners` caller |

**Nine sites, not eight** (the draft's count was wrong too), and **eight of the nine reach
`with_listeners` only through `Server::new`**, which delegates at `mod.rs:236`
(`Self::with_listeners(config, ServerListeners::default(), Some(log_reload_handle)).await`).
Neither signature changes, so the practical consequence is unchanged — but it matters for
§Step 3, because the sites that call `local_addr()` are reached through `new`, and `new`'s
signature is the narrower contract this proposal must not touch.

*(Review correction, with evidence: the review said "7/8 call `new` … sim_helpers ×5". The grep
returns six `sim_helpers` hits — :170, :235, :283, :333, :472, :515 — so the true split is 8 of 9,
not 7 of 8. The review's path correction for `main.rs` was right and is applied.)*

## Problem

### 1. The interface of `init_infrastructure` is 39 fields wide, and 23 of them are re-typed by hand

`InitResult` (`init.rs:34-104`) is not a type: it is a parameter list turned sideways. Its 39 public
fields are returned at `init.rs:454-495` and then transcribed into `Server` at `mod.rs:459-508`,
where **23 lines have the exact form `name: infra.name`** — the field name appears four times per
subsystem object in total (once in `InitResult`, once in the `Ok(InitResult{…})`, once in `Server`,
once in `Ok(Self{…})`). Nothing decides anything at any of the four sites.

This is the definition of a shallow interface in the round's vocabulary: the caller must know 39
names to use one function, and the function's leverage per unit of interface is close to zero,
because every name it exposes is a name the caller already had to declare.

### 2. `InitResult` conflates state the server *keeps* with ingredients it *consumes*, and the type does not say which

Set arithmetic over the two struct bodies (`init.rs:34-104` vs `mod.rs:62-228`):

- **27 retained** — also declared on `Server`: `acl_manager`, `admin_listener`, `client_registry`,
  `cluster_bus_listener`, `config_manager`, `conn_monitor`, `function_registry`, `health_checker`,
  `http_listener`, `is_replica_flag`, `keyspace_stats`, `latency_histograms`, `listener`,
  `metrics_recorder`, `new_conn_senders`, `periodic_snapshot_handle`, `periodic_sync_handle`,
  `prometheus_recorder`, `recovery_stats`, `registry`, `rocks_store`, `shard_memory_used`,
  `shard_senders`, `shared_maxmemory`, `snapshot_coordinator`, `tls_listener`, `tls_runtime`.
- **12 consumed** — never stored on `Server`, used once inside `with_listeners` and dropped:
  `eviction_config`, `new_conn_receivers`, `num_shards`, `recovered_raft_storage`,
  `recovered_replication`, `recovered_stores`, `repl_state_save_slot`, `shard_monitor`,
  `shard_receivers`, `slowlog_next_id`, `task_registry`, `wal_config`.

("Used once inside `with_listeners`" is accurate but coarse: those 12 are consumed across all four
phases and the constructor tail, not in one place. The per-field table is under
[Step 2](#step-2--each-phase-returns-bundles-and-says-what-is-consumed), because where they are
consumed constrains how `BootIngredients` may be written.)

A reader of `InitResult` cannot tell these apart without diffing it against `Server` by hand. The
same unmarked split exists in the two later phases: `ReplicationInitResult` (`replication_init.rs`
:22-47, 10 fields) and `ClusterInitResult` (`cluster_init.rs:43-66`, 11 fields) each mix values the
server stores with values it forwards into `spawn_shard_workers` and drops.

### 3. The grouping is proven in-tree and discarded at exactly one point

`bind_listeners` returns `BoundListeners { resp, admin_resp, http, cluster_bus, tls }`
(`listeners.rs:12-23`), doc-commented at `:11` "All bound listeners for the server, **ready to be
handed to `Server`**". `init_infrastructure` then does:

```rust
let bound = bind_listeners(config, listeners).await?;
let listener = bound.resp;
let admin_listener = bound.admin_resp;
let http_listener = bound.http;
let cluster_bus_listener = bound.cluster_bus;
let tls_listener = bound.tls;
```
(`init.rs:185-190`)

Those five locals become five `InitResult` fields, five `Server` fields, and **five near-identical
accessors** (`local_addr` :547, `admin_resp_addr` :555, `http_addr` :560, `cluster_bus_addr` :565,
`tls_addr` :570 — four of them the same one-liner `self.X.as_ref().map(|l| l.local_addr())`). Every
one of those 20 declarations exists because a bundle was destructured for no reason.

The `prometheus_recorder`/`metrics_recorder` pair shows the same shape from the consumer side:
`subsystems.rs` gates three separate spawn decisions on `self.prometheus_recorder` being `Some`
(:171, :308, :320), and one of them carries a hand-written safety comment asserting a cross-field
invariant the type system is not being asked to hold — `// SAFETY: http_listener is Some when
prometheus_recorder is Some` (`subsystems.rs:224`). That invariant relates two fields that live in
two different flat bags.

### 4. LIVE defect found while tracing ownership — `shared_maxmemory` has no writer after boot (two variants)

Traced call path, every hop verified:

1. `init.rs:397` — `let shared_maxmemory = Arc::new(AtomicU64::new(config.memory.maxmemory));`
2. `init.rs:483` → `mod.rs:503` — moved into `Server.shared_maxmemory` (`mod.rs:215`).
3. `subsystems.rs:308-314` — cloned into `SystemMetricsCollector::spawn_collector`, gated on
   `self.prometheus_recorder.is_some()`.
4. `telemetry/src/system.rs:90-92` — every 5 s: `let maxmem = self.maxmemory.load(Relaxed); if
   maxmem > 0 { MemoryMaxmemoryBytes::set(&*self.recorder, maxmem as f64); }`
5. `CONFIG SET maxmemory` lands at `runtime_config.rs:1731-1736`, whose `apply` is
   `mgr.runtime.write().unwrap().maxmemory = v;` — a field of the `RwLock<RuntimeConfig>`, **not**
   the atomic. Its `Propagation::Eviction` handler (`runtime_config.rs:3626-3628`) calls
   `shard_notifier.notify_eviction_change()`, which reaches the shards and nothing else.

A workspace-wide grep for `shared_maxmemory` returns **six hits, all of them the declaration,
mint, move, or read above — there is no `.store(` anywhere**. Consequence, with metrics enabled:
`frogdb_memory_maxmemory_bytes` reports the boot-time value forever, and when the boot value is the
`0` default the gauge is never emitted at all even after the operator sets a limit. Eviction itself
is unaffected — it reads the runtime config through the shard notifier — so this is an
**observability-only defect**, which by house policy (misleading data is not acceptable) is still a
defect.

Preconditions for **variant A** to bite: Prometheus enabled (`prometheus_recorder.is_some()`, which
is `config.http.enabled` — `init.rs:143-149`) **and** a runtime `CONFIG SET maxmemory`. Both are
ordinary operation, so this is ruled **LIVE**, not latent.

#### Variant B — OTLP-only mode, where the gauge has no periodic writer *at all*

Re-derivation at `159cb7a2` found a **second, strictly worse variant** the draft missed. There are
two places that emit `MemoryMaxmemoryBytes`, and they are gated on **different** conditions:

| emitter | site | gate |
| --- | --- | --- |
| one-shot at boot, from `config.memory.maxmemory` | `record_process_identity_gauges` `init.rs:526-543` (the `if config.memory.maxmemory > 0` at :537), called at `init.rs:176` | `has_metrics_backend` = `prometheus_recorder.is_some() \|\| otlp_recorder.is_some()` (`init.rs:160`, branch :175) |
| every 5 s, from the atomic | `SystemMetricsCollector` (`telemetry/src/system.rs:90-92`), spawned at `subsystems.rs:308-314` | `self.prometheus_recorder.is_some()` **only** (`subsystems.rs:308`) |

In **OTLP-only mode** (`http.enabled = false` + `metrics.otlp-enabled = true`) the first gate
passes and the second does not: `prometheus_recorder` is `None` (`init.rs:143-149` gates its
construction on `config.http.enabled`), so `SystemMetricsCollector` **never spawns**. The gauge is
emitted exactly once, at boot, and there is no periodic writer to go stale — there is no writer
at all. `CONFIG SET maxmemory` in that mode changes nothing an OTLP consumer can observe, ever.

This is the same structural cause seen from the other side: the boot emitter and the periodic
emitter of *one* metric are gated on two different fields living in the same flat bag, and nothing
in the type system relates them — the same shape as the hand-written cross-field `// SAFETY`
comment at `subsystems.rs:224` (§Problem 3). **The hotfix must state which variant it fixes**; see
§Hotfix, which fixes A and records B as an explicit residual rather than pretending to cover it.

Note the contrast that makes this a structural finding rather than a stray bug: its sibling
`shard_memory_used` **does** have a writer, because it was given an owner — it is handed to
`ShardWorker::set_shard_memory_used` (`shards.rs:239` → `core/shard/worker.rs:295-297`) and lives
inside `ShardObservability` (`core/shard/types.rs:169`), read back through
`ShardObservability::shard_memory_used()` (:273). The atomic that sits in a flat bag on `Server`
lost its writer; the atomic that sits inside a named subsystem struct kept one.

### 5. Neither `mod.rs` nor `subsystems.rs` has a single in-crate test

`server/` files of comparable weight all carry `#[cfg(test)]` modules: `init.rs:564`,
`replication_init.rs:410`, `cluster_init.rs:1055`, `register.rs:254`, `shard_supervisor.rs:148`,
`startup.rs:110`, `checkpoint_quiesce.rs:149`. `mod.rs` (598 lines) and `subsystems.rs` (930 lines)
have **none**.

For the methods, the reason is mechanical: the only way to obtain a `Server` is
`Server::with_listeners`, which binds sockets, opens RocksDB and runs recovery, and all 47 fields
are private with no other constructor. So any assertion about `check_split_brain_logs`
(`subsystems.rs:836-845`) or `run_startup_latency_test` (:848-886) — both `pub(super) fn(&self)` —
has to be made through a full integration server or not at all.

**The draft over-claimed here and the correction matters.** `record_version_metrics`
(`subsystems.rs:892-930`) is **already a free function** — `fn record_version_metrics(recorder:
&Arc<dyn MetricsRecorder>, cluster_state: Option<&Arc<ClusterState>>)`, called from the spawned
ticker at `:327` with both arguments cloned out of `self` at `:321-322`. It takes no `&Server`, so
it is constructible and callable in a unit test **today**, and its 39 lines of version-gate and
mixed-version logic are untested for a different reason: `subsystems.rs` simply has no test module
at all. That is a gap this proposal can close as a side effect of opening one, but it is **not**
an unlock this proposal's restructuring provides, and it is not counted as one.

### 6. Verified cosmetic defect: an orphaned comment block — **owned by 61, not by this proposal**

`mod.rs:305-311` is a **byte-identical duplicate** of `mod.rs:313-319` (the "Ship the
function-library registry to every full-syncing replica…" paragraph), separated by blank line 312.
The second copy continues at :320-324 and documents the `if let` at :325. The first copy documents
nothing. Zero behavior. It survived 76 commits — which is itself evidence about how carefully a
598-line constructor gets read.

**The draft claimed this as one of its own carve-outs and that claim is withdrawn**, for two
reasons, both re-verified on disk at `159cb7a2`:

1. **Proposal 61 already owns it.** `61:647` files it as its own hotfix H1 — *"Delete
   `server/src/server/mod.rs:305-312` — the duplicated comment block at `:305-311` … **plus the
   blank line at `:312`**, so the surviving block is not preceded by two blank lines"* — and 61's
   step 2 deletes the entire `:305-342` region regardless (`61:650`). Two proposals cannot both
   land the same deletion.
2. **The draft's own arithmetic was wrong.** It said ":305-311, 8 lines including the separating
   blank"; `:305-311` is seven lines and excludes the blank at `:312`. 61's `:305-312` is the
   correct range and the correct reason. Deferring is not just politeness — 61 states the rule
   this proposal had stated incorrectly.

The finding is kept here as *evidence for the §Problem 5 argument* (a 598-line constructor is not
read carefully), not as a deliverable.

### Why this is shallow, in the round's vocabulary

The interface of `init_infrastructure` is everything a caller must know to use it: 39 field names,
their types, and — undocumented — which 12 of them must be consumed before the constructor ends.
Its implementation is 390 lines of real work (recovery, RocksDB open, snapshot coordinator wiring,
pre-snapshot hook). The leverage is high; the interface is 39 names wide, so the **depth is not**.
The fix is not to make the implementation smaller. It is to shrink the interface to the five or six
nouns the implementation is already organised around, which is what the shard did.

There is no seam here to preserve or destroy: nothing in the tree substitutes an alternative
`InitResult`, and there is exactly one adapter (`init_infrastructure` itself). Per the round's rule
— one adapter is a hypothetical seam — this proposal **does not introduce a trait**. It introduces
structs. Traits over these structs are proposal 64's business, and 64 is the second adapter that
would make a seam real.

## Proposed change

### Step 1 — five subsystem structs, one flat remainder

`Server`'s 47 fields partition exactly (6 + 11 + 5 + 8 + 8 + 9 = 47). The names deliberately echo
`core/shard/types.rs` — `Server<X>` to the shard's `Shard<X>` — with one forced exception,
`ServerTelemetry`, explained directly below the table:

| New struct | Fields (from `Server`) | Count |
| --- | --- | --- |
| **`ServerListenerSet`** (re-use `BoundListeners`, add `tls_runtime`) | `listener`, `admin_listener`, `http_listener`, `cluster_bus_listener`, `tls_listener`, `tls_runtime` | 6 |
| **`ServerTelemetry`** | `metrics_recorder`, `prometheus_recorder`, `keyspace_stats`, `latency_histograms`, `shared_tracer`, `latency_baseline`, `health_checker`, `conn_monitor`, `_task_monitor_handle`, `shared_maxmemory`, `shard_memory_used` | 11 |
| **`ServerPersistence`** | `rocks_store`, `snapshot_coordinator`, `recovery_stats`, `periodic_sync_handle`, `periodic_snapshot_handle` | 5 |
| **`ServerCluster`** | `cluster_state`, `node_id`, `raft`, `network_factory`, `slot_migration`, `failure_detector`, `failure_detector_handle`, `role_manager_handle` | 8 |
| **`ServerReplication`** | `replication_tracker`, `replica_handler`, `replica_frame_rx`, `primary_replication_handler`, `replication_quorum_checker`, `replication_self_fence`, `shared_replication_offset`, `is_replica_flag` | 8 |
| *(flat remainder)* | `config`, `registry`, `client_registry`, `config_manager`, `acl_manager`, `function_registry`, `shard_senders`, `new_conn_senders`, `shard_supervisor_handle` | 9 |

**Naming, corrected.** The draft called the second bundle `ServerObservability`. That name is
**already taken in the same crate**: `frogdb-server/crates/server/src/server_observability.rs:17`
declares `pub struct ServerObservability`, the production impl of `frogdb_core::ObservabilityConfig`
(the node's installed collectors), referenced from `connection/deps.rs:276` and `:290`,
`server/subsystems.rs:110`, and `connection/hotshards_conn_command.rs:4`. Two same-crate types with
one name is a non-starter, and renaming the *existing* one is worse — it is a seam implementation
with four referents and its own doc-comment contract. The bundle is therefore **`ServerTelemetry`**
(`telemetry` is already the corpus noun: `frogdb-telemetry` is where every handle in the bundle
comes from). Its `Server` field is `telemetry`, not `observability`, so the two never read alike at
a call site. The other five names were re-checked by grep at `159cb7a2` and are unused anywhere in
`frogdb-server`: `ServerListenerSet`, `ServerPersistence`, `ServerCluster`, `ServerReplication`,
`SharedCollaborators`, `BootIngredients`.

The remainder is kept flat on purpose and mirrors `ShardWorker`'s own remainder (`store`,
`message_rx`, `shard_senders`, `registry` at `worker.rs:111-126`): these are the ambient
collaborators every subsystem reaches for, not the property of any one of them.

`Server` becomes 14 fields. The `Ok(Self { … })` at `mod.rs:459-508` becomes roughly 14 lines with
zero repeated-name transcription, because each phase function returns its bundle whole.

### Step 2 — each phase returns bundles, and says what is consumed

`init_infrastructure` returns

```rust
pub(super) struct InitResult {
    pub listeners: ServerListenerSet,
    pub telemetry: ServerTelemetry,
    pub persistence: ServerPersistence,
    pub shared: SharedCollaborators,   // registry, client_registry, config_manager, acl_manager,
                                       // function_registry, shard_senders, new_conn_senders
    pub boot: BootIngredients,         // the 12 consumed fields, named as such
}
```

`BootIngredients` is the load-bearing half of this step: it turns the undocumented retained/consumed
split of §Problem 2 into something the compiler tracks. Anything left in it when the constructor
ends is a value that was silently dropped, which today is invisible.

**The draft over-simplified where those 12 go, and the correction constrains the design.** It said
`with_listeners` "destructures `boot` into `spawn_shard_workers` and it is gone". Re-derived at
`159cb7a2`, only **7 of the 12 are consumed solely by phase 4**; the rest are consumed earlier, so
`boot` cannot be a value that is moved once:

| ingredient | consumed at | phase |
| --- | --- | --- |
| `recovered_replication` | `mod.rs:267` (arg to `init_replication`) | 2 |
| `repl_state_save_slot` | `mod.rs:282` (`.set(handler.clone())`) | 2 |
| `recovered_raft_storage` | `mod.rs:347` (arg to `init_cluster`) | 3 |
| `num_shards` | `mod.rs:353` **and** `:379` | 3 **and** 4 |
| `task_registry` | `mod.rs:455-457` (`.spawn_collector(…)`) | constructor tail |
| `shard_receivers`, `new_conn_receivers`, `recovered_stores`, `wal_config`, `eviction_config`, `slowlog_next_id`, `shard_monitor` | `mod.rs:379-407` (`ShardSpawnContext`) | 4 |

So `BootIngredients` is consumed **field-by-field across four phases**, not moved wholesale into
one call, and `num_shards` is read twice (it is `Copy`, so that is free; the others are moves).

The compiler-tracked-consumption claim survives this, but only under one constraint that must be
stated rather than discovered in review: **`BootIngredients` must not implement `Drop`**, because
Rust permits partial moves out of a struct only when it has no `Drop` impl. Given that, the
field-by-field consumption above is legal exactly as written, and `boot` is simply never passed
whole to anything. The compiler still refuses nothing and reports nothing about a *forgotten*
field — `#[warn(unused)]` does not fire on struct fields — so the enforcement is a **review
affordance, not a compile error**: the reviewer of a future phase sees a named `boot` whose fields
are each visibly moved somewhere, instead of 12 names diffed by hand against `Server`'s body. That
is a weaker claim than the draft made, and it is the accurate one.

`ReplicationInitResult` and `ClusterInitResult` get the same two-part shape: the retained half *is*
`ServerReplication` / `ServerCluster`, the forwarded half stays a small named struct
(`replication_init.rs`: `broadcaster`, `primary_addr`, `replication_identity`,
`shared_replication_offset` are read at `mod.rs:359-361,393`; `cluster_init.rs`: `role_controller`
is cloned at `mod.rs:402`).

### Step 3 — collapse the five address accessors

The five accessors — `local_addr` `:547`, `admin_resp_addr` `:555`, `http_addr` `:560`,
`cluster_bus_addr` `:565`, `tls_addr` `:570`, spanning `mod.rs:543-572` with their doc comments —
become one method on `ServerListenerSet` plus five one-line delegations kept on `Server` for source
compatibility with the **nine** external construction sites and their `local_addr()` calls
(`frogdb-server/crates/test-harness/src/server.rs`,
`frogdb-server/crates/server/tests/common/sim_helpers.rs`,
`frogdb-server/crates/browser-tests/tests/common/mod.rs`). **No public signature changes**, on
either `with_listeners` or `new`. The delegations are three lines each, not five, and the
`expect("listener not yet taken")` at `mod.rs:550` moves to the one place that owns the `Option`.

### Step 4 — `subsystems.rs` field-path retarget only

`start_subsystems` and `shutdown_subsystems` read server fields directly; each read becomes
`self.<bundle>.<field>`. This is a mechanical rename with no logic change, and it is the whole of
this proposal's contact with 930 lines. Two small wins fall out and should be taken:
`ServerTelemetry::metrics_enabled()` replaces the three duplicated
`self.prometheus_recorder.is_some()` gates (:171, :308, :320), and the cross-field assertion at
:224 (`// SAFETY: http_listener is Some when prometheus_recorder is Some`) becomes a statement
about two bundles that can, in a follow-up, be made structural. **This proposal does not change
that invariant's representation** — it only puts the two fields where a later change could.

**Name the kind of win, because it is not depth.** The six new structs are plain-data holders:
public fields, interface ≈ implementation, no behavior of their own beyond
`ServerTelemetry::metrics_enabled()` and the one address method. None of them is a deep module and
none is claimed to be. The payoff is **locality** — every fact about how this node wires its
metrics gating lands in one struct, so §Problem 4's two variants become one place to fix and one
place to test, and the §Problem 3 cross-field `// SAFETY` comment names two bundles instead of
floating over one 47-field bag. The module that actually gets deeper is `init_infrastructure`,
whose interface drops from 39 names to 5. The precedent in `connection/deps.rs` is the same kind of
win and was taken for the same reason: `ObservabilityDeps` holds nine public fields and no logic,
and it is still the reason `connection/` has unit tests.

### Deletion test, applied honestly

- **`ServerListenerSet`** — delete it and you get back five loose fields, five accessors, five
  destructuring lines and the `Option` unwrap comment. Complexity reappears verbatim; the type
  already exists as `BoundListeners` and is being thrown away. **Keep.**
- **`ServerTelemetry`** — delete it and the 11 fields scatter, the three `is_some()` gates come
  back, and `shared_maxmemory` returns to having no owner (§Problem 4). Complexity reappears.
  **Keep.**
- **`ServerPersistence`** — delete it and the RocksDB flush at `subsystems.rs:820-826` no longer
  sits next to the two periodic handles it must be ordered against. Complexity reappears. **Keep.**
- **`ServerCluster` / `ServerReplication`** — delete them and `ClusterInitResult` /
  `ReplicationInitResult` still exist as producers, so the fields would simply be re-flattened at
  the constructor exactly as today. Complexity reappears. **Keep.**
- **`ConfigPlane` (`config` + `config_manager`) — REJECTED, contra the lane brief.** `self.config`
  is read **44 times** in `subsystems.rs` alone (`grep -o 'self\.config\b' | wc -l` at `159cb7a2`;
  the draft said 50 and was wrong — the conclusion is unaffected), by every subsystem without
  exception, and
  `runtime.rs:24` reads it too. It is ambient input, not any subsystem's state. Deleting a
  two-field wrapper around two things that are already single fields and already passed as single
  things makes **no** complexity reappear — it is a rename with an extra `.` in front of it. It
  fails the deletion test and is not proposed.
- **`BootIngredients`** — delete it and the 12 consumed fields go back to being
  indistinguishable from the 27 retained ones. That distinction is currently derivable only by
  cross-referencing two struct bodies by hand, which is how §Problem 2 was found. **Keep.**

## Testability improvement

**The draft's version of this section was wrong on two of its three examples and is rewritten from
the source.** It claimed three `subsystems.rs` helpers "today have no test because their receiver
is `&Server`" and that narrowing the receiver is what unlocks them. Re-derived at `159cb7a2`:

| helper | actual receiver + reads | does this proposal unlock it? |
| --- | --- | --- |
| `record_version_metrics` `:892-930` | **already a free fn**: `(recorder: &Arc<dyn MetricsRecorder>, cluster_state: Option<&Arc<ClusterState>>)`, no `self` at all | **No.** Testable today; untested only because `subsystems.rs` has no test module. The draft's "needs `ServerTelemetry` + `cluster_state`" was fiction. |
| `check_split_brain_logs` `:836-845` | `&self`, but reads exactly two things: `self.config.persistence.data_dir` (:837) and `self.metrics_recorder` (:841). **No cluster state is read** | **Barely.** `config` stays in the flat remainder, so the honest narrowing is "make it a free fn taking `(&Config, &Arc<dyn MetricsRecorder>)`" — which is available today and needs no bundle. The draft's "needs `ServerCluster`" was fiction. |
| `run_startup_latency_test` `:848-886` | `&mut self`; reads `self.config.latency.{startup_test, startup_test_duration_secs, warning_threshold_us}` and **writes** `self.latency_baseline` (:884) | **Yes, modestly.** The write is what stops it being a free fn today. Under the partition it becomes a method on `ServerTelemetry` (which owns `latency_baseline`) taking `&Config` — an 11-field receiver plus config instead of a 47-field socket-binding object. |

So the honest score is **one genuine receiver-narrowing unlock out of three**, not three. Two of the
three are untested for a plainer reason that this proposal *does* address, just not by the
mechanism the draft claimed: **`subsystems.rs` has no `#[cfg(test)]` module at all**, and adding
one — which the bundles make worth doing, because a test module with no constructible receiver has
little to test — is what actually gets those 39 lines of version-gate logic and that split-brain
branch under test. That is a real improvement and it should be claimed as itself.

The precedent in `connection/deps.rs` is real and is the model, with one detail the draft got
wrong: `ObservabilityDeps`'s constructor is a **plain `impl Default`** (`deps.rs:279-293`), not a
`#[cfg(test)]` one — it is compiled into production and simply happens to be what the tests use.
`ConnectionConfig::default_for_testing` (`deps.rs:215-233`) *is* `#[cfg(test)]`-gated (`:217`). Both
shapes are available; pick per bundle, and prefer `#[cfg(test)]` for the ones holding real
`TcpListener`s or a RocksDB handle so no production path can accidentally construct an empty one.

**Honest limits.** This does *not* make `Server` itself constructible in a unit test:
`ServerListenerSet` holds real `TcpListener`s and `ServerPersistence` holds a real RocksDB handle,
so `with_listeners` remains the only way to get a whole server. The claim is narrower and real —
one helper becomes unit-testable by receiver-narrowing, two more become worth testing because the
file gains a test module, and the *object* does not become constructible at all. No integration
test is replaced, and no existing test changes behavior; the nine external construction sites
compile unmodified.

A regression test for §Problem 4 is the more valuable artifact and is described under Hotfix.

## Spec / LOCKED-area impact

`frogdb-server` is **not** a locked crate and has no mutation gate. No FM-tagged test moves crates,
and nothing here changes a failure-mode behavior. Two spec touchpoints, both verified by reading:

1. **`replication-failure-modes.md:1047` (FM-REPLICATION-049) — prose goes stale, and the linter
   will NOT catch it.** The Invariant cell contains, verbatim: "`Server::with_listeners` reads
   `infra.listener.local_addr()?.port()`". That is the line at `mod.rs:261`. Step 1 renames the
   expression to `infra.listeners.resp.local_addr()?.port()`. `scripts/failure-modes.py` binds
   **only** backticked `Forced by` test names against `// FM-` tags — **invariant prose is never
   parsed**, so `just lint-failure-modes` stays green over a stale sentence. **The spec prose edit
   is a required deliverable of this proposal, not an optional follow-up**, and it must be in the
   same commit as the rename.

   *Correction to the draft:* it called `mod.rs:260` "the `// FM-REPLICATION-049` tag comment".
   It is **not a tag** — `:260` reads ``// renders as `slaveN:port=0` and nobody can dial
   (FM-REPLICATION-049).``, an id mentioned in prose. `failure-modes.py` draws exactly this
   distinction itself (`FM_TAG_LINE_RE`, `:98`, documented `:92-97`: *"A comment that merely
   mentions an id in prose … is not a tag"*), so the linter never bound it and never will. The
   line still stays put and still explains the statement; it simply is not the thing the draft
   said it was, and the gap it leaves is the same gap this bullet is about.
2. **`persistence-failure-modes.md:413`** says recovery failure "is fatal: there is a single `?` at
   the server's `init_infrastructure`, no phase-level catch, no retry, and no degraded mode." This
   proposal **keeps the name `init_infrastructure` and keeps the single `?`** (`init.rs:271-281`
   is untouched). No spec edit needed — and this is a hard constraint on the refactor, not an
   observation: splitting `init_infrastructure` into separately-fallible phases is out of scope
   precisely because it would invalidate that row.

No FM tag in the two phase files is on a line this proposal touches — only those files'
return-type declarations change. **The draft's line lists mixed real tags with prose mentions**;
re-derived at `159cb7a2` against `FM_TAG_LINE_RE`:

- **Real `// FM-` tags (linter-bound):** `cluster_init.rs:1129`, `:1169`, `:1206`, `:1241`, `:1564`
  (all `// FM-REPLICATION-024`), `:1763` (`// FM-REPLICATION-048`); `replication_init.rs:417`,
  `:461` (both `// FM-REPLICATION-047`). Eight tags, all inside `#[cfg(test)]` modules
  (`cluster_init.rs:1055`, `replication_init.rs:410`) — i.e. nowhere near the return-type
  declarations at `cluster_init.rs:43-66` and `replication_init.rs:22-47`.
- **Prose mentions, not tags:** `cluster_init.rs:1532` (inside a `///` doc sentence),
  `replication_init.rs:112`, `:119`, `:161`. These bind to nothing and constrain nothing.

The conclusion the draft drew is unchanged and is now supported by the right evidence.

**Domain-vocabulary deliverable (`CONTEXT.md`).** This proposal introduces **seven nouns** into the
server's composition vocabulary — `ServerListenerSet`, `ServerTelemetry`, `ServerPersistence`,
`ServerCluster`, `ServerReplication`, `SharedCollaborators`, `BootIngredients`. Per the
[domain-docs discipline](../../../agents/domain.md), `frogdb-server/CONTEXT.md`'s `## Language`
section is where a node-level noun gets fixed, and it currently has no entry for any part of server
composition. **Adding those seven — in the same commit as Step 1, under a new
`### Server composition` subsection — is a required deliverable, not a follow-up.** Two constraints
from that file, both checked: `ServerCluster` must be defined so it cannot be read as the
**Raft Metadata Plane** (`CONTEXT.md:27-31`, which owns cluster *metadata* and never touches the
data path; `ServerCluster` is merely this node's handles to it), and none of the seven entries may
use **orchestrator** or **peer** — banned at `CONTEXT.md:30-31` and `:14` respectively, and
recorded again as resolved ambiguities at `:178`.

`just lint-gates` / the seam-lint family: no gate covers server field access, and no redirect,
clock, metrics-emission or durable-ack call site changes. Metrics *emission* is untouched; only the
path by which the recorder handle is reached changes.

## Risks / scope boundaries vs sibling proposals

### The 63/64/65 trio — proposed ordering: **63 → 64 → 65** (behind 61; full chain **61 → 63 → 64 → 65**)

All three live in server init/composition, so state the file sets precisely:

| | Owns (rewrites) | Touches mechanically |
| --- | --- | --- |
| **63** (this) | `mod.rs`, `init.rs` | `subsystems.rs` (field paths), return types of `replication_init.rs`/`cluster_init.rs`, call site in `shards.rs` |
| **64** subsystem-trait-lifecycle | `subsystems.rs` (+ new `subsystems/` module) | `mod.rs` (`start_subsystems` call), `runtime.rs:22,32` |
| **65** init-cluster-phases | `cluster_init.rs` | `mod.rs:348-372` (the ~20-arg call + destructure) |

**63 first, because 63 defines the nouns that 64's lifecycle traits and 65's phase outputs both
have to name** — landing it first means each successor writes against final field paths once
instead of retargeting them afterwards. If 64 landed first it would define traits over ungrouped
fields and every impl would be retargeted by 63; if 65 landed first, 63 would rewrite the same
`mod.rs:348-372` region a second time.

64 and 65 are **disjoint from each other** (`subsystems.rs` vs `cluster_init.rs`) once 63 is in, so
their relative order is free.

The overlap with **64 is real but shallow**: 63 touches `subsystems.rs` as a rename only and adds no
trait, no lifecycle verb, and no new module — deliberately, so that 64 has an unclaimed design
space. Conversely 64 should not invent its own grouping of `Server`'s fields; that is this
proposal's output.

### 61 is a hard predecessor, not a disjoint sibling

**The draft listed 61 among "59/60/61/62 … all in `frogdb-cluster*`/`slot_migration`. Disjoint."
That is wrong, and it was the review's sharpest catch.** Re-derived against 61 as it sits on disk
at `159cb7a2`:

| 61 claims | line in 61 | overlap with 63 |
| --- | --- | --- |
| `server/src/server/mod.rs` is one of its touched files | `61:52` | 63 rewrites this file |
| `init_replication`'s **call site** `mod.rs:264-275` gains one argument, `&infra.function_registry` | `61:52`, `61:650` | `:264-275` is inside `with_listeners` `:244-509`, which 63 rewrites wholesale |
| `mod.rs:305-342` is **deleted** — duplicated comment block, the always-`Some` guard, and the function-snapshot wiring closure | `61:52`, `61:650` | same region, same constructor |
| H1 hotfix deletes `mod.rs:305-312` | `61:647` | the deletion 63's draft had claimed as its own carve-out |

**Ruling: 61 lands BEFORE 63.** Three reasons, in order of weight:

1. **61 net-deletes ~38 lines of the region 63 rewrites** (`:305-342` out, one argument line in). A
   refactor should transcribe the *smaller* constructor. If 63 landed first, 61 would then delete
   38 lines out of freshly-restructured code and 63 would have spent effort carrying a block whose
   only future is deletion — including a duplicated comment 63 would have made look intentional.
2. **61's `mod.rs` edit is semantic, 63's is mechanical.** 61 moves a wiring closure into a factory
   in `replication_init.rs` and adds a parameter; 63 promises a diff with "no new `if`, no new `?`,
   no reordered statement". Landing the semantic change first keeps that promise auditable —
   reviewing 63 means reading a pure move, not a move tangled with someone else's rewiring.
3. **61 is already last in its own ruled chain** (`tag-hotfix → 53 → 55 → 54 → 61`, `61:555`), so
   "61 before 63" costs nothing: it sequences 63 after a chain that was going to run anyway, in a
   lane 63 otherwise does not touch.

**Consequences for this proposal, stated so the PR does not re-derive them:**

- 63's cosmetic carve-out is **withdrawn** (§Problem 6) — 61 H1 owns it.
- 63's `init_replication` call site is `mod.rs:264-275` **as 61 leaves it** (one extra argument),
  and 63's `ServerReplication` / `SharedCollaborators` split must account for
  `infra.function_registry` being read at that call site as well as at `:376-407`.
- The `if let Some(ref handler) = repl.primary_replication_handler` guard at `mod.rs:325` and its
  closure are **gone** before 63 starts, so 63's constructor rewrite is against a `with_listeners`
  roughly 470 lines long, not 509.
- 63's full ordering statement is therefore **61 → 63 → 64 → 65**, with 41 and 54 also ahead of it
  per the bullets below.

### Other overlapping proposals

- **41** (persistence small dedups) cites `init.rs:297,300,405`. Line 405 is inside the
  `wal_config` region that becomes `BootIngredients`. **Textual conflict only** — 41 changes what
  the values are, 63 changes where they are carried. Land 41 first if both are scheduled; it is
  smaller.
- **44** (RocksStore open options) cites `shards.rs:65`. 63 touches `shards.rs` only at the
  `ShardSpawnContext` construction site (`mod.rs:376-407`), not inside. Disjoint.
- **48** (FCALL cross-shard) references `subsystems.rs:559`, inside the `AcceptorContext` assembly
  (`:521-571`). Re-checked on disk: 48 cites that line **twice and read-only** (`48:91`, `48:569`),
  as the provenance of `self.allow_cross_slot`; `subsystems.rs` appears nowhere in 48's files
  table. The draft called this a "textual conflict" — it is not even that. Line `:559` is
  `allow_cross_slot: self.config.server.allow_cross_slot_standalone`, and `config` stays in 63's
  **flat remainder**, so 63 does not retarget that line at all. The nearest lines 63 does retarget
  in the same hunk are `is_replica: self.is_replica_flag` (`:565`) and
  `conn_monitor: self.conn_monitor` (`:566`). **Disjoint; 48 only needs its citation re-lined if
  63 lands first.**
  *(Push-back on the review: it gave the block as `:521-569` and said the only retargets in it are
  `is_replica_flag` and `conn_monitor`. The block is `:521-571` — `let acceptor_ctx =
  AcceptorContext {` at `:521`, closing `};` at `:571` — and 63 retargets roughly fourteen `self.`
  reads across it, including `snapshot_coordinator` `:530`, `recovery_stats` `:533`, the five
  `ClusterDeps` fields `:536-540`, `replication_tracker` `:541`,
  `primary_replication_handler` `:542`, `metrics_recorder` `:548`, `shared_tracer` `:549`,
  `keyspace_stats` `:554`. The review's two-field claim is right only about the immediate
  neighbourhood of 48's cited line, which is the part that matters for a rebase and is what the
  bullet now says.)*
- **54** (replica connection wiring) owns `replication_init.rs` internals (:223-302). 63 changes
  only that file's return type. Adjacent, not overlapping — but if 54 lands first, 63's
  `ServerReplication` must match 54's final field set.
- **59/60/62** (cluster event router, migration table, handoff finalizer) — all in
  `frogdb-cluster*`/`slot_migration`. **Disjoint.**
- **61** (primary snapshot hooks) — **NOT disjoint**; it is a hard predecessor. See
  [the section above](#61-is-a-hard-predecessor-not-a-disjoint-sibling).
- **66** (shard-worker-builder) owns `shards.rs` internals; 63 feeds `ShardSpawnContext` from
  `boot` instead of loose locals. Coordinate the context's field list; otherwise disjoint.
- **70** (ACL) will touch server dispatch, not server composition; `acl_manager` stays a flat field
  under this proposal precisely so 70 does not have to negotiate a bundle. **Disjoint.**

### Risks

- **Pure-mechanical claim is the whole safety argument.** Every step is a move or rename; if any
  hunk changes an evaluation order, the four-phase construction order
  (`init_infrastructure` → `init_replication` → `init_cluster` → `spawn_shard_workers`, `mod.rs`
  :254, :264, :348, :376) is what breaks, and it breaks at boot, loudly. Review discipline: the
  diff must contain no new `if`, no new `?`, and no reordered statement inside `with_listeners`.
- **Turmoil feature gating must survive the move.** `tls_runtime` carries
  `#[cfg(not(feature = "turmoil"))]` at its declaration and at its move (`mod.rs:505-506`), so
  `ServerListenerSet` needs the attribute on the field, and any constructor of that struct needs it
  too. This is the most likely place for a `--features turmoil` build break; `just lint-turmoil` /
  `lint-turmoil-features` cover it.
- **Bundle boundaries are a judgment call and one is genuinely arguable**: `is_replica_flag` is
  placed in `ServerReplication`, but it is read by connection-level code as a role predicate and
  could as defensibly be flat. Placing it wrong costs one field move later, not a redesign.
- **Churn against concurrent work.** `mod.rs` is the most-edited file in `server/` (76 commits) and
  this proposal rewrites two thirds of it. It should land as one commit on a quiet base, not
  incrementally.

## Effort

**M.** Roughly: 5 new struct declarations + `SharedCollaborators` + `BootIngredients` (~150 lines,
mostly moved doc comments); `init.rs` return-site rewrite (~60 lines); `mod.rs` constructor and
struct body (~200 lines net deletion of transcription); `subsystems.rs` mechanical retarget (~80
touched lines across 930); two phase-file return types; one spec prose edit
(`replication-failure-modes.md:1047`); seven `frogdb-server/CONTEXT.md` glossary entries. No
behavior change, no new test required for the refactor itself, and the nine external construction
sites are untouched.

### Hotfix — `shared_maxmemory` has no writer (S, independently landable)

**The draft's one-sentence version of this fix does not compile, and the corrected shape is
already in the tree three fields away.** The draft said *"`CONFIG SET maxmemory`'s `apply` at
`runtime_config.rs:1733` must also store into the atomic that `SystemMetricsCollector` reads"*.
That is unimplementable as stated:

- `apply` closures are `|mgr, v|` where `mgr: &ConfigManager` (`runtime_config.rs:1732-1735`).
  `ConfigManager` has no handle to `Server.shared_maxmemory` and no way to acquire one.
- The atomic is minted at `init.rs:397` — **136 lines after** `config_manager` is wrapped in an
  `Arc` at `init.rs:261`, so it cannot be injected post-construction either (no `&mut` exists).

**Use the `max_clients` pattern verbatim.** `maxclients` is the same shape of knob — a scalar that
`CONFIG SET` writes and a non-config consumer reads through an `Arc<AtomicU64>` — and the tree
already solves it by making `ConfigManager` the *owner* of the atomic and every consumer a
borrower. Five edits, each mirroring a line that already exists:

| step | model line | new line |
| --- | --- | --- |
| 1. field | `max_clients: Arc<AtomicU64>` `runtime_config.rs:766` | `maxmemory_flag: Arc<AtomicU64>` beside it |
| 2. seed | `max_clients: Arc::new(AtomicU64::new(config.server.max_clients as u64))` `:993` | `maxmemory_flag: Arc::new(AtomicU64::new(config.memory.maxmemory))` |
| 3. write | `mgr.max_clients.store(v, Ordering::Relaxed)` inside `Maxclients`' `apply` `:2331` | the same statement added to `Maxmemory`'s `apply` `:1733`, beside the existing `mgr.runtime.write().unwrap().maxmemory = v;` |
| 4. vend | `pub fn max_clients_flag(&self) -> Arc<AtomicU64>` `:3578` | `pub fn maxmemory_flag(&self)`, same body |
| 5. consume | `self.config_manager.max_clients_flag()` `subsystems.rs:158`, `:564` | `init.rs:397` becomes `let shared_maxmemory = config_manager.maxmemory_flag();` |

Two deliberate divergences from the `max_clients` model, both stated so they are decisions and not
oversights:

- **`get` is not moved onto the atomic.** `Maxclients`' `get` reads the atomic (`:2328`);
  `Maxmemory`'s must keep reading `mgr.runtime.read().unwrap().maxmemory` (`:1731`), because that
  field is what the eviction path consumes via `Propagation::Eviction` →
  `notify_eviction_change()` (`:3626-3628`). The atomic stays a **mirror maintained for the
  metrics reader**, not a second source of truth, and step 3 writes both under the one `apply`.
- **`Ordering::Relaxed` is correct here.** The store happens while the `RwLock<RuntimeConfig>`
  write guard is held, and the sole reader is a 5-second sampler
  (`telemetry/src/system.rs:90-92`). No ordering is being relied on across threads.

**Regression test**, in `runtime_config.rs`'s existing `#[cfg(test)]` module (`:3764`): set
`maxmemory` through the `ConfigManager` and assert both `maxmemory_flag().load(Relaxed)` and the
runtime-config field advanced together. No server boot, no sockets, no RocksDB. Pin it to the
*pair* rather than to the atomic alone — the defect was precisely that the two could diverge.

**Residual, stated explicitly rather than papered over: this fixes variant A only.** In OTLP-only
mode (§Problem 4 variant B) `SystemMetricsCollector` never spawns, so giving the atomic a writer
changes nothing an OTLP consumer sees — the gauge is still emitted exactly once, at boot. Fixing B
means the periodic collector must be gated on "any real backend" rather than on
`prometheus_recorder.is_some()` (`subsystems.rs:308`), and **`Server` does not retain the fact it
would need**: `has_metrics_backend` is a local in `init_infrastructure` (`init.rs:160`) and the
OTLP recorder is swallowed into the `metrics_recorder` fan-out at `:167`. So variant B's fix is a
retained flag plus a gate change — which is exactly `ServerTelemetry::metrics_enabled()` from
§Step 4. **Variant B is therefore a named follow-up on 63, not part of the hotfix**, and the
hotfix PR must say so in as many words so the row is not assumed closed.

This is still the right fix independent of 63; what 63 adds afterwards is an owner
(`ServerTelemetry`) so the next such atomic cannot be minted without one. **Do not fold the hotfix
into the refactor commit** — a behavior fix buried in a 200-line mechanical diff is unreviewable.

*(The draft listed a second carve-out here, the orphaned comment block at `mod.rs:305-311`. It is
withdrawn — proposal 61's H1 owns that deletion and states its range correctly. See §Problem 6.)*
