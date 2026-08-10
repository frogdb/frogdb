# Proposal 63 — `Server` adopts the decomposition `ShardWorker` already has

Round 38 · lane: server composition · effort **M** · **not** a LOCKED crate (`frogdb-server` has no
mutation gate) · **not spec-first** (no behavior change proposed; the one live defect found is
carved out as an independently-landable hotfix, not folded in)

Covers exploration-lane candidate **SV1** ("Server god-object + `InitResult` ~40-field
transcription"). Verified against `6e99f567`.

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
  five-field `ConnectionDeps` (`deps.rs:303-309`), two of which carry test constructors
  (`ObservabilityDeps::default` `deps.rs:279-293`, `ConnectionConfig::default_for_testing`
  `deps.rs:217-233`).
- One crate down, **`ShardWorker` — which does strictly more work than `Server` — is already
  decomposed exactly this way**: 27 fields in a 104-line body (`core/src/shard/worker.rs:106-209`),
  of which **9 are subsystem structs** (`ShardIdentity`, `ShardPersistence`, `ShardObservability`,
  `ShardEviction`, `ShardVll`, `ShardCluster`, `ShardTracking`, `ShardScripting`,
  `ShardSubscriptions`), the rest a small flat remainder (store, receivers, registry).
- In `server/` itself, `bind_listeners` returns a bundle whose doc comment says it is "ready to be
  handed to `Server`" (`listeners.rs:12`) — and `init_infrastructure` un-hands it into five loose
  locals on the next five lines (`init.rs:186-190`).

So this proposal is not "invent a grouping". It is **stop flattening the groupings that already
exist**, using the shard's own vocabulary. `Server` holds `listeners`, `observability`,
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
owner, so there is nowhere for the writer to go. Full call path in §Problem 4; it is offered as a
standalone hotfix that does not wait on the refactor.

The `ConfigPlane` bundle named in the lane brief is **rejected here** on the deletion test — see
§"Deletion test, applied honestly".

## Files involved (verified at `6e99f567`)

| File | Lines | Role in this proposal |
| --- | --- | --- |
| `frogdb-server/crates/server/src/server/mod.rs` | 598 | **Primary.** `Server` struct :62-228, `with_listeners` :244-509, accessors :514-597 |
| `frogdb-server/crates/server/src/server/init.rs` | 669 | **Primary.** `InitResult` :34-104, `init_infrastructure` :107-496 |
| `frogdb-server/crates/server/src/server/listeners.rs` | 125 | Read-only reference: `BoundListeners` :12-23 is re-used as-is |
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **Touched mechanically only** — field paths retargeted (`self.x` → `self.observability.x` etc.) |
| `frogdb-server/crates/server/src/server/runtime.rs` | 37 | Untouched (reads `self.config` only, :24) |
| `frogdb-server/crates/server/src/connection/deps.rs` | 309 | Precedent, not edited |
| `frogdb-server/crates/core/src/shard/worker.rs` | — | Precedent (`ShardWorker` :106-209), not edited |
| `frogdb-server/crates/core/src/shard/types.rs` | — | Precedent (`ShardObservability` :155-170 and siblings), not edited |
| `frogdb-server/crates/server/src/server/replication_init.rs` | 552 | Return-type site only: `ReplicationInitResult` :22-47 |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | 1938 | Return-type site only: `ClusterInitResult` :43-66 |
| `frogdb-server/crates/server/src/server/shards.rs` | 367 | Call-site only: `ShardSpawnContext` :20-… fed from bundles |

Construction sites that must keep compiling (the entire external surface of `Server`, verified):
`frogdb-server/src/main.rs:144`, `test-harness/src/server.rs:697`,
`server/tests/common/sim_helpers.rs:170,235,283,333,472,515`,
`browser-tests/tests/common/mod.rs:40`. All call `Server::with_listeners(config, listeners, …)`,
whose signature does **not** change.

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

A reader of `InitResult` cannot tell these apart without diffing it against `Server` by hand. The
same unmarked split exists in the two later phases: `ReplicationInitResult` (`replication_init.rs`
:22-47, 10 fields) and `ClusterInitResult` (`cluster_init.rs:43-66`, 11 fields) each mix values the
server stores with values it forwards into `spawn_shard_workers` and drops.

### 3. The grouping is proven in-tree and discarded at exactly one point

`bind_listeners` returns `BoundListeners { resp, admin_resp, http, cluster_bus, tls }`
(`listeners.rs:12-23`), doc-commented "All bound listeners for the server, **ready to be handed to
`Server`**". `init_infrastructure` then does:

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

### 4. LIVE defect found while tracing ownership — `shared_maxmemory` has no writer after boot

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

Preconditions for it to bite: metrics enabled (`prometheus_recorder.is_some()`) **and** a runtime
`CONFIG SET maxmemory`. Both are ordinary operation, so this is ruled **LIVE**, not latent.

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
have **none**. The reason is mechanical: the only way to obtain a `Server` is
`Server::with_listeners`, which binds sockets, opens RocksDB and runs recovery, and all 47 fields
are private with no other constructor. Every assertion about `check_split_brain_logs`
(`subsystems.rs:836-845`), `run_startup_latency_test` (:848-886) or `record_version_metrics`
(:892-930) therefore has to be made through a full integration server or not at all.

### 6. Verified cosmetic defect: an 8-line orphaned comment block

`mod.rs:305-311` is a **byte-identical duplicate** of `mod.rs:313-319` (the "Ship the
function-library registry to every full-syncing replica…" paragraph), separated by blank line 312.
The second copy continues at :320-324 and documents the `if let` at :325. The first copy documents
nothing. 8 dead lines, zero behavior. This is the only *verified* correctness-adjacent finding in
`mod.rs` besides §4, and it survived 76 commits — which is itself evidence about how carefully a
598-line constructor gets read.

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
`core/shard/types.rs`:

| New struct | Fields (from `Server`) | Count |
| --- | --- | --- |
| **`ServerListenerSet`** (re-use `BoundListeners`, add `tls_runtime`) | `listener`, `admin_listener`, `http_listener`, `cluster_bus_listener`, `tls_listener`, `tls_runtime` | 6 |
| **`ServerObservability`** | `metrics_recorder`, `prometheus_recorder`, `keyspace_stats`, `latency_histograms`, `shared_tracer`, `latency_baseline`, `health_checker`, `conn_monitor`, `_task_monitor_handle`, `shared_maxmemory`, `shard_memory_used` | 11 |
| **`ServerPersistence`** | `rocks_store`, `snapshot_coordinator`, `recovery_stats`, `periodic_sync_handle`, `periodic_snapshot_handle` | 5 |
| **`ServerCluster`** | `cluster_state`, `node_id`, `raft`, `network_factory`, `slot_migration`, `failure_detector`, `failure_detector_handle`, `role_manager_handle` | 8 |
| **`ServerReplication`** | `replication_tracker`, `replica_handler`, `replica_frame_rx`, `primary_replication_handler`, `replication_quorum_checker`, `replication_self_fence`, `shared_replication_offset`, `is_replica_flag` | 8 |
| *(flat remainder)* | `config`, `registry`, `client_registry`, `config_manager`, `acl_manager`, `function_registry`, `shard_senders`, `new_conn_senders`, `shard_supervisor_handle` | 9 |

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
    pub observability: ServerObservability,
    pub persistence: ServerPersistence,
    pub shared: SharedCollaborators,   // registry, client_registry, config_manager, acl_manager,
                                       // function_registry, shard_senders, new_conn_senders
    pub boot: BootIngredients,         // the 12 consumed fields, named as such
}
```

`BootIngredients` is the load-bearing half of this step: it turns the undocumented retained/consumed
split of §Problem 2 into something the compiler tracks. `with_listeners` destructures `boot` into
`spawn_shard_workers` and it is gone; anything left in it at the end of the constructor is a value
that was silently dropped, which today is invisible.

`ReplicationInitResult` and `ClusterInitResult` get the same two-part shape: the retained half *is*
`ServerReplication` / `ServerCluster`, the forwarded half stays a small named struct
(`replication_init.rs`: `broadcaster`, `primary_addr`, `replication_identity`,
`shared_replication_offset` are read at `mod.rs:359-361,393`; `cluster_init.rs`: `role_controller`
is cloned at `mod.rs:402`).

### Step 3 — collapse the five address accessors

The five accessors at `mod.rs:547-573` become one method on `ServerListenerSet` plus five
one-line delegations kept on `Server` for source compatibility with the eight external construction
sites and their `local_addr()` calls (`test-harness/src/server.rs`, `sim_helpers.rs`,
`browser-tests/tests/common/mod.rs`). **No public signature changes.** The delegations are three
lines each, not five, and the `expect("listener not yet taken")` at `mod.rs:550` moves to the one
place that owns the `Option`.

### Step 4 — `subsystems.rs` field-path retarget only

`start_subsystems` and `shutdown_subsystems` read server fields directly; each read becomes
`self.<bundle>.<field>`. This is a mechanical rename with no logic change, and it is the whole of
this proposal's contact with 930 lines. Two small wins fall out and should be taken:
`ServerObservability::metrics_enabled()` replaces the three duplicated
`self.prometheus_recorder.is_some()` gates (:171, :308, :320), and the cross-field assertion at
:224 (`// SAFETY: http_listener is Some when prometheus_recorder is Some`) becomes a statement
about two bundles that can, in a follow-up, be made structural. **This proposal does not change
that invariant's representation** — it only puts the two fields where a later change could.

### Deletion test, applied honestly

- **`ServerListenerSet`** — delete it and you get back five loose fields, five accessors, five
  destructuring lines and the `Option` unwrap comment. Complexity reappears verbatim; the type
  already exists as `BoundListeners` and is being thrown away. **Keep.**
- **`ServerObservability`** — delete it and the 11 fields scatter, the three `is_some()` gates come
  back, and `shared_maxmemory` returns to having no owner (§Problem 4). Complexity reappears.
  **Keep.**
- **`ServerPersistence`** — delete it and the RocksDB flush at `subsystems.rs:820-826` no longer
  sits next to the two periodic handles it must be ordered against. Complexity reappears. **Keep.**
- **`ServerCluster` / `ServerReplication`** — delete them and `ClusterInitResult` /
  `ReplicationInitResult` still exist as producers, so the fields would simply be re-flattened at
  the constructor exactly as today. Complexity reappears. **Keep.**
- **`ConfigPlane` (`config` + `config_manager`) — REJECTED, contra the lane brief.** `self.config`
  is read **50 times** in `subsystems.rs` alone, by every subsystem without exception, and
  `runtime.rs:24` reads it too. It is ambient input, not any subsystem's state. Deleting a
  two-field wrapper around two things that are already single fields and already passed as single
  things makes **no** complexity reappear — it is a rename with an extra `.` in front of it. It
  fails the deletion test and is not proposed.
- **`BootIngredients`** — delete it and the 12 consumed fields go back to being
  indistinguishable from the 27 retained ones. That distinction is currently derivable only by
  cross-referencing two struct bodies by hand, which is how §Problem 2 was found. **Keep.**

## Testability improvement

The concrete unlock is that **the bundles, not `Server`, become the test surface**. Three helpers
in `subsystems.rs` are pure logic over a subset of fields and today have no test because their
receiver is `&Server`:

- `check_split_brain_logs` (`subsystems.rs:836-845`) needs `ServerCluster` + `ServerObservability`.
- `record_version_metrics` (:892-930) needs `ServerObservability` + `cluster_state`.
- `run_startup_latency_test` (:848-886) needs `ServerObservability` + `config`.

Narrowing each receiver from a 47-field socket-binding object to a 5-11 field struct is what makes
them constructible in-crate — the same move that gave `connection/` its tests via
`ObservabilityDeps::default()` (`deps.rs:279-293`) and
`ConnectionConfig::default_for_testing` (`deps.rs:217-233`). Each new bundle gets the matching
`#[cfg(test)]` constructor, and `mod.rs`/`subsystems.rs` get their first `#[cfg(test)]` modules.

**Honest limits.** This does *not* make `Server` itself constructible in a unit test:
`ServerListenerSet` holds real `TcpListener`s and `ServerPersistence` holds a real RocksDB handle,
so `with_listeners` remains the only way to get a whole server. The claim is narrower and real —
the *helpers* become unit-testable, the *object* does not. No integration test is replaced, and no
existing test changes behavior; the eight external construction sites compile unmodified.

A regression test for §Problem 4 is the more valuable artifact and is described under Hotfix.

## Spec / LOCKED-area impact

`frogdb-server` is **not** a locked crate and has no mutation gate. No FM-tagged test moves crates,
and nothing here changes a failure-mode behavior. Two spec touchpoints, both verified by reading:

1. **`replication-failure-modes.md:1047` (FM-REPLICATION-049) — prose goes stale, and the linter
   will NOT catch it.** The Invariant cell contains, verbatim: "`Server::with_listeners` reads
   `infra.listener.local_addr()?.port()`". That is the line at `mod.rs:261`, carrying its own
   FM-REPLICATION-049 comment at :260. Step 1 renames the expression to
   `infra.listeners.resp.local_addr()?.port()`. `scripts/failure-modes.py` binds **only** backticked
   `Forced by` test names against `// FM-` tags — **invariant prose is never parsed**, so
   `just lint-failure-modes` stays green over a stale sentence. **The spec prose edit is a required
   deliverable of this proposal, not an optional follow-up**, and it must be in the same commit as
   the rename. The `// FM-REPLICATION-049` tag comment at `mod.rs:260` stays exactly where it is,
   attached to the same statement.
2. **`persistence-failure-modes.md:413`** says recovery failure "is fatal: there is a single `?` at
   the server's `init_infrastructure`, no phase-level catch, no retry, and no degraded mode." This
   proposal **keeps the name `init_infrastructure` and keeps the single `?`** (`init.rs:271-281`
   is untouched). No spec edit needed — and this is a hard constraint on the refactor, not an
   observation: splitting `init_infrastructure` into separately-fallible phases is out of scope
   precisely because it would invalidate that row.

No FM tag in `cluster_init.rs` (:1129, :1169, :1206, :1241, :1532, :1564, :1763) or
`replication_init.rs` (:112, :119, :161, :417, :461) is on a line this proposal touches — only those
files' return-type declarations change.

`just lint-gates` / the seam-lint family: no gate covers server field access, and no redirect,
clock, metrics-emission or durable-ack call site changes. Metrics *emission* is untouched; only the
path by which the recorder handle is reached changes.

## Risks / scope boundaries vs sibling proposals

### The 63/64/65 trio — proposed ordering: **63 → 64 → 65**

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

### Other overlapping proposals

- **41** (persistence small dedups) cites `init.rs:297,300,405`. Line 405 is inside the
  `wal_config` region that becomes `BootIngredients`. **Textual conflict only** — 41 changes what
  the values are, 63 changes where they are carried. Land 41 first if both are scheduled; it is
  smaller.
- **44** (RocksStore open options) cites `shards.rs:65`. 63 touches `shards.rs` only at the
  `ShardSpawnContext` construction site (`mod.rs:376-407`), not inside. Disjoint.
- **48** (FCALL cross-shard) references `subsystems.rs:559`, inside the `AcceptorContext` assembly
  (:521-571). 63 retargets field paths in that block. **Textual conflict, no semantic conflict**;
  whichever lands second rebases mechanically.
- **54** (replica connection wiring) owns `replication_init.rs` internals (:223-302). 63 changes
  only that file's return type. Adjacent, not overlapping — but if 54 lands first, 63's
  `ServerReplication` must match 54's final field set.
- **59/60/61/62** (cluster runtime, migration table, snapshot hooks, handoff finalizer) — all in
  `frogdb-cluster*`/`slot_migration`. **Disjoint.**
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

**M.** Roughly: 5 new struct declarations + `BootIngredients` (~150 lines, mostly moved doc
comments); `init.rs` return-site rewrite (~60 lines); `mod.rs` constructor and struct body (~200
lines net deletion of transcription); `subsystems.rs` mechanical retarget (~80 touched lines across
930); two phase-file return types; one spec prose edit. No behavior change, no new test required for
the refactor itself, and the eight external construction sites are untouched.

### Independently landable ahead of the refactor

Two carve-outs, both of which stand alone and neither of which blocks or is blocked by the
restructure:

1. **Hotfix (real defect, S)** — give `shared_maxmemory` a writer. `CONFIG SET maxmemory`'s `apply`
   at `runtime_config.rs:1733` must also store into the atomic that
   `SystemMetricsCollector` reads (`telemetry/src/system.rs:90`). The regression test is a unit
   test in `frogdb-server/crates/server` that sets `maxmemory` through the `ConfigManager` param and
   asserts the atomic advanced — no server boot needed. This is the *right* fix independent of 63;
   what 63 adds afterwards is an owner (`ServerObservability`) so the next such atomic cannot be
   minted without one. **Do not fold this into the refactor commit** — a behavior fix buried in a
   200-line mechanical diff is unreviewable.
2. **Cosmetic (XS)** — delete the orphaned duplicate comment block at `mod.rs:305-311` (8 lines
   including the separating blank). Zero risk, and it removes a block that would otherwise be
   carried along by the constructor rewrite and look intentional.
