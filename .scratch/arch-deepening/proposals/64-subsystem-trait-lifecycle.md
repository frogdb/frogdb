# Proposal 64 — one named shutdown order, one named collaborator set: `start_subsystems` stops being a god-function

Round 38 · lane: server composition · effort **M** · **no LOCKED crate touched** (verified) ·
**not spec-first** (no failure-mode row changes; two live defects found are carved out as
independently-landable hotfixes)

Covers exploration-lane candidate **SV2** ("`start_subsystems` god-fn — `subsystems.rs:76-695`,
~620 lines over a 50-field `&mut Server`; `shutdown_subsystems` `:698-833` hand-mirrored; TODO
`:251` shutdown_tx unwired; ZERO unit tests in file. Sol: Subsystem trait + const array loop
(`PRE_DISPATCH_ORDER` shape), shutdown = reverse iterate").

Verified against **`159cb7a2`** (worktree `arch-round-38-99`; lane base was `08c143d6`; first
authored against `ade5ab80`). Every line citation below was re-read from the tree at that SHA
after review.

**The lane's sketch is half right and half wrong, and the wrong half is the load-bearing half.**
The `PRE_DISPATCH_ORDER` shape does fit — but only the *shutdown* half, and **"shutdown = reverse
iterate" is a behavior change**: today's teardown is provably *not* the reverse of today's
startup, and the places it diverges are exactly the places carrying written-down reasons (§2.2).
This proposal pins the real order instead of reversing it, and shows why the trait-array shape
does *not* fit the startup half (§4.1).

## Summary

`subsystems.rs` (930 lines) holds two functions and no tests. `start_subsystems`
(`:76-695`, **620 lines**) borrows `&mut Server` — 47 fields (`mod.rs:62-228`) — derives **14**
collaborator objects as function locals (nine of them shared by two or more consumers, §1a),
spawns 12 tasks, adopts 4 handles built
elsewhere, and returns a 13-field `SubsystemHandles` (`:34-56`) carrying **14** `JoinHandle`s.
`shutdown_subsystems` (`:698-833`, **136 lines**) is a hand-written mirror: 18 ordered steps, 11
of which stop handles (14 handles across 11 steps) and 7 of which are non-handle finalizers
(mark-not-ready, shard signal, snapshot quiesce, downstream drain, tracer, replication-state
save, RocksDB flush) whose *positions between* the handle stops are load-bearing and documented
only in prose — and, in one case, documented in a *different file* with `std::process::abort()`
as the cost of getting it wrong (§2.2, constraint **O2**).

Nothing in the file has an interface. The consequences are a census, not an opinion:

* **Zero tests.** `grep -c "cfg(test)" subsystems.rs` → `0`. Five of the twelve sibling files in
  `server/` do have `mod tests` (`checkpoint_quiesce.rs:150`, `cluster_init.rs:1056`,
  `replication_init.rs:411`, `startup.rs:111`, `shard_supervisor.rs:149`). This file is the
  outlier, and the reason is structural: both functions take `&mut Server`, and a `Server`
  cannot be built without binding four listeners, opening RocksDB and running recovery.
* **Two spawned tasks have no handle at all** — `:320-330` (version-metrics ticker) and
  `:507-516` (cursor-store evictor). Neither is stopped by anything. A third,
  `_task_monitor_handle` (`mod.rs:197`, spawned `mod.rs:455-457`), is held only to be dropped,
  and dropping a `tokio::task::JoinHandle` *detaches* rather than aborts. **Three tasks in this
  proposal's scope outlive `run_until`** (`cluster_init.rs` has two more, deliberately out of
  scope — §3), in a codebase where the test harness runs many servers inside one runtime
  (`test-harness/src/server.rs:720-726` spawns `run_until` in-process; `shutdown_mut` `:1110`
  awaits it).
* **Both operator-facing shutdown surfaces are inert, for the same missing seam.** The
  `:251` TODO is not scaffolding — trace it and it terminates at a 503 and an error string
  (§2.3). This is **LIVE**.
* **The shutdown ladder cannot be exercised without a live server**, so every ordering constraint
  in it is enforced by a comment and nothing else. Two of them have teeth: acceptors stopped
  before the downstream drain (the server-side complement of FM-REPLICATION-017, comment
  `:702-704`), and the `alive` flag cleared *before* the shards are told to stop — the invariant
  `shard_supervisor.rs:41-49` names explicitly, whose violation makes the supervisor read a
  benign teardown as a shard-worker crash and take the production fail-stop:
  `AbortFailStop::on_shard_failure` → `std::process::abort()` (`shard_supervisor.rs:50-64`,
  abort at `:63`).

The deepening is two modules, deliberately asymmetric because the two halves are not the same
shape:

1. **`SubsystemContext`** — a named type for the **nine** of the fourteen derived collaborators
   that are genuinely shared or identity-carrying (§1a; the other five have exactly one consumer
   and stay locals of the spawner that reads them, so the context does not become the `Ctx` bag
   §4.1 rejects). Their sharing invariants ("one collector means the three can never disagree",
   `:98-104`; "built before the status collector so `/status` reports the fence reason from
   *this* object", `:122-124`) are load-bearing facts that today exist only as comments inside a
   function body. Moving them behind a constructor makes them an **interface** and gives the
   file its first testable surface.
2. **`ShutdownStep` + `const SHUTDOWN_ORDER`** — the `PRE_DISPATCH_ORDER` shape
   (`connection/dispatch.rs:124-141`, driver `:354-388`, pinning tests `:850-1010`) applied to
   teardown, driven over a small `ShutdownTargets` **seam** so the order is assertable against a
   recording double with no server, no sockets and no RocksDB.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **the change.** `SubsystemHandles` `:34-56` (13 fields / 14 handles); `start_subsystems` `:76-695`; the derived-collaborator block `:78-168` + `:387-390`, `:460-473`, `:493-516`, `:521-571`; the 12 spawn sites `:302`, `:309`, `:323`, `:371`, `:421`, `:440`, `:483`, `:509`, `:590`, `:613`, `:635`, `:653-656`; the 4 adopted handles `:671-676`; `shutdown_subsystems` `:698-833`; `record_version_metrics` `:892-930` (free fn, zero tests); the misplaced doc comment `:26-31` |
| `frogdb-server/crates/server/src/server/runtime.rs` | 37 | **edited (small).** `run_until` `:13-36` — the entire lifecycle driver, and **the only** call site of both `start_subsystems` (`:22`) and `shutdown_subsystems` (`:32`); gains the in-process shutdown signal (hotfix H3a) and is where `SubsystemContext` is threaded |
| `frogdb-server/crates/server/src/server/shard_supervisor.rs` | 256 | **not edited — the source of ordering constraint O2.** `AbortFailStop`'s doc `:41-49` states the invariant `shutdown_subsystems` must uphold (*"the node's `alive` flag, which `shutdown_subsystems` clears **before** telling workers to stop"*); `impl FailStopHandler for AbortFailStop` `:50-64` is the cost of breaking it — `std::process::abort()` at `:63`. Also the 5th sibling `mod tests` (`:149`) |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | — | **not edited. The shape being copied**: `PRE_DISPATCH_ORDER` `:124-141`, driver loop `:354-388`, `unreachable!` totality proof `:388`, pinning tests `:851-1010` (`order_index` `:856`, exactly-once `:868-895`, the `MUST_PRECEDE` pair list `:908-976`, constraint suite `:982-1004`). Also the **evidence** for §2.3: `ServerWideOp::Shutdown` `:247-249` |
| `frogdb-server/crates/server/src/admin/handlers.rs` | — | **not edited by the main change.** `AdminState` `:12-27` (`shutdown_tx` `:24`), its sole reader `shutdown()` `:399-411` (returns `SERVICE_UNAVAILABLE` `:409` when `None`). Edited by hotfix H3a only if the `Option` is tightened, and by whichever of H3a/H3b rules on the discarded save flag (`:403`) |
| `frogdb-server/crates/server/src/observability_server.rs` | — | **not edited.** Route registration `:241` — proof `POST /admin/shutdown` is reachable |
| `frogdb-server/crates/server/src/server/mod.rs` | 598 | **not edited by C1-C4** (deliberately — see 63 edge). **Edited by hotfix H2 only**, and only to remove one field: `_task_monitor_handle` `:197` (declaration), its spawn `:455-457`, and its line in the `Ok(Self { … })` literal (`:499`). Otherwise cited only: `Server`'s 47 fields `:62-228`, `run()` `:514-516` |
| `frogdb-server/crates/server/src/server/init.rs` | 669 | **not edited.** Source of two adopted handles: `periodic_sync_handle` `:284`, `periodic_snapshot_handle` `:292-314` |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | 1938 | **not edited.** Source of the adopted `failure_detector_handle`. Proposal 65's file |
| `frogdb-server/crates/test-harness/src/server.rs` | — | **not edited.** The in-process lifecycle that makes leaked tasks matter: the shutdown oneshot `:694`, `run_until` spawn `:720-726`, `shutdown_mut` `:1110-1122`, `Drop` `:1129-1141` |
| `.scratch/hardening/specs/replication-failure-modes.md` | 1571 | **not edited.** FM-REPLICATION-017 `:422-433` — Invariant `:429` (cites `primary/mod.rs:518`, **not** this file), `Forced by` `:431` |
| `.scratch/hardening/specs/cluster-failure-modes.md` | — | **not edited.** FM-CLUSTER-046 `:721-731` — its NOT-observable `:727` names the leaked-task-past-shutdown hazard class for a different subsystem; Invariant `:728` |
| `website/docs-spec/specs/operations/clustering.md` | — | **not edited by the main change.** `:94-98` — the doc that promises `POST /admin/shutdown` as a real endpoint. Touched by H3a |

`frogdb-server/crates/server` (crate `frogdb-server`) is **not** a locked crate. No file under
`frogdb-txn`, `frogdb-vll`, `frogdb-persistence`, `frogdb-recovery`, `frogdb-replication`,
`frogdb-replication-runtime`, `frogdb-cluster` or `frogdb-cluster-runtime` is edited (§5).

## Problem

### 1. The 620-line body is two different modules wearing one function

Read `start_subsystems` in dependency order rather than statement order and it separates cleanly.

**1a — the derived collaborator set (~130 lines, no tasks).** Fourteen objects are constructed
from `&mut Server` and then read elsewhere in the body. **Only nine are shared or
identity-carrying**; the remaining five have exactly one consumer each and are marked `local`:

| collaborator | built | consumed by | in the context? |
|---|---|---|---|
| `start_time` | `:78` | `StatusCollector` `:157`, `ServerInfo` `:214` | ✅ 2 consumers |
| `mode: LiveMode` | `:87-96` | `StatusCollector` `:162`, `ServerDebugProvider` `:203` | ✅ 2 consumers |
| `hot_shard_collector` | `:105-108` | `observability_collectors` `:111`, `StatusCollector` `:164`, `DebugState` `:222` | ✅ 3 consumers |
| `observability_collectors` | `:109-112` | `ObservabilityDeps` `:556` | **local** to the acceptor-ctx builder |
| `self_fence_gate` | `:125-130` | `quorum_checker` `:132`, `write_fence` `:139` | ✅ 2 consumers |
| `quorum_checker` | `:131-135` | `ClusterDeps` `:544` | ✅ **identity half** (see below) |
| `write_fence` | `:138-144` | `StatusCollector` `:166` | ✅ **identity half** (see below) |
| `status_collector` | `:151-168` | HTTP `:265`, `ObservabilityDeps` `:555` | ✅ 2 consumers |
| `info_replication_state` | `:387-390` | `ClusterDeps` `:543` | **local** to the acceptor-ctx builder |
| `pubsub_forwarder` | `:460-473` | `ClusterDeps` `:545` | **local** to the acceptor-ctx builder |
| `monitor_broadcaster` | `:493-495` | `ObservabilityDeps` `:551` | **local** to the acceptor-ctx builder |
| `hotkey_session` | `:503` | `ObservabilityDeps` `:553` | **local** to the acceptor-ctx builder |
| `cursor_store` | `:506` | evictor task `:509-515`, `AdminDeps` `:532` | ✅ 2 consumers |
| `acceptor_ctx` | `:521-571` | three acceptor spawns `:580`, `:599`, `:627` | ✅ 3 consumers |

The membership rule is **not** "everything `start_subsystems` happens to bind". A context that
absorbs the five single-consumer locals would be the 50-field `&mut Server` bag one level down —
exactly what §4.1 rejects for the startup trait — so each of those five is built inside
`build_acceptor_ctx`, the one function that reads it, and never becomes a field.

Two rows are single-consumer and stay anyway, for a stated reason: `quorum_checker` and
`write_fence` are the **two halves of the one identity invariant** the context exists to
guarantee (`:122-124`, §1a bullet 2). Each is consumed once, but by *another context member*
(`acceptor_ctx` `:544` and `status_collector` `:166` respectively), and T4's `Arc::ptr_eq`
assertion is only expressible if both are reachable from the built context. `observability_collectors`
is the mirror case resolved the other way: it carries the hot-shard identity, but it derives it
from `hot_shard_collector` (`:111`), which *is* a context field — so the identity is still
assertable through `acceptor_ctx` and the collector itself stays a local.

Every one of these is a *fact about how the running server is wired*, and several carry
correctness reasoning that exists nowhere else:

* `:98-104` — "One collector means the three can never disagree, and its thresholds are the
  ConfigManager's own shared atomics… so `CONFIG SET hotshards-*` retunes all three at once."
* `:122-124` — "Built before the status collector so `/status` can report the fence reason from
  *this* object: the write gate's verdict and the reported reason are then the same evaluation,
  not two that can drift."
* `:138-143` — why `write_fence` re-derives from `self_fence_gate` rather than from
  `replication_quorum_checker` (a `dyn QuorumChecker` cannot be re-cast).

None of these is checkable. There is no type whose construction can be unit-tested to assert
"the status collector and the write gate consult the same object", so the invariant survives
only as long as the next editor reads the comment.

**1b — the twelve spawns.** Each is 5-70 lines, each conditional on a different field being
`Some`, three of them `#[cfg(not(feature = "turmoil"))]`-gated. The HTTP block alone is
`:171-305` — 134 lines nested four deep, and the `AdminState` construction that carries the
live defect (§2.3) sits at depth four inside it.

**Deletion test.** Delete `start_subsystems` and its 620 lines reappear verbatim inside
`run_until` — it hides nothing. Delete a hypothetical `SubsystemContext` and the table above
reappears as 130 lines of locals plus three comment blocks that nothing can check. The second is
a module; the first is a place.

### 2. The teardown mirror

#### 2.1 It is written by hand, twice removed from the thing it mirrors

`shutdown_subsystems` (`:698-833`) is 18 steps. Eleven stop handles (14 handles across 11 steps); seven are finalizers that
touch `self` directly. There is no mechanism connecting the two functions: adding a field to
`SubsystemHandles` compiles fine with no matching teardown step (the struct is constructed
positionally at `:678-694`, and the shutdown function destructures nothing — it reads
`handles.<field>` one at a time). **Two fields have already escaped**, and they escaped by never
becoming fields at all (`:320-330`, `:507-516`).

#### 2.2 Today's teardown is **not** the reverse of today's startup

This is the decisive fact, and it kills the lane's "shutdown = reverse iterate".

Spawn order of the 10 handles created in this function (adopted handles are excluded — they are
spawned in `init.rs` and `cluster_init.rs`, i.e. **before** all ten):

`http_server`(:302) → `system_collector`(:309) → `cluster_bus`(:371) → `replica`×2(:421,:440) →
`backlog_ttl`(:483) → `acceptor`(:590) → `admin_acceptor`(:613) → `tls_acceptor`(:635) →
`cert_watcher`(:653)

Actual teardown order, read from `:698-833`, with the **seven** non-handle finalizers in place
(rows 1, 3, 7, 12, 15, 16, 17):

| # | step | line | handle? |
|---:|---|---|---|
| 1 | mark not ready | `:700` | — |
| 2 | abort main + admin + TLS acceptors | `:705-712` | ✅ ×3 |
| 3 | send `ShardMessage::Shutdown` to every shard | `:715-717` | — |
| 4 | **await** shard supervisor | `:724-726` | ✅ (adopted) |
| 5 | abort periodic sync | `:729-731` | ✅ (adopted) |
| 6 | abort periodic snapshot | `:734-736` | ✅ (adopted) |
| 7 | poll until no snapshot in progress | `:739-745` | — |
| 8 | abort HTTP server | `:748-750` | ✅ |
| 9 | abort system collector | `:751-753` | ✅ |
| 10 | abort cluster bus | `:754-756` | ✅ |
| 11 | abort replica conn + frame consumer | `:759-762` | ✅ ×2 |
| 12 | **await** `shutdown_downstream_sessions(2s)` | `:769-773` | — |
| 13 | abort backlog-TTL ticker | `:776-778` | ✅ |
| 14 | abort failure detector | `:781-783` | ✅ (adopted) |
| 15 | shut down tracer | `:786-789` | — |
| 16 | save replication state (gated on `!is_replica`) | `:799-808` | — |
| 17 | flush RocksDB | `:820-826` | — |
| 18 | abort cert watcher | `:829-832` | ✅ |

Reverse-spawn order would begin `cert_watcher, tls_acceptor, admin_acceptor, acceptor, …`. The
real order begins with the **acceptors** and ends with the **cert watcher** — the two ends are
swapped.

##### The seven ordering constraints, with provenance

The order is a **partial order**, not a stack discipline. Seven edges are load-bearing. They do
**not** all have the same evidential status, and the difference matters for T2: a pinning test's
failure message must quote a reason that exists, not one this proposal invented.

| # | edge (`runs_before`) | source of the reason | failure mode if violated | provenance |
|---|---|---|---|---|
| **O1** | `MarkNotReady` → `AwaitShardSupervisor` | `:719-723` | the supervisor reads worker completions as crashes rather than as expected teardown. Transitively implied by O2 + O3; kept as its own pair because it is the edge the comment actually states | **quoted** |
| **O2** | `MarkNotReady` → `SignalShards` | `shard_supervisor.rs:41-49` | **the worst one in the ladder.** A worker that terminates while `alive` is still set is fatal by design: the supervisor logs CRITICAL and invokes `AbortFailStop::on_shard_failure` → `std::process::abort()` (`shard_supervisor.rs:50-64`, abort `:63`). A clean shutdown would `SIGABRT` the node. | **quoted, from another file** |
| **O3** | `SignalShards` → `AwaitShardSupervisor` | `:719-721` | the await never completes — workers were never told to stop | **quoted** |
| **O4** | `StopAcceptors` → `DrainDownstreamSessions` | `:702-704` *and* `:764-768` (the same edge stated from both ends) | a PSYNC accepted after the drain registers a downstream session nothing will end — server-side complement of **FM-REPLICATION-017** (§5) | **quoted** |
| **O5** | `StopPeriodicSnapshot` → `AwaitSnapshotQuiesce` | **no comment at either site** (`:733-736`, `:738-745`) | the `while in_progress()` poll (`:741-743`) never terminates — a live ticker re-arms a snapshot after each wait | **reconstructed here** |
| **O6** | `SaveReplicationState` → `FlushStore` | `:792-793` | the persisted durable offset is not bounded by the flushed data | **quoted** |
| **O7** | `DrainDownstreamSessions` → `StopCertWatcher` | **inferred** from the *spawn-site* comment `:647-651` (the watcher is *"deliberately not tied to the TLS listener"* because the cluster bus, replication links and admin port use TLS without a TLS client port) | the drain, which is still writing over TLS links, could lose certificate reloads mid-drain | **inferred; plausibly not load-bearing** |

**O2 is the constraint this proposal exists to make assertable.** It is stated in
`shard_supervisor.rs`, about `subsystems.rs`, and nothing in `subsystems.rs` mentions it. Today
the only thing holding it is that `self.health_checker.shutdown()` happens to be the first
statement of the function; any reorder that moved the shard signal above it would compile, pass
every test in the repo, and turn clean shutdowns into `SIGABRT`s under exactly the conditions a
worker actually returns.

**O5 and O7 are honest about their weaker status.** O5's reason is a termination argument this
proposal derives from the code, not a quotation — T2 pins it with that derivation as its message,
labelled as such. O7 is weaker still: the spawn-site comment justifies *why the watcher is spawned
independently of the TLS listener*, which is a startup fact, not a teardown one. **Ruling: pin O7
as "preserved, rationale inferred"** — the restructure must not move the cert watcher, but the
constraint suite should not claim a written reason it does not have. If review prefers, O7 can be
dropped from `MUST_PRECEDE` entirely and left to T3's full-sequence equality, which pins the
position without asserting a cause.

That partial-order shape is precisely the situation `PRE_DISPATCH_ORDER` was built for — *"the ~15
load-bearing Redis interceptions (AUTH-before-NOAUTH, MULTI-queue-before-pause,
arity-before-pause, …) — is the `const` array, not the layout of an `if`-ladder"*
(`dispatch.rs:335-339`) — and precisely the situation a reverse-iterate would silently destroy.

#### 2.3 The `:251` TODO is **LIVE**, and it is bigger than the TODO says

`subsystems.rs:251`:

```rust
shutdown_tx: None, // TODO: wire up shutdown channel from Server
```

Traced end to end at HEAD:

1. `:240-256` is the **only** construction of `AdminState` in the workspace (`grep -rn
   "AdminState {"`). Its `shutdown_tx` is therefore *always* `None`.
2. `admin/handlers.rs:405-410` is `shutdown_tx`'s only reader: `Some` → send and reply
   `{"status":"shutting_down"}` (`:406-407`); `None` → `StatusCode::SERVICE_UNAVAILABLE`
   (`:409`).
3. `observability_server.rs:241` registers `POST /admin/shutdown` on the protected router. The
   route is live whenever `http.enabled && admin.enabled`.
4. `website/docs-spec/specs/operations/clustering.md:94-98` lists it under **"Admin HTTP API —
   real endpoints"**. The sibling entry on the same line, `POST /admin/transfer-leader`, carries
   an explicit *"(**returns not-implemented** … state this honestly)"*. `/admin/shutdown` carries
   no such caveat.

**Consequence: a documented, routed, bearer-protected operator endpoint returns 503 in every
configuration.** Not dead scaffolding — a promise the binary cannot keep.

It is worse than the TODO suggests, because the *other* shutdown surface fails for the same
reason. `dispatch.rs:247-249`:

```rust
// SHUTDOWN: signaling the main server is not wired up in this mode,
// so we return a directive error rather than tearing down here.
ServerWideOp::Shutdown => Response::error(
    "ERR SHUTDOWN is not supported in this mode. Use Ctrl+C to stop the server.",
),
```

`ShutdownCommand` is a fully registered, ADMIN-flagged command (`commands/server.rs:196-234`)
whose shard-side executor deliberately errors because it "Executes via
`ConnectionHandler::dispatch_server_wide` (`handle_shutdown`)" — and `handle_shutdown` **does
not exist** (`grep -rn "handle_shutdown"` returns only that comment). The refusal string appears
nowhere else in the tree, including tests.

So **both** ways an operator can ask a FrogDB node to stop are inert, and both are blocked by
exactly one absence: nothing inside the process can complete `run_until`'s `shutdown: F`
(`runtime.rs:13-27`). The signal has only ever arrived from outside — `shutdown_signal()` in
production (`mod.rs:515`) or a test-harness oneshot (`test-harness/src/server.rs:694`).

The architectural cause is this proposal's subject: the object that would own the signal is
`Server`, and `Server` is `&mut`-borrowed through a 620-line function whose `AdminState`
construction is four levels of nesting deep. The TODO's own wording — *"wire up shutdown channel
**from Server**"* — is a description of that reach.

Both surfaces are pre-tracked: `.scratch/testing-improvements-round2/issues/open/81-…:279` (F16,
*"`POST /admin/shutdown` is permanently inert"*, re-confirmed **still-valid** at `:351` citing
this exact line). The RESP half does not appear to be tracked anywhere. Redis's own
`unit/shutdown.tcl` and `integration/shutdown.tcl` are **declared out of scope** by the
regression suite (`redis-regression/src/lib.rs:50-54`, *"Server lifecycle — different shutdown /
logging model"*), so no compatibility test catches either.

### 3. Three tasks outlive the server

| task | spawned | spawns when | handle | stopped by |
|---|---|---|---|---|
| version-metrics ticker (15 s) | `:323-329` | only if `self.prometheus_recorder.is_some()` (`:320`) | **dropped on the floor** | nothing |
| cursor-store evictor (30 s) | `:509-515` | **always** (`:507-516`, unconditional block) | **dropped on the floor** | nothing |
| tokio-metrics collector (10 s) | `mod.rs:455-457` | **always** — `TaskRegistry::spawn_collector` (`telemetry/src/task_monitors.rs:45-49`) returns a bare `JoinHandle` from a raw `tokio::spawn` (`:50`) | `_task_monitor_handle` `mod.rs:197` | nothing — `run_until(mut self)` consumes `Server`, and dropping a `JoinHandle` detaches |

The first holds `Arc<dyn MetricsRecorder>` + `Option<Arc<ClusterState>>` and keeps writing
`frogdb_active_version` / `frogdb_cluster_mixed_version` / `frogdb_version_gate_active` for a
node that no longer exists. The second holds `Arc<AggregateCursorStore>` and, with it, every
materialised `Vec<Row>` still parked in the map (`cursor_store.rs:12-27`). The third holds every
`TaskMonitor` in the registry.

**Blast radius, stated precisely.** A detached tokio task dies with the runtime that owns it, and
the runtime that owns these is the one the *test* is running on — for a `#[tokio::test]`, one per
test function. So the leak does not accumulate across a whole test binary; it accumulates across
the servers started **within a single test**, for that test's lifetime. Two of the three always
spawn (cursor evictor, task-monitor collector); the version-metrics ticker needs
`prometheus_recorder`, which most tests do not configure. The realistic figure for a
multi-server test is therefore **2 per server, not 3** — a restart-in-place test that cycles the
same node ten times carries ~20 live tickers against dead recorders and dead cursor stores while
it runs, plus the memory they pin.

That is smaller than "3N per test binary", and it does not change the ruling. `test-harness/src/server.rs:720-726`
spawns `run_until` in-process and `shutdown_mut` (`:1110`) awaits its completion — the harness's
whole point is that a server can be stopped and another started inside one runtime, which is the
precondition. **Ruling stands: LIVE under in-process multi-server runs (the default test
topology); latent in the production binary, where the process exits.**

The codebase already treats this hazard class as a failure mode elsewhere — FM-CLUSTER-046's
NOT-observable (`cluster-failure-modes.md:727`) explicitly forbids *"the reconciler keeping the
consumer — and through it the storage engine and its RocksDB lock — alive past shutdown, which
breaks restart-in-process"*, and its Invariant (`:728`) solves it with a weak sender
(`state.rs:517-527`). None of the three tasks above holds a RocksDB handle, so this is a resource
leak rather than a restart failure — but it is the same shape, unowned here.

**Scope note: "three tasks" is the count for `subsystems.rs` + `Server`, not for the node.** Two
further handle-less spawns exist in `cluster_init.rs` — the role-change consumer (`:687-691`) and
the self-role reconciler (`:706`), both `spawn(async move { … });` with the handle discarded.
They are **out of scope by R7** (64 does not touch `cluster_init.rs`; that file is proposal 65's),
and FM-CLUSTER-046 above is exactly the row that governs the reconciler. For completeness, the two
`crate::net::spawn` calls in `role_manager.rs` (`:759`, `:774`) are **not** examples of this: their
handles are stored in `RealReplicaStream { conn, consumer }` (`:788-801`) and aborted by its
`Drop` impl (`:813`). They are owned.

### 4. The one testable function in the file is untestable in practice

`record_version_metrics` (`:892-930`) is a free function of `(&Arc<dyn MetricsRecorder>,
Option<&Arc<ClusterState>>)` with no I/O — the single most unit-testable thing in
`subsystems.rs`. It has **zero** tests, because its only caller is a 15-second ticker
(`:323-329`) inside the god-function, and the file has no `mod tests` to put one in. Its
mixed-version detection (`:905-913`) compares versions **lexicographically**; the derived
`mixed` boolean is unaffected (min ≠ max iff ≥2 distinct strings), but nothing pins that, and
the whole function is skipped when `cluster_state` is `None`, so three gauges are simply absent
in standalone mode. Absent-not-faked is the right call — but it is an undocumented, untested
one.

## Why this shape, in the vocabulary

The lane proposed one shape for both halves. They need different ones.

**The startup half is a data-dependency DAG, and a trait array would make it shallower.** A
`trait Subsystem { fn start(&self, ctx: &Ctx) -> Option<JoinHandle<()>> }` iterated over a
`const` array requires every subsystem to accept the *same* argument. But the HTTP block needs
`status_collector` + `hot_shard_collector` + `role_manager_handle` + a `take()`n listener; the
replica block needs a `take()`n handler, a `take()`n receiver, and performs a **side effect
whose ordering is load-bearing** (`register_boot_replica_handler` at `:405-406`, which
`:399-404` says *must* precede the acceptor spawn); the three acceptors share one
`AcceptorContext` that is itself built from nine prior values. Forcing them through one
signature means a `Ctx` bag holding the union of all their inputs, each subsystem reaching in
for its subset — which is the 50-field `&mut Server` problem reproduced one level down, with
`Option::take` racing across trait-object calls. Apply the **deletion test** to that trait: delete
it, and the twelve spawn blocks reappear *unchanged*, because none of them was ever the same
shape as another. Complexity does not vanish, it relocates. **One adapter means a hypothetical
seam** — and twelve one-off adapters that share no varying behaviour is twelve hypothetical
seams. **Rejected.**

What the startup half actually needs is the thing the DAG keeps re-deriving: a **module** for the
shared collaborators, whose **interface** states the sharing invariants (`:98-104`, `:122-124`,
`:138-143`) that comments carry today. `SubsystemContext::build(&mut Server) -> SubsystemContext`
is **deep**: ~130 lines of construction and four correctness arguments behind one constructor
and a dozen accessors. Its **leverage** is that the twelve spawners each name only what they
read; its **locality** is that "which object does `/status` report the fence reason from" is one
question with one answer at one place, checkable by a unit test that constructs the context and
asserts pointer identity — impossible today at any cost.

**The shutdown half is order-only, and that is exactly what a `const` array is for.** Every step
takes no arguments beyond the handles and `self`; the entire content of the module is *which
step runs before which, and why*. `PRE_DISPATCH_ORDER` is the proven local precedent for this,
down to the pinning-test structure (`dispatch.rs:850-1010`) that asserts each documented
constraint by name.

The **seam** goes between the driver and everything it reaches — the seven finalizers that touch
`Server`, *and* the step boundaries themselves. `ShutdownTargets` has **two real adapters** —
`Server` in production, a recording double in tests — which satisfies "two adapters means a real
seam". The double turns the driver into a pure function from `SHUTDOWN_ORDER` to a call log, so
the order becomes directly assertable with no listeners, no sockets and no RocksDB.

Two design points that a naive version of this seam gets wrong, both worth stating before C3:

* **The trait is not seven methods.** A `ShutdownTargets` with one method per finalizer variant is
  a hand-mirror of the enum — the exact failure mode §2.1 indicts `shutdown_subsystems` for, moved
  behind a trait. Adding a finalizer would mean editing the enum *and* the trait *and* two impls,
  with nothing forcing agreement. One method taking the step (`run_finalizer(step)`) has the same
  recording power and a single point of exhaustiveness.
* **The finalizers are not the whole ladder.** Thirteen of the eighteen steps stop a handle by
  calling `JoinHandle::abort()` on `SubsystemHandles` directly (all except the shard-supervisor
  `await` at `:724-726`), and a `ShutdownTargets` double never sees those — it would record 7 of
  18 steps and T3's "the call log equals the expected sequence" claim would be false as written.
  The driver, not the target, must do the recording: it enters every step through the seam.

## Proposed change

### C1 — `SubsystemContext`

```rust
/// What `start_subsystems` derives from `Server` and shares between two or
/// more consumers. Built once, before any task exists.
///
/// Membership is earned, not automatic: a value read by exactly one spawner
/// stays a local of that spawner (§1a). Nine values qualify, and three facts
/// are guaranteed *by construction* rather than by convention:
///
/// * one `HotShardCollector` reaches `FROGDB.HOTSHARDS`, `/status` and the
///   debug UI, so the three cannot disagree;
/// * `status_collector`'s write-fence reporter is the *same object* as
///   `quorum_checker`, so a rejection and its reported reason are one
///   evaluation;
/// * `acceptor_ctx` is built after every collaborator it names, so the three
///   acceptor ports are configured identically apart from their `PortSpec`.
pub(super) struct SubsystemContext { /* the nine ✅ rows of §1a */ }

impl SubsystemContext {
    pub(super) fn build(server: &mut Server) -> Result<Self> { /* :78-168 moved
        verbatim, then `build_acceptor_ctx` below */ }

    /// `:387-390`, `:460-473`, `:493-506`, `:521-571` — the acceptor context and
    /// the five single-consumer values it is the only reader of
    /// (`info_replication_state`, `pubsub_forwarder`, `monitor_broadcaster`,
    /// `hotkey_session`, `observability_collectors`), which are locals here
    /// rather than fields above.
    fn build_acceptor_ctx(server: &mut Server, /* the eight ✅ rows */) -> AcceptorContext { … }
}
```

Accessors are `pub(super)`; the fields stay private so the identity guarantees cannot be
sidestepped. `build` takes `&mut Server` because `acceptor_ctx` does
`std::mem::take(&mut self.new_conn_senders)` (`:558`) — that stays, and moves *into* the
constructor where a test can observe it.

**The `ClusterDeps` / `AdminDeps` / `ObservabilityDeps` struct literals (`:527-557`) move
verbatim and stay literals.** No named constructor is introduced for any of them. This is a
constraint, not an aesthetic preference: sibling 67 deletes `ClusterDeps::cluster`
(`connection/deps.rs:137-161`) as caller-less, and adopting it here would resurrect a caller — of
a constructor that cannot even set `pubsub_forwarder` (R5).

The cursor-evictor spawn currently sits *inside* this range (`:507-516`); C2 lifts it out into
`spawn_cursor_evictor` so `build` contains no `spawn` at all. That is what makes the constructor
testable without a runtime, and it is the same move H2 needs to give the evictor a handle.

### C2 — twelve named spawners

Each spawn site becomes a free function in `subsystems.rs` taking exactly what it reads:

```rust
fn spawn_http_server(server: &mut Server, ctx: &SubsystemContext)
    -> Result<Option<JoinHandle<()>>>;   // :171-305, 134 lines
fn spawn_cluster_bus(server: &mut Server, ctx: &SubsystemContext)
    -> Option<JoinHandle<()>>;           // :336-378
fn spawn_replica_tasks(server: &mut Server)
    -> Option<(JoinHandle<()>, JoinHandle<()>)>;  // :393-457, incl. the :405 registration
fn spawn_acceptors(server: &mut Server, ctx: &SubsystemContext) -> Acceptors;  // :575-645
/* … 8 more */
```

`start_subsystems` collapses to ~50 lines: build the context, call the spawners in the order
below, take the four adopted handles (`:671-676`), set ready (`:668`), return.

**Startup order is preserved exactly**, and is enforced by two different mechanisms:

* Eleven of the twelve orderings are **data dependencies the compiler enforces** — a spawner
  cannot run before the context it borrows exists.
* The twelfth is not, and gets a named assertion: `register_boot_replica_handler` (`:405-406`)
  must precede `spawn_acceptors`, because *"no `REPLICAOF` can race ahead of this call"*
  (`:399-404`). Making the acceptor spawner consume an `AcceptorsArmed` zero-sized token that
  only `spawn_replica_tasks` can mint turns that comment into a compile error. (If that reads as
  too clever in review, a `debug_assert!` on a flag is the fallback; the comment must not remain
  the only enforcement either way.)

No spawn is moved, added, removed or reordered by C1+C2.

### C3 — `ShutdownStep` + `const SHUTDOWN_ORDER` + the `ShutdownTargets` seam

```rust
/// Everything the driver reaches that is not a `JoinHandle`.
/// Two adapters: `Server` (production) and `RecordingTargets` (tests).
pub(super) trait ShutdownTargets {
    /// Called by the driver for **every** step, before the step runs — the
    /// recording seam. Production `Server` takes the default no-op; the test
    /// double pushes `step` onto its log. Without this the double would only
    /// ever see the 7 finalizer steps, never the 11 handle-stop steps.
    fn enter_step(&mut self, _step: ShutdownStep) {}

    /// The seven finalizer steps, dispatched by variant rather than by one
    /// method each — a method-per-variant trait is the same hand-mirror this
    /// proposal is removing (see *Why this shape*). Non-finalizer variants are unreachable
    /// here; the driver never calls this for them.
    async fn run_finalizer(&mut self, step: ShutdownStep);
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum ShutdownStep {
    MarkNotReady, StopAcceptors, SignalShards, AwaitShardSupervisor,
    StopPeriodicSync, StopPeriodicSnapshot, AwaitSnapshotQuiesce,
    StopHttpServer, StopSystemCollector, StopVersionMetrics, StopCursorEvictor,
    StopTaskMonitor, StopClusterBus, StopReplicaTasks, DrainDownstreamSessions,
    StopBacklogTtl, StopFailureDetector, ShutdownTracer, SaveReplicationState,
    FlushStore, StopCertWatcher,
}

/// THE canonical teardown order. Deliberately **not** the reverse of startup —
/// see the `MUST_PRECEDE` table in `tests` for the seven constraints (§2.2)
/// and their provenance.
pub(super) const SHUTDOWN_ORDER: [ShutdownStep; 21] = [ /* §2.2's 18 rows, in order,
   with StopVersionMetrics + StopCursorEvictor + StopTaskMonitor inserted beside
   StopSystemCollector */ ];
```

`shutdown_subsystems` becomes the driver:

```rust
for step in SHUTDOWN_ORDER {
    targets.enter_step(step);
    match step.kind() {
        StepKind::Handle => step.stop_handle(&mut handles).await,
        StepKind::Finalizer => targets.run_finalizer(step).await,
    }
}
```

Each arm of `stop_handle` / `run_finalizer` is the corresponding block from `:698-833`, moved
verbatim. Both `match`es are exhaustive over `ShutdownStep`, so a new variant is a compile error
in exactly two places and `enter_step` needs no edit at all.

**Three new steps, no reorder.** `StopVersionMetrics`, `StopCursorEvictor` and `StopTaskMonitor`
are the fix for §3 — **one variant per task, three tasks, three variants**. Riding two tasks on
one arm would work but defeats C4's whole mechanism (one field, one arm, one variant is what makes
an unread field a compile error), so the arithmetic is: **18 steps today → 21 with H2**, and T1's
`len()` assertion pins 21. Without H2 the array is `[ShutdownStep; 18]` and the three variants do
not exist (see R1's split option).

The three are placed adjacent to `StopSystemCollector` because all three are metrics/housekeeping
tickers holding no protocol state, and nothing else in the ladder observes them. That placement is
the only *addition* to the order; positions 1-18 are byte-for-byte the sequence in §2.2.
`_task_monitor_handle` is moved off `Server` (`mod.rs:197`, `:455-457`, `:499`) into
`SubsystemHandles` — the one `mod.rs` edit in this proposal, and the reason H2 has a sequencing
constraint against 63 (see R4).

### C4 — exhaustiveness, so the next handle cannot escape

`SubsystemHandles` gains no fields it can hide. `stop_handle` takes `&mut SubsystemHandles` and
every arm does `handles.<field>.take()`, so the compiler flags an unread field under
`#[deny(dead_code)]`, and a test (T3 above) asserts every field is `None` once the driver
finishes. This is why C3 spends a variant per task rather than riding three tasks on one arm: the
guarantee is *one field ↔ one arm ↔ one variant*, and a shared arm reintroduces exactly the gap
the two floor-dropped tasks of §3 fell through. The third (`_task_monitor_handle`) escaped a
different way — it was never a `SubsystemHandles` field at all, it was a `Server` field held only
to be dropped — which C3's move fixes and this mechanism then keeps fixed.

## Testability improvement

The file goes from **0 tests** to a `mod tests` that covers the parts that actually carry risk.

| # | test | what it forces | possible today? |
|---|---|---|---|
| T1 | every `ShutdownStep` variant appears in `SHUTDOWN_ORDER` exactly once; `len() == 21` (`== 18` if H2 is deferred) | a step added and never scheduled, or scheduled twice | no |
| T2 | **the constraint suite** — a `MUST_PRECEDE: &[(ShutdownStep, ShutdownStep)]` pair list validated in a loop against `order_index` (`dispatch.rs:908-976` + `:982-1004`, exactly that shape) covering all seven edges **O1-O7** of §2.2, each carrying its source as the failure message and each labelled *quoted* / *reconstructed* / *inferred* per the provenance column | a reorder that silently breaks the FM-REPLICATION-017 complement (**O4**), the snapshot-quiesce termination (**O5**), the save-before-flush bound (**O6**), or — the one with `process::abort()` behind it — the clear-`alive`-before-signalling-shards invariant of `shard_supervisor.rs:41-49` (**O2**) | no |
| T3 | drive `SHUTDOWN_ORDER` over a `RecordingTargets` double (`enter_step` logging **every** step, `run_finalizer` logging the seven) and `SubsystemHandles` filled with `spawn(pending())`; assert the log equals `SHUTDOWN_ORDER` element-for-element **and** every handle field is `None` and `is_finished()` on every spawned task | the whole ladder, end to end, in milliseconds, with no listeners/sockets/RocksDB | no |
| T4 | `SubsystemContext::build` on a minimal server: `Arc::ptr_eq` between `status_collector`'s write-fence reporter and `quorum_checker`; between the hot-shard collector reached via `/status`, `ObservabilityDeps` and `DebugState` | the two identity invariants that are comments at `:98-104` and `:122-124` | no |
| T5 | `record_version_metrics` (`:892-930`) directly: `mixed = false` for one version, `true` for two, gauges absent for `None` cluster state, `VersionGateActive` emitted once per `VERSION_GATES` entry | the only pure function in the file | technically, but there is no test module to hold it |
| T6 | `SubsystemContext::build` + `spawn_replica_tasks` asserts `register_boot_replica_handler` ran before any acceptor exists | `:399-404`'s race window | no |

T2 and T3 are the ones that matter. They are also the ones that make the *next* change to this
file safe: a proposal that adds a subsystem (or 63/65 restructuring what feeds it) currently has
no way to know it broke teardown ordering short of a flaky integration failure.

T3's "every task terminal" assertion needs one `tokio::task::yield_now().await` (or a short
`join` on each handle) after the driver returns, because `JoinHandle::abort()` is a request the
runtime services at the next poll, not a synchronous kill. This is the only piece of T3 that is
not a pure data comparison, and it is the reason the ordering claim is carried by the
`enter_step` log rather than by observing task deaths.

## Spec / LOCKED impact

**No locked crate is edited.** `frogdb-server` is not one of the four locked areas (ADR-0002 to
ADR-0004; gates: txn 0.90, persistence 0.85, replication 0.85, cluster 0.80). Every file in the
Files table is under `frogdb-server/crates/server/` (plus `test-harness`, read-only), plus one
website doc under H3a. **No
`just mutants-gate` run is required**, and `just mutants-diff` has no locked crate to target.

**No FM-tagged test is touched.** The nearest row is **FM-REPLICATION-017** (*"a PSYNC that
arrives behind the shutdown drain is refused, not half-served"*,
`replication-failure-modes.md:422-433`). Read precisely:

* its **Invariant** (`:429`) cites `PrimaryReplicationHandler::draining` and `handle_psync`
  (`primary/mod.rs:518`) — **not** `subsystems.rs`;
* its **`Forced by`** (`:431`) names `psync_after_the_shutdown_drain_is_refused` and
  `shutdown_downstream_sessions_ends_a_streaming_session`, both at
  `frogdb-server/crates/replication/src/replica_session.rs:3485` and `:3435`. Neither constructs
  a `Server` or calls `shutdown_subsystems`.

So the spec's invariant lives **inside** the handler and is fully forced there. What lives in
`subsystems.rs` is its *server-side complement* — the drain must be reached only after the
acceptors are down (`:702-704`, constraint **O4**) — which is unpinned today and which T2 pins.
That is strictly additive coverage of an existing invariant, not a new claim, so **this proposal
is not spec-first** and needs no row edit.

The same holds for **O2**, the fail-stop invariant: its written home is a Rust doc comment in
`shard_supervisor.rs` (`:41-49`), not a failure-mode row, and no `FM-` tag names it — `grep`
confirms `shard_supervisor.rs` carries none. T2 gives it its first executable form without
creating a spec obligation.

**If** the implementation chooses to tag T2 into FM-REPLICATION-017's `Forced by`, then
`.scratch/hardening/specs/replication-failure-modes.md:431` must be edited in the same commit and
`just lint-failure-modes` (part of `just lint`, `Justfile:293`/`:319`) re-run — the lint enforces
spec↔test agreement in both directions. Note the guidance that a forcing test should live in the
mutated crate: T2 lives in `frogdb-server`, not `frogdb-replication`, so it would contribute
nothing to the replication crate's score. **Recommendation: do not tag it**; keep T2 as a
server-crate ordering pin and leave the spec row alone.

`just lint-gates` (`Justfile:329`, fifteen chokepoint gates) — none of the fifteen governs task
spawning or teardown ordering, so no gate is added or affected. The metrics-chokepoint and
clock-seam gates already cover the moved code; the move is verbatim, so their verdicts are
unchanged.

## Risks / scope boundaries

### R1 — ordering is behavior, and this proposal's whole job is not to change it

The mitigation is that the order is *written down* before it is *moved*: §2.2's 18-row table plus
the O1-O7 provenance table are the acceptance artefacts, T2 is their executable form, and C3 says
explicitly that positions 1-18 are preserved. The only intentional deltas are the three inserted
stop-steps of C3 and the three tasks they stop, which is a **fix** (§3) and must be called out in
the PR body as a behavior change, not folded in silently. If review prefers zero behavior delta in
the restructure, split: land C1+C2+C3 with `SHUTDOWN_ORDER.len() == 18` and no
`StopVersionMetrics`/`StopCursorEvictor`/`StopTaskMonitor` variants, then H2 separately (which
then also carries the one `mod.rs` edit, keeping the restructure's file set to
`subsystems.rs` + `runtime.rs`).

### R2 — `&mut Server` and `Option::take` interleaving

**Seven** `Option::take()` sites move, covering eight fields: `http_listener` `:226-228`,
`cluster_bus_listener` `:339-341`, `replica_handler` **and** `replica_frame_rx` (one tuple pattern,
`:394`), `listener` `:575-577`, `admin_listener` `:597`, `tls_listener` `:624` — plus
`std::mem::take(&mut self.new_conn_senders)` `:558`, which is a different operation on a
non-`Option` field and moves into `build_acceptor_ctx`. Splitting into `build` + twelve spawners
means each take lands in exactly one function; a take that lands in the wrong one produces a
`None` at runtime, not a compile error (three of the seven are followed by `.expect(...)` —
`:229`, `:342`, `:578`). Mitigation: the move is mechanical and each `expect` message names its
precondition; T3's handle-exhaustiveness assertion plus the existing integration suite (any
`TestServer::start` failure surfaces immediately) covers it. Reviewers should diff take-sites
specifically.

### R3 — `#[cfg(not(feature = "turmoil"))]` arms

**Two** handle fields are feature-gated (`tls_acceptor` `:41-42`, `cert_watcher` `:44-45`), and
there are **seventeen** `cfg`-gated sites in the file overall — the ones this restructure moves
are the spawn-side blocks at `:272-293`, `:361-369`, `:623-656` and `:584`/`:604`, the
`AcceptorContext` field pair `:567-570`, the `SubsystemHandles` construction at `:685-687`, and
the two teardown arms `:709-712` and `:829-832`. `SHUTDOWN_ORDER`'s length differs between
the two builds, so T1's `len()` assertion and the array itself must be `cfg`-split, or the two
steps kept as always-present variants whose `apply` is a no-op under `turmoil`. **Prefer the
latter** — one array, one length, one constraint suite, and the turmoil build then also proves
it did not lose a step. `just lint-turmoil-features` / `just lint-turmoil` (`Justfile:319`) must
both pass.

### R4 — trio boundary: 63 → **64**, 65 free

The three SV proposals partition server composition; state the split precisely because the file
names sound overlapping:

| proposal | owns | this proposal's relationship |
|---|---|---|
| **63 — server-subsystem-bundles (SV1)** | `server/mod.rs` (`Server`'s 47 field declarations, `:62-228`) + `server/init.rs` (where they are constructed) | **64's C1-C4 edit neither.** But 64's twelve spawners read `self.<field>` by name, so a 63 rename/regroup rebases 64 mechanically. **63 lands first.** Hotfix **H2 is the one exception** — see below. |
| **64 — this proposal (SV2)** | `server/subsystems.rs` + `server/runtime.rs` (+ `server/mod.rs`, H2 only) | — |
| **65 — init-cluster-phases (SV3)** | `server/cluster_init.rs` | **Textually disjoint from 64.** 64 only *adopts* `failure_detector_handle` at `:671` and never edits its construction. 64 and 65 can run in parallel. |

**H2 collides with 63, and the collision is a single field.** H2 removes `_task_monitor_handle`
from `Server` (`mod.rs:197`, spawn `:455-457`, literal line `:499`) and puts the handle in
`SubsystemHandles`. 63 **keeps** that field and files it inside its `ServerObservability` bundle
(`63:217` lists `_task_monitor_handle` among the bundle's eleven members). Both cannot be right.
Sequence explicitly, either way works:

* **H2 before 63** (preferred — H1/H2 are advertised as pre-restructure hotfixes): 63's
  `ServerObservability` is authored with **ten** members, not eleven, and 63's row loses
  `_task_monitor_handle` before it is written. Cheapest, because the field is gone rather than
  moved twice.
* **H2 after 63**: H2 removes the member from the already-landed `ServerObservability` bundle and
  retargets the spawn line. Same end state, one extra edit, and 63's bundle-size claims need a
  one-word amendment.

**What must not happen** is H2 and 63 landing without either party knowing, because both compile
independently and the loser is silently re-added.

**Trio ordering is disputed on disk — three documents, three answers.** This proposal said
63 → 64 with 65 parallel; `63:355` says **63 → 64 → 65**; `65:464` says **63 → 65 → 64**. All
three agree on 63 first, and 63 itself notes at `:371-372` that "64 and 65 are disjoint from each
other (`subsystems.rs` vs `cluster_init.rs`) once 63 is in, so their relative order is free" —
which is also this proposal's reading, re-verified: `cluster_init.rs` appears in 64's Files table
as **not edited**. **Stated preference: 63 → 64, 65 any time after 63.** The 64/65 edge is
genuinely free, so this is a scheduling question, not a correctness one — **defer to the round's
ruling on sequencing** rather than treating any of the three proposals as authoritative about the
others.

One correction to a sibling's claim, for whoever reconciles them: `63:362` lists 64 as touching
"`mod.rs` (`start_subsystems` call), `runtime.rs:22,32`". The `start_subsystems` call is **not**
in `mod.rs` — `runtime.rs:22` is its only call site in the workspace, and `runtime.rs:32` is
`shutdown_subsystems`'s. 64 does touch `mod.rs`, but for the H2 field, not for a call site.

### R5 — sibling proposals on disk

Checked every proposal in `.scratch/arch-deepening/proposals/` that names a `server/` composition
file:

| sibling | overlap | verdict |
|---|---|---|
| **61 — primary-snapshot-hooks** | cites `subsystems.rs:76` in its Files table marked *evidence only* (`61:49`, `:100`, `:115`); edits `server/mod.rs:305-342` + `replication_init.rs` | **No conflict.** 64's only `mod.rs` edit is H2's removal of `_task_monitor_handle` (`:197`, `:455-457`, `:499`) — disjoint from `:305-342`. If 64 lands first, 61's three `subsystems.rs:76` citations drift by line number only. |
| **62 — handoff-finalizer-move** | `slot_migration/mod.rs`, `cluster-runtime/handoff_barrier.rs`, `cluster_init.rs` | **No overlap** — `subsystems.rs` does not appear in 62's Files table. Confirmed 62 is on disk at HEAD. |
| **59 — cluster-event-router** | `cluster_init.rs` only (`59:53`), with an explicit criterion keeping that diff empty | **No overlap.** |
| **57, 58** | `cluster_init.rs` as evidence only | **No overlap.** |
| **48 — fcall-cross-shard** | cites `subsystems.rs:559` (`allow_cross_slot`) as one hop of a **trace**, not an edit (`48:91`) | **No conflict**; line-number drift only. |
| **41, 49** | `server/init.rs` | **No overlap** with 64; edge is with 63. |
| **05 — role-manager** | landed round-8 proposal, cites old line numbers | historical only. |
| **66 — shard-worker-builder** | `core/shard/builder.rs`, `worker.rs`, `types.rs` | **No overlap, and 66 says so.** `66:528-529`: *"`subsystems.rs` (930 lines) is not read or written here. Shard spawning is `mod.rs` Phase 4 and is not currently a `Subsystem`"* — with a conditional if 64 made shard spawning one. It does not: C2's twelve spawners are exactly today's twelve spawn sites, and shards are not among them. **No coordination needed.** |
| **67 — server-small-dedups (SV5/6/7)** | `connection/builder.rs` (deleted), `connection.rs`, `connection/deps.rs` | **Adjacent, one live trap.** 67 declares `subsystems.rs` **read-only and owned by 64** (`67:59`: *"Owned by sibling 64 — 67 must not edit it"*), citing `:527`/`:536`/`:545` as evidence only. Its SV5 **deletes `ClusterDeps::cluster` (`connection/deps.rs:137-161`)** because its sole caller is `builder.rs:124` (`67:289`). **The trap:** if C1 replaced the `ClusterDeps` struct literal at `:535-546` with that named constructor, SV5's deletion would resurrect a caller. **C1 does not.** The ten-field literal moves verbatim into `build_acceptor_ctx`; no named constructor is adopted. `67:293-296` names the same condition and reaches the same resolution ("if 64 wants a named cluster-deps constructor it should write a correct one — this one cannot set `pubsub_forwarder`"). **Verdict: no collision, provided C1 keeps the literal.** Recorded here so a later editor does not "tidy" it into the doomed constructor. |

### R6 — open issues that already live in this block

`.scratch/arch-deepening/issues/open/11` (STATUS bypasses the collector) and `…/12` (status mode
frozen at startup) both carry **Resolution** sections — issue 11's dated **2026-07-20** (`11-…:45`),
issue 12's 2026-07-21 (`12-…:40`) — whose landed code is
`:151-168` and `:80-96` respectively — the exact lines C1 moves into `SubsystemContext`. They are
resolved-in-code but still filed under `open/`. No coordination is needed; they are evidence of
how this block accretes, and their invariants become T4's assertions. Worth closing them in the
same pass. `…/06` (replica-handler shutdown watch) likewise resolved, and its deleted dead save
path is documented in the comment now at `:809-817`.

### R7 — what this proposal deliberately does not do

It does not touch `cluster_init.rs`'s own background spawns — **including the two handle-less ones
at `:687-691` (role-change consumer) and `:706` (self-role reconciler)**, which are the same leak
shape as §3 but belong to proposal 65's file and, for the reconciler, to FM-CLUSTER-046. §3's
"three tasks" is the count within this proposal's scope, not a node-wide census. It does not
restructure `Server`'s fields (63's job), does not change the four adopted handles' construction
sites, and does not add a `Subsystem` trait for startup (§4.1 explains why). It also does not
attempt graceful (non-`abort`) teardown for any task that aborts today — every `abort()` stays an
`abort()`.

## Effort

**M.** ~620 lines restructured (moved, not rewritten), ~90 lines of new interface, ~200 lines of
new tests, ~140 lines net deleted from the god-function body.

| step | content | size |
|---|---|---|
| 1 | `SubsystemContext` + `build` — the §1a table moved verbatim; T4 | **S** — ~130 moved, ~50 new |
| 2 | Twelve spawners + `start_subsystems` reduced to ~50 lines; T6 and the `AcceptorsArmed` token | **M** — ~380 moved |
| 3 | `ShutdownTargets` seam (`enter_step` + `run_finalizer`) + `Server` adapter | **XS** — ~60 new |
| 4 | `ShutdownStep` + `SHUTDOWN_ORDER` + driver; the 18 arms moved verbatim; T1, T2 (the O1-O7 `MUST_PRECEDE` list), T3 | **M** — ~140 moved, ~150 test |
| 5 | T5 (`record_version_metrics`) | **XS** |

Steps 1-2 and 3-4 are independently landable in either order (they touch disjoint halves of the
file), which keeps the review diffs under ~400 lines each.

### Independently-landable hotfixes

| # | content | size | behavior? |
|---|---|---:|---|
| **H1** | `subsystems.rs:26-31` — the doc comment *"Handles for all spawned subsystem tasks. / Collected during startup so shutdown can cleanly stop everything."* is attached to **`const BACKLOG_TTL_TICK: Duration`** (`:32`), running straight into that constant's own doc. `SubsystemHandles` (`:34`) has **no** doc comment at all. Rustdoc currently renders a `Duration` constant as "handles for all spawned subsystem tasks". Pure docs move. | **XS** | none |
| **H2** | Capture the three unstopped tasks (§3): give the version-metrics ticker (`:320-330`) and cursor evictor (`:507-516`) fields in `SubsystemHandles`, move `_task_monitor_handle` off `Server` (`mod.rs:197`, `:455-457`, `:499`) into it, and abort all three beside `system_collector`. Also switch the two raw `tokio::spawn` calls to `crate::net::spawn` for consistency with the other ten (note: `frogdb_net::spawn` **is** `tokio::spawn` — `crates/net/src/lib.rs:45` — so this is cosmetic, **not** a turmoil defect; do not claim otherwise). **Sequence against 63 — see R4**: 63 files this field inside its `ServerObservability` bundle (`63:217`), so H2 must land before 63 (bundle authored with ten members) or after it (bundle amended). | **S** | **yes** — three tasks that ran forever now stop. Needs T3, and a PR note. |
| **H3a** | **HTTP surface only.** `run_until` (`runtime.rs:13-36`) mints a `watch::channel` before `start_subsystems`, `Server` carries the sender, `AdminState.shutdown_tx` (`subsystems.rs:251`) becomes `Some(tx)`, and `run_until` selects on the caller's `shutdown` future *and* the receiver. Production `run()` (`mod.rs:514-516`) keeps passing `shutdown_signal()` unchanged. Turns `POST /admin/shutdown` from a permanent 503 into the endpoint the docs promise. | **S**, ~40 lines | **yes, on one operator surface.** |
| **H3b** | **RESP `SHUTDOWN`.** Strictly larger: the sender has to reach `ConnectionHandler::dispatch_server_wide` (`dispatch.rs:247-249`), which means a new field on the connection dependency structs (`connection/deps.rs`) threaded from **every** `AcceptorContext` construction — production `subsystems.rs:521-571` and the test builder `acceptor.rs:390` (`AcceptorContext` literal `:402`) — plus reply-semantics decisions Redis makes that FrogDB has never had to make (below). | **M** | **yes, on the second operator surface.** |

**H3 is two hotfixes, not one, and the split is not cosmetic.** H3a is a channel and a `select`
inside one function whose only call sites are `runtime.rs:22`/`:32`. H3b is plumbing across the
connection layer plus a protocol-semantics question:

* **A successful `SHUTDOWN` normally sends no reply at all** — Redis closes the connection instead,
  because replying then dying is a lie about ordering. FrogDB's `dispatch_server_wide` returns a
  `Response` on every arm, so "succeed silently" is a shape the dispatcher does not currently have.
  Deciding this is a design call, not a wiring one.
* **`SHUTDOWN [NOSAVE|SAVE]` is currently ignored on both surfaces.** The HTTP handler discards it
  outright (`admin/handlers.rs:403`: `let _ = body; // save flag reserved for future use`), and the
  RESP path never gets far enough to look. Wiring the signal without ruling on the argument turns a
  clean refusal into a silent mis-honour, which is worse.

**Both should be filed as their own issues, not folded into 64**, and H3b should not be filed
without H3a's ruling in hand. Between them they also need: their own tests (RESP `SHUTDOWN`
terminates the node; `POST /admin/shutdown` with a valid bearer token terminates it and a
subsequent `PING` fails — the acceptance criterion already written at
`.scratch/testing-improvements-round2/issues/open/81-…:326`), an ACL/authorisation review (the
bearer gate on that route is itself untested and default-open — `…/issues/open/40-…`), and a doc
pass on `website/docs-spec/specs/operations/clustering.md:94-98`. 64 makes both *easy* — the signal
has one obvious owner once `SubsystemContext` exists — but 64 does not need either, and neither
needs 64.

H1 is safe to land ahead of the restructure unconditionally. H2 is too, subject to the 63
sequencing above, and both shrink the restructure's diff.
