# Proposal 05 — A RoleManager deep module owning Role Promotion / Role Demotion

## Summary

Two problems, one root cause.

**Architecture problem.** There is no module that owns "what role is this node, and
what happens when it changes." Role state is scattered across at least three
disconnected representations:

- `is_replica: Arc<AtomicBool>` — the data-path flag (write guard, `ROLE`/`INFO`
  reflection, frame-consumer stop signal), threaded by hand through the acceptor,
  connection deps, connection guards, shard workers, and `Server`.
- `NodeRole` (`Primary`/`Replica`) — a *separate* enum living entirely in the Raft
  cluster state machine (`frogdb-cluster`), mutated by `ClusterCommand::Failover`.
- The replica connection lifecycle — a `ReplicaReplicationHandler` + frame-consumer
  task that is constructed **once at boot** from static config and never
  reconstructed.

"Promote" and "demote" are therefore not operations on any object; they are an
implicit, undocumented protocol smeared across a command handler, an atomic store,
a Raft log entry, a split-brain event consumer, and a boot-time init function. No
two of these three representations are kept in sync by anything.

**Correctness gap (latent bug).** Runtime `REPLICAOF <host> <port>` (Role Demotion)
is an unwired stub. It parses arguments, logs, and returns `+OK` — it flips no flag,
constructs no replica handler, opens no connection to the new primary. The code
comment claims "the server's replication manager will initiate the connection," but
**there is no replication manager at runtime**: `init_replication` runs only at
startup, driven purely by config. Confirmed by an exhaustive grep (see below): no
`replicaof_tx`, no `set_primary`, no `reconfigure_replica`, no `ReplicationManager`
runtime object, no channel the handler sends on. `CommandContext` exposes only
`is_replica: bool` and `is_replica_flag: Option<Arc<AtomicBool>>` — no trigger the
handler could pull. A client that issues `REPLICAOF host port` against a running
node receives `+OK` and remains a fully writable primary. The success surface is a
lie.

A single `RoleManager` deep module resolves both: it becomes the one owner of the
role flag, the current-primary target, and the streaming handle, and it exposes
`promote()` / `demote(primary_addr)` as real operations — closing the runtime
demotion gap as a side effect of giving role transitions a home.

## Files involved (verified paths + counts)

All paths under `frogdb-server/crates/` unless noted.

| File | Lines | Role in the fragmentation |
| --- | --- | --- |
| `server/src/commands/replication.rs` | 565 | `ReplicaofCommand::execute`: the NO-ONE promote branch pokes role state directly; the host/port demote branch is the no-op stub |
| `server/src/server/replication_init.rs` | 198 | `init_replication`: startup-only construction of primary/replica/standalone handlers from config |
| `server/src/failure_detector.rs` | 827 | `trigger_auto_failover`: flips role via `ClusterCommand::Failover` to Raft — never touches `is_replica` |
| `server/src/acceptor.rs` | 572 | threads `is_replica: Arc<AtomicBool>` (field decl `:131`, propagation `:179`, test default `:413`) |
| `server/src/connection.rs` | 785 | holds `is_replica: Arc<AtomicBool>` (`:171`, from config `:264`) |
| `server/src/connection/guards.rs` | 867 | `run_pre_checks` READONLY write-guard reads the flag (`:257`) |
| `server/src/connection/deps.rs` | — | `is_replica: Arc<AtomicBool>` dep field (`:194`, default `:218`) |
| `server/src/connection/builder.rs` | — | constructs the flag (`:261`) |
| `server/src/server/mod.rs` | 447 | stores `is_replica_flag` (`:188`), wires it into subsystems (`:292`, `:371`) |
| `server/src/server/shards.rs` | — | pushes flag into each worker (`:44`, `:188`) |
| `server/src/server/subsystems.rs` | — | passes flag to `consume_frames` (`:310`) and connection build (`:427`); spawns the boot-only replica tasks |
| `server/src/server/cluster_init.rs` | — | creates the single `is_replica_flag` from config (`:578`–`:583`); split-brain **demotion event consumer** (`:441`–`:500+`) that only *logs*, never reconfigures |
| `replication/src/apply.rs` | — | `consume_frames` reads the flag to stop on promotion (`:104`, `:115`) |
| `cluster/src/types.rs` | — | `NodeRole` enum + `Display` (`master`/`slave` wire-compat) (`:37`–`:49`) |
| `cluster/src/state.rs` | — | Raft state machine mutates `NodeRole`; emits `DemotionEvent` (`:180`–`:360`) |
| `core/src/command.rs` | — | `CommandContext.is_replica` (`:1079`) + `is_replica_flag` (`:1086`) — the only role handles a command sees |

## Problem — architecture

Role is not represented once; it is represented three times, in three subsystems
that do not reference each other:

1. **Data-path flag** — `is_replica: Arc<AtomicBool>`, created in `cluster_init.rs`
   from `config.replication.is_replica()` and cloned into: the acceptor, every
   connection (`deps` → `guards`), every shard worker, and the replica frame
   consumer. Reads: `guards.rs:257` (rejects `WRITE` commands with `READONLY`),
   `apply.rs:115` (frame consumer breaks when the flag goes false), and the
   per-command `ctx.is_replica` snapshot used by `ROLE`/`WAIT`. The **only**
   runtime writer is `REPLICAOF NO ONE`. Nothing ever stores `true` at runtime.

2. **Topology role** — `NodeRole::{Primary,Replica}`, owned exclusively by the Raft
   metadata plane (`cluster/src/state.rs`). `trigger_auto_failover` writes one
   `ClusterCommand::Failover { old_primary_id, new_primary_id, force }` log entry;
   the state machine flips the enum and emits a `DemotionEvent`. This never touches
   the `AtomicBool`.

3. **Streaming lifecycle** — a `ReplicaReplicationHandler` + `consume_frames` task,
   built once in `init_replication` and spawned once in `subsystems.rs`, gated on
   `config.replication.is_replica()` at boot. There is no runtime constructor.

The consequence: "what actually flips on promote/demote" is an implicit contract
that no single reader can see. Promotion means "store `false` in the atomic *and*
let the frame consumer notice *and* let `ROLE` report master." Demotion — the
symmetric operation — has **no implementation at all**, because there is no place it
would live. The split-brain `DemotionEvent` consumer in `cluster_init.rs:441` is the
one component that reacts to a Raft demotion at runtime, and all it does is *log
divergent writes*: it does not set `is_replica_flag = true`, does not build a
replica handler, does not connect to the new primary. So even the Raft-driven
failover path leaves the demoted node's *data path* still behaving as a primary.

## Problem — correctness gap

`ReplicaofCommand::execute` (`commands/replication.rs:47`–`98`) has two branches.

The **promote** branch (`NO ONE`) does real work — it writes role state in two
places:

```rust
// Check for "NO ONE" to stop replication
if arg1.eq_ignore_ascii_case("no") && arg2.eq_ignore_ascii_case("one") {
    // Stop replication, become standalone primary
    tracing::info!("REPLICAOF NO ONE - stopping replication, promoting to primary");

    // Clear the replica flag so ROLE, INFO, and write guards
    // all reflect the new primary status immediately.
    ctx.is_replica = false;
    if let Some(ref flag) = ctx.is_replica_flag {
        flag.store(false, std::sync::atomic::Ordering::Release);
    }

    return Ok(Response::ok());
}
```

The **demote** branch (`<host> <port>`) parses, logs, and returns `+OK` — nothing
else:

```rust
// Parse host and port
let host = arg1.to_string();
let port: u16 = arg2.parse().map_err(|_| CommandError::InvalidArgument {
    message: "invalid port number".to_string(),
})?;

if port == 0 {
    return Err(CommandError::InvalidArgument {
        message: "port cannot be 0".to_string(),
    });
}

tracing::info!(
    host = %host,
    port = port,
    "REPLICAOF - configuring as replica"
);

// Return OK - the actual connection happens asynchronously
// The server's replication manager will initiate the connection
Ok(Response::ok())
```

The comment on the last two lines is the misleading surface:

> `// Return OK - the actual connection happens asynchronously`
> `// The server's replication manager will initiate the connection`

**What is missing, backed by grep:**

- No runtime "replication manager" exists. `init_replication` (`replication_init.rs`)
  reads `config.replication.is_primary()` / `is_replica()` exactly once and
  constructs handlers from static config; it has no re-entry point and no caller
  outside boot.
- Grep of the entire `frogdb-server` for any trigger the handler could rely on —
  `replicaof_tx`, `set_primary`, `reconfigure_replica`, `ReplicationManager`, a
  `watch`/`mpsc` the handler sends on — returns **nothing**. The only `demote`/
  `promote` hits are (a) the Raft cluster state machine (`ClusterCommand::Failover`,
  topology-plane, unrelated to this handler), (b) failure-detector *priority*
  scoring, and (c) hyperloglog/collection encoding promotion. None is reachable
  from `ReplicaofCommand`.
- `CommandContext` (`core/src/command.rs`) gives a command handler only
  `is_replica: bool` (`:1079`) and `is_replica_flag: Option<Arc<AtomicBool>>`
  (`:1086`). There is no sender, no handler handle, no primary-target setter.
- The host/port branch does not even flip `is_replica_flag` to `true`, so the node
  does not so much as *report* itself as a replica after the command — `ROLE` and
  the write guard still say primary. The node stays writable.

**Net effect:** `REPLICAOF host port` against a live node is a silent no-op that
returns success. This is a latent correctness bug, not merely an architecture smell.

## Why it is shallow

In Ousterhout's terms this is a textbook *shallow module*: a wide, confident
interface (`REPLICAOF host port` → `+OK`) sitting on top of a **missing**
implementation. The interface promises a role transition; the body performs none.
Worse than a shallow module that does a little — this one advertises a capability it
does not have.

There is also *no seam at which a role transition is expressed*. Because role lives
as a raw atomic plus a disconnected enum plus a boot-only lifecycle, there is nowhere
for a caller to say "become a replica of X" and have the system make it true. The
promote path only works because it is a *removal* (store `false`, let a consumer
notice) that needs no new resources; demotion needs to *acquire* resources (open a
socket, start a stream), and with no owning module there is no code that could.
Every current role write is a poke at one of the three fragments, chosen ad hoc by
whichever subsystem happens to be holding a clone of the atomic.

## Proposed change

Introduce a `RoleManager` deep module (in `frogdb-server`, most naturally beside the
replication wiring) that owns the role triad behind a narrow interface:

```rust
/// Owns this node's replication *role* and the lifecycle that a role change
/// implies. The single writer of the data-path role flag.
pub struct RoleManager {
    /// The one data-path flag. Replaces the hand-threaded Arc<AtomicBool>;
    /// everyone who reads role reads it through here (or a handle it vends).
    is_replica: Arc<AtomicBool>,
    /// Current primary we are replicating from (None when primary/standalone).
    primary_target: Option<SocketAddr>,
    /// Live streaming handle (connection task + frame consumer), or None.
    streaming: Option<StreamingHandle>,
    /// Factory that opens/starts a replica stream to a primary address.
    /// Injected so tests can substitute a fake — see Testability.
    streamer: Arc<dyn ReplicaStreamer>,
}

impl RoleManager {
    /// Role Promotion: become a writable primary. Stops any inbound stream,
    /// clears the flag. Idempotent.
    pub async fn promote(&mut self) { /* stop streaming; is_replica=false */ }

    /// Role Demotion: become a replica of `primary_addr`. Stops any existing
    /// stream, sets the flag, opens the new stream. Idempotent per target.
    /// THIS is what REPLICAOF host port is currently missing.
    pub async fn demote(&mut self, primary_addr: SocketAddr) { /* ... */ }

    /// Handle to the data-path flag, for the write guard / ROLE / INFO / frame
    /// consumer. They keep reading an Arc<AtomicBool>; only the *writer* moves.
    pub fn role_flag(&self) -> Arc<AtomicBool> { self.is_replica.clone() }
}
```

Wiring:

- `ReplicaofCommand::execute` stops poking the atomic directly. Both branches call
  the manager: `NO ONE` → `role_manager.promote()`; `host port` →
  `role_manager.demote(addr)`. (Because `execute` is sync and today only has the
  atomic in `CommandContext`, the command enqueues a role-transition request that
  the manager task drains — mirroring how `PSYNC`/`WAIT` already hand off to
  out-of-band handlers via a connection-level op. The manager owns the async work;
  the command owns only the parse + request.)
- `init_replication` becomes "ask the `RoleManager` to enter the boot role"
  (`promote()` for primary/standalone, `demote(primary_addr)` for a configured
  replica). Boot and runtime demotion then share **one** code path instead of the
  runtime path being empty.
- The failure detector's demotion consumer (`cluster_init.rs:441`) calls
  `role_manager.demote(new_primary_addr)` *after* logging split-brain divergence, so
  a Raft-driven failover actually reconfigures the data path — closing the second
  half of the same gap.

**Raft Metadata Plane boundary (explicit).** `RoleManager` *reflects* role; it does
not own cluster topology consensus. `NodeRole`, slot ownership, and the Config Epoch
stay owned by the Raft metadata plane in `frogdb-cluster`. In cluster mode the flow
is one-directional: Raft decides the topology change (`ClusterCommand::Failover`,
epoch bump) → emits the demotion/promotion event → the event handler calls
`RoleManager::demote/promote` to make the *local data path* match the consensus
decision. `RoleManager` never writes `NodeRole` and never initiates a topology
change. In standalone replication mode (no Raft), `REPLICAOF` is the only driver and
`RoleManager` is the sole role authority. This keeps a single data-path role owner
without double-owning consensus.

## Before / After

**Before** — `ReplicaofCommand::execute`, the two live branches
(`commands/replication.rs:63`–`97`):

```rust
// Check for "NO ONE" to stop replication
if arg1.eq_ignore_ascii_case("no") && arg2.eq_ignore_ascii_case("one") {
    // Stop replication, become standalone primary
    tracing::info!("REPLICAOF NO ONE - stopping replication, promoting to primary");

    // Clear the replica flag so ROLE, INFO, and write guards
    // all reflect the new primary status immediately.
    ctx.is_replica = false;
    if let Some(ref flag) = ctx.is_replica_flag {
        flag.store(false, std::sync::atomic::Ordering::Release);
    }

    return Ok(Response::ok());
}

// Parse host and port
let host = arg1.to_string();
let port: u16 = arg2.parse().map_err(|_| CommandError::InvalidArgument {
    message: "invalid port number".to_string(),
})?;

if port == 0 {
    return Err(CommandError::InvalidArgument {
        message: "port cannot be 0".to_string(),
    });
}

tracing::info!(
    host = %host,
    port = port,
    "REPLICAOF - configuring as replica"
);

// Return OK - the actual connection happens asynchronously
// The server's replication manager will initiate the connection
Ok(Response::ok())
```

**After** — both branches express a role transition at one seam; the host/port
branch actually starts streaming:

```rust
// Role Promotion: become a writable primary.
if arg1.eq_ignore_ascii_case("no") && arg2.eq_ignore_ascii_case("one") {
    tracing::info!("REPLICAOF NO ONE - Role Promotion to primary");
    ctx.role_transition(RoleTransition::Promote)?; // hands off to RoleManager
    return Ok(Response::ok());
}

// Role Demotion: become a replica of <host> <port>.
let host = arg1.to_string();
let port: u16 = arg2.parse().map_err(|_| CommandError::InvalidArgument {
    message: "invalid port number".to_string(),
})?;
if port == 0 {
    return Err(CommandError::InvalidArgument {
        message: "port cannot be 0".to_string(),
    });
}
let addr = resolve_primary(&host, port)?;

tracing::info!(host = %host, port = port, "REPLICAOF - Role Demotion");

// Enqueue the transition; RoleManager sets the flag, tears down any existing
// stream, and opens the replica stream to `addr`. No longer a no-op.
ctx.role_transition(RoleTransition::Demote(addr))?;
Ok(Response::ok())
```

(`RoleManager::demote` performs the store + stream-start that the old branch never
did. `ROLE`/`INFO`/the write guard immediately read the flag the manager set.)

## Testability improvement

Today a role transition can only be exercised against a live 2-node cluster over
real TCP sockets: promotion is observable only via `ROLE`/write-guard side effects,
and **demotion cannot be tested at all because it does nothing**. The three role
fragments have no seam a unit test can grab.

With `RoleManager` owning `promote()`/`demote(addr)` behind an injected
`ReplicaStreamer` trait, each transition is a pure in-process unit test:

- `promote()` clears the flag and stops the (fake) stream — assert flag == false,
  streamer.stop called.
- `demote(addr)` sets the flag, tears down any prior stream, and starts a new one to
  `addr` — assert flag == true, streamer.start(addr) called. **This is the currently
  untestable runtime-demotion path**, now covered without a socket.
- promote→demote→promote sequencing and idempotency, verified against the fake
  streamer's call log.

The real `ReplicaStreamer` (wrapping `ReplicaReplicationHandler` + `consume_frames`)
is swapped in only in integration tests, keeping the fast unit tests hermetic.

## Risks / open questions

- **Cluster-mode failover interaction (primary risk).** `RoleManager` must be a pure
  reflector of Raft decisions in cluster mode — it must never write `NodeRole`,
  bump the Config Epoch, or initiate a topology change, or we double-own consensus.
  The demotion consumer wiring must call `RoleManager` *after* the Raft commit +
  split-brain logging, never before. Ordering (log divergence → flip data-path role
  → open stream to new primary) needs to be pinned down and tested.
- **Where does the runtime demote target resolve to?** In cluster mode the new
  primary's address comes from the topology (node table), not from client-supplied
  host/port. `REPLICAOF host port` issued by a client in cluster mode should likely
  be rejected (as Redis rejects `REPLICAOF` in cluster mode); only the failover event
  should drive `demote`. Decide the policy.
- **Sync command → async transition handoff.** `Command::execute` is synchronous and
  currently only carries the atomic. The `RoleTransition` enqueue mechanism needs a
  channel/connection-level op analogous to the existing `PSYNC`/`WAIT` handoff. Low
  risk (the pattern exists) but it is new plumbing.
- **In-flight writes during demotion.** Demotion must set the read-only flag before
  (or atomically with) starting the inbound stream, so no client write races the
  transition. The manager owning the ordering is the point — but the exact fence vs.
  the shard workers needs care.
- **`NodeRole` vs. `is_replica` reconciliation.** As a follow-up, consider whether
  the data-path flag should be *derived* from `RoleManager` rather than existing as a
  parallel truth — but reconciling the two representations is out of scope for the
  minimal fix and belongs in a later pass.

## Effort estimate

**M.** The `RoleManager` module itself is small and its interface narrow, and the
read sites don't change (they keep reading an `Arc<AtomicBool>` handle). The effort
is concentrated in three places: (1) building the sync-command → async-transition
handoff for `REPLICAOF` (new plumbing, but a well-worn pattern from `PSYNC`/`WAIT`);
(2) making `init_replication`, `subsystems.rs`, and the demotion consumer route
through `promote`/`demote` instead of their bespoke paths; and (3) getting
cluster-mode failover ordering + policy right, which is the genuinely subtle part and
needs concurrency/integration tests. Not L, because the surface area of *readers* is
untouched and no new consensus machinery is introduced — the deep module mostly
consolidates plumbing that already exists and supplies the one missing operation.
