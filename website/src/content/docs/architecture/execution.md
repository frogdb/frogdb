---
title: "Command Execution"
description: "The command execution pipeline, Command trait interface, arity validation, command flags, and dispatch flow."
sidebar:
  order: 5
---
The command execution pipeline, Command trait interface, arity validation, command flags, and dispatch flow.

## Command Flow

```
1. Connection accepted by Acceptor
   +-- Assigned to Thread N via round-robin (connection pinned for I/O)

2. Client sends: SET mykey myvalue
   +-- Thread N's event loop receives bytes

3. Protocol parser (RESP2)
   +-- Parses into ParsedCommand { name: "SET", args: ["mykey", "myvalue"] }

4. Command lookup
   +-- Registry.get("SET") -> SetCommand

5. Key routing check
   +-- SetCommand.keys(args) -> ["mykey"]
   +-- CRC16("mykey") % 16384 % num_shards -> Shard M

6. If M == N (local):
   +-- Execute directly on local store

   If M != N (remote):
   +-- Send ShardMessage::Execute to Shard M
   +-- Await response via oneshot channel

7. Execute command
   +-- SetCommand.execute(ctx, args)
   +-- ctx.store.set("mykey", StringValue::new("myvalue"))

8. Persistence (async)
   +-- Append to WAL batch

9. Encode response
   +-- Protocol.encode(Response::Ok) -> "+OK\r\n"

10. Send to client
```

---

## Command Trait

Each command is a type implementing the `Command` trait
(`frogdb-server/crates/core/src/command.rs`). The trait has two required
methods and derives everything else from a single declarative specification:

```rust
pub trait Command: Send + Sync {
    /// The single source of truth for this command's mechanics.
    fn spec(&self) -> &'static CommandSpec;

    /// Execute the command against the local shard.
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes])
        -> Result<Response, CommandError>;

    // name(), arity(), flags(), execution_strategy(), wal_strategy(),
    // keys(), keys_with_flags() are default methods that read from spec().
}
```

`CommandSpec` is a `'static` struct carrying the command's `name`, `arity`,
`flags`, `strategy` (execution strategy), key specification, per-key access
specification, WAL strategy, keyspace-event specification, and related routing
metadata. Because the derived accessors all read from `spec()`, a command
declares its mechanics once. For example, `GetCommand`:

```rust
impl Command for GetCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GET",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            strategy: ExecutionStrategy::Standard,
            // ...access, wal, event, lookup, mutation, requires_same_slot
        };
        &SPEC
    }
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes])
        -> Result<Response, CommandError> { /* ... */ }
}
```

Connection-level commands (AUTH, HELLO, CONFIG, and other commands handled
directly by the connection handler rather than routed to a shard) implement the
sibling `ConnectionCommand` trait, which likewise carries a `CommandSpec` and an
`execute` against a connection context. See [Command Registry](#command-registry).

---

## CommandError

Error handling uses a `CommandError` enum with variants for wrong arity, wrong type, syntax errors, out-of-range values, and other command-specific errors. See [architecture.md](/architecture/architecture/) for the key error variants.

---

### Arity

Arity is checked against the argument count **excluding** the command name (the
name is held separately on `ParsedCommand`, and validation calls
`arity.check(args.len())`). This is the opposite convention to Redis's
`COMMAND` output, which includes the command token.

| Mode | Description | Example |
|------|-------------|---------|
| `Fixed(n)` | Exactly n arguments | `GET key` = `Fixed(1)` |
| `AtLeast(n)` | Minimum n arguments | `DEL key [key ...]` = `AtLeast(1)` |
| `Range { min, max }` | Between min and max arguments inclusive | `EXPIRE key seconds [NX\|XX\|GT\|LT]` = `Range { min: 2, max: 4 }` |

---

### Command Flags

| Flag | Description | Behavioral Effect |
|------|-------------|-------------------|
| WRITE | Modifies data | Replicated, blocked during readonly mode |
| READONLY | Read-only operation | Allowed on replicas |
| FAST | O(1) or O(log N) | Excluded from slowlog by default |
| BLOCKING | May block client | Special shard handling, timeout support |
| MULTI_KEY | Operates on multiple keys | Requires slot validation |
| PUBSUB | Pub/Sub command | Routed to pub/sub subsystem |
| SCRIPT | Script-related | Subject to script restrictions |
| NOSCRIPT | Cannot be called from scripts | Rejected inside EVAL |
| LOADING | Allowed during loading | Available before full startup |
| STALE | Allowed on stale replicas | Available during sync |
| SKIP_SLOWLOG | Never logged to slowlog | Internal/meta commands |
| RANDOM | Non-deterministic output | Affects script replication |
| ADMIN | Administrative command | Restricted by ACL +@admin |
| NONDETERMINISTIC | Output varies between runs | Affects replication mode |
| NO_PROPAGATE | Not replicated to replicas | Connection-local state changes |
| MOVABLEKEYS | Key positions depend on argument values | Keys resolved by inspecting args (SORT, EVAL, XREAD); reported by `COMMAND INFO` |

Flags are a `bitflags` set (`CommandFlags`) in
`frogdb-server/crates/core/src/command.rs`.

### Execution Strategy

Each command declares an execution strategy that determines how it is dispatched:

| Strategy | Description | Example Commands |
|----------|-------------|-----------------|
| Standard | Execute on the owning shard | GET, SET, HGET |
| ConnectionLevel | Execute on connection's shard, no routing | AUTH, SELECT, CLIENT |
| Blocking | May suspend, needs waiter registration | BLPOP, BRPOP, BLMOVE |
| ScatterGather | Fan out to all shards, merge results | DBSIZE, KEYS, SCAN 0 |
| RaftConsensus | Requires Raft quorum (cluster mode) | Cluster config changes |
| AsyncExternal | Offloaded to async task | BGSAVE, DEBUG SLEEP |
| ServerWide | Broadcast to all shards, aggregate | FLUSHDB, FLUSHALL |

### Flag Combination Effects

| Combination | Effect |
|-------------|--------|
| `WRITE | MULTI_KEY` | Scatter-gather write (MSET, DEL) |
| `READONLY | MULTI_KEY` | Scatter-gather read (MGET, EXISTS) |
| `WRITE | BLOCKING` | Blocking list operation, modifies data |
| `WRITE | SCRIPT` | Lua script, single atomic execution |
| `READONLY | STALE` | Safe to execute on stale replica |
| `ADMIN | NO_PROPAGATE` | Admin command, don't replicate |

**Mutual Exclusivity:**

| Flag A | Flag B | Relationship |
|--------|--------|--------------|
| `WRITE` | `READONLY` | Mutually exclusive (one must be set) |
| `FAST` | `BLOCKING` | Mutually exclusive |
| `PUBSUB` | `MULTI_KEY` | Mutually exclusive |

---

## Arity Validation Details

### Validation Order (Canonical)

FrogDB validates commands in this order (matching Redis):

1. **Parse** - Extract command name and args from RESP frame
2. **Lookup** - Find command in registry (unknown -> ERR unknown command)
3. **Arity** - Check argument count (wrong -> ERR wrong number of arguments)
4. **Auth** - Check authentication state if required
5. **ACL** - Check user permissions
6. **Execute** - Run command logic

Arity is checked BEFORE auth for performance (reject malformed commands early without auth overhead).

### Subcommand Arity Handling

Commands with subcommands (CLIENT, CONFIG, ACL, etc.) use hierarchical arity validation. The parent command requires at least 1 arg (the subcommand name), and each subcommand has its own arity.

**Commands with Subcommands:**

| Parent Command | Subcommands |
|----------------|-------------|
| CLIENT | GETNAME, SETNAME, ID, INFO, LIST, KILL, PAUSE, UNPAUSE, REPLY, NO-EVICT |
| CONFIG | GET, SET, REWRITE, RESETSTAT |
| ACL | LIST, GETUSER, SETUSER, DELUSER, CAT, GENPASS, WHOAMI, LOG, LOAD, SAVE |
| DEBUG | OBJECT, SLEEP, STRUCTSIZE, HASHING, SET-ACTIVE-EXPIRE, RELOAD, LOCKTABLE, WAITQUEUE, VLL |
| MEMORY | USAGE, DOCTOR, STATS, MALLOC-SIZE, PURGE |
| SLOWLOG | GET, LEN, RESET |
| CLUSTER | INFO, NODES, SLOTS, MEET, ADDSLOTS, DELSLOTS, FAILOVER, ... |
| OBJECT | ENCODING, FREQ, IDLETIME, REFCOUNT, HELP |
| SCRIPT | LOAD, EXISTS, FLUSH, KILL, DEBUG |
| LATENCY | DOCTOR, GRAPH, HISTORY, LATEST, RESET, HELP |

---

## Command Registry

Commands are registered at startup into a `CommandRegistry`
(`frogdb-server/crates/core/src/registry.rs`), not a `lazy_static` map. The
server calls `frogdb_commands::register_all(&mut registry)`
(`frogdb-server/crates/commands/src/lib.rs`, invoked from the server's
`register.rs`), which registers each command struct by name (case-insensitive):

```rust
pub fn register_all(registry: &mut CommandRegistry) {
    registry.register(basic::GetCommand);
    registry.register(basic::SetCommand);
    registry.register(basic::DelCommand);
    // ... every command
}
```

Each registry entry is a `CommandEntry` (an alias for `CommandImpl`), a tagged
union so that a registered command carries **exactly one** execution path and
there is no never-called stub:

```rust
pub enum CommandImpl {
    /// Shard-local executor: execute(&mut CommandContext).
    /// Standard, ScatterGather, Blocking, and ServerWide commands.
    Shard(Arc<dyn Command>),
    /// Connection-level executor: execute(&ConnCtx).
    /// Migrated connection-level commands (e.g. CONFIG).
    Connection(&'static dyn ConnectionCommand),
}
```

`register()` inserts a `Shard` entry; `register_connection()` inserts a
`Connection` entry. In debug builds, registration validates that a command's
declared `ExecutionStrategy` agrees with its entry variant — a `Connection`
executor must declare `ExecutionStrategy::ConnectionLevel` — so a migrated
command's never-reached path is unrepresentable rather than a latent routing
bug. Lookup (`get_entry`) upper-cases the command name.

This registry is the authoritative surface of the command set. The generated
command-compatibility matrix (work item S1, `commands-gen`) is produced by
dumping `register_all()` into `commands.json`; the matrix is **derived** from the
registry, not hand-maintained. Command counts therefore live in the generated
matrix, not in this page.

---

## Type Handling Rules

### Type-Checking Commands (Return WRONGTYPE)

Most commands expect a specific type and return an error if the key holds a different type.

### Type-Overwriting Commands (Replace Regardless of Type)

`SET`, `GETSET`, `SETEX`, `SETNX`, `PSETEX` unconditionally overwrite the key, changing its type if necessary.

### Type-Agnostic Commands (Work on Any Type)

`DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `TTL`, `PERSIST`, `RENAME`, `OBJECT`, `DUMP`, `RESTORE` operate on keys regardless of their type.

---

## CommandContext Definition

The `CommandContext` struct (`frogdb-server/crates/core/src/command.rs`)
provides commands access to execution state. Its current shape:

```rust
pub struct CommandContext<'a> {
    pub store: &'a mut dyn Store,
    pub shard_senders: &'a Arc<Vec<ShardSender>>,
    pub shard_id: usize,
    pub num_shards: usize,
    pub conn_id: u64,
    pub protocol_version: ProtocolVersion,
    pub replication_tracker: Option<&'a Arc<ReplicationTrackerImpl>>,
    pub cluster_state: Option<&'a Arc<ClusterState>>,
    pub node_id: Option<u64>,
    pub raft: Option<&'a Arc<ClusterRaft>>,
    pub network_factory: Option<&'a Arc<ClusterNetworkFactory>>,
    pub quorum_checker: Option<&'a dyn QuorumChecker>,
    pub command_registry: Option<&'a Arc<CommandRegistry>>,
    pub is_replica: bool,
    pub is_replica_flag: Option<Arc<AtomicBool>>,
    pub role_controller: Option<Arc<dyn RoleController>>,
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub effects: CommandEffects,
}
```

Most commands only need `store`. `shard_senders` is used by multi-key commands
that coordinate scatter-gather; the cluster/replication fields are `None` unless
those subsystems are enabled. `effects` is the command's out-buffer for
everything it produces besides the `Response` (keyspace events, WAL actions),
drained by the execution seam. This struct changes as features land; treat the
listing as current-shape, not stable API.

---

## Replication Integration

### Replication in Command Flow

```
Client Request -> Parse Command -> Execute Command -> WAL Append -> Response
                                                        |
                                    +--- Async mode ---> Response sent immediately
                                    +--- Sync mode ----> Block until min_replicas_to_write ACK
```

Sequence numbers are assigned at WAL append time, not at command execution time. This ensures monotonically increasing sequences for replication ordering.

### Replica Command Handling

| Command Type | Replica Behavior |
|--------------|------------------|
| Read (`READONLY` flag) | Execute locally, may return stale data |
| Write (`WRITE` flag) | Return `-READONLY` error |
| Replication commands | Execute (PSYNC, REPLCONF, etc.) |
| Admin commands | Depends on command (INFO allowed, SHUTDOWN not) |
