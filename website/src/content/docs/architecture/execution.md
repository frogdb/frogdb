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
   +-- hash("mykey") % num_shards -> Shard M

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

Each command implements a `Command` trait that provides: name, arity, flags, key extraction, execution strategy, WAL strategy, and an execute method.

---

## CommandError

Error handling uses a `CommandError` enum with variants for wrong arity, wrong type, syntax errors, out-of-range values, and other command-specific errors. See [architecture.md](/architecture/architecture/) for the key error variants.

---

### Arity

| Mode | Description | Example |
|------|-------------|---------|
| Fixed(n) | Exactly n arguments (including command name) | GET = Fixed(2) |
| AtLeast(n) | Minimum n arguments | DEL = AtLeast(2) |
| Range(min, max) | Between min and max arguments | SET = Range(3, 7) |

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
| TRACKS_KEYSPACE | Triggers keyspace notifications | Used by client tracking |

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
| DEBUG | SLEEP, STRUCTSIZE, HASHING, DUMP-VLL-QUEUE, DUMP-CONNECTIONS |
| MEMORY | USAGE, DOCTOR, STATS, MALLOC-SIZE, PURGE |
| SLOWLOG | GET, LEN, RESET |
| CLUSTER | INFO, NODES, SLOTS, MEET, ADDSLOTS, DELSLOTS, FAILOVER, ... |
| OBJECT | ENCODING, FREQ, IDLETIME, REFCOUNT, HELP |
| SCRIPT | LOAD, EXISTS, FLUSH, KILL, DEBUG |
| LATENCY | DOCTOR, GRAPH, HISTORY, LATEST, RESET, HELP |

---

## Command Registry

Commands are registered at startup in a static hashmap:

```rust
lazy_static! {
    static ref COMMANDS: HashMap<&'static str, Box<dyn Command>> = {
        let mut m = HashMap::new();
        m.insert("GET", Box::new(GetCommand));
        m.insert("SET", Box::new(SetCommand));
        m.insert("DEL", Box::new(DelCommand));
        // ... register all commands
        m
    };
}

fn dispatch(cmd: &ParsedCommand) -> Result<Response, Error> {
    let handler = COMMANDS.get(cmd.name.to_uppercase().as_str())
        .ok_or(Error::UnknownCommand)?;
    handler.execute(cmd)
}
```

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

The `CommandContext` struct provides commands access to execution state:

```rust
pub struct CommandContext<'a> {
    pub store: &'a mut dyn Store,
    pub shard_senders: &'a Arc<Vec<mpsc::Sender<ShardMessage>>>,
    pub shard_id: usize,
    pub num_shards: usize,
    pub conn_id: u64,
    pub protocol_version: ProtocolVersion,
    pub replication_tracker: Option<&'a Arc<ReplicationTrackerImpl>>,
    pub replication_state: Option<&'a Arc<RwLock<ReplicationState>>>,
    pub cluster_state: Option<&'a Arc<ClusterState>>,
    pub node_id: Option<u64>,
    pub raft: Option<&'a Arc<ClusterRaft>>,
    pub network_factory: Option<&'a Arc<ClusterNetworkFactory>>,
    pub quorum_checker: Option<&'a dyn QuorumChecker>,
    pub command_registry: Option<&'a Arc<CommandRegistry>>,
    pub dirty_delta: i64,
}
```

Most commands only need `store`. The shard_senders field is used by multi-key commands that need scatter-gather coordination.

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
