# FrogDB Command Execution

This document details the command execution pipeline, command trait interface, arity validation, and command flags.

## Command Flow

```
1. Connection accepted by Acceptor
   └── Assigned to Shard N based on client hash

2. Client sends: SET mykey myvalue
   └── Shard N's event loop receives bytes

3. Protocol parser (RESP2)
   └── Parses into ParsedCommand { name: "SET", args: ["mykey", "myvalue"] }

4. Command lookup
   └── Registry.get("SET") -> SetCommand

5. Key routing check
   └── SetCommand.keys(args) -> ["mykey"]
   └── hash("mykey") % num_shards -> Shard M

6. If M == N (local):
   └── Execute directly on local store

   If M != N (remote):
   └── Send ShardMessage::Execute to Shard M
   └── Await response via oneshot channel

7. Execute command
   └── SetCommand.execute(ctx, args)
   └── ctx.store.set("mykey", FrogString::new("myvalue"))

8. Persistence (async)
   └── Append to WAL batch

9. Encode response
   └── Protocol.encode(Response::Ok) -> "+OK\r\n"

10. Send to client
```

---

## Command Trait

```rust
pub trait Command: Send + Sync {
    /// Command name (e.g., "GET", "SET", "ZADD")
    fn name(&self) -> &'static str;

    /// Expected argument count
    fn arity(&self) -> Arity;

    /// Command behavior flags
    fn flags(&self) -> CommandFlags;

    /// Execute the command
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;

    /// Extract key(s) from arguments for routing
    fn keys(&self, args: &[Bytes]) -> Vec<&[u8]>;
}
```

---

## Arity

Specifies the expected number of arguments for a command:

```rust
pub enum Arity {
    /// Exactly N arguments (e.g., GET = Fixed(1))
    Fixed(usize),

    /// At least N arguments (e.g., MGET = AtLeast(1))
    AtLeast(usize),

    /// Between min and max arguments inclusive
    Range { min: usize, max: usize },
}
```

### Examples

| Command | Arity | Description |
|---------|-------|-------------|
| GET | `Fixed(1)` | Exactly 1 argument |
| SET | `Range { min: 2, max: 6 }` | 2-6 arguments (key, value, [EX/PX/NX/XX]) |
| MGET | `AtLeast(1)` | 1 or more keys |
| PING | `Range { min: 0, max: 1 }` | 0 or 1 argument |

---

## Command Flags

Bitflags describing command behavior for routing and optimization:

```rust
bitflags! {
    pub struct CommandFlags: u32 {
        /// Command modifies data (SET, DEL, ZADD)
        const WRITE = 0b0000_0001;

        /// Command only reads data (GET, ZRANGE)
        const READONLY = 0b0000_0010;

        /// O(1) operation, suitable for latency-sensitive paths
        const FAST = 0b0000_0100;

        /// May block the connection (BLPOP, BRPOP)
        const BLOCKING = 0b0000_1000;

        /// Operates on multiple keys (MGET, MSET, DEL with multiple keys)
        const MULTI_KEY = 0b0001_0000;

        /// Pub/sub command, connection enters pub/sub mode
        const PUBSUB = 0b0010_0000;

        /// Script execution (EVAL, EVALSHA)
        const SCRIPT = 0b0100_0000;
    }
}
```

---

## Flag Usage

Flags inform the router and execution engine about command characteristics:

| Flag | Purpose |
|------|---------|
| `WRITE` | Commands with this flag update the WAL |
| `READONLY` | Commands can be load-balanced to replicas (future) |
| `FAST` | O(1) commands, suitable for hot paths |
| `BLOCKING` | Commands require special timeout handling |
| `MULTI_KEY` | Commands may trigger scatter-gather coordination |
| `PUBSUB` | Connection enters pub/sub mode after execution |
| `SCRIPT` | Lua script execution with atomicity requirements |

### Example Flag Combinations

| Command | Flags |
|---------|-------|
| GET | `READONLY \| FAST` |
| SET | `WRITE \| FAST` |
| MGET | `READONLY \| MULTI_KEY` |
| DEL | `WRITE \| MULTI_KEY` |
| BLPOP | `WRITE \| BLOCKING` |
| EVAL | `WRITE \| SCRIPT` |
| SUBSCRIBE | `PUBSUB` |

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
