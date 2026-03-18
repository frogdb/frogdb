# Getting Started with FrogDB

FrogDB is a Redis-compatible in-memory database with built-in event sourcing, multi-threaded execution, and persistent storage.

## Building from Source

FrogDB is written in Rust. To build:

```bash
git clone https://github.com/your-org/frogdb.git
cd frogdb
cargo build --release
```

The server binary is located at `target/release/frogdb-server`.

## Starting the Server

Start FrogDB with default settings:

```bash
./target/release/frogdb-server
```

By default, the server listens on `0.0.0.0:6379`. To customize, pass a configuration file:

```bash
./target/release/frogdb-server --config frogdb.toml
```

See [Configuration](../operators/configuration.md) for available options.

## Connecting

FrogDB speaks the Redis wire protocol (RESP2/RESP3). Connect with `redis-cli` or any Redis client library:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

## Basic Operations

```
> SET greeting "hello world"
OK

> GET greeting
"hello world"

> DEL greeting
(integer) 1

> GET greeting
(nil)
```

FrogDB supports the standard Redis command set including strings, hashes, lists, sets, sorted sets, and streams. For details, see the [Command Reference](commands.md).

## What's Different from Redis?

FrogDB is wire-compatible with Redis, but there are some intentional differences:

- **Single database**: No `SELECT` command (except `SELECT 0` as a no-op)
- **Hash slot enforcement**: Multi-key operations require keys in the same hash slot, even in standalone mode. Use hash tags like `{user:1}` to colocate related keys.
- **Strict Lua key validation**: Scripts must declare all keys in the KEYS array

For a full list, see [Compatibility](compatibility.md).

## Next Steps

- [Commands](commands.md) -- Supported command groups and FrogDB extensions
- [Event Sourcing](event-sourcing.md) -- FrogDB's built-in ES.* commands
- [Transactions](transactions.md) -- MULTI/EXEC and WATCH
- [Lua Scripting](lua-scripting.md) -- EVAL/EVALSHA usage
- [Pub/Sub](pub-sub.md) -- Publish/subscribe messaging
- [Limits](limits.md) -- Size and resource limits
- [Compatibility](compatibility.md) -- Differences from Redis
