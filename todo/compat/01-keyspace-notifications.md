# 1. Keyspace Notifications (25 tests — `pubsub_tcl.rs`)

**Status**: Not implemented
**Scope**: Implement `CONFIG SET notify-keyspace-events` and the keyspace notification pub/sub
mechanism that publishes events on `__keyevent@*__` and `__keyspace@*__` channels.

## Work

- Add `notify-keyspace-events` config parameter (string flags: K, E, g, $, l, s, h, z, x, e, t, m, A)
- Hook into command execution to emit events on key mutations
- Integrate with existing pub/sub broadcast infrastructure
- Handle expired/evicted key events from the eviction and TTL subsystems
- Support event masking (selective notification types)

**Key files to modify**: `config/`, `connection/handlers/pubsub.rs`, `core/src/shard/`, eviction module

## Tests

- `Keyspace notifications: we receive keyspace notifications`
- `Keyspace notifications: we receive keyevent notifications`
- `Keyspace notifications: we can receive both kind of events`
- `Keyspace notifications: we are able to mask events`
- `Keyspace notifications: general events test`
- `Keyspace notifications: list events test`
- `Keyspace notifications: set events test`
- `Keyspace notifications: zset events test`
- `Keyspace notifications: hash events test ($type)`
- `Keyspace notifications: stream events test`
- `Keyspace notifications:FXX/FNX with HSETEX cmd`
- `Keyspace notifications: expired events (triggered expire)`
- `Keyspace notifications: expired events (background expire)`
- `Keyspace notifications: evicted events`
- `Keyspace notifications: test CONFIG GET/SET of event flags`
- `Keyspace notifications: new key test`
- `Keyspace notifications: overwritten events - string to string`
- `Keyspace notifications: type_changed events - hash to string`
- `Keyspace notifications: both overwritten and type_changed events`
- `Keyspace notifications: configuration flags work correctly`
- `Keyspace notifications: RESTORE REPLACE different type - restore, overwritten and type_changed events`
- `Keyspace notifications: SET on existing string key - overwritten event`
- `Keyspace notifications: setKey on existing different type key - overwritten and type_changed events`
- `Keyspace notifications: overwritten and type_changed events for RENAME and COPY commands`
- `Keyspace notifications: overwritten and type_changed for *STORE* commands`
