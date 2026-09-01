# Two commands accept arguments and silently discard them (`RESTORE IDLETIME/FREQ`, `CLIENT KILL USER`)

Status: ready-for-agent
Type: bug (correctness)
Area: commands / connection

## Problem

Both parse an argument, validate nothing, and drop it. Silent acceptance is worse than an error:
the caller believes the option took effect.

### 1. `RESTORE key ttl payload [IDLETIME s] [FREQ f]`

```rust
// frogdb-server/crates/server/src/commands/persistence.rs:121-124
b"IDLETIME" | b"FREQ" => {
    // Skip these options and their arguments
    i += 1;
}
```

The values are discarded, so a restored key gets default access metadata. The metadata is real
and readable — `OBJECT IDLETIME` returns `meta.last_access` and `OBJECT FREQ` returns
`meta.lfu_counter` (`frogdb-server/crates/commands/src/generic.rs:440-455`), and `deserialize`
already hands `RESTORE` a `metadata` value it then partly overwrites. A backup/restore or
cluster-migration round-trip therefore loses LRU/LFU state, which changes eviction victims.

Redis also validates: negative `IDLETIME` → `ERR Invalid IDLETIME value, must be >= 0`;
`FREQ` outside 0..=255 → `ERR Invalid FREQ value, must be >= 0 and <= 255`. FrogDB accepts
anything, including a non-integer.

### 2. `CLIENT KILL ... USER <username>`

```rust
// frogdb-server/crates/server/src/connection/client_conn_command.rs:445-452
b"USER" => {
    // USER filter - noop for now (ACL not implemented)
    i += 1;
    ...
    // TODO: Implement in Phase 10.5 (ACL)
}
```

ACL landed: connections carry an authenticated user (`connection/state.rs:886` `authenticate`,
:911 `authenticated_user`), and the permission guard reads it (`permission_guard.rs:54`). The
filter is dropped, so `CLIENT KILL USER alice` kills every client matching the *other* filters
regardless of user — an over-broad kill, not a no-op. Redis also errors on an unknown username
(`ERR No such user '<name>'`) rather than matching nothing.

## Ruling

- `RESTORE`: apply `IDLETIME` to the restored key's `last_access` and `FREQ` to its `lfu_counter`,
  and reject out-of-range/non-integer values with Redis's exact error strings.
- `CLIENT KILL USER`: match against the connection's authenticated username; unauthenticated
  connections match only the default user. Unknown username → `ERR No such user '<name>'`.
  Requires plumbing the username into `ClientInfo`/`KillFilter` — see
  [issue 29](../../../redis-feel/issues/) for the `user=` field in `CLIENT LIST`, which needs the
  same plumbing and should land with it.

## Acceptance criteria

- [ ] `RESTORE k 0 <payload> IDLETIME 100` → `OBJECT IDLETIME k` ≈ 100 (not ~0)
- [ ] `RESTORE k 0 <payload> FREQ 42` under an LFU policy → `OBJECT FREQ k` = 42
- [ ] `RESTORE k 0 <payload> FREQ 300` and `IDLETIME -1` produce Redis's exact error strings
- [ ] `CLIENT KILL USER alice` kills only `alice`'s connections; a `bob` connection survives
- [ ] `CLIENT KILL USER nosuchuser` → `ERR No such user 'nosuchuser'`
- [ ] The `TODO: Implement in Phase 10.5` comment is gone

Size: S-M
