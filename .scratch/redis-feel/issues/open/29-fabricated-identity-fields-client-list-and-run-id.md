# `CLIENT LIST` reports `user=default resp=2 redir=-1` for every client, and `INFO run_id` is a constant

Status: needs-triage
Type: bug (introspection accuracy) / ruling needed
Area: connection / info

## Problem

### `CLIENT LIST` / `CLIENT INFO`

```rust
// frogdb-server/crates/core/src/client_registry/info.rs:97
"... events=r cmd={} user=default redir=-1 resp=2 lib-name={} lib-ver={}"
```

`user`, `redir` and `resp` are literals — `ClientInfo` has no field for any of them
(`info.rs:15-51`). Both underlying facts are real:

- **`user`** — ACL is implemented; the connection holds an authenticated user
  (`server/src/connection/state.rs:886` `authenticate`, `:911` `authenticated_user`), used by the
  permission guard (`permission_guard.rs:54`). A client authenticated as `alice` is reported as
  `default`.
- **`resp`** — RESP3 is implemented; `HELLO 3` flips `state.protocol_version`
  (`auth_conn_command.rs:383-386`, `:466`). Every RESP3 client is reported as `resp=2`.
- **`redir`** — client tracking with redirection exists (`CLIENT TRACKING ... REDIRECT`,
  `GETREDIR`, `client_conn_command.rs:98`), so a redirecting client's target id is knowable.

[Issue 09](../../../redis-feel/issues/) added the *missing* fields (`watch`, `rbs`, `rbp`,
`tot-net-in`, `tot-net-out`) and deliberately omitted `io-thread` rather than fake it. These three
were not caught because they were already present — with invented values, which is worse than
absent. `CLIENT KILL USER` drops its filter for the same missing plumbing — see
[issue 24](../../../redis-feel/issues/); they should land together.

### `INFO run_id`

```
run_id:frogdb0000000000000000000000000000000000
```

(`info/sections.rs:96`, `commands/info.rs:208`) — identical across restarts and across every node
in a cluster. Redis clients and tooling use `run_id` to detect that a server was restarted or
replaced, which this value defeats. FrogDB does have live identities: a node id and a PSYNC
replication id (`info/mod.rs:646` — "the live PSYNC id when present").

## Why `needs-triage`

`user` and `resp` are mechanical once `ClientInfo` carries them. The two open questions:

1. **`run_id` semantics** — reuse the node id (stable across restarts, so it does *not* signal a
   restart), the PSYNC replid (changes on failover, closest to Redis's meaning), or mint a fresh
   per-process random id at boot (exactly Redis's semantics)? The third matches Redis but adds an
   identity that nothing else in FrogDB uses.
2. **`redir`** — report the tracking redirect target, or omit the field when tracking is off? Redis
   always emits it (`-1` when not redirecting), so `-1` is truthful for a non-tracking client; the
   bug is only that a *redirecting* client also reports `-1`.

## Acceptance criteria (draft, pending ruling)

- [ ] A client authenticated as `alice` shows `user=alice`; an unauthenticated one shows `default`
- [ ] A `HELLO 3` client shows `resp=3`; a RESP2 client shows `resp=2`
- [ ] A `CLIENT TRACKING ON REDIRECT <id>` client shows `redir=<id>`
- [ ] `run_id` follows the ruling and is documented in `frogdb-server/CONTEXT.md` if it deviates
      from Redis's per-process semantics
- [ ] Regression coverage asserts the values against live state, not literals

Size: S-M
