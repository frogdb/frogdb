# 18 — `DEBUG PUBSUB LIMITS` waits on shard 0 with a hard-coded 5s instead of `scatter_gather_timeout`

Status: needs-triage

## What to build

`DebugHandler::pubsub_limits` sends `SearchMsg::GetPubSubLimitsInfo` to `shard_senders[0]`
(`frogdb-server/crates/server/src/connection/debug_handler.rs:173-175`) and then waits on the
reply with `let timeout = std::time::Duration::from_secs(5);` (`:178`), rather than the
connection's own `scatter_gather_timeout`. It also hand-rolls its own two failure strings —
`"ERR timeout waiting for shard pub/sub info"` (`:186`) and `"ERR failed to query shard pub/sub
info"` (`:190`).

`scatter_gather_timeout` is an operator-visible, live-mutable knob. It is declared at
`frogdb-server/crates/config/src/server.rs:44-46`, threaded through `ConnectionConfig`
(`acceptor.rs:176`), exposed as a `CONFIG SET` parameter (`runtime_config.rs:2038-2040`), and
honored by every other shard round-trip on the connection path — `guards.rs:924`,
`routing.rs:253`/`:295`, `persistence_handler.rs:231`/`:333`, `info_handler.rs:47`,
`search/hybrid.rs:95-96`, `pubsub_conn_command.rs:282`/`:927`, and `scatter.rs:28`. This sixth
shard-0 read is the only one that ignores it. An operator who lowers the timeout to shed load on
a degraded node, or raises it on a slow one, silently does not affect `DEBUG PUBSUB LIMITS`.
That is a **live** policy divergence on main today, not a latent one — the arm is reachable and
the default (5000 ms) merely happens to coincide with the hard-coded literal, which is exactly
what makes the drift invisible until someone retunes the knob.

The mechanical fix is one line — `self.core.scatter_gather_timeout` (or whichever field the
`DebugHandler` view already borrows; `PreDispatchView` carries it at `guards.rs:111` and
`ConnectionHandler` populates it at `guards.rs:132`). What makes this an issue rather than a
drive-by edit is the config-plumbing question attached: should a *debug introspection* read
share the client-facing knob, or does it want its own (shorter, so `DEBUG` never hangs a
diagnostic session behind a raised production timeout)? Rule that first; the code change follows
in either case. Note that this path deliberately cannot be absorbed by proposal 67's
`query_shard0` helper — it carries `SearchMsg`, not `CoreMsg::ScatterRequest`, so unifying it
would mean a second one-caller adapter for a second message family. Proposal 67 explicitly
declines the fold and files this instead.

## Acceptance criteria

- [ ] `DEBUG PUBSUB LIMITS` observes the configured `scatter-gather-timeout-ms` (or a
      deliberately chosen, documented debug-specific timeout) rather than a literal `5`
- [ ] The two bespoke error strings are reconciled with whatever the other shard-0 reads emit on
      timeout, or the divergence is justified in a comment at the site
- [ ] Regression test `debug_pubsub_limits_honors_scatter_gather_timeout` sets
      `CONFIG SET scatter-gather-timeout-ms` to a value distinguishable from 5000 and asserts the
      command's wait bound tracks it (pin the observable, not the literal)
- [ ] `just test frogdb-server debug_pubsub_limits` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 67
(`.scratch/arch-deepening/proposals/67-server-small-dedups.md`), *"Out of scope, but file an
issue: the sixth shard-0 send"* (proposal `:685-712`, effort-table row `:724`), confirmed by the
proposal-87 review as "H2 CONFIRMED owned by 67". The orchestrator dispatch attributed this cite
to proposal 73; `debug_handler.rs:173` does not appear in proposal 73's plan entry.

## Comments
