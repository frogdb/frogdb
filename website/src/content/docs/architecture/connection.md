---
title: "Connection Management"
description: "The pinned-connection model, connection state, backpressure, and per-user rate limiting."
sidebar:
  order: 13
---
The pinned-connection model, connection state, backpressure, and per-user rate limiting.

## Connection model

Each accepted connection is assigned to one shard worker for its entire lifetime
(**pinned**). The connection's own state — authentication, protocol version,
transaction queue, subscriptions — lives on the task that owns the socket and is
never shared across threads. When a command needs data on a different shard, the
connection coordinates with that shard through bounded message-passing rather than
by touching another thread's state.

Pinning keeps per-connection state single-owner (no locks, good cache locality)
while the message-passing layer lets any connection reach any shard. For the shard
worker model this builds on, see [Concurrency Model](/architecture/concurrency/).

---

## Connection establishment

The accept loop lives in `frogdb-server/crates/server/src/acceptor.rs`. For each
inbound connection it:

1. **Accepts the TCP socket** and sets `TCP_NODELAY`.
2. **Checks `maxclients`** — before any TLS work. If the per-port connection count
   is at the limit, the server writes `-ERR max number of clients reached` and
   closes the socket. The admin port is exempt from this check.
3. **Assigns a shard** round-robin and mints a connection id.
4. **Performs the TLS handshake** if TLS is enabled on the port.
5. **Initializes `ConnectionState`** and starts the connection task.

The `maxclients` gate runs *before* the TLS handshake so that a server already at
its connection limit does not spend CPU on handshakes it is going to reject.

### Shard assignment

Assignment is a private round-robin counter — there is no pluggable assigner
trait, and assignment does not consider the client address:

```rust
// frogdb-server/crates/server/src/acceptor.rs (illustrative)
struct RoundRobinAssigner {
    next: AtomicUsize,
    num_shards: usize,
}

impl RoundRobinAssigner {
    fn assign(&self) -> usize {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        idx % self.num_shards
    }
}
```

Once assigned, the connection coordinates with every other shard over
message-passing channels; the assignment only fixes which worker owns the socket.

---

## Connection state

`ConnectionState` (in `frogdb-server/crates/server/src/connection/state.rs`) is a
**nested** structure, not a flat bag of fields. The independent concerns —
transaction, pub/sub, keyspace tracking, auth, and blocking — each have their own
sub-state:

- `transaction: TransactionState` — the MULTI queue lives at `transaction.queue`
  (`Option<Vec<ParsedCommand>>`); watched keys at `transaction.watches`, typed
  `HashMap<Bytes, (usize, u64)>` (shard id and the key version recorded at WATCH
  time).
- `pubsub: PubSubState` — channel, pattern, and sharded subscription sets. There
  is no `pubsub_mode` boolean; whether the connection is in pub/sub mode is
  *derived* by `PubSubState::in_pubsub_mode()` (true when any subscription set is
  non-empty).
- `tracking: TrackingState` — client-side-caching invalidation tracking.
- `blocked: Option<BlockedState>` — set while a blocking command is in flight (see
  [Blocking Commands](/architecture/blocking/)).
- Identity and protocol fields: `id`, `addr`, `created_at`, `protocol_version`,
  `hello_received`, `name`, and the ancillary flags `reply_mode`, `asking`,
  `readonly`, plus `local_stats`.

These sub-states are mutated through methods on `ConnectionState`, not by reaching
into public fields.

---

## Connection state model (conceptual)

It is useful to think of a connection as being in one of four modes — NORMAL,
TRANSACTION, PUB/SUB, or BLOCKED — but this is a *lens over the real fields*, not a
literal state enum. There is no `ConnectionState` mode enum and no assertion
enforcing that the modes are mutually exclusive; each is just a check on an
independent field (`transaction.queue.is_some()`, `pubsub.in_pubsub_mode()`,
`blocked.is_some()`).

| From | Trigger | Effect |
|------|---------|--------|
| NORMAL | MULTI | `transaction.queue` becomes `Some`; subsequent commands are queued |
| TRANSACTION | EXEC / DISCARD | Queue is executed / cleared; back to NORMAL |
| NORMAL | SUBSCRIBE / PSUBSCRIBE | Subscription added; `in_pubsub_mode()` becomes true |
| PUB/SUB | UNSUBSCRIBE (all) | Subscription sets empty; `in_pubsub_mode()` becomes false |
| NORMAL | BLPOP / BRPOP / BLMOVE / … | `blocked` becomes `Some` until data, timeout, or `CLIENT UNBLOCK` |

In RESP2, a connection in pub/sub mode may only issue the subscribe/unsubscribe
family plus `PING`, `QUIT`, and `RESET`; RESP3 lifts this restriction. While a
blocking command is in flight the connection is awaiting its result and issues no
other command until it returns. Command semantics inside these modes follow Redis;
the deltas that matter are documented under
[Compatibility & Correctness](/compatibility/overview/).

**No idle-connection timeout.** FrogDB does not currently run a server-side idle
reaper — there is no Redis-style `timeout` that disconnects idle clients, and no
`tcp-keepalive` setting. A connection stays open until the client disconnects,
`CLIENT KILL` closes it, or a fatal protocol/I/O error occurs. The only
command-level deadline is the timeout a blocking command carries itself.

---

## Backpressure

FrogDB bounds the memory pressure a slow or overloaded client can create with
bounded channels and OS-level TCP backpressure rather than per-client
output-buffer caps.

Each shard has a bounded `tokio::sync::mpsc` message channel of
`SHARD_CHANNEL_CAPACITY = 1024`
(`frogdb-server/crates/server/src/server/util.rs`); new-connection handoff uses a
separate bounded channel of `NEW_CONN_CHANNEL_CAPACITY = 256`. Because the channel
is bounded, a task that awaits `send(...)` suspends when the channel is full. The
chain for a client that stops reading its responses is:

```
Client stops reading responses
  -> OS TCP send buffer fills
  -> the shard worker blocks writing to that socket
  -> that shard's bounded message channel (1024) fills
  -> other connections' sends to that shard await (backpressure)
```

The effect is self-limiting: a stuck client cannot force unbounded buffering, but
it can add latency for other connections whose work routes through the same shard.
FrogDB does **not** implement per-client `client-output-buffer-limit` thresholds.
Memory pressure from clients is instead handled by `maxmemory-clients` client
eviction (a separate mechanism; a connection can opt out with `CLIENT NO-EVICT`).

---

## Per-ACL-user rate limiting

Rate limiting is enforced independently of channel backpressure: backpressure
protects against slow *consumers*, while rate limiting enforces a per-user
*throughput* policy. It uses a token bucket
(`frogdb-server/crates/acl/src/ratelimit.rs`):

- **Shared per ACL user.** `RateLimitRegistry` keeps one `Arc<RateLimitState>` per
  username, so all of a user's connections draw from the same buckets.
- **Two buckets** — commands per second and bytes per second — each with a
  one-second burst capacity (the bucket starts full and refills proportional to
  elapsed time, clamped to one second's worth).
- **Per-command enforcement** via `try_acquire`; exceeding a bucket returns
  `ERR rate limit exceeded: commands per second` or
  `ERR rate limit exceeded: bytes per second`.
- **EXEC-time atomic batch consumption.** A MULTI/EXEC transaction consumes tokens
  for the whole batch at once via `try_acquire_batch(command_count, total_bytes)`
  (`frogdb-server/crates/server/src/connection/transaction.rs`), so a transaction
  is admitted or rejected as a unit rather than command by command.
- **Exemptions.** `AUTH`, `HELLO`, `PING`, `QUIT`, and `RESET` are never rate
  limited, and admin-port connections are exempt entirely.

The `ratelimit:cps=N` / `ratelimit:bps=N` ACL rule syntax and the full exemption
policy are documented under [Security](/operations/security/), the canonical home
for ACL configuration.
