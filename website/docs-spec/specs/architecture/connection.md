# Spec: architecture/connection.md
Status: update (several code artifacts on the page — a trait, a struct layout, a
state-machine assertion, and an output-buffer table — do not exist in source)
Audiences: A3 (architecture-curious), A5 (contributors)

Goal: The reader understands FrogDB's connection model: each accepted connection
is pinned to one shard worker (round-robin) for its lifetime and coordinates with
any shard via bounded message-passing; the accept flow (maxclients before TLS);
the real `ConnectionState` layout (nested transaction / pub-sub / tracking / block
state, not flat fields); how backpressure works (bounded per-shard mpsc channel,
capacity 1024, plus TCP send-buffer backpressure); and how per-ACL-user token-
bucket rate limiting enforces throughput policy independently of backpressure. The
reader leaves able to map connection lifecycle and rate limiting to source.

Not in scope:
- Wire protocol / codec — architecture/protocol (link).
- Blocking-command mechanics — architecture/blocking (link); this page notes the
  blocked state but not the wakeup machinery.
- Pub/sub delivery internals and ACL command syntax details — link to the relevant
  pages; this page covers connection state and rate-limit enforcement only.

Sources of truth (author MUST read; the current page contains fabricated code, so
verify every struct/trait/const against source):
- `frogdb-server/crates/server/src/acceptor.rs` — accept loop, maxclients check
  before TLS, the private `RoundRobinAssigner` (`assign(&self) -> usize`, field
  `num_shards`, `next.fetch_add(..) % num_shards`), connection task spawn, admin
  port exemption.
- `frogdb-server/crates/server/src/connection/state.rs` — the real
  `ConnectionState` struct (nested `TransactionState`, `PubSubState`,
  `TrackingState`, `blocked`, plus `hello_received`, `reply_mode`, `asking`,
  `readonly`, `local_stats`, …). `PubSubState::in_pubsub_mode()`.
- `frogdb-server/crates/server/src/server/util.rs` — `SHARD_CHANNEL_CAPACITY = 1024`
  (and new-connection channel 256).
- `frogdb-server/crates/server/src/server/init.rs` — `mpsc::channel(SHARD_CHANNEL_CAPACITY)`.
- `frogdb-server/crates/acl/src/ratelimit.rs` — token bucket, `RateLimitRegistry`
  (shared `Arc<RateLimitState>` per username), 1-second capacity, `try_acquire_batch`.
- `frogdb-server/crates/server/src/connection/guards.rs` — rate-limit enforcement,
  exempt commands (AUTH/HELLO/PING/QUIT/RESET), admin-port exemption.
- `frogdb-server/crates/server/src/connection/handlers/transaction.rs` — EXEC-time
  atomic batch token consumption.
- `frogdb-server/crates/acl/src/parser.rs` — `ratelimit:cps=N` / `ratelimit:bps=N`
  syntax.

Existing content: current `architecture/connection.md`. The pinned-connection
concept, the accept-flow ordering, the backpressure chain, and the rate-limiting
section are essentially correct. FACTUAL DISCREPANCIES to fix:

1. **`ConnectionAssigner` trait is fabricated.** There is no trait — only a private
   `struct RoundRobinAssigner`. Its method is `assign(&self) -> usize` (takes **no**
   `SocketAddr`), and its field is `num_shards` (not `num_threads`). Assignment is
   round-robin (`fetch_add % num_shards`), not addr-based. Replace the trait/impl
   snippet with the real struct, or describe the mechanism in prose without
   inventing a trait. Keep the correct point that connections then coordinate with
   shards via message-passing.
2. **`ConnectionState` field layout is wrong.** The flat-field struct on the page
   misrepresents the real nested layout. Corrections: `tx_queue` → `transaction:
   TransactionState` (queue at `transaction.queue`); `watches` → `transaction.watches`,
   type `HashMap<Bytes, (usize, u64)>` (not `HashMap<Bytes, u64>`); `subscriptions`/
   `patterns` → nested under `pubsub: PubSubState`; `pubsub_mode` is **not a field**
   — it is the method `PubSubState::in_pubsub_mode()`. Present the struct as
   nested, and either show the real top-level fields or describe them; do not
   reproduce a flat struct that does not compile.
3. **State-machine "mutually exclusive (enforced by debug assertion)" is
   unsupported.** There is no state enum and no `debug_assert!` enforcing
   exclusivity; state is independent fields (`transaction`, `pubsub`, `blocked`).
   Keep the NORMAL/TRANSACTION/PUBSUB/BLOCKED conceptual model and the transition/
   allowed-command/timeout tables (they are useful and behaviorally accurate — but
   verify each row against handler code), but remove the false "enforced by debug
   assertion" claim. Frame the four states as a conceptual view over the real
   fields, not a literal enum.
4. **Output buffer limits table is fabricated.** No per-client output-buffer-limit
   config or the cited byte values (32MB/8MB, 256MB/64MB) exist. The related real
   mechanism is `maxmemory-clients` client eviction (different concept). Remove the
   table. Replace with either (a) the real `maxmemory-clients` behavior if this
   page should cover it, or (b) nothing, deferring memory-pressure handling to the
   appropriate page. Do not present Redis's `client-output-buffer-limit` defaults
   as FrogDB's.
5. Backpressure chain is correct: the per-shard mpsc channel really is
   `SHARD_CHANNEL_CAPACITY = 1024`. Keep it; cite the const. Keep the "TCP
   send-buffer fills → shard worker blocks on write → channel fills → coordinator
   blocks" chain (verify the coordinator step against source).
6. Rate limiting: keep the *enforcement mechanism* (token bucket, EXEC-time atomic
   batch consumption, admin-port exemption, backpressure-independence) — that is
   this page's concern. The `ratelimit:cps`/`bps` rule syntax and the exempt-command
   list are owned by [Security](/operations/security/) (canonical home); describe
   them briefly and link there rather than restating the full syntax/list.

Structure (H2/H3 outline):

## Connection model
- Pinned-to-one-shard-worker for the connection's lifetime (round-robin
  assignment); coordinates with any shard via bounded message-passing. Why pinning
  (cache locality, no cross-thread connection state). Cross-link
  architecture/concurrency for the shard model.

## Connection establishment
- Accept flow: TCP accept → maxclients check (before TLS, admin port exempt) →
  TLS handshake (if enabled) → round-robin shard assignment → `ConnectionState`
  init + connection ID. Keep the "maxclients before TLS" rationale. Replace the
  assigner code with the real `RoundRobinAssigner`.

## Connection state
- The real nested `ConnectionState`: identity/auth/protocol fields plus
  `transaction`, `pubsub`, `tracking`, `blocked`, and the ancillary fields
  (`asking`, `readonly`, `reply_mode`, …). Note `in_pubsub_mode()` is derived.

## Connection state model (conceptual)
- The four-state conceptual view (NORMAL/TRANSACTION/PUBSUB/BLOCKED) as a lens over
  the real fields — explicitly not an enum. Transition table, allowed-commands-per-
  state table, idle-timeout-by-state table (pub/sub and blocked do not idle-timeout).
  Verify each row against handler behavior; drop the debug-assertion claim.

## Backpressure
- Bounded per-shard mpsc channel (`SHARD_CHANNEL_CAPACITY = 1024`) + TCP
  send-buffer backpressure; the full chain and its cross-connection latency
  implication. Contrast with the (removed) output-buffer-limit story: FrogDB
  relies on bounded channels + `maxmemory-clients`, not per-client output caps.

## Per-ACL-user rate limiting
- The enforcement mechanism only: token bucket, shared per ACL user across
  connections (`RateLimitRegistry`), 1-second burst capacity, EXEC-time atomic
  batch consumption, admin-port exemption, and how it is independent of channel
  backpressure. The `ratelimit:cps`/`bps` rule syntax and the exempt-command list
  live in Security (canonical home) — link there rather than restating them.

Generated data: none embedded. Config defaults referenced (maxclients,
maxmemory-clients, rate-limit fields) via `<ConfigDefault>` (config-reference.json,
docs-gen/S6) rather than hardcoded.

Drift guards:
- S7 code-path check covers `acceptor.rs`, `connection/state.rs`,
  `server/util.rs`, `server/init.rs`, `acl/ratelimit.rs`, `connection/guards.rs`.
- `SHARD_CHANNEL_CAPACITY` is a named const — safe to cite by name; if a number is
  quoted, re-verify on edit.
- The `ConnectionState` field list is hand-mirrored; a reviewer diffs it against
  `state.rs` on each edit (no generator). Prefer describing structure over
  reproducing the struct verbatim to reduce drift surface.
- No fabricated config tables: any limit/threshold on this page must resolve to a
  real config key (checkable against config-reference.json) or a named source
  const. This is the exact failure that produced the output-buffer table.
