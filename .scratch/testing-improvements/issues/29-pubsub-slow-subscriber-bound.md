# No client-output-buffer-limit for pub/sub; slow subscriber = unbounded server memory (untested)

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: pubsub

## Context

`PubSubSender` is an `mpsc::UnboundedSender<PubSubMessage>` (`frogdb-server/crates/core/src/pubsub.rs:22`), and the per-connection receiver on the delivery side is likewise unbounded (`frogdb-server/crates/server/src/connection/pubsub_conn_command.rs:270`). There is no `client-output-buffer-limit`-style config anywhere in the codebase (grep across config + server turns up nothing), and no test exercises a slow/non-reading subscriber. If a subscriber stops reading from its socket while publishers keep sending, the unbounded mpsc channel grows without bound — a real denial-of-service memory-exhaustion vector, confirmed live: the writer task awaits the full (blocked) socket write while publishers keep `send()`-ing into the unbounded channel with no backpressure.

Note this is distinct from the cross-shard keyspace-notification hop, which *is* bounded (`try_send` + drop + `KeyspaceNotificationsDropped` metric, unit-tested at `keyspace_coordinator.rs:261`) — the client-facing pub/sub delivery path is the unguarded one.

Redis's answer is `client-output-buffer-limit pubsub <hard> <soft> <soft-seconds>` — exceeding the hard limit or staying over the soft limit for the configured duration disconnects the client. This is partly a feature gap (no bound exists) and partly a test gap (whatever policy is chosen needs a regression pin) — file as a test task that pins the currently-chosen policy, with the understanding that "the policy" may need to be implemented first.

## What to build

- Decide and implement (or confirm a decision already exists) a bound policy: either a Redis-style `client-output-buffer-limit pubsub` config with hard/soft/soft-seconds semantics, or a simpler fixed cap with disconnect-on-exceed.
- Integration/turmoil test: subscribe a client, stop it from reading its socket, flood publishes from another client, assert server memory/queue growth is bounded and the slow subscriber is eventually disconnected (or whatever the chosen policy dictates) rather than growing unbounded.
- If a full config-driven limit is out of scope for this task, at minimum add a test that documents and pins current unbounded behavior as a known, deliberate gap (so a future change is a visible decision, not a silent one) — but prefer landing the bound if feasible.

## Acceptance criteria

- [ ] A test exists that floods an unread subscriber connection and asserts bounded memory/queue growth (post-fix), or explicitly and visibly pins current unbounded behavior as a documented known-gap (pre-fix / if scope is test-only).
- [ ] If a bound is implemented: config surface exists (e.g. `client-output-buffer-limit pubsub`), default matches or reasonably approximates Redis's defaults, and disconnect-on-exceed behavior is tested.
- [ ] Test distinguishes this path from the already-bounded cross-shard keyspace-notification hop (`keyspace_coordinator.rs:261`) — does not conflate the two.

## Blocked by

None - can start immediately (implementation scope depends on whether a bound is added or the current state is deliberately pinned — resolve during triage).

## References

- `frogdb-server/crates/core/src/pubsub.rs:22` (`PubSubSender = mpsc::UnboundedSender<PubSubMessage>`)
- `frogdb-server/crates/server/src/connection/pubsub_conn_command.rs:270` (unbounded per-connection receiver)
- `frogdb-server/crates/core/src/keyspace_coordinator.rs:261` (contrast: bounded cross-shard hop, `try_send` + drop + metric, already unit-tested)

## Resolution

Landed the bound (the issue's preferred outcome — feasible and it eliminates the
DoS). The client-facing pub/sub delivery path now enforces a per-connection
byte-budget output-buffer limit, mirroring Redis's
`client-output-buffer-limit pubsub`.

### Design

`PubSubSender` is no longer a bare `mpsc::UnboundedSender`. It is a small struct
wrapping the unbounded channel plus a shared `Arc<OutputBudget>` (an
`AtomicUsize queued_bytes` + `hard_limit` + `overflowed`/`dropped` flags), cloned
across every shard's sender and the single per-connection receiver
(`frogdb-server/crates/core/src/pubsub.rs`):

- `PubSubSender::send` estimates each message's buffered size
  (`PubSubMessage::approx_buffer_size`, payload+channel+pattern+framing overhead)
  and, once `queued_bytes + size` would exceed `hard_limit`, **drops** the
  message instead of enqueuing it, latching `overflowed` and bumping `dropped`.
  The subscriber is still counted as a recipient (`Ok`) — matching Redis, where a
  client that trips its buffer limit is counted for `PUBLISH` and disconnected
  asynchronously. `Err(PubSubClosed)` is returned only when the receiver is gone,
  preserving callers' "closed ⇒ not a subscriber" counting.
- `PubSubReceiver::recv`/`try_recv` decrement `queued_bytes` as messages drain,
  so a subscriber that keeps up never trips the limit.
- The connection delivery loop (`connection.rs`) selects on
  `PubSubReceiver::recv_or_overflow`, which returns either `Drained::Message` (a
  queued message to flush) or `Drained::Overflowed`. `OutputBudget` carries a
  `tokio::sync::Notify` fired on the false→true overflow transition, so overflow
  is surfaced **even when the mpsc has already drained empty** (the flood was
  dropped, so a bare `recv()` would park forever and never re-check the latch —
  the deadlock this wake mechanism closes). On `Drained::Overflowed` (or the
  post-flush `has_overflowed()` check) the loop logs, increments the new
  `frogdb_pubsub_output_buffer_disconnects_total` counter, and breaks —
  dropping the socket and disconnecting the slow subscriber (best-effort).
- `hard_limit == 0` disables the bound. The internal cross-shard
  keyspace-notification hop and other in-process feeds construct the channel via
  `PubSubSender::unbounded()` (limit 0), so this change caps **only** the
  client-facing path and does not double-cap the already-bounded keyspace hop.

### Config

New `server.pubsub-output-buffer-hard-limit` (bytes), default 32 MiB — Redis's
`client-output-buffer-limit pubsub` hard default. Threaded
`ServerConfig` → `AcceptorContext` → `ConnectionConfig` → `ConnectionHandler` →
`PubSubIo::ensure_pubsub_channel`. Test harness gained a matching
`TestServerConfig::pubsub_output_buffer_hard_limit` override.

Soft-limit / soft-seconds semantics were intentionally NOT implemented: the hard
limit alone bounds memory and closes the DoS, and the soft window adds
timer-tracking state for marginal benefit. Left as a possible future refinement.

### Feasibility note (best-effort disconnect)

A byte-accurate synchronous disconnect the instant a client's write blocks is not
achievable without preempting an in-progress `write_all().await` in the delivery
loop's flush branch (the top-level `select!`/`ClientHandle::killed()` cannot
preempt a branch body already awaiting). That is out of scope. What IS
guaranteed and is the actual DoS fix: **memory stays bounded** (excess messages
are dropped at enqueue, never buffered), and the disconnect fires on the next
drain after a flush. In the worst case (a fully-wedged socket write) the
connection buffers at most `hard_limit` bytes and no more.

### Tests

- `frogdb-server/crates/core/src/pubsub.rs` unit tests: `output_budget_*`
  (stalled subscriber stays bounded, drops latch overflow, draining frees the
  budget, unbounded channel never overflows, send reports closed when receiver
  dropped), plus `recv_or_overflow_surfaces_overflow_on_an_empty_channel` (flood
  past the limit, drain to empty, assert `Drained::Overflowed` is still surfaced
  within 1s — the deadlock regression) and
  `recv_or_overflow_prefers_draining_a_queued_message`.
- `frogdb-server/crates/server/tests/integration_pubsub.rs::test_slow_subscriber_output_buffer_bound_disconnects`:
  32 KiB limit, subscriber subscribes then stops reading, publisher floods
  ~16 MiB (4000 × 4 KiB messages) — sized past the kernel socket-buffer ceiling
  (server `SO_SNDBUF` autotunes to ≤4 MiB on Linux and would otherwise absorb the
  whole flood, so the userspace budget never overflows; the subscriber's
  `SO_RCVBUF` is capped to 128 KiB to keep that ceiling bounded and the drain
  fast). A separate control connection stays responsive throughout, the
  subscriber then drains a bounded prefix and hits EOF (server disconnected it),
  and `frogdb_pubsub_output_buffer_disconnects_total` records the teardown.
  Verified green on both macOS and the aarch64 Linux testbox. Comments explicitly
  distinguish this client-facing path from the already-bounded
  keyspace-notification hop.

### Acceptance criteria

- [x] Test floods an unread subscriber and asserts bounded growth + disconnect.
- [x] Bound implemented: config surface (`server.pubsub-output-buffer-hard-limit`),
  default approximates Redis (32 MiB hard), disconnect-on-exceed tested.
- [x] Tests distinguish this path from the cross-shard keyspace-notification hop.
