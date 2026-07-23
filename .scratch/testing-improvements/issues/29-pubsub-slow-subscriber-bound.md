# No client-output-buffer-limit for pub/sub; slow subscriber = unbounded server memory (untested)

Status: needs-triage
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
