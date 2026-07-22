# Proposal 17 — Shard pub/sub registration is a barrier, not a count: return a bare ack

## Summary

The six shard-side pub/sub registration handlers (`handle_subscribe`/`unsubscribe`/`psubscribe`/
`punsubscribe`/`ssubscribe`/`sunsubscribe`) each return a `Vec<usize>` whose elements are **loop
indices** (`i + 1` on subscribe, `i` on unsubscribe), tagged `// Placeholder count` in the source.
Those vectors travel back through a `oneshot::Sender<Vec<usize>>` on six `ShardMessage` variants and
are then **discarded** by the one and only consumer, which awaits the channel purely as a barrier
("await its ack so the registration is visible on the shard before the client sees the
confirmation"). The Interface of the Internal Shard's pub/sub registration seam therefore advertises
a per-shard subscriber count it cannot compute and no one reads — a fabricated integer whose only
defense against a future caller trusting it is a prose comment. This proposal replaces the fake
`Vec<usize>` with an explicit registration ack (`oneshot::Sender<()>`), making the barrier the typed
contract and deleting the fake-integer trap. The client-visible subscription count is unaffected: it
already comes solely from `ConnectionState::add_subscription`, computed before the shard is even
messaged.

## Problem

The pub/sub registration path crosses one Seam twice: the server-side connection command
(`PubSubIo::subscribe_kind` / `unsubscribe_kind`) batches one message per destination Internal Shard
and awaits an ack; the ShardWorker receives that message, mutates its `ShardSubscriptions` table, and
replies. The reply is typed `Vec<usize>` and is meant — by the field name `counts` and the
`// Placeholder count` comments — to carry per-shard subscriber counts. It carries nothing of the
sort:

1. **The value is fabricated from loop position, not measured.** `handle_subscribe` maps each channel
   to `i + 1` and `handle_unsubscribe` to `i` — the enumerate index, which has no relationship to how
   many subscribers the channel actually has. The underlying `ShardSubscriptions::subscribe` returns
   a `bool` (was-newly-inserted), which the handler throws away in favor of the index.

2. **No consumer reads it.** The server awaits the `oneshot` and immediately drops the payload
   (`let _ = response_rx.await;`). The load-bearing count the client sees — `SUBSCRIBE` replies with
   "you are now subscribed to N channels" — is the **per-connection** count returned by
   `ConnectionState::add_subscription`, produced *before* the shard message is sent. The shard round
   trip contributes ordering, never a number.

3. **The type lies about the module's output.** A `oneshot::Sender<Vec<usize>>` promises the caller a
   vector of meaningful counts. The real output is "registration is now durable in my table" — a
   unit fact. The interface is wider than the truth, and the gap is patched with comments
   (`// Placeholder count`, "the per-shard subscriber counts, unused here"). This is precisely the
   shallow-interface / fake-value smell: prose defends an invariant that a type could enforce.

Contrast the sibling `Publish` / `ShardedPublish` variants, whose `oneshot::Sender<usize>` carries a
**real** subscriber count that the server genuinely sums (`local_count + remote_count`), and the
`PubSubIntrospection` path, whose `NumSub` count is computed for real from the table. Those integers
are load-bearing and stay. Only the six *registration* handlers fabricate.

The barrier the round trip actually provides is a real, tested contract:
`test_subscribe_publish` and the cross-shard keyspace tests depend on a `PUBLISH` issued after a
`SUBSCRIBE` confirmation seeing the registration. Today that contract is smuggled inside a return
type that claims to be about counting. Making the ack explicit turns the barrier into the thing the
signature says, and removes an integer that a future caller could read and trust as a subscriber
count (e.g. wiring it into a `SUBSCRIBE` reply to "match Redis more closely"), silently shipping loop
indices to clients — the observability-accuracy failure mode this project explicitly rejects.

## Evidence (verified file:line)

All paths under `/Users/nathan/workspace/workspace-3/frogdb-server/`. Line numbers verified against
the current tree.

- **Fabricated counts (six handlers):** `crates/core/src/shard/pubsub.rs:9-131`.
  - `handle_subscribe` returns `i + 1` per channel, comment `// Placeholder count` (L20-24).
  - `handle_unsubscribe` returns `i` per channel, comment `// Placeholder remaining count`
    (L42-45).
  - `handle_psubscribe` (L58-71), `handle_punsubscribe` (L80-89), `handle_ssubscribe` (L99-112),
    `handle_sunsubscribe` (L121-130) — identical index-as-count pattern.
  - Header comment L15-16: "The count is just a placeholder here since we don't track across shards."
- **Underlying table returns `bool`, not a count:** `crates/core/src/pubsub.rs:278,291,323,338,368,381`
  — `subscribe`/`unsubscribe`/`psubscribe`/`punsubscribe`/`ssubscribe`/`sunsubscribe` all return
  `bool`, discarded by the handlers.
- **The `oneshot::Sender<Vec<usize>>` typing (six `ShardMessage` variants):**
  `crates/core/src/shard/message.rs:178,185,193,200,222,229` (`Subscribe`, `Unsubscribe`,
  `PSubscribe`, `PUnsubscribe`, `ShardedSubscribe`, `ShardedUnsubscribe`). Compare the real-count
  siblings `Publish` (L207) and `ShardedPublish` (L236): `oneshot::Sender<usize>`.
- **Wiring (send the fabricated vec back):** `crates/core/src/shard/dispatch_pubsub.rs:16-17,24-25,
  33-34,41-42,75-76,83-84` — `let counts = self.handle_*(...); let _ = response_tx.send(counts);`.
- **Server discards the payload; barrier is the real intent:**
  `crates/server/src/connection/pubsub_conn_command.rs`. Subscribe path L374-396 — comment L376-379
  "Await each shard's ack (the per-shard subscriber counts, unused here — the client-visible count
  is the per-connection one above) so that by the time the confirmation reaches the client, a
  PUBLISH processed after it is guaranteed to see the registration"; `let _ = response_rx.await;`
  L394. Unsubscribe path L437-443 (`let _ = response_rx.await;` L442). The two `subscribe_msg` /
  `unsubscribe_msg` function pointers are typed `oneshot::Sender<Vec<usize>>` at L95 and L97.
- **Client-visible count is per-connection, produced before the shard message:**
  `crates/server/src/connection/pubsub_conn_command.rs:360` — `let count =
  self.state.add_subscription(spec.kind, channel.clone());`, comment L358-359 "the confirmation
  count is the per-connection count, so it is known before the shard replies." Unsubscribe twin at
  L427 (`remove_subscription`).
- **The barrier's tested contract:** `crates/server/tests/integration_pubsub.rs:10`
  (`test_subscribe_publish`) and the cross-shard keyspace notification tests (L206, L261, L308) all
  rely on a `PUBLISH`/keyevent issued after a `SUBSCRIBE` confirmation observing the registration.
- **Only these two files touch the variants** (verified by grep for `ShardMessage::Subscribe` etc.):
  the producer is `pubsub_conn_command.rs` (the `SubKindSpec` table, L112-169), the consumer is
  `dispatch_pubsub.rs`. No test or mock constructs them; `message.rs:670-677` only name them in a
  `Debug`-label match. So the blast radius is exactly three files plus the `ShardMessage` enum.

**Evidence discrepancy vs. the candidate:** the candidate cited paths as `core/src/shard/pubsub.rs`,
`core/src/shard/dispatch_pubsub.rs`, `server/src/connection/pubsub_conn_command.rs`. The real paths
are prefixed `frogdb-server/crates/` (`frogdb-server/crates/core/...`,
`frogdb-server/crates/server/...`). All cited **line numbers** are accurate. The candidate's discard
comment range "375-395" is effectively correct (comment L376-379, `response_rx.await` at L394). Core
premise fully verified.

## Proposed design (Rust interface sketch)

The registration reply is a unit ack. Change the six `ShardMessage` variants and the six handlers to
carry `()` instead of a fabricated `Vec<usize>`. Signatures/types only:

```rust
// crates/core/src/shard/message.rs — six variants change their response channel type.
// Was: response_tx: oneshot::Sender<Vec<usize>>
Subscribe          { channels: Vec<Bytes>, conn_id: ConnId, sender: PubSubSender,
                     response_tx: oneshot::Sender<()> },
Unsubscribe        { channels: Vec<Bytes>, conn_id: ConnId,
                     response_tx: oneshot::Sender<()> },
PSubscribe         { patterns: Vec<Bytes>, conn_id: ConnId, sender: PubSubSender,
                     response_tx: oneshot::Sender<()> },
PUnsubscribe       { patterns: Vec<Bytes>, conn_id: ConnId,
                     response_tx: oneshot::Sender<()> },
ShardedSubscribe   { channels: Vec<Bytes>, conn_id: ConnId, sender: PubSubSender,
                     response_tx: oneshot::Sender<()> },
ShardedUnsubscribe { channels: Vec<Bytes>, conn_id: ConnId,
                     response_tx: oneshot::Sender<()> },
// Publish / ShardedPublish keep oneshot::Sender<usize> — that count is real and consumed.
```

```rust
// crates/core/src/shard/pubsub.rs — handlers return nothing; the enumerate/index is gone.
impl ShardWorker {
    pub(crate) fn handle_subscribe(&mut self, channels: Vec<Bytes>, conn_id: ConnId,
                                   sender: PubSubSender);          // was -> Vec<usize>
    pub(crate) fn handle_unsubscribe(&mut self, channels: Vec<Bytes>, conn_id: ConnId);
    pub(crate) fn handle_psubscribe(&mut self, patterns: Vec<Bytes>, conn_id: ConnId,
                                    sender: PubSubSender);
    pub(crate) fn handle_punsubscribe(&mut self, patterns: Vec<Bytes>, conn_id: ConnId);
    pub(crate) fn handle_ssubscribe(&mut self, channels: Vec<Bytes>, conn_id: ConnId,
                                    sender: PubSubSender);
    pub(crate) fn handle_sunsubscribe(&mut self, channels: Vec<Bytes>, conn_id: ConnId);
    // Bodies: for-loop over channels calling self.subscriptions.subscribe(...) for its
    // side effect; the `bool` return of ShardSubscriptions::subscribe stays ignored (it is
    // genuinely not needed on this path); check_thresholds/reset_thresholds unchanged.
}
```

```rust
// crates/core/src/shard/dispatch_pubsub.rs — send the unit ack after the mutation.
ShardMessage::Subscribe { channels, conn_id, sender, response_tx } => {
    self.handle_subscribe(channels, conn_id, sender);
    let _ = response_tx.send(());   // barrier ack: registration is now visible in this shard
}
// ... same shape for the other five.
```

```rust
// crates/server/src/connection/pubsub_conn_command.rs — the two fn-pointer fields on
// SubKindSpec change their oneshot payload type; call sites drop `Vec<usize>` from the ack.
struct SubKindSpec {
    // ...
    subscribe_msg:   fn(Vec<Bytes>, ConnId, PubSubSender, oneshot::Sender<()>) -> ShardMessage,
    unsubscribe_msg: fn(Vec<Bytes>, ConnId, oneshot::Sender<()>) -> ShardMessage,
    // ...
}
// subscribe_kind / unsubscribe_kind: `let (response_tx, response_rx) = oneshot::channel();`
// becomes oneshot::channel::<()>() by inference; `let _ = response_rx.await;` is unchanged —
// it was already discarding, now it discards `()`. The comment shrinks to "await the shard's
// registration ack (the PUBLISH-visibility barrier)".
```

Optional depth (call out, do not necessarily do): give the ack a named marker instead of `()` —
`enum Registered { Ack }` or a `struct Registered;` — so the barrier reads as an intentional
contract at the `send`/`await` sites rather than an anonymous unit. `()` is the minimal, idiomatic
choice; a named marker trades one type for slightly better self-documentation. Recommend `()` unless
review prefers the marker.

## Migration plan (ordered steps)

1. **`message.rs`:** change the `response_tx` type on the six registration variants from
   `oneshot::Sender<Vec<usize>>` to `oneshot::Sender<()>`. Leave `Publish`/`ShardedPublish` as
   `usize`.
2. **`pubsub.rs`:** drop the `-> Vec<usize>` return and the `enumerate()`/`i + 1`/`i` construction
   from all six handlers; keep the loop body (the `self.subscriptions.*` calls) and the
   threshold-check calls exactly as-is. Delete the `// Placeholder count` comments.
3. **`dispatch_pubsub.rs`:** for the six arms, call the handler for its effect then
   `let _ = response_tx.send(());`.
4. **`pubsub_conn_command.rs`:** update the two `SubKindSpec` fn-pointer field types and the six
   static table entries' closures (`Subscribe`/`Unsubscribe`/`PSubscribe`/`PUnsubscribe`/
   `ShardedSubscribe`/`ShardedUnsubscribe` builders) to take `oneshot::Sender<()>`. The
   `oneshot::channel()` sites infer `()`; the `let _ = response_rx.await;` lines are unchanged.
   Retune the L376-379 comment to describe a barrier, not "unused counts."
5. **Compiler-drive the rest:** the change is confined to the four files above; `cargo check`
   (`just check frogdb-core` then `just check frogdb-server`) surfaces any missed site. No cross-crate
   signature ripple beyond `core` → `server` (which already imports `ShardMessage`).

Crate-direction note: `core` owns `ShardMessage` and the handlers; `server` is the downstream
consumer. The change only narrows a type `core` already exports and `server` already builds — no new
dependency edge, and `persistence`/`replication`/`cluster` are untouched (they do not reference these
variants).

## Test plan

- **Barrier contract stays green (the real reason the round trip exists):**
  `integration_pubsub::test_subscribe_publish` and the cross-shard keyspace tests
  (`test_cross_shard_keyevent_notification_delivered`,
  `test_cross_shard_keyspace_notification_delivered`, `test_cross_shard_expired_keyevent_delivered`)
  must pass unchanged — they already encode "PUBLISH after SUBSCRIBE confirmation sees the
  registration," which is exactly the barrier the `()` ack now names. Run
  `just test frogdb-server pubsub` and the redis-regression pub/sub suites
  (`pubsub_regression`, `pubsubshard_regression`, `pubsub_tcl`, `cluster_sharded_pubsub_tcl`).
- **Client-visible count is unaffected:** `test_pubsub_numsub`, `test_pubsub_numpat`, and the
  `SUBSCRIBE`/`UNSUBSCRIBE` confirmation-count assertions (e.g. `test_unsubscribe`,
  `test_unsubscribe_all`) pin that the reply count comes from `add_subscription`/`remove_subscription`
  and NUMSUB from the introspection path — neither touches the deleted `Vec<usize>`. These are the
  regression guard proving the fabricated vector was never the client count.
- **New focused unit test (optional, core):** a `dispatch_pubsub` test that drives
  `ShardMessage::Subscribe` and asserts the `oneshot` resolves to `()` after the subscription is
  present in `self.subscriptions` — pins "ack fires only after the mutation is durable in the table,"
  the barrier invariant, now expressible without asserting anything about a fake integer.
- **Type-level guard:** the compiler now rejects any future attempt to read a subscriber count off
  the registration ack — the previous `Vec<usize>` made that a silent runtime mistake; `()` makes it
  a build error.

## Risks & alternatives

- **Behavioral parity:** zero client-observable change. The discarded value was never sent to a
  client; the barrier semantics (`await` before confirming) are preserved byte-for-byte. Low risk.
- **Redis/Valkey/Dragonfly cross-check:** none of the three derive the `SUBSCRIBE` reply count from a
  per-shard tally. Redis and Valkey are single-threaded and report the connection's own subscription
  count locally. Dragonfly is sharded but likewise reports the connection-local count, registering on
  the owning shard for delivery, not to aggregate a per-shard subscriber number into the reply. FrogDB
  already matches this: the count is per-connection (`add_subscription`). Removing the fake per-shard
  vector moves FrogDB *toward* their model, not away — there is no feature being dropped.
- **Alternative A — make the count real.** Have each handler return the true subscriber count for each
  channel from its table and have the server aggregate. Rejected: the client count is per-connection
  by Redis semantics, not a cross-shard subscriber tally, so a real per-shard count still would not be
  the number the client should see. It would compute an accurate integer that nothing should consume —
  trading a fake value for an unused-but-real one, still failing the deletion test. The correct move is
  to delete the value, not to make it honest.
- **Alternative B — named marker (`struct Registered;`).** Slightly more self-documenting at the
  `send`/`await` sites than `()`; costs a type. Defer to reviewer taste; `()` is the minimal idiomatic
  default and is what the sketch assumes.
- **Alternative C — fire-and-forget (drop the `response_tx` entirely).** Rejected: the round trip is
  load-bearing as a barrier. Removing it would let a `PUBLISH` race ahead of a not-yet-visible
  registration, breaking `test_subscribe_publish` and the cross-shard keyspace guarantees. The ack must
  stay; only its *payload* is fake.
- **ADR interaction:** none. ADR-0001 (Raft owns cluster metadata; the data path never traverses Raft)
  and ADR-0002 (single database) are both untouched — this is an internal typing change on the Internal
  Shard message seam, entirely on the data/pub-sub path, which does not go through the Raft Metadata
  Plane.

## Effort

**S.** Four files, one enum, six handlers, two fn-pointer field types; compiler-guided and confined to
the `core` → `server` seam with no cross-crate signature churn beyond a type narrowing `server` already
consumes. No async, ordering, or storage changes — the `oneshot` barrier is preserved; only its payload
type shrinks. The existing pub/sub suites are the regression guard and need no rewrite.

## Related

None.

## Adversarial review

**Verdict: CONFIRMED (no issues).** The reviewer opened every cited `file:line` and confirmed each
matches the proposal. No critical, major, or minor issues required a fix; the proposal is sound as
written.

Verified points:

1. All six registration handlers in `frogdb-server/crates/core/src/shard/pubsub.rs` (L14-131) return
   `Vec<usize>` built from `enumerate()` indices tagged `// Placeholder count`.
2. The underlying `ShardSubscriptions::{subscribe,unsubscribe,psubscribe,punsubscribe,ssubscribe,
   sunsubscribe}` all return `bool` (pubsub.rs:278,291,323,338,368,381), discarded by the handlers.
3. The six `ShardMessage` variants carry `oneshot::Sender<Vec<usize>>` (message.rs:178,185,193,200,
   222,229); `Publish`/`ShardedPublish` carry the real `oneshot::Sender<usize>` (L207,236), correctly
   kept by the proposal.
4. `dispatch_pubsub.rs` sends the fabricated vec; the server discards it via
   `let _ = response_rx.await;` (pubsub_conn_command.rs:394,442).
5. The client-visible count comes solely from `state.add_subscription`/`remove_subscription`
   (state.rs:732-766), produced before the shard round trip.
6. The `SubKindSpec` fn-pointers are typed `oneshot::Sender<Vec<usize>>` (L95,97).
7. Grep confirms the only other references to the six variants are the `Debug`-label match at
   message.rs:670-677 — no test, mock, replication/cluster/persistence site constructs them; blast
   radius is exactly the four files (three handler/wiring files plus the `ShardMessage` enum in
   message.rs).

Feasibility confirmed: pure type narrowing, no borrow-checker/async/crate-direction/wire-compat
concern; the barrier is preserved by `send(())`. Barrier tests (`test_subscribe_publish` asserting
`Integer(1)` from the per-connection count; cross-shard keyspace tests) are present and independent of
the discarded vec. Redis/Valkey/Dragonfly parity claim (connection-local subscription count, not
per-shard aggregate) is accurate.

Minor doc-precision nits the reviewer flagged as *not requiring change*: the "three files plus the
enum" phrasing at L97 equals the "four files" count used elsewhere (the enum lives in message.rs, one
of the four), and the candidate-path discrepancy is already self-disclosed in the Evidence section.
No edits were made; the proposal passed review as written.
