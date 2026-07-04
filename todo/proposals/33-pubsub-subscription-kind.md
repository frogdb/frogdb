# Proposal: Pub/Sub Subscription-Kind Table

Status: implemented
Date: 2026-07-04

## Problem

FrogDB has three subscription kinds — broadcast channels (`SUBSCRIBE`), patterns (`PSUBSCRIBE`),
and sharded channels (`SSUBSCRIBE`) — and until this change it had **six near-identical handlers**
(`handlers/pubsub.rs:32-410`), one per kind per direction. Each subscribe handler was the same
control flow, hand-copied: arity check → `admit_subscriptions(KIND, len)` → per-argument loop of
`add_subscription` + `ensure_pubsub_channel` + send `ShardMessage::{Kind}Subscribe` + push
`PubSubConfirmation::{Kind}`. Each unsubscribe handler was the same loop plus
`rearm_subscription_warning` appended. What actually *varied* between the six was pure data:

| Varies | Channel | Pattern | Sharded |
|---|---|---|---|
| `SubKind` | `Channel` | `Pattern` | `Sharded` |
| routing | coordinator shard 0 | coordinator shard 0 | `shard_for_key` |
| `ShardMessage` | `Subscribe`/`Unsubscribe` | `PSubscribe`/`PUnsubscribe` | `ShardedSubscribe`/`ShardedUnsubscribe` |
| confirmation | `Subscribe`/`Unsubscribe` | `PSubscribe`/`PUnsubscribe` | `SSubscribe`/`SUnsubscribe` |
| limit error | "max subscriptions" | "max pattern subscriptions" | "max sharded subscriptions" |

That is the shallow-seam signature again: kind-to-kind divergence is **data** (a routing policy, two
message constructors, two confirmation constructors, two strings), but it was encoded as **control
flow** copied six times. The invariants that must hold for *every* kind — "admit against the
per-connection limit on every subscribe", "re-arm the 80% warning latch on every unsubscribe" —
existed only as a convention repeated in six bodies. A seventh copy (or an edit to one of the six)
could drop either invariant with no compiler complaint; the broadcast and pattern subscribe bodies
already differed from each other in only three tokens, which is exactly the kind of duplication that
drifts.

Three secondary problems rode along:

- **Per-channel messaging defeated the batch interface core already offers.** Every
  `ShardMessage::{,P,Sharded}{Sub,Unsub}scribe` variant carries `channels: Vec<Bytes>` and replies
  `Vec<usize>` (`core/src/shard/message.rs:148-206`) — the batch seam exists — yet every handler sent
  `channels: vec![channel.clone()]`, one message per channel inside the loop (`pubsub.rs:67,117,168,
  218,337,388`). `SUBSCRIBE` with N channels was N coordinator-shard messages instead of one.
- **Dead oneshot plumbing.** Each of those sends created a fresh `oneshot::channel()` whose receiver
  was immediately discarded (`let (response_tx, _response_rx) = …`). The shard dutifully computed and
  sent per-channel counts (`dispatch_pubsub.rs`) into a dropped receiver — result plumbing that
  looked like a request/response contract but wasn't one. Worse, because nothing waited, the client
  could read its `subscribe` confirmation *before* the shard had processed the registration; a
  `PUBLISH` racing in from another connection could then legitimately count zero subscribers.
  Integration tests papered over this with `sleep(50ms)` ("give the subscription time to register").
- **The shard-0-as-coordinator invariant lived only in comments.** Broadcast pub/sub registers every
  subscriber on shard 0 so PUBLISH delivers exactly once with an unmultiplied count. That invariant
  was asserted in prose at four sites in `pubsub.rs` (and relied on in `handlers/client.rs:707-711`
  and `ShardMessage::PublishKeyspace`), but the code said `shard_senders[0]` — a bare literal with no
  name to grep for and nothing anchoring the comment to the index.

And in `cluster_pubsub.rs`, the two cross-node forwarding paths (`broadcast_publish:40-108`,
`forward_spublish:114-157`) duplicated the same RPC skeleton: build `ClusterRpcRequest` →
`tokio::time::timeout(2s, network.send_rpc(…))` → a hand-written 4-arm match (expected shape /
unexpected shape / transport error / timeout). The 2-second magic literal appeared twice (`:77`,
`:138`), and `forward_spublish`'s unexpected-shape arm did `Some(0)` — a peer answering with the
*wrong response variant* (a protocol bug) was folded into "zero subscribers", indistinguishable from
a healthy empty node except for a log line. Only the trivial `Local`-variant paths were tested
(`:160-177`); the mapping logic itself was untestable because it was welded to the concrete
`ClusterNetwork` (a struct, not a trait — no mock exists).

**Deletion test.** If a subscription-kind descriptor were already a deep module, deleting it would
force ~330 lines of sextuplicated handler control flow back into the callers, plus the per-shard
batching, plus the ack barrier. Before this change none of that could be deleted because the
kind-to-kind variation lived *in* the callers.

## Current state (as implemented)

### 1. The subscription-kind table (`handlers/pubsub.rs`)

One `SubKindSpec` struct captures everything that varies between kinds; three `static` entries
(`CHANNEL_SPEC`, `PATTERN_SPEC`, `SHARDED_SPEC`) are the whole table:

```rust
struct SubKindSpec {
    kind: SubKind,                 // which per-connection set (state.rs vocabulary, reused)
    routing: SubRouting,           // Broadcast (coordinator) | ByChannelKey (shard_for_key)
    route_command: &'static str,   // cluster slot-routing name (sharded kind only)
    arity_error: &'static str,
    limit_error: &'static str,
    subscribe_msg:   fn(Vec<Bytes>, ConnId, PubSubSender, oneshot::Sender<Vec<usize>>) -> ShardMessage,
    unsubscribe_msg: fn(Vec<Bytes>, ConnId, oneshot::Sender<Vec<usize>>) -> ShardMessage,
    subscribed:      fn(Bytes, usize) -> PubSubConfirmation,
    unsubscribed:    fn(Option<Bytes>, usize) -> PubSubConfirmation,
}
```

Plain `fn` pointers (non-capturing closures coerce in statics) rather than trait objects: the
constructors are total, data-only functions, so the table stays `static` with zero allocation and
the call sites stay direct calls. A table-of-fn-pointers was chosen over enum-with-methods because
the *callers* never match on the kind — they only ever need "the whole row", and a struct row makes
"add a field ⇒ every entry must supply it" a compile error, which is the drift-protection this seam
exists to buy.

The six public handlers are now one-line delegations into a single generic pair:

```rust
pub(crate) async fn handle_subscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
    self.subscribe_kind(&CHANNEL_SPEC, args).await
}
```

`subscribe_kind` / `unsubscribe_kind` own the shared invariants **once**:

- the per-connection limit and 80% warning latch are consulted before anything is inserted
  (`admit_subscriptions`) — on *every* subscribe, because there is only one subscribe;
- the 80% latch is re-armed after *every* unsubscribe batch, same reason;
- confirmations report the local per-connection count in argument order; the sharded kind's cluster
  redirects (`coordinator.route()` per channel, ASKING consumed once per command) interleave in
  place, exactly as before;
- empty-arg unsubscribe expands to "all subscriptions of this kind", and unsubscribing from nothing
  yields the single null-channel confirmation.

### 2. Batched shard messages + a real ack

`group_channels_by_shard(routing, channels, num_shards)` groups accepted channels by destination:
broadcast kinds collapse onto the coordinator; the sharded kind groups by `shard_for_key`. Each
destination shard now receives exactly **one** `ShardMessage` carrying all of its channels — the
`Vec<Bytes>` field finally carries a vec. `BTreeMap` keeps send order deterministic (testable).

The dead oneshot plumbing was **kept and made real**: the handler now awaits each shard's
`response_rx` before returning the confirmations. Decision rationale: the per-shard counts in the
reply are still unused (the client-visible count is the *per-connection* count, per Redis semantics,
known locally before any shard is involved), but awaiting the reply is a **registration barrier** —
by the time the client reads its `subscribe` confirmation, the shard has processed the registration,
so a PUBLISH issued after the confirmation can never miss the subscriber. Redis gets this for free
by being single-threaded; FrogDB previously did not have it at all (hence the `sleep(50ms)` in the
old tests). Batching makes the barrier cheap: one round trip per destination shard per command, not
per channel. The new integration tests publish immediately after the confirmation with **no sleep**
and assert `Integer(1)` — that is the barrier under test.

### 3. `BROADCAST_SHARD`

```rust
pub(crate) const BROADCAST_SHARD: usize = 0;
```

lives in `handlers/pubsub.rs` with the exactly-once invariant documented on it. All shard-0
references in the pub/sub handlers (`subscribe`/`unsubscribe` routing via `SubRouting::Broadcast`,
`handle_publish`) go through it. Other files that lean on the same invariant
(`handlers/client.rs:707-711` tracking-BCAST redirect, `cluster_bus.rs:189`) were owned by
concurrent work and are follow-up call sites — see open questions.

### 4. `send_pubsub_rpc` (`cluster_pubsub.rs`)

One helper owns the timeout and the four-arm mapping both forwarding paths used to spell out:

```rust
async fn send_pubsub_rpc<F>(
    target: NodeId,
    op: &'static str,
    rpc: F,                                            // the send_rpc future
    extract: fn(&ClusterRpcResponse) -> Option<usize>, // names the expected shape
) -> Result<usize, PubSubRpcError>
where F: Future<Output = Result<ClusterRpcResponse, ClusterError>>
```

- `PUBSUB_RPC_TIMEOUT` (2s) is named once instead of two inline literals.
- A protocol-shape mismatch is now a **warn-logged, distinguishable** `PubSubRpcError::
  UnexpectedResponse` — not a silent `Some(0)`. Callers still fold every failure into `0` for the
  client-visible subscriber total (a dead node genuinely contributes zero subscribers, and Redis
  reports only a count), but the failure *mode* is now typed at the seam: shape mismatch = peer bug
  = `warn!`; transport error / timeout = partition noise = `debug!`.
- **Mock decision:** `ClusterNetwork` is a concrete struct (no trait), so instead of introducing a
  network trait + mock solely for this test, the helper is generic over the RPC *future*. The
  timeout/mapping logic — the part that was untested — is exercised with plain `async` blocks
  (expected shape, mismatched shape, transport error, and a `start_paused` never-completing future
  for the timeout arm). This is a strictly smaller seam than a mocked network type and tests exactly
  the logic the helper owns; the `connect`/`send_rpc` transport itself is covered by the cluster
  integration tests (`integration_pubsub.rs` cross-node suite).

### Why this is the right depth

- **Locality.** Every cross-kind invariant (admit, re-arm, count-in-arg-order, batch-per-shard, ack
  barrier) has exactly one home. Adding a hypothetical fourth subscription kind is one table row —
  and the row type forces it to answer every question the generic pair will ask.
- **Leverage.** ~380 lines of handler bodies became ~150 lines of table + two generic functions; the
  batching and the ack barrier came *out of* the unification rather than being bolted onto six
  copies. N-channel SUBSCRIBE went from N coordinator messages to 1.
- **Interface ≤ implementation.** The public surface did not change at all: same six
  `handle_*` signatures, same wire behavior (modulo the ack barrier strengthening, below), same
  confirmation seam (`PubSubConfirmation`, proposal 26) — the table is entirely private vocabulary.

## Behavior notes (wire-visible deltas)

- **Confirmation timing.** Confirmations for a multi-channel command are now written after *all* of
  the command's shard registrations are acked, rather than interleaved send-by-send. Per-frame
  content and order are unchanged; only the (unobservable) intra-command timing differs — and the
  observable consequence is strictly stronger: publish-after-confirmation cannot miss.
- **Shard registration granularity.** A shard receiving `channels: [a, b, c]` registers them in one
  message-handling step instead of three. `dispatch_pubsub` already handled the vec case; no core
  change was needed (core `ShardMessage` variants untouched, per the batching-uses-existing-
  signatures constraint).

## Testing impact

- **Unit (handler, `handlers/pubsub.rs::tests`):** broadcast routing collapses N channels into one
  coordinator batch; sharded routing produces ≤ 1 message per shard, covers every channel on its
  `shard_for_key` owner, and preserves argument order within a batch; single-shard topology
  degenerates to one batch; and the table's kind ↔ confirmation-label pairing is pinned so a table
  edit cannot cross-wire a `psubscribe` confirmation onto the sharded row.
- **Unit (`cluster_pubsub.rs::tests`):** `send_pubsub_rpc` over pure futures — expected shape ⇒
  `Ok(count)`; mismatched shape ⇒ `UnexpectedResponse` (not zero); transport error ⇒ `Rpc`; pending
  future + paused clock ⇒ `Timeout`; extractor cross-matching pinned. The previously-only tests
  (`Local` no-ops) retained.
- **Integration (`integration_pubsub.rs`):** three new tests drive the batched path over the wire on
  the default 4-shard topology — multi-channel SUBSCRIBE (ascending counts in arg order, then
  PUBLISH per channel with **no registration sleep**, asserting the ack barrier), multi-channel
  SSUBSCRIBE across shards (same, via SPUBLISH/smessage), and multi-channel UNSUBSCRIBE (descending
  counts, then PUBLISH ⇒ 0). The 30+ existing pub/sub integration tests (confirmation shapes RESP2/
  RESP3, MULTI/EXEC paths, cluster MOVED/ASKING redirects, slot-migration sunsubscribe) pin the
  refactor's behavior-preservation.
- **Regression:** `redis-regression` `pubsub_tcl` / `pubsub_regression` / `pubsubshard_tcl` /
  `pubsubshard_regression` / `cluster_sharded_pubsub_tcl` suites cover Redis-parity semantics over
  the same handlers.

## Risks / open questions

- **The ack barrier adds a shard round trip to subscribe/unsubscribe.** Previously fire-and-forget;
  now one awaited oneshot per destination shard per command. Subscribe/unsubscribe are control-plane
  operations (~1% of connections per the lazy `pubsub_tx` comment), and batching bounds the cost at
  `min(channels, shards)` round trips, so this is judged cheap for the ordering guarantee it buys.
  If a workload ever subscribes on a hot path, the barrier could be made kind-configurable in the
  table — one field, not six edits.
- **`BROADCAST_SHARD` call-site coverage is partial.** `handlers/client.rs:707-711` (tracking BCAST
  redirect) and `cluster_bus.rs:189` also encode shard 0 as coordinator, and
  `handlers/debug.rs`/`handlers/search/*` use `shard_senders[0]` for *different* (metadata-
  coordinator) reasons that should not be conflated with the pub/sub invariant. Routing the
  remaining pub/sub-invariant sites through the const is a follow-up once concurrent work on those
  files lands; whether the search/debug "shard 0" deserves its own named constant is a separate
  question.
- **Failure counts are still folded to zero at the forwarder boundary.** `PubSubRpcError` stops at
  `broadcast_publish`/`forward_spublish`; the client cannot distinguish "peer down" from "no
  subscribers", which matches Redis (PUBLISH returns a count, never an error, under cluster
  partitions). If an operator-facing signal is wanted, the typed error is now in place to feed a
  metrics counter — one `match` at the fold site.
- **Duplicate channels in one command** (`SUBSCRIBE a a`) ride the batch as duplicates; shard-side
  registration is set-based and idempotent, and per-connection counts already handled this (the
  second insert reports an unchanged count). Behavior identical to the pre-refactor loop, now in one
  place if it ever needs changing.

## Correctness flags

1. **Publish-after-subscribe race — CONFIRMED and fixed.** Pre-change, subscribe confirmations were
   written without waiting for the shard to process the registration (receiver dropped at
   `pubsub.rs:66,116,167,217,336,387`), so a client that received `+subscribe` and *then* had a peer
   PUBLISH could see the message counted to zero subscribers. The existing tests' `sleep(50ms)`
   comments ("give the subscription time to register") are the tell. The awaited ack closes this;
   the new no-sleep integration tests fail without it.
2. **`forward_spublish` shape mismatch folded to `Some(0)` — CONFIRMED and fixed.** A peer answering
   `PubSubForward` with any other variant was logged but returned as a zero count
   (`cluster_pubsub.rs:144-147` pre-change), indistinguishable at every layer above from an empty
   node. Now a typed `UnexpectedResponse` at the seam (still folded to 0 for the client, by design —
   see open questions).
