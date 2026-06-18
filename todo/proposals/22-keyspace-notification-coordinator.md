# Proposal: Cross-Shard Keyspace Notification Coordinator

Status: proposed
Date: 2026-06-17

## Problem

A keyspace notification has two halves that must agree on *one* subscription table: the half that
**registers** a subscriber, and the half that **emits** an event. In multi-shard mode they don't.
SUBSCRIBE registers every subscriber on the broadcast coordinator shard (shard 0); a write, active
expiry, or eviction emits the matching `__keyspace@0__` / `__keyevent@0__` message into *the
key-owner shard's own* subscription table. When the key lives on any shard other than shard 0, the
event is published into a table that has no subscribers, and the subscriber that registered on shard
0 hears nothing. **In multi-shard mode keyspace events are silently never delivered.**

The rule that binds the two halves — *"subscriptions live on shard 0; an event emitted on the
key-owner shard must be routed to shard 0 to reach them"* — is written **nowhere**. It is not a
type, not a function, not even a comment. It is an emergent property of two files that were each
written correctly in isolation and are wrong only in combination:

| Half | What it does | Where | file:line |
|---|---|---|---|
| Register | SUBSCRIBE always sends `ShardMessage::Subscribe` to `shard_senders[0]` so each subscriber is registered once and PUBLISH delivers once | `handlers/pubsub.rs` | `pubsub.rs:62-74` |
| Emit | A write/expire/evict on the key-owner shard calls `self.subscriptions.publish(...)` — *that shard's own table* | `shard/keyspace_notify.rs` | `keyspace_notify.rs:49-59` |

Each half cites a sound local reason. `handle_subscribe` centralizes on shard 0 "so each subscriber
is registered exactly once and PUBLISH delivers each message exactly once" (`pubsub.rs:62-64`) — and
regular PUBLISH honors that contract: `handle_publish` sends `ShardMessage::Publish` to
`shard_senders[0]` (`pubsub.rs:246-252`), so a normal `PUBLISH` reaches every subscriber. The emit
half publishes locally because, viewed alone, "publish the event into the subscription table" is the
obvious thing to do — and on a *single* shard it is even correct (the key-owner shard *is* shard 0).
The mismatch only exists at the seam between the two, and no module owns that seam.

The contrast sharpens it: **PUBLISH works, keyspace-emit does not**, for the same destination
(shard 0's table) — because PUBLISH routes there explicitly and keyspace-emit does not. The
`@0` in the channel name is the *database* index (FrogDB's single logical DB), and is correct; the
defect is the *shard* the publish lands on, which the channel string says nothing about.

This is not a latent edge case. It is documented as a known gap
(`todo/proposals/INDEX.md:219-221`, "Cross-shard keyspace notifications lost"), and the existing
regression test for keyspace emission had to be pinned to a single shard to dodge it:
`test_lrem_emits_keyspace_notification` (`integration_pubsub.rs:48-58`) opens with
`num_shards: Some(1)` and the comment *"Cross-shard keyspace-event delivery is a separate
concern."* The test passes only because shard 0 owns the key.

**Deletion test.** The keyspace-notification *behavior* is real and well-used — emit sites in the
post-execution pipeline (`post_execution.rs:187`), the active-expiry loop (`event_loop.rs:161,181`),
and both eviction paths (`eviction.rs:232,295`). We cannot delete the feature. What we *cannot
delete today* is the routing rule, because it does not exist as an artifact — it is split, unwritten,
across `handle_subscribe` and `emit_keyspace_notification`. If the routing rule were already a deep
module, deleting that module would delete: the single-shard workaround on the LREM test, the
INDEX "known gap" entry, and the entire class of "the subscriber registered *there* but the event
fired *here*" bug. That none of those can be deleted today — that the rule has no home to delete — is
the signal the seam is missing. The complexity is wrongly encapsulated (smeared across two files),
not absent.

This proposal owns the **emit→subscriber routing decision**: a `KeyspaceNotificationCoordinator`
that makes "which shard's subscription table does this event land in" a single, named, unit-testable
choice, and fixes delivery in one place. This is a documented round-4 follow-up.

## Current state

### Registration always lands on shard 0 (`pubsub.rs:62-74`)

```rust
// handle_subscribe
let pubsub_tx = self.ensure_pubsub_channel();
let (response_tx, _response_rx) = oneshot::channel();
let _ = self.core.shard_senders[0]
    .send(ShardMessage::Subscribe {
        channels: vec![channel.clone()],
        conn_id: self.state.id,
        sender: pubsub_tx,
        response_tx,
    })
    .await;
```

PSUBSCRIBE (`pubsub.rs:162`), UNSUBSCRIBE (`pubsub.rs:113`), and PUNSUBSCRIBE (`pubsub.rs:208`) all
target `shard_senders[0]` identically. Shard 0 is the broadcast coordinator: **every** broadcast
subscriber lives in `shard 0`'s `ShardSubscriptions` table and nowhere else.

### Regular PUBLISH routes to shard 0 — and therefore works (`pubsub.rs:242-252`)

```rust
// handle_publish
// Broadcast pub/sub uses shard 0 as the coordinator so the count
// reflects actual unique subscribers rather than being multiplied by
// the number of shards.
let (response_tx, response_rx) = oneshot::channel();
let _ = self.core.shard_senders[0]
    .send(ShardMessage::Publish {
        channel: channel.clone(),
        message: message.clone(),
        response_tx,
    })
    .await;
```

The `Publish` message is handled on shard 0's worker, which publishes into shard 0's table
(`dispatch_pubsub.rs:42-59`: `self.subscriptions.publish(&channel, &message)`). Registration and
PUBLISH agree on shard 0. (In cluster mode `handle_publish` additionally fans out to other nodes via
`cluster_pubsub.rs:40-108` — out of scope here; see open questions.)

### Keyspace emit publishes into the *key-owner* shard's table (`keyspace_notify.rs:28-60`)

```rust
pub(crate) fn emit_keyspace_notification(
    &self,
    key: &[u8],
    event_name: &str,
    event_type: KeyspaceEventFlags,
) {
    let flags_bits = self.notify_keyspace_events.load(Ordering::Relaxed);
    if flags_bits == 0 {
        return; // Fast path: notifications disabled  (< 1ns: one atomic load + branch)
    }

    let flags = KeyspaceEventFlags::from_bits_truncate(flags_bits);
    if !flags.intersects(event_type) {
        return;
    }

    let event_bytes = Bytes::from(event_name.to_string());

    // __keyspace@0__:<key> -> event_name
    if flags.contains(KeyspaceEventFlags::KEYSPACE) {
        let channel = Bytes::from(format!("__keyspace@0__:{}", String::from_utf8_lossy(key)));
        self.subscriptions.publish(&channel, &event_bytes);   // <-- THIS shard's table
    }

    // __keyevent@0__:<event_name> -> key
    if flags.contains(KeyspaceEventFlags::KEYEVENT) {
        let channel = Bytes::from(format!("__keyevent@0__:{event_name}"));
        let key_bytes = Bytes::copy_from_slice(key);
        self.subscriptions.publish(&channel, &key_bytes);     // <-- THIS shard's table
    }
}
```

`self.subscriptions` is the *emitting* worker's own table (`worker.rs:73`,
`subscriptions: ShardSubscriptions`). On shard 3, `self.subscriptions` is shard 3's table; the
subscriber is in shard 0's table. The publish reaches nobody. (See Correctness flags.)

The emit sites — all on the key-owner shard worker, all synchronous:

```rust
// post_execution.rs:185-189  (write commands)
WriteEffectKind::KeyspaceNotifications => {
    for &(handler, args) in summary.writes {
        self.emit_keyspace_notifications_for_command(handler, args);  // -> emit_keyspace_notification
    }
}

// event_loop.rs:161  (active expiry, whole-key TTL)
self.emit_keyspace_notification(key, "expired", KeyspaceEventFlags::EXPIRED);
// event_loop.rs:181  (active expiry, hash emptied via field TTL -> generic del)
self.emit_keyspace_notification(key, "del", KeyspaceEventFlags::GENERIC);

// eviction.rs:232 (demotion) and eviction.rs:295 (deletion)
self.emit_keyspace_notification(key, "evicted", KeyspaceEventFlags::EVICTED);
```

### Why this is single-threaded and synchronous

The emitting worker runs the shard event loop on one thread; `emit_keyspace_notification` is called
inline from command post-execution, the expiry sweep, and eviction. There is no `await` in the emit
path and there must not be one: blocking the key-owner worker to wait on another shard would stall
that shard's entire command stream and risks a cross-shard stall (shard N waiting on shard 0 while
shard 0 waits on shard N). Any cross-shard hand-off has to be non-blocking. The mailbox already
supports this: `ShardSender::try_send` (`message.rs:59-73`) is the non-blocking enqueue.

## Proposed design

Introduce `KeyspaceNotificationCoordinator`: one small per-shard value that owns the single decision
the emit sites must *not* make themselves — **which subscription table a keyspace event lands in**.
Emit sites stop naming `self.subscriptions` and instead state only "publish this keyspace event";
the coordinator routes it to the shard where subscribers actually live.

### The seam

```rust
/// Owns the one rule that today is split, unwritten, across `handle_subscribe`
/// (subscribers register on the broadcast coordinator shard, shard 0) and
/// `emit_keyspace_notification` (events fire on the key-owner shard): a
/// keyspace event must be published into the *coordinator shard's* table, not
/// the emitting shard's. Centralizing it here makes registration and emission
/// physically unable to disagree.
pub struct KeyspaceNotificationCoordinator {
    topology: Topology,
}

enum Topology {
    /// Single-shard / standalone, OR this worker *is* the coordinator shard
    /// (shard 0). The emitting table and the subscriber table are the same, so
    /// publish straight into the local table — the synchronous fast path,
    /// byte-for-byte what the code does today.
    Local,
    /// A non-coordinator shard in multi-shard mode. Forward the event to shard
    /// 0's mailbox; shard 0's worker publishes it into the table where the
    /// subscribers live.
    Sharded { coordinator_shard: ShardSender },
}

impl KeyspaceNotificationCoordinator {
    /// Route one already-formatted keyspace/keyevent message to the subscriber
    /// table. Called ONLY after the enabled + mask checks have passed, so the
    /// < 1ns disabled fast path never reaches this function.
    fn publish(&self, local: &ShardSubscriptions, channel: Bytes, payload: Bytes) {
        match &self.topology {
            // Fast path unchanged: synchronous publish into the local table.
            Topology::Local => {
                local.publish(&channel, &payload);
            }
            // Cross-shard: non-blocking hand-off to the coordinator shard.
            Topology::Sharded { coordinator_shard } => {
                if coordinator_shard
                    .try_send(ShardMessage::PublishKeyspace { channel, payload })
                    .is_err()
                {
                    // Mailbox full or closed. Keyspace notifications are
                    // best-effort / at-most-once in Redis, so drop + count
                    // rather than block the key-owner worker.
                    // metrics: frogdb_keyspace_notifications_dropped_total
                }
            }
        }
    }
}
```

A new fire-and-forget mailbox message carries the cross-shard event:

```rust
// shard/message.rs — new variant
/// Forwarded keyspace notification: publish into THIS (coordinator) shard's
/// subscription table. Fire-and-forget — no response_tx, because the emitting
/// shard does not (and must not) wait.
PublishKeyspace { channel: Bytes, payload: Bytes },
```

```rust
// shard/dispatch_pubsub.rs — new arm, mirrors the existing Publish arm
ShardMessage::PublishKeyspace { channel, payload } => {
    // On shard 0: the table where every broadcast subscriber is registered.
    self.subscriptions.publish(&channel, &payload);
}
```

A new variant (rather than reusing `ShardMessage::Publish`) is deliberate: `Publish` carries a
`response_tx: oneshot::Sender<usize>` (`message.rs:178-183`) that the emit path would have to
fabricate and discard. `PublishKeyspace` is response-free, which *is* the at-most-once contract
expressed in the type.

### Before / after: the emit site

Before (`keyspace_notify.rs:49-59`) — the emit site names the wrong table, and there is no place
that says it is the wrong table:

```rust
if flags.contains(KeyspaceEventFlags::KEYSPACE) {
    let channel = Bytes::from(format!("__keyspace@0__:{}", String::from_utf8_lossy(key)));
    self.subscriptions.publish(&channel, &event_bytes);
}
if flags.contains(KeyspaceEventFlags::KEYEVENT) {
    let channel = Bytes::from(format!("__keyevent@0__:{event_name}"));
    let key_bytes = Bytes::copy_from_slice(key);
    self.subscriptions.publish(&channel, &key_bytes);
}
```

After — the emit site states the event; *where it lands* is owned behind the seam:

```rust
if flags.contains(KeyspaceEventFlags::KEYSPACE) {
    let channel = Bytes::from(format!("__keyspace@0__:{}", String::from_utf8_lossy(key)));
    self.keyspace_notify.publish(&self.subscriptions, channel, event_bytes.clone());
}
if flags.contains(KeyspaceEventFlags::KEYEVENT) {
    let channel = Bytes::from(format!("__keyevent@0__:{event_name}"));
    self.keyspace_notify.publish(&self.subscriptions, channel, Bytes::copy_from_slice(key));
}
```

The `&self.subscriptions` argument is the *local* table, used only on the `Local` fast path; it is
threaded in (rather than owned by the coordinator) because `ShardSubscriptions` is a plain
owned-by-value field on the worker (`worker.rs:73`, non-`Arc`, mutated under `&mut self` by
SUBSCRIBE) and cannot be co-owned by a sibling field without interior mutability. The routing
*decision* lives entirely in the coordinator; the local table is just the destination it picks on
the fast path. (See open questions for the alternative of sharing the table.)

The disabled fast path is untouched: `emit_keyspace_notification` still does the
`notify_keyspace_events.load(Ordering::Relaxed)` + branch at the top (`keyspace_notify.rs:34`),
and `emit_keyspace_notifications_for_command` still short-circuits at `keyspace_notify.rs:71-74`,
both *before* the coordinator is ever consulted. The coordinator is reached only once an event has
actually fired.

### Why this is the right depth

- **Locality.** The routing rule — "subscriptions on shard 0, events to shard 0" — collapses from an
  unwritten property of two files into one method on one type. Today, to know whether keyspace
  delivery is correct you must read `handle_subscribe` *and* `emit_keyspace_notification` *and*
  notice they target different tables. After, you read `KeyspaceNotificationCoordinator::publish`.
  If the coordinator shard ever moves off shard 0, that is a one-constant edit, not a two-file hunt
  that silently breaks delivery if you miss one site.
- **Leverage.** A ~40-line type fixes delivery for *every* emit site at once — post-execution,
  active expiry, both eviction paths — because they all funnel through the one `publish`. The shallow
  alternative (teach each of the four emit sites to compute "am I shard 0, else forward") would
  re-smear the rule across four call sites, the same disease in a new place.
- **Deletion test.** The migration deletes the single-shard pin on `test_lrem_emits_keyspace_notification`
  (`integration_pubsub.rs:54-55`) and the INDEX "known gap" entry (`INDEX.md:219-221`), and makes the
  cross-shard mismatch unrepresentable. If the new type could not delete those, it would be in the
  wrong place.
- **Not a new adapter layer.** This reuses the mechanism PUBLISH already uses (a `ShardMessage`
  routed to shard 0, published via `self.subscriptions.publish` in `dispatch_pubsub.rs`). It does not
  invent a parallel delivery path; it routes keyspace emission onto the *same* path PUBLISH proved
  works, and forbids the emit sites from reaching past it to the local table.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Behavior-preserving **except** Phase 2,
which is the delivery fix — and is gated by the new tests, not by changing single-shard behavior.

1. **Phase 0 — add the message + dispatch, no routing change.** Add
   `ShardMessage::PublishKeyspace { channel, payload }` (`message.rs`), its `name()` arm
   (`message.rs:616` region), the pub/sub dispatch grouping (`event_loop.rs:229` region), and the
   `dispatch_pubsub.rs` arm that calls `self.subscriptions.publish`. Nothing sends it yet.
   `just check frogdb-server`.
2. **Phase 1 — introduce `KeyspaceNotificationCoordinator`, wire `Local` only.** Add the type with
   `Topology::Local`, construct it on every worker, and repoint the two `self.subscriptions.publish`
   calls in `emit_keyspace_notification` (`keyspace_notify.rs:51,58`) to
   `self.keyspace_notify.publish(&self.subscriptions, ...)`. With `Topology::Local` everywhere this
   is byte-for-byte the current behavior; unit-test the routing decision in isolation (below). Pure
   refactor; behavior identical.
3. **Phase 2 — select topology + fix delivery (the bug).** At worker construction, choose
   `Topology::Local` when `num_shards() == 1` *or* `shard_id() == 0` (the coordinator shard, whose
   local table already *is* the subscriber table), and `Topology::Sharded { coordinator_shard:
   shard_senders[0].clone() }` otherwise. After this phase a key on shard N>0 expiring/evicting/being
   written delivers its keyspace event to a subscriber on shard 0. The multi-shard delivery tests
   (below) pass; the single-shard test passes unchanged.
4. **Phase 3 — drop the single-shard workaround + close the gap.** Remove `num_shards: Some(1)` and
   its comment from `test_lrem_emits_keyspace_notification` (`integration_pubsub.rs:50-58`) so the
   default-topology test now exercises cross-shard delivery, and strike the
   "Cross-shard keyspace notifications lost" entry from `INDEX.md:219-221`.
5. **Gate.** Add a grep gate to `just lint`: outside `keyspace_notify.rs` and the coordinator, no
   `self.subscriptions.publish` in an emit context — keyspace events must go through the coordinator.

## Testing impact

- **Routing decision, no live cluster.** `KeyspaceNotificationCoordinator::publish` is the unit under
  test. With a stub `ShardSubscriptions` for `Local` and a stub `ShardSender` (a bounded channel) for
  `Sharded`, assert: `Local` publishes into the local table and never enqueues; `Sharded` enqueues
  exactly one `PublishKeyspace { channel, payload }` with the expected channel/payload and does *not*
  touch the local table. This is the test that becomes possible *because* the rule now has a home —
  today there is nothing to call.
- **Multi-shard delivery (fails today).** Start a server with `num_shards >= 2`, pick a key whose
  `shard_for_key` is not 0, `SUBSCRIBE __keyevent@0__:set` (or `__keyspace@0__:<key>`), `SET` the
  key, and assert the subscriber receives the event. Today the message is published into the
  key-owner shard's empty table and the read times out; after Phase 2 it arrives. Run the same for an
  expired key (active expiry) and an evicted key (maxmemory) so all three emit classes are pinned.
- **Single-shard unchanged.** `test_lrem_emits_keyspace_notification` keeps passing through Phase 2
  (it hits `Topology::Local`), and in Phase 3 it passes again with the pin removed.
- **Coordinator-shard keys still local.** A key that hashes to shard 0 in a multi-shard server must
  still deliver via the `Local` fast path (no self-send hop). Add a test that fixes a key onto shard
  0 and asserts delivery, guarding the `shard_id() == 0 => Local` branch.
- **Mailbox-full is lossy, not blocking.** Saturate shard 0's mailbox, emit a keyspace event on
  another shard, and assert the emitting worker does not stall (it keeps serving commands) and the
  `frogdb_keyspace_notifications_dropped_total` counter increments. This pins the at-most-once choice.

## Risks / open questions

- **`ShardSubscriptions` ownership.** The table is a plain owned field on the worker (`worker.rs:73`),
  read by `publish` (`&self`) but mutated by SUBSCRIBE (`&mut self`), so the coordinator cannot
  co-own it without interior mutability. The chosen design threads `&self.subscriptions` into the
  `Local` branch, keeping zero new sharing. The alternative — wrap the publish-path maps in an `Arc`
  so the coordinator owns the local handle and the seam becomes the literal
  `self.keyspace_notify.publish(channel, payload)` — is cleaner at the call site but adds an `Arc`
  hop and a refactor of `ShardSubscriptions`; deferred unless a second consumer wants the handle.
- **Ordering and at-most-once.** Cross-shard events are forwarded asynchronously, so an event from
  shard 3 and an event from shard 5 have no defined relative order at shard 0, and a forwarded event
  may arrive after a later same-key PUBLISH. Redis keyspace notifications are explicitly best-effort,
  fire-and-forget, with no ordering or delivery guarantee, so this matches Redis semantics. Worth
  stating in docs that multi-shard keyspace ordering is per-shard, not global.
- **Mailbox backpressure.** If shard 0's mailbox is full, `try_send` fails and we drop the event
  (+metric) rather than block — blocking the key-owner worker would stall its command stream and
  could deadlock against shard 0. This means a saturated coordinator shard *loses* keyspace events;
  acceptable under the at-most-once contract, but the dropped-counter must be observable so operators
  can see it. Awaiting (`send`) is explicitly rejected for the deadlock risk.
- **Self-send when the key is on shard 0.** Selecting `Local` for `shard_id() == 0` avoids a
  pointless enqueue-onto-own-mailbox for keys that already live on the coordinator shard (and keeps
  the common single-shard path synchronous). The cost is that "the routing rule" has two true
  predicates (`num_shards()==1 || shard_id()==0`); both collapse to "the emitting table is the
  subscriber table," which is the real invariant and is documented on `Topology::Local`.
- **Cluster cross-node delivery is a deeper, separate gap.** Regular PUBLISH fans out to other nodes
  via `cluster_pubsub.rs:40-108`; keyspace emit never reaches that forwarder, so in *cluster* mode a
  subscriber on a different node also misses events. This proposal fixes within-node multi-shard
  routing only. The coordinator is the natural home for the cross-node policy too (route to shard 0,
  then have shard 0 fan out via the forwarder), but that is out of scope here and should be its own
  follow-up.
- **Sharded keyspace channels.** This proposal covers broadcast keyspace channels
  (`__keyspace@0__` / `__keyevent@0__`), which register on shard 0. It does not change SSUBSCRIBE
  sharded channels (`pubsub.rs:272-343`), which already route to the owning shard via
  `shard_for_key`; keyspace events are not emitted on sharded channels today.

## Correctness flags

1. **Cross-shard keyspace events are never delivered — CONFIRMED. 🔴** SUBSCRIBE registers every
   broadcast subscriber on shard 0 (`pubsub.rs:62-74`: `shard_senders[0].send(Subscribe { .. })`),
   but `emit_keyspace_notification` publishes the event into the *emitting* (key-owner) shard's own
   table (`keyspace_notify.rs:51,58`: `self.subscriptions.publish(...)`). For any key not owned by
   shard 0 in a multi-shard server, the `__keyspace@0__:<key>` / `__keyevent@0__:<event>` message is
   published into a table with no subscribers, and the subscriber registered on shard 0 receives
   nothing — across all emit classes: write commands (`post_execution.rs:187`), active expiry
   (`event_loop.rs:161,181`), and eviction (`eviction.rs:232,295`). The contrast confirms the
   diagnosis: regular PUBLISH targets shard 0 (`pubsub.rs:246-252`) and *does* reach subscribers; the
   only difference is the destination shard, and keyspace-emit picks the wrong one. Note the `@0` in
   the channel is the DB index and is correct — the defect is the shard, which the channel name does
   not encode. This is documented as a known gap (`INDEX.md:219-221`) and the existing emit test is
   pinned to a single shard to avoid it (`integration_pubsub.rs:50-58`). Fix: route emission to the
   coordinator shard (shard 0) through `KeyspaceNotificationCoordinator`, the same destination
   PUBLISH already uses.

2. **The routing rule has no owner — CONFIRMED (root cause, not a second bug).** The invariant
   "subscriptions live on shard 0, so keyspace events must be sent to shard 0" is expressed in zero
   places: not a type, not a function, not a comment. It is an emergent mismatch between
   `handle_subscribe` and `emit_keyspace_notification`, each locally reasonable. That absence is why
   flag 1 was free to appear and persist: there is no single site whose correctness review would have
   caught it. Fix: give the rule a home (`KeyspaceNotificationCoordinator::publish`) so registration
   and emission can no longer silently disagree.
</content>
</invoke>
