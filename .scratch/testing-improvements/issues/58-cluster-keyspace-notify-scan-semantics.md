# Define and test cluster-mode keyspace notification emitter + SCAN per-node semantics

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 1/3 (score 1)
Area: Cluster / Pub/Sub

## Context

Zero cluster-mode tests exist for keyspace notifications (grep `keyspace|keyevent` in
`integration_cluster.rs` → 0 hits) or for `SCAN` in cluster mode (grep `"SCAN"` in
`integration_cluster.rs` → 0 hits). Two related but distinct semantics are undocumented and
unverified:

- **Which node emits a keyspace notification event** for a given key mutation in a multi-node
  cluster — only the slot-owning primary? all primaries? replicas too? Nothing pins this today.
- **SCAN's per-node cursor semantics** in cluster mode — does `SCAN` issued against one node only
  iterate that node's locally-owned slots, requiring a client to fan out `SCAN` to every primary and
  union the results for a full-keyspace scan? Is the union guaranteed free of duplicates and free of
  cross-node "bleed" (a key appearing via a non-owning node's scan)?

Verdict CONFIRMED L1/C1.

## What to build

1. Define and document the intended contract: keyspace notifications are emitted only by the
   slot-owning primary (not replicas, not non-owning primaries); `SCAN` on a given node iterates only
   that node's locally-owned keys, and a full-keyspace scan requires client-side fan-out across every
   primary with result union.
2. Integration test: subscribe to `__keyevent@0__:set` (or equivalent) on every node in a multi-node
   cluster; write a key; assert the event fires exactly once, and only via the owning primary's
   subscription channel.
3. Integration test: write a known key set spread across primaries by hash slot; `SCAN` each primary
   to completion; assert the union of results equals the full written keyspace with no duplicates and
   no cross-node bleed.

## Acceptance criteria

- [x] Contract documented (code comment and/or docs) for which node emits keyspace notifications in
      cluster mode.
- [x] Integration test: a keyspace event for a key mutation is observed exactly once, only via the
      owning primary's subscription channel, and not observed via other primaries' channels.
- [x] Contract documented for `SCAN` per-node semantics in cluster mode (local-slots-only; full
      coverage requires client-side fan-out across primaries).
- [x] Integration test: `SCAN`-to-completion on each primary in a multi-primary cluster; union of
      results equals the full written keyspace, with no duplicates and no cross-node bleed.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/F-cluster.md #11 (`keyspace-notifications-and-scan-in-cluster-undefined`)
- .scratch/testing-improvements/audit/verdicts-F.md #11 (CONFIRMED L1/C1)
- frogdb-server/crates/server/tests/integration_cluster.rs (grep `keyspace`/`keyevent`/`SCAN` → 0 hits)

## Resolution

Both semantics were **already correct and Redis-compatible**; they were simply undocumented and
untested. No production code changed — the work was to define the contract, verify it against the
current emission paths, document it, and pin it with integration tests.

### Contract, as verified in code

**(a) Keyspace notifications are node-local; only the slot-owning primary emits.**

A keyed write executes on the primary that owns the slot. That node's shard worker calls
`emit_keyspace_notification` (`frogdb-server/crates/core/src/shard/keyspace_notify.rs:28`), which
formats `__keyspace@0__:<key>` / `__keyevent@0__:<event>` and hands them to
`KeyspaceNotificationCoordinator::publish`
(`frogdb-server/crates/core/src/shard/keyspace_coordinator.rs:94`). The coordinator's only routing
decision is *intra-node*: `Local` publishes into the worker's own subscription table, `Sharded`
forwards to internal shard 0 (where broadcast subscribers register). It never touches
`ClusterPubSubForwarder`, so an event never crosses the cluster bus.

This is exactly Redis's behavior: `notifyKeyspaceEvent` calls `pubsubPublishMessage` directly, while
only the `PUBLISH` *command* calls `clusterPropagatePublish`. The contrast is live in FrogDB too —
`ClusterPubSubForwarder::broadcast_publish`
(`frogdb-server/crates/server/src/cluster_pubsub.rs:125`) does fan `PUBLISH` out to every node — so
the node-locality of notifications is a real routing difference, not an absence of cross-node
plumbing. Client consequence: a cluster-wide keyspace-notification consumer must hold one
subscription per primary.

Replica note (checked, not claimed beyond the code): a replica applies the primary's stream through
the ordinary execution path (`ReplicaCommandExecutor::apply_single` sends a plain `CoreMsg::Execute`,
`frogdb-server/crates/server/src/replication/executor.rs:56`), so a replica with notifications
enabled emits its own local copy as it applies each write. Delivery still never crosses nodes in
either direction. Cluster-replica emission is *not* covered by a test here because cluster nodes
keep the default `replication.role = "standalone"` and are not wired into the data-path replication
role (the same harness limitation pinned by issue 37) — a cluster replica receives no write stream
to emit from.

**(b) `SCAN` iterates one node's keyspace only.**

`handle_scan` (`frogdb-server/crates/server/src/connection/scatter.rs:43`) walks the node's own
internal shards and stops at `self.num_shards`; there is no cluster fan-out and no slot filter. None
is needed — a node only holds keys for slots it owns, so what it can return is already exactly its
own slots. The cursor encodes `(internal shard, position)`, so it is meaningless on another node. A
full-keyspace scan is therefore a client-side fan-out across every primary plus a union; that union
is duplicate-free and bleed-free because slot ownership is exclusive. `DBSIZE`/`KEYS` are per-node
for the same reason.

### Documentation

`website/src/content/docs/architecture/clustering.md` — new section **"Node-Local Surfaces:
Keyspace Notifications and SCAN"** (after *Node-to-Node Communication*), covering both halves of the
contract, the `PUBLISH`-crosses-the-bus contrast, the per-primary fan-out requirement for consumers,
re-fan-out after migration/failover, and the non-portability of a `SCAN` cursor across nodes.

The same contract is restated as a block comment above the new tests in `integration_cluster.rs`
with the source pointers, so the pin and its rationale sit together.

### Tests

Added to `frogdb-server/crates/server/tests/integration_cluster.rs` (plus three shared helpers:
`slot_ownership`, `owner_of_slot`, `scan_to_completion`, and `expect_pubsub_message`):

1. `test_cluster_keyspace_notification_fires_only_on_owning_primary` — 3-node cluster;
   `notify-keyspace-events KEA` enabled on **every** node (so a silent non-owner cannot be explained
   by its own flags being unset); one long-lived subscriber per node on both
   `__keyspace@0__:<key>` and `__keyevent@0__:set`; one `SET` sent directly to the slot's owning
   primary. Asserts the owner observes exactly the keyspace+keyevent pair and nothing more, and that
   no other primary observes anything. A trailing ordinary `PUBLISH` on the same channel is the
   **control**: it reaches all three subscribers, proving each subscription was live and that
   cross-node broadcast works — so the silence above it is genuinely node-local emission, not a
   mis-registered subscriber.
2. `test_cluster_scan_is_per_node_and_unions_to_full_keyspace` — 3-node cluster; 48 keys, one per
   slot spread across the whole ring, each written *directly* to its owning primary (never via a
   redirect, so ownership is pinned rather than tolerated); `SCAN`-to-completion on every node.
   Asserts: every key a node yields hashes into a slot that node owns (no bleed); no duplicates
   within a node; no key returned by two nodes; every slot-owning primary contributed a non-empty
   result; each node returned strictly fewer than all 48 keys (per-node, never cluster-wide); and
   the union equals the full written keyspace.

### Verification

`just test frogdb-server "test_cluster_keyspace_notification_fires_only_on_owning_primary|test_cluster_scan_is_per_node_and_unions_to_full_keyspace"` — 4 runs, 2/2 passed each time
(~3.6-4.9s per test). `just fmt-check` clean; `cargo clippy -p frogdb-server --all-targets -- -D
warnings` clean.

### Follow-ups (not in scope here)

- Keyspace-notification emission across a **slot migration**: which node emits for a key written
  during MIGRATING/IMPORTING, and whether a subscriber must re-fan-out at `CompleteSlotMigration`.
  Related to gap #9 (`sharded-pubsub-subscriber-not-dropped-on-slot-move`).
- Cluster-replica emission, blocked on the cluster-replica data-path wiring noted in issue 37.
