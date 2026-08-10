# Proposal 82 — `ChannelTable`: give the pub/sub shard table a reverse index and an incremental counter

Round 38 · lane: protocol / net / core · candidate **PN4** · effort **M** (core table) + **S**
(message/dispatch fold), independently landable · **no locked crate**, **no FM tag on any touched
file**, **two seam lints in scope — both preserved by construction, see §Seam-lint clearance**

**Verified at HEAD `ee5efee9`** (worktree `arch-round-38-99`, branch `main`). Every line number
below was re-derived against this tree, not taken from the lane brief. Every `pubsub.rs` line
number the brief cited is **still correct** at HEAD. **Three brief claims are corrected**:

| Brief claim | Correction |
|---|---|
| "the keyspace-notification routing rule lives in `scripts/error-sanitize.py`" | It does not. Both pub/sub gates are **inline bash in the `Justfile`**: `lint-keyspace-notify-routing` at `Justfile:1051-1067` and `lint-pubsub-confirmation-seam` at `Justfile:1128-1156`. `scripts/error-sanitize.py` has no pub/sub rule. This matters: the gates are `grep -rEn` over *literal spellings*, so they are silently defeatable by a rename (§Seam-lint clearance). |
| "the O(n²) is the linear pattern scan per publish" | Real, but **not** the dominant term, and not quadratic in the way named. The measured dominant cost is `check_thresholds_after_subscribe` (`core/src/pubsub.rs:881-934`), which runs a **full walk of both channel maps plus a `HashSet` build over every pattern on every single SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE**. Measured: at the documented `MAX_UNIQUE_CHANNELS_PER_SHARD = 100_000` ceiling (`pubsub.rs:285`), removing this recount alone is a **946× total-time reduction**. For PSUBSCRIBE, the duplicate scan the brief blamed is **~3 % of the cost**; the recount is the other ~97 % (§Problem 1). |
| "subscription handling is O(n²)" (unqualified) | Precisely: **three** distinct super-linear sites, with different shapes and different fixes — the per-subscribe threshold recount (O(C+S+P) per op), the per-psubscribe duplicate scan (O(P) per op), and the slot-drain recount (O(K·M·S) per migration). All three fall out of the *same* missing structure: there is no reverse `ConnId -> channels` index and no maintained counter (§Proposed change). |

Four findings the brief did not name, all verified at HEAD:

1. **A LIVE correctness defect** on the slot-migration drain path: two different authorities compute
   one user-visible count, and the drain never repairs connection-side state (§Problem 4). Not a
   one-line fix → filed as an issue candidate, **not** claimed as a hotfix.
2. **Four dead methods** in `ShardSubscriptions` with zero production callers — they pass the
   deletion test outright (§Problem 3, hotfix **H1**).
3. **Six byte-identical method pairs** and **six identical message variants** whose only difference
   is a field *name* (§Problem 2).
4. **A stale premise in the pub/sub oracle's own doc header** (`testing/src/pubsub_oracle.rs:23-27`):
   it asserts there is no drop path on regular PUBLISH, which stopped being true when the output-
   budget drop at `core/src/pubsub.rs:164-190` landed (§Problem 6, hotfix **H2**).

The precedent for the fix is **already in the same crate**: `TrackingTable`
(`core/src/tracking.rs:80-89`) is the exact shape proposed here — forward map plus reverse index —
and `InvalidationRegistry`'s doc at `tracking.rs:40-41` literally describes itself as *"analogous
to `ShardSubscriptions` for pub/sub"*. The analogy runs one way only: tracking got the reverse
index, pub/sub did not.

## Summary

`ShardSubscriptions` (`core/src/pubsub.rs:518-532`) is a **shallow module**: three raw collections
behind twenty-odd methods that each re-derive, by scanning, facts the table could have maintained.
Because `BROADCAST_SHARD = 0` funnels *all* broadcast subscription state onto one shard's table
node-wide, the scan lengths are node-wide, not shard-local.

The proposal is to introduce a `ChannelTable` module inside `frogdb-core` holding a forward map
(`channel -> ConnId -> sender`), a reverse index (`ConnId -> HashSet<channel>`), and an
incrementally-maintained subscription count — then express `ShardSubscriptions` as three instances
of it (channels, sharded channels, patterns). This makes the three super-linear sites O(1) or
O(subscriptions-for-this-connection), deletes six duplicated method bodies, and collapses six
`PubSubMsg` registration variants and their six dispatch arms and six handlers into one each.

The **interface stays the same width** while the **implementation depth increases**: callers keep
calling `subscribe`/`publish`/`remove_connection`, and stop being able to observe *how* counts are
derived. That is the depth argument — today the counting strategy leaks out through the cost of
every call site.

## Files involved

Verified paths and line counts at `ee5efee9`.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/core/src/pubsub.rs` | 1523 (prod `:1-955`, tests `:956-1523`, 32 test fns) | **Primary.** `ShardSubscriptions` `:518-532` and all of its methods. |
| `frogdb-server/crates/core/src/shard/pubsub.rs` | 132 | **Primary.** Six near-identical handlers `:14-86`; sole caller of the drain `:93`. |
| `frogdb-server/crates/core/src/shard/dispatch_pubsub.rs` | 175 | **Primary.** Six near-identical dispatch arms `:10-47`, `:73-91`. **Sole exemption in the `lint-keyspace-notify-routing` gate** — see §Seam-lint clearance. |
| `frogdb-server/crates/core/src/shard/message.rs` | `PubSubMsg` at `:275-372` | **Primary.** Six registration variants with identical field *types*. |
| `frogdb-server/crates/core/src/tracking.rs` | 682 | **Read-only.** Precedent: `TrackingTable` `:80-89`, `remove_connection` `:191-204`. |
| `frogdb-server/crates/core/src/shard/keyspace_coordinator.rs` | 287 | **Read-only.** `publish` `:94-97` calls `local.publish(` — gate-exempt by spelling. |
| `frogdb-server/crates/server/src/connection/pubsub_conn_command.rs` | 1128 | **READ-ONLY. Owned by proposal 68 (primary) and touched by future 90.** Cited only as precedent + as the file the confirmation gate greps. §Risks. |
| `frogdb-server/crates/server/src/connection/state.rs` | — | **Read-only.** `PubSubState` `:30-43`; `remove_subscription` `:624-641` returns the connection-global sharded count. Evidence for §Problem 4. |
| `frogdb-server/crates/server/src/connection.rs` | — | **Read-only.** Delivery arm `:695-753` renders confirmations without touching `self.state`. Evidence for §Problem 4. |
| `frogdb-server/crates/cluster-runtime/src/pubsub.rs` | 771 | **Read-only, LOCKED**, carries FM-CLUSTER-067/068/069/070 (`:469-751`). Explicitly **out of scope**. |
| `frogdb-server/crates/testing/src/pubsub_oracle.rs` | 1138 | **Read-only** except hotfix H2 (a doc comment). |
| `Justfile` | `:1051-1067`, `:1128-1156` | **Read-only.** The two gates that must stay live. |

Test surface that must stay green: `core/src/pubsub.rs` inline tests (32), `integration_pubsub.rs`
(6247 lines), `concurrency_pubsub.rs` (167), `testing/fuzz/fuzz_targets/pubsub_glob_pattern.rs` (23).

## Problem

### 1. The scale guard is the thing that does not scale (dominant cost, measured)

`check_thresholds_after_subscribe` (`core/src/pubsub.rs:881-934`) exists to warn when a shard
approaches its subscription ceilings. It is called on **every** subscribe, from three sites:
`core/src/shard/pubsub.rs:24` (SUBSCRIBE), `:49` (PSUBSCRIBE), `:74` (SSUBSCRIBE).

It early-returns at `:886` **only when all three `warned_*_90` flags are already true**. In the
normal case — flags false, i.e. the entire life of the shard up to the 90 % warning — it calls:

- `total_subscription_count()` `:859-863` — `channel_subs.values().map(len).sum()` +
  `pattern_subs.len()` + `sharded_subs.values().map(len).sum()` → **O(C + S)** over both maps;
- `unique_channel_count()` `:866-868` — O(1), fine;
- `unique_pattern_count()` `:871-877` — `pattern_subs.iter().map(...).collect::<HashSet<_>>().len()`
  → **O(P) plus a fresh heap allocation per call**.

So each SUBSCRIBE walks every channel bucket and every sharded bucket on the shard, and each
PSUBSCRIBE additionally builds a throwaway `HashSet` over every pattern on the shard. With
`BROADCAST_SHARD = 0`, "the shard" is "the node" for all broadcast traffic.

The cost does not disappear after the warning fires: `reset_thresholds_if_needed` `:937-953` is
guarded by `if self.warned_* &&`, so once a flag latches, the identical recounts **migrate to the
unsubscribe path**. The system never stops paying; it changes which command pays.

**Measured.** A standalone harness modelling the exact loop shapes and container types
(`HashMap<Vec<u8>, HashMap<ConnId,()>>`, `Vec<(Vec<u8>, ConnId)>`), compiled `rustc -O`:

```
SUBSCRIBE  n=   1000  total=      1.6ms  per-op=     1.6us
SUBSCRIBE  n=  10000  total=    140.1ms  per-op=    14.0us
SUBSCRIBE  n=  50000  total=   2908.8ms  per-op=    58.2us
SUBSCRIBE  n= 100000  total=  12577.7ms  per-op=   125.8us
SUBSCRIBE(no recount) n= 100000 total=     13.3ms  per-op=  0.133us
PSUBSCRIBE n=   1000  total=     10.0ms  per-op=    10.0us
PSUBSCRIBE n=   5000  total=    260.6ms  per-op=    52.1us
PSUBSCRIBE n=  10000  total=   1111.2ms  per-op=   111.1us
PSUBSCRIBE(dup scan only) n=  10000 total=     32.0ms
```

Readings:

- SUBSCRIBE 10×n → ~87–90× time. Clean quadratic in total work, i.e. **linear per-op growth**:
  1.6 µs → 125.8 µs per subscribe between n=1 000 and n=100 000.
- n=100 000 is not an extrapolation — it is exactly `MAX_UNIQUE_CHANNELS_PER_SHARD`
  (`pubsub.rs:285`), the ceiling the code documents for itself. At that documented ceiling the
  recount costs **12.58 s of CPU** across the fill, versus **13.3 ms** without it: **946×**.
- PSUBSCRIBE 1 000 → 10 000 is 111×. Of the 1111 ms, the duplicate scan the brief blamed accounts
  for **32 ms**. **~97 % of PSUBSCRIBE cost is the threshold recount**, not the scan.

The inversion is worth stating plainly: a guard whose purpose is to notice that the table has grown
large is implemented by re-measuring the whole table on every insert, and it becomes cheap on the
subscribe path only *after* it has already fired.

### 2. Six method pairs and six message variants that differ only in a field name

`ShardSubscriptions` `:518-532` holds `channel_subs`, `pattern_subs`, `sharded_subs` and three
`warned_*_90` flags. The broadcast and sharded halves are **byte-identical implementations** that
differ only in which field they name:

| Broadcast | Sharded twin |
|---|---|
| `subscribe` `:546-555` | `ssubscribe` `:636-645` |
| `unsubscribe` `:559-569` | `sunsubscribe` `:649-659` |
| `get_connection_channels` `:572-583` | `get_connection_sharded_channels` `:703-714` |
| `channels` `:807-813` | `shard_channels` `:831-837` |
| `numsub` `:816-828` | `shard_numsub` `:840-852` |

Plus a sixth duplication *inside a single method*: `remove_connection` `:785-800` contains the same
`retain` body twice, at `:787-790` and `:796-799`.

The duplication propagates outward. `PubSubMsg` (`core/src/shard/message.rs:275-372`) has eleven
variants, of which **six are registration variants with identical field types** — `Subscribe`
`:284`, `Unsubscribe` `:295`, `PSubscribe` `:305`, `PUnsubscribe` `:316`, `ShardedSubscribe` `:340`,
`ShardedUnsubscribe` `:351` — differing only in whether the field is called `channels` or
`patterns`. Those six variants get **six dispatch arms** (`shard/dispatch_pubsub.rs:10-19`,
`:20-28`, `:29-38`, `:39-47`, `:73-82`, `:83-91`), each of which is literally
`self.handle_x(...); let _ = response_tx.send(());`, and **six handlers** in `shard/pubsub.rs`
(`:14-28`, `:31-36`, `:39-53`, `:56-61`, `:64-78`, `:81-86`), each of which loops and then calls
either `check_thresholds_after_subscribe` (`:24`, `:49`, `:74`) or `reset_thresholds_if_needed`
(`:35`, `:60`, `:85`). `handle_introspection` `:106-131` continues the pattern: five arms, three of
them pairwise duplicates.

That is **one axis of variation (which kind of subscription) expressed six times in four files**.
The kind is not a behavioural difference — it is a table selector.

The contrast is instructive: the *connection* side already solved this. `pubsub_conn_command.rs`
carries `SubKindSpec` `:77-99` with three statics (`CHANNEL_SPEC` `:102-121`, `PATTERN_SPEC`
`:124-143`, `SHARDED_SPEC` `:146-165`) and puts the control flow **once** in `subscribe_kind`
`:311-404` / `unsubscribe_kind` `:414-454`. The shard side never received the same treatment.

### 3. Four methods with zero production callers (deletion test)

Applying the deletion test honestly — remove it and see whether anything has to be reinvented:

| Method | Production callers | Test-only callers |
|---|---|---|
| `get_connection_channels` `:572-583` | **0** | `pubsub.rs:1215` |
| `get_connection_patterns` `:614-623` | **0** | `pubsub.rs:1220` |
| `get_connection_sharded_channels` `:703-714` | **0** | `pubsub.rs:1323-1330` |
| `pattern_count` `:626-628` | **0** | tests only |

Nothing has to be reinvented. The real "unsubscribe from everything" path does not consult the
shard table at all: it reads the connection's own `HashSet` via
`self.state.subscriptions(spec.kind)` (`pubsub_conn_command.rs:417`). These four methods exist to
be tested. They are the shallow-module tell — an interface that grew to expose internals for
verification, because the internals were not verifiable through the real interface.

The reverse index this proposal adds makes three of them O(1)-correct *if* anyone ever wants them
back — so deleting them now is safe in both directions.

### 4. LIVE: the slot-migration drain has two authorities for one count, and repairs neither side

`drain_sharded_channels_for_slot` (`core/src/pubsub.rs:668-700`) is called on slot migration
(`shard/pubsub.rs:93`, its only caller). For each removed channel, for each subscriber, it computes
the count sent in the synthetic `SUnsubscribe` confirmation (`:682-687`):

```rust
let remaining = self
    .sharded_subs
    .values()                                   // :685 - full scan of the map
    .filter(|subs| subs.contains_key(&conn_id)) // :686
    .count();                                   // :687
```

Two independent problems, both verified:

**(a) Wrong authority.** This `remaining` is a **shard-local** count. The normal SUNSUBSCRIBE path
returns the **connection-global** count — `state.rs:635-636` returns
`self.pubsub.sharded_subscriptions.len()`, across all shards, which is Redis's semantics. So the
same client receives counts on two different scales depending on whether the unsubscribe was
client-initiated or migration-initiated.

**(b) Connection state is never repaired.** The confirmation is rendered straight to the wire: the
delivery arm at `connection.rs:695-753` calls `pubsub_msg.to_response_with_protocol(...)` →
`feed_response` and **never touches `self.state`**. So after a drain, `ConnectionState`'s
`sharded_subscriptions` set still contains the migrated-away channels — permanently. Consequences:
every *subsequent* SUNSUBSCRIBE confirmation on that connection reports an inflated count, and the
`MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION = 10_000` budget (`pubsub.rs:278`) never regains headroom.

**Cost, measured.** The same harness, draining 5 % of a shard's sharded channels:

```
DRAIN 5% of S=   1000  notifications=    50  total=      1.2ms
DRAIN 5% of S=  10000  notifications=   500  total=    104.1ms
DRAIN 5% of S=  50000  notifications=  2500  total=   1921.6ms
```

S×5 → 18.5× time, ≈ S^1.8 — the O(K·M·S) shape. This runs **inline on the shard event loop during
slot migration**, i.e. during exactly the window where the cluster is already under stress.

**Test coverage: zero.** `drain_sharded_channels_for_slot` has no test anywhere in the tree, and no
`FM-` row forces it (§Spec / LOCKED clearance). The count divergence is invisible today because
nothing looks.

This is **not** a one-line fix — repairing (b) requires a message back to the connection task, which
is a design decision, not an edit. Recorded here as an issue candidate. (a) alone is fixable once
(b) has an owner; fixing (a) without (b) would make the numbers consistent and still wrong.

### 5. Per-publish pattern scan

`publish` `:722-756` iterates every pattern registered on the shard for every published message
(`:741`, `for (pattern, compiled, _, sender) in &self.pattern_subs`). This is genuinely O(P) per
PUBLISH and it is the site the brief named. It is real, and it is the **least** of the three costs
under the measured workloads, because P is bounded by `MAX_UNIQUE_PATTERNS_PER_SHARD = 10_000`
(`pubsub.rs:287`) whereas C is bounded at 100 000.

It is also the one site this proposal does **not** claim to make sub-linear. Glob matching against
arbitrary patterns has no index that is both correct and general; Redis, Valkey and DragonflyDB all
scan the pattern list per publish for the same reason. What the proposal *does* fix here is the
**duplicate-detection scan** on the registration side (`psubscribe` `:591-601`, `:593` — a linear
walk of `pattern_subs` looking for `(conn_id, pattern)`), which becomes a hash lookup through the
reverse index. Claiming more than that would be dishonest.

### 6. The oracle's stated premise is stale (doc-only)

`testing/src/pubsub_oracle.rs:23-27` asserts, as the justification for its exactly-once bracket:

> "…delivers **synchronously** … into each live subscriber's **unbounded** mpsc queue. There is no
> `try_send`/drop on the regular-PUBLISH path … (The only dropping path in the codebase is the
> *cross-shard keyspace-notification* hop, which regular PUBLISH never takes.)"

At HEAD that is false. `PubSubSender::send` (`core/src/pubsub.rs:164-190`) **drops** the message
when the shared byte budget is exhausted, latches an overflow flag, and still returns `Ok(())` so
the subscriber is counted. The budget is live in production: connections are built with
`PubSubSender::channel(self.pubsub_output_buffer_hard_limit)` (`pubsub_conn_command.rs:271`),
default `32 MiB` (`core/src/pubsub.rs:41`, mirrored `config/src/server.rs:94`).

The oracle's *assertions* are not wrong in practice — 32 MiB is far above anything the workload
generator produces, and the drop path also tears the connection down — but its written justification
no longer matches the code, which is precisely the kind of stale premise that makes a future reader
trust an oracle further than the code supports. Doc-only; hotfix **H2**.

### 7. Why the missing structure is the single root cause

Every symptom above is the same absence: **the table can only answer questions by scanning, because
it stores one direction and no aggregate.**

- "How many subscriptions on this shard?" → scan both maps (Problem 1).
- "Does this connection already have this pattern?" → scan the pattern vec (Problem 1/5).
- "How many sharded channels does this connection still hold?" → scan the map (Problem 4).
- "Which channels does this connection hold?" → scan the map (Problem 3).
- Cleanup on disconnect → `retain` over everything, twice (Problem 2, `:785-800`).

`TrackingTable` (`tracking.rs:80-89`), in the same crate, stores both directions and comments its
`remove_connection` (`:191-204`) with *"Use the reverse index for O(1) cleanup"*. `ShardSubscriptions`
is the odd one out in its own crate, and `tracking.rs:40-41` names the analogy without having
propagated the structure.

## Proposed change

### The module

Introduce **`ChannelTable`** in `frogdb-core` (its own module, `core/src/pubsub/channel_table.rs`,
or a clearly delimited section of `pubsub.rs` — either is fine; the module boundary matters, the
file boundary does not). It owns, for one *kind* of subscription:

- forward: `HashMap<Bytes, HashMap<ConnId, PubSubSender>>` — channel → subscribers;
- reverse: `HashMap<ConnId, HashSet<Bytes>>` — connection → its channels on this shard;
- `subscription_count: usize` — maintained incrementally on insert/remove, never recomputed.

Interface (narrow — this is the whole of it): `subscribe`, `unsubscribe`, `channels_for(conn)`,
`remove_connection(conn) -> usize`, `deliver(channel, payload) -> usize`, `len_channels()`,
`len_subscriptions()`, `numsub(channels)`, `matching_channels(pattern)`.

`ShardSubscriptions` becomes three fields: `channels: ChannelTable`, `sharded: ChannelTable`,
`patterns: PatternTable`. `PatternTable` is `ChannelTable` plus the compiled `GlobPattern` and an
ordered iteration for `publish` — it keeps the linear match scan (§Problem 5) but gains the reverse
index, which is what kills the duplicate-detection scan.

The threshold check becomes three field reads and three comparisons. No walk, no allocation.

### The message fold

Collapse the six `PubSubMsg` registration variants (`shard/message.rs:284, :295, :305, :316, :340,
:351`) into two — `Register { kind: SubscriptionKind, names: Vec<Bytes>, conn_id, sender,
response_tx }` and `Unregister { kind, names, conn_id, response_tx }` — where `SubscriptionKind` is
the same three-way distinction the connection side already models as `SubKindSpec`
(`pubsub_conn_command.rs:77-99`). Six dispatch arms collapse to two; six handlers collapse to two.

This is deliberately kept as a **separately landable** step (see §Effort): the table change is
valuable on its own, and the message fold touches a wider blast radius.

### Depth and locality

**Depth.** Today `ShardSubscriptions` has ~20 public methods over 3 raw fields, and several of them
exist only so tests can see inside (§Problem 3). After the change the same *behaviour* is reachable
through a narrower interface whose implementation callers cannot feel: they cannot tell whether a
count was maintained or scanned, which is exactly the property that is missing today (they can — it
costs 125 µs).

**Locality.** The rule "a subscription belongs to exactly one connection and one channel, and the
count of them is N" is currently enforced in five places that each re-derive N differently
(`:859-863`, `:684-687`, `state.rs:635-636`, `:866-868`, `:871-877`). After the change it is
enforced where the mutation happens. The count-authority divergence in §Problem 4 is *possible*
only because the rule has no home.

**Leverage.** One structural addition (a reverse index + a counter) removes three super-linear
sites, six duplicated bodies, four dead methods, and creates the seam where §Problem 4's defect can
be fixed at all. The precedent in `tracking.rs` means the shape is already validated in this crate
under this crate's concurrency model.

### Deletion test, applied honestly

- **`ChannelTable` itself** — does it pass? Delete it and you must reinvent the reverse index in
  three places, or go back to scanning. It carries state and invariants, not just calls. **Passes.**
- **`PatternTable` as a distinct type** — weaker. It is `ChannelTable` plus one field. If it turns
  out to need no distinct invariants, it should be a `ChannelTable<Meta>` with `Meta = GlobPattern`
  rather than a second type. **Flagged as an implementation-time decision, not asserted here.**
- **The `SubscriptionKind` fold** — delete it and you get the six variants back verbatim. But note
  it is a *deduplication*, not a new capability: it passes the test on maintenance grounds
  (one axis, one expression), not on depth grounds. Sized `S` accordingly and landable last.

## Testability improvement

Concrete, not aspirational:

1. **§Problem 4's defect becomes testable.** Today asserting "the count in a migration-drain
   confirmation matches the count in a subsequent client SUNSUBSCRIBE" requires reaching through
   two crates. With `remove_connection`/`unsubscribe` returning a maintained count from one
   authority, the assertion is a unit test in `core/src/pubsub.rs`.
2. **The four dead accessors stop being the test interface.** Tests at `pubsub.rs:1215`, `:1220`,
   `:1323-1330` currently assert against methods no production path uses — they verify the test
   surface, not the product. After the change they assert `channels_for(conn)` on the table that
   `remove_connection` actually consults.
3. **Threshold behaviour becomes assertable without filling the table.** Today verifying the 90 %
   warning means inserting ~90 000 subscriptions. With a maintained counter, the warning logic is a
   pure function of three integers and testable directly.
4. **`drain_sharded_channels_for_slot` gains its first test.** It has none at HEAD. The proposal's
   acceptance should include one, whether or not the count defect is fixed in the same change.
5. **Property/fuzz reach.** `pubsub_glob_pattern.rs` (23 lines) fuzzes matching only. A reverse
   index makes a table-level property viable: *for all op sequences, forward and reverse agree and
   `subscription_count` equals the forward sum* — a self-checking invariant the current shape
   cannot express.

Note honestly: the 6247-line `integration_pubsub.rs` and the 1138-line oracle already cover
delivery semantics well. This proposal does **not** claim a coverage gap in delivery. The gap is in
*bookkeeping*, and it is total.

## Spec / LOCKED clearance

- **`frogdb-core` is not a locked crate** and carries no mutation gate. ADRs 0002–0004 cover
  txn+vll, persistence+recovery, replication+replication-runtime, cluster+cluster-runtime.
- **FM tags on touched files: none.** `rg 'FM-'` returns zero hits in `core/src/pubsub.rs`,
  `core/src/shard/pubsub.rs`, `core/src/shard/dispatch_pubsub.rs`, `core/src/shard/message.rs`.
  (The nearest hit in the crate is an unrelated `FM-PERSISTENCE-022` at `core/src/shard/worker.rs:349`.)
  So `just lint-failure-modes` has nothing to say about this change, and no spec row needs amending.
- **`frogdb-cluster-runtime/src/pubsub.rs` (771 lines) IS locked** and carries FM-CLUSTER-067/068/
  069/070 at `:469-751`. Cross-node pub/sub forwarding is **explicitly out of scope**; this proposal
  touches the shard-local table only. Any change that altered the cross-node contract would be
  spec-first and belongs in a different proposal.
- **Replicated PUBLISH does not exist in FrogDB.** `pubsub_spec()` (`pubsub_conn_command.rs:739-760`)
  sets `wal: WalStrategy::NoOp` and `event: EventSpec::NotApplicable`; grepping `replication/` and
  `replication-runtime/` for PUBLISH yields only unrelated `publish_backlog` / feed-gate hits.
  Cross-node delivery is the cluster bus alone. **The replication path is therefore not in scope**,
  and the absence is itself a documented-worthy Redis deviation (Redis propagates PUBLISH to
  replicas) — recorded here, not fixed here.

## Seam-lint clearance

Both pub/sub gates are inline bash in the `Justfile` (the brief's `scripts/error-sanitize.py`
attribution is wrong — see the corrections table). `lint-gates` (`Justfile:329`) runs the
compile-free subset on every commit via lefthook and in CI's `seam-gates` job; `lint`
(`Justfile:319`), `pre-commit` (`:1381`) and `check-all` (`:1384`) all reach it.

### Gate 1 — `lint-keyspace-notify-routing` (`Justfile:1051-1067`)

Mechanism: `grep -rEn --include='*.rs' --exclude='dispatch_pubsub.rs'` for the literal pattern
`self\.subscriptions\.publish\(` under `crates/core/src/shard`. Intent: keyspace notifications must
route through the coordinator, so `dispatch_pubsub.rs` is the sole file allowed to publish directly.

**How this proposal preserves it — and the trap.** The gate matches a *spelling*, not a call graph.
Both live matches are in `dispatch_pubsub.rs` — `:53` (PUBLISH) and `:71` (forwarded
`PublishKeyspace`). `keyspace_coordinator.rs:94-97` is exempt because it is spelled
`local.publish(`, not `self.subscriptions.publish(`.

The refactor's obvious move — making `subscriptions` a struct of three tables and writing
`self.subscriptions.channels.publish(...)` — **would stop matching the regex**. The gate would go
green not because the invariant held but because it stopped being checkable: a **dead gate**, the
worst outcome. Therefore:

> **Constraint (binding on implementation):** `ShardSubscriptions` must keep a method named exactly
> `publish` invoked as `self.subscriptions.publish(` at `dispatch_pubsub.rs:53` and `:71`. The
> internal delegation to `ChannelTable`/`PatternTable` happens *inside* that method. No call site
> outside `dispatch_pubsub.rs` may acquire a table-level publish path.

Acceptance check: after the change, `grep -rEn 'self\.subscriptions\.publish\(' crates/core/src/shard`
must still return exactly the two `dispatch_pubsub.rs` lines — i.e. the gate must be verified
*positively* (it still finds them in the exempt file), not merely observed to pass.

### Gate 2 — `lint-pubsub-confirmation-seam` (`Justfile:1128-1156`)

Two rules. (a) The label pattern
`b"(subscribe|unsubscribe|psubscribe|punsubscribe|ssubscribe|sunsubscribe)"` is forbidden **in
`pubsub_conn_command.rs`** — confirmations must be built through the one owner. (b) The null-array
pattern `b"\*-1` is forbidden across `crates/server/src` except `codec.rs`.

**How this proposal preserves it.** The sole owner is `PubSubConfirmation` (`core/src/pubsub.rs:303-325`,
six variants) and its `items()` (`:329-358`), where the six byte literals legitimately live
(`:332, :335, :338, :341, :344, :347`) — in `frogdb-core`, outside the gate's file glob. This
proposal:

- **does not move `PubSubConfirmation` or `items()` out of `frogdb-core`**, and does not change the
  RESP3-Push-vs-RESP2-Array rule they own;
- **does not touch `pubsub_conn_command.rs` at all** (read-only here — it is proposal 68's primary,
  and future 90's), so it cannot introduce a forbidden literal there. That file has zero such
  literals today precisely because it builds confirmations through fn pointers into
  `PubSubConfirmation`;
- **touches no `b"*-1"` literal** anywhere.

The one place the refactor comes near this seam is the synthetic `SUnsubscribe` built inside
`drain_sharded_channels_for_slot` (`pubsub.rs:688-690`). It already goes through
`PubSubConfirmation::SUnsubscribe` and must continue to — the fix for §Problem 4 changes the
*value* of `count`, never the construction path.

Acceptance check: `just lint-gates` green **and** `just lint` green, with the positive check above
run by hand.

## Behaviour changes

Deliberate, and all of them are bug fixes rather than semantic drift:

1. **None on the delivery path.** PUBLISH/SPUBLISH ordering, synchronicity, subscriber counting and
   the confirmation wire shape are untouched. The 6247-line integration suite and the oracle are the
   regression net and must pass unchanged.
2. **`drain_sharded_channels_for_slot`'s `count` field** would change from shard-local to
   connection-global *if* §Problem 4 is fixed in the same change. That is a user-visible change to a
   currently-wrong value. If the fix is deferred, the drain's count stays as-is and the issue stands.
3. **Four public methods disappear** (H1). All four are `pub` on a `frogdb-core` type, but the crate
   is internal to the workspace and the grep shows zero non-test callers.

## Risks and scope boundaries

### vs. proposal 68 (PubSubKind)

68 owns `server/src/connection/pubsub_conn_command.rs` as **primary** and cites
`core/src/pubsub.rs` as **read-only**. This proposal is the mirror image: `core/src/pubsub.rs`
primary, `pubsub_conn_command.rs` **read-only**. As long as both hold their stated stance the two
are **disjoint** — no shared edited file.

One soft edge: if 68 lands a shared `PubSubKind` in a place this proposal's `SubscriptionKind` could
reuse, the message fold should reuse it rather than introduce a second three-way enum. **Sequencing
preference: 68 first**, then this proposal's fold adopts its enum. If this proposal lands first, the
fold should define `SubscriptionKind` in `frogdb-core` and 68 should consume it. Either order works;
what must not happen is two enums.

### vs. proposal 90 (future, `CommandSpec::DEFAULT` mechanical sweep)

90 rewrites every `CommandSpec` static in the workspace, including the nine at
`pubsub_conn_command.rs:767-804` and `pubsub_spec()` at `:739-760`. **This proposal edits no
`frogdb-commands` file and no `CommandSpec` static**, so there is **no conflict edge** — by
construction of the read-only stance on `pubsub_conn_command.rs`, not by luck.

### vs. the locked cluster crate

`cluster-runtime/src/pubsub.rs` is locked with four FM rows. Out of scope, stated above. The risk is
scope creep during implementation: a reviewer should reject any diff in this proposal that touches
that file.

### Concurrency risk

`ShardSubscriptions` lives on the shard event loop and is `&mut`-accessed from a single task; the
reverse index inherits that discipline and adds no synchronisation. The concrete risk is
**index divergence** — forward and reverse disagreeing after a partial failure. Mitigation is the
property test in §Testability item 5, and keeping every mutation behind the two methods that touch
both maps.

### Sizing risk

`core/src/pubsub.rs` is 1523 lines with 32 inline tests, and the churn touches most of the struct's
methods. This is a `M`, not an `S`, and it should not be attempted in the same commit as the message
fold.

## Effort

| Step | Scope | Size | Depends on |
|---|---|---|---|
| A. `ChannelTable` + reverse index + maintained counter; `ShardSubscriptions` expressed over it | `core/src/pubsub.rs` | **M** | — |
| B. Threshold check reads maintained counters (deletes the per-op recount) | `core/src/pubsub.rs`, `core/src/shard/pubsub.rs` | **S** | A |
| C. Fix §Problem 4 (count authority + connection-state repair) | `core/src/pubsub.rs` + a new message to the connection task | **M** | A; design decision required |
| D. `SubscriptionKind` fold: 6 `PubSubMsg` variants → 2, 6 dispatch arms → 2, 6 handlers → 2 | `shard/message.rs`, `shard/dispatch_pubsub.rs`, `shard/pubsub.rs` | **S–M** | A; prefer after 68 |

**Overall: M**, with A+B as the valuable core (they are what deliver the measured 946×) and C/D
independently schedulable. A+B alone is a defensible single PR.

## Independently-landable hotfixes

### H1 — delete four dead methods · **LATENT** · **claimed**

`get_connection_channels` (`core/src/pubsub.rs:572-583`), `get_connection_patterns` (`:614-623`),
`get_connection_sharded_channels` (`:703-714`), `pattern_count` (`:626-628`) have zero production
callers. Delete the four fns and the three tests that exist only to call them (`:1215`, `:1220`,
`:1323-1330`). Not one line, but purely subtractive and independent of everything above.

### H2 — correct the oracle's stale premise · **LIVE (doc)** · **claimed**

`testing/src/pubsub_oracle.rs:23-27`. Sketch:

```diff
-//!   subscriber's **unbounded** mpsc queue. There is no `try_send`/drop on the
-//!   regular-PUBLISH path: a message is delivered to a live subscriber's queue
-//!   or the subscriber's connection is already gone. (The only dropping path in
-//!   the codebase is the *cross-shard keyspace-notification* hop, which regular
-//!   PUBLISH never takes.)
+//!   subscriber's mpsc queue. The queue is unbounded in length but bounded in
+//!   bytes: `PubSubSender::send` (core/src/pubsub.rs:164-190) drops the message
+//!   and latches an overflow flag once the connection's shared budget
+//!   (default 32 MiB, `DEFAULT_PUBSUB_OUTPUT_BUFFER_HARD_LIMIT`) is exhausted,
+//!   after which the connection is torn down. Oracle workloads stay far below
+//!   that budget, so within these runs delivery is drop-free; the other dropping
+//!   path is the *cross-shard keyspace-notification* hop, which regular PUBLISH
+//!   never takes.
```

### H3 — deduplicate the twin `retain` in `remove_connection` · **LATENT** · **not claimed**

`core/src/pubsub.rs:785-800` runs the same `retain` body at `:787-790` and `:796-799`. A one-line
fix in spirit, but it is exactly the code step A rewrites — landing it separately guarantees a
conflict. **Deliberately left to step A.**

### H4 — §Problem 4's count divergence · **LIVE** · **NOT a hotfix, filed as an issue candidate**

Named here so it is not lost: `drain_sharded_channels_for_slot` sends a shard-local count where the
rest of the system sends a connection-global one (`pubsub.rs:684-687` vs `state.rs:635-636`), and
never repairs `ConnectionState` (`connection.rs:695-753` proves the confirmation bypasses state).
Requires a new message to the connection task. Recorded, not claimed.

**Security policy note:** no security-classified finding arose in this lane. Per standing policy,
had one arisen it would be recorded here only, not fixed.
