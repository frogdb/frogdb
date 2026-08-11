# Proposal 82 — `ChannelTable`: give the pub/sub shard table a reverse index and an incremental counter

Round 38 · lane: protocol / net / core · candidate **PN4** · effort **M** (core table) + **S**
(message/dispatch fold), independently landable · **no locked crate**, **no FM-tagged region
edited** (rev 2: narrowed from "no FM tag on any touched file" — see §Spec / LOCKED clearance),
**three seam lints in scope — two preserved by construction, one (`lint-continuation-lock`)
requires an explicit pin update in step D; see §Seam-lint clearance**

> **Revision 2** (post adversarial review). Verdict was AMEND: the thesis was independently
> reproduced (941× against this proposal's 946×; the recount confirmed as root cause, and
> super-quadratic at scale — these numbers are conservative). Six findings amended, H4 broadened,
> plus one amendment this proposal found on its own (the `lint-continuation-lock` arm-count pin).
> Full accounting in §Revision ledger.

**Verified at HEAD `ee5efee9`** (worktree `arch-round-38-99`, branch `main`). Every line number
below was re-derived against this tree, not taken from the lane brief. Every `pubsub.rs` line
number the brief cited is **still correct** at HEAD. **Three brief claims are corrected**:

| Brief claim | Correction |
|---|---|
| "the keyspace-notification routing rule lives in `scripts/error-sanitize.py`" | It does not. Both pub/sub gates are **inline bash in the `Justfile`**: `lint-keyspace-notify-routing` at `Justfile:1051-1067` and `lint-pubsub-confirmation-seam` at `Justfile:1128-1156`. `scripts/error-sanitize.py` has no pub/sub rule. This matters: the gates are `grep -rEn` over *literal spellings*, so they are silently defeatable by a rename (§Seam-lint clearance). |
| "the O(n²) is the linear pattern scan per publish" | Real, but **not** the dominant term, and not quadratic in the way named. The measured dominant cost is `check_thresholds_after_subscribe` (`core/src/pubsub.rs:881-934`), which runs a **full walk of both channel maps plus a `HashSet` build over every pattern on every single SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE**. Measured: at the documented `MAX_UNIQUE_CHANNELS_PER_SHARD = 100_000` ceiling (`pubsub.rs:285`), removing this recount alone is a **946× total-time reduction** — a batch-1 harness-total figure; the per-command claim is 70–126 µs of shard-loop CPU (§Problem 1 qualifiers 1–2). For PSUBSCRIBE, the duplicate scan the brief blamed is **~3 % of the cost**; the recount is the other ~97 % (§Problem 1). |
| "subscription handling is O(n²)" (unqualified) | Precisely: **three** distinct super-linear sites, with different shapes and different fixes — the per-subscribe threshold recount (O(C+S+P) per op), the per-psubscribe duplicate scan (O(P) per op), and the slot-drain recount (O(S) per drained subscriber — cheap for one slot, Θ(S²) across a full resharding; §Problem 4). All three fall out of the *same* missing structure: there is no reverse `ConnId -> channels` index and no maintained counter (§Proposed change). |

Four findings the brief did not name, all verified at HEAD:

1. **A LIVE correctness defect** on the slot-migration drain path: two different authorities compute
   one user-visible count, and the drain never repairs connection-side state (§Problem 4). Not a
   one-line fix → filed as an issue candidate, **not** claimed as a hotfix.
2. **Four dead methods** in `ShardSubscriptions` with zero production callers — they pass the
   deletion test outright (§Problem 3, hotfix **H1**).
3. **Five byte-identical method pairs**, one further duplication *inside* `remove_connection`, and
   **six registration message variants in two groups of three** whose only difference is a field
   *name* (§Problem 2).
4. **A stale premise in the pub/sub oracle's own doc header** (`testing/src/pubsub_oracle.rs:23-27`):
   it asserts there is no drop path on regular PUBLISH, which stopped being true when the output-
   budget drop at `core/src/pubsub.rs:165-197` landed (§Problem 6, hotfix **H2**).

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
O(subscriptions-for-this-connection), deletes five duplicated method pairs plus one intra-method
duplication, and collapses six `PubSubMsg` registration variants, their six dispatch arms and their
six handlers into **two** each — reusing the `SubKind` enum that already exists on the connection
side rather than introducing a new one.

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
| `frogdb-server/crates/core/src/shard/message.rs` | `PubSubMsg` at `:275-372`; `PubSubMsg::probe_type_str` at `:1040-1057` | **Primary (steps A–D use the enum; step D edits both regions).** Six registration variants with identical field *types*, **and their six USDT probe-name arms** `:1044-1047`, `:1050-1051`. |
| `frogdb-server/crates/core/src/tracking.rs` | 682 | **Read-only.** Precedent: `TrackingTable` `:80-89`, `remove_connection` `:191-204`. |
| `frogdb-server/crates/core/src/shard/keyspace_coordinator.rs` | 287 | **Read-only.** `publish` `:94-107`; the `local.publish(` call is at `:98` — gate-exempt by spelling. |
| `frogdb-server/crates/server/src/connection/pubsub_conn_command.rs` | 1128 | **Read-only for steps A/B/C (verified). STEP D EDITS `:77-99` + `:102-165`** — `SubKindSpec` sheds its two fn-pointer fields (`subscribe_msg` `:90`, `unsubscribe_msg` `:93`) and the six closures that fill them (`:108`, `:114`, `:130`, `:136`, `:152`, `:158`) collapse to two. Call sites `:392`, `:445` follow. **Proposal 68's primary deletion file** — see §Risks. |
| `frogdb-server/crates/server/src/connection/state.rs` | — | **Read-only for steps A/B/D; STEP D READS `SubKind` `:291-299` and step D+ hoists it** (see §The message fold). `PubSubState` `:30-43`; `remove_subscription` `:624-639` returns the connection-global sharded count (`:636`); `admit_subscriptions` headroom check `:573-576`; `exit_pubsub` `:667-670` (wholesale reset at `:668`). Evidence for §Problem 4. |
| `frogdb-server/crates/server/src/connection.rs` | — | **Read-only.** Delivery arm `:700-754` renders confirmations without touching `self.state`. Evidence for §Problem 4. |
| `scripts/continuation-lock-gate.py` | `:81` | **STEP D EDITS ONE LINE.** `DISPATCH["dispatch_pubsub.rs"] = ("PubSubMsg", 11)` is a hard count pin; the 6→2 fold makes it 7. §Seam-lint clearance, Gate 3. |
| `frogdb-server/crates/server/tests/integration_pubsub.rs` | 6247 | **Read-only, must stay green.** `test_ssubscribe_client_receives_sunsubscribe_on_slot_migration` `:1277-1400` is the (degenerate) existing coverage of the drain — §Problem 4. |
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

**Four honesty qualifiers on that 946×** (all verified; none of them changes the conclusion):

1. **Batch size.** The recount runs **once per handler invocation**, not once per channel:
   `handle_subscribe` (`shard/pubsub.rs:14-27`) loops the batch and then calls the check once
   (`:24`); `handle_psubscribe` `:39-52` and `handle_ssubscribe` `:64-77` are the same shape. The
   benchmark models **one channel per SUBSCRIBE command** (batch = 1), which is the common client
   shape but the worst case for this cost. Total recount work is **Θ(n²/2B)** in batch size `B`:
   at B = 1000 the recount cost falls to roughly baseline. The headline figure is therefore the
   **batch-1 ceiling**, and it should be read as such.
2. **What the ratio measures.** 946× is a **harness-total ratio** across a full fill — not an
   end-to-end client latency ratio, which is dominated by network and connection-task work the
   harness does not model. The defensible per-command claim is the direct one: at the documented
   ceiling this guard burns **70–126 µs of pure shard-event-loop CPU per subscribe command**, on
   the single-threaded loop that also serves every publish for `BROADCAST_SHARD`.
3. **The ceiling is not enforced.** `MAX_UNIQUE_CHANNELS_PER_SHARD` (`:285`) is a **warning
   threshold only** — its three uses (`:905`, `:913`, `:944`) are the 90 % warn and the re-arm; no
   code path refuses a subscribe on it. `n` can exceed 100 000, so the benchmark is a **floor**,
   not a worst case.
4. **The flags latch at very different times.** `MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD` is `1_000_000`
   (`:282`), so `warned_total_90` needs **900 000** subscriptions — in the measured 100 000-channel
   scenario `total_subscription_count()` **never** hands off, and SUBSCRIBE pays the O(C+S) walk
   forever. The genuine hand-off happens only for patterns, at 9 000 (`MAX_UNIQUE_PATTERNS_PER_SHARD`
   `:288` × 90 %), and even then it just moves the identical recount onto PUNSUBSCRIBE. Both
   readings support the case; stating them precisely is what makes them usable.

The inversion is worth stating plainly: a guard whose purpose is to notice that the table has grown
large is implemented by re-measuring the whole table on every insert, and it becomes cheap on the
subscribe path only *after* it has already fired — and for the total counter, in practice, never.

> **Companion defect, out of scope, recorded not claimed.** `unique_channel_count` `:866-868` is
> `self.channel_subs.len()` — **broadcast channels only**. It is the sole input to the
> `MAX_UNIQUE_CHANNELS_PER_SHARD` warning (`:905-917`), so **the unique-channel threshold never
> warns for sharded channels** no matter how many SSUBSCRIBEs a shard accumulates. This is a
> behaviour bug in the guard, not a performance one, and fixing it is a policy question (should
> sharded channels share the broadcast ceiling, or get their own?). It is *adjacent* to this
> proposal — the maintained counters make either answer a field read — but deciding it is not this
> proposal's business. Filed alongside H4 as an issue candidate.

### 2. Five method pairs and six message variants that differ only in a field name

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

That is **five** pairs, not six. Plus a further duplication *inside a single method*:
`remove_connection` `:785-800` contains the same `retain` body twice, at `:787-790` and `:796-799`
— a sixth instance of the same shape, but an intra-method one.

The duplication propagates outward. `PubSubMsg` (`core/src/shard/message.rs:275-372`) has eleven
variants, of which **six are registration variants with identical field types** — `Subscribe`
`:284`, `Unsubscribe` `:295`, `PSubscribe` `:305`, `PUnsubscribe` `:316`, `ShardedSubscribe` `:340`,
`ShardedUnsubscribe` `:351`. Precisely, they are **two groups of three**: the three subscribe
variants carry `(names, conn_id, sender, response_tx)` and the three unsubscribe variants carry
`(names, conn_id, response_tx)` — within each group the only difference is whether the field is
called `channels` or `patterns`. Hence the fold target is **two** variants, not one.
Those six variants get **six dispatch arms** (`shard/dispatch_pubsub.rs:10-19`,
`:20-28`, `:29-38`, `:39-47`, `:73-82`, `:83-91`), each of which is literally
`self.handle_x(...); let _ = response_tx.send(());`, and **six handlers** in `shard/pubsub.rs`
(`:14-28`, `:31-36`, `:39-53`, `:56-61`, `:64-78`, `:81-86`), each of which loops and then calls
either `check_thresholds_after_subscribe` (`:24`, `:49`, `:74`) or `reset_thresholds_if_needed`
(`:35`, `:60`, `:85`). `handle_introspection` `:106-131` continues the pattern: **five arms, four of
which form two pairs** — `Channels`/`ShardChannels` (`:111-114`, `:122-125`) and
`NumSub`/`ShardNumSub` (`:115-118`, `:126-129`) — with `NumPat` (`:119-121`) unpaired, because
there is no sharded equivalent of a pattern.

That is **one axis of variation (which kind of subscription) expressed six times in four files**.
The kind is not a behavioural difference — it is a table selector.

The contrast is instructive: the *connection* side already solved this. `pubsub_conn_command.rs`
carries `SubKindSpec` `:77-99` with three statics (`CHANNEL_SPEC` `:102-121`, `PATTERN_SPEC`
`:124-143`, `SHARDED_SPEC` `:146-165`) and puts the control flow **once** in `subscribe_kind`
`:311-404` / `unsubscribe_kind` `:414-454`. The shard side never received the same treatment.

**And the three-way enum this proposal needs already exists there too**: `SubKind { Channel,
Pattern, Sharded }` at `server/src/connection/state.rs:291-299` is already `SubKindSpec`'s **first
field** (`:79`) and is already threaded through `admit_subscriptions` (`:566`),
`add_subscription`, `remove_subscription` (`:624`), `subscriptions(kind)` and
`rearm_subscription_warning`. The kind axis is modelled once, on the connection side, and simply
does not cross the message boundary — the six variants *are* that crossing. See §The message fold.

### 3. Four methods with zero production callers (deletion test)

Applying the deletion test honestly — remove it and see whether anything has to be reinvented:

Caller attribution re-derived by `rg` over the whole workspace (rev 2 — the rev-1 table
mis-assigned the call sites; corrected here and propagated into §Testability item 2 and H1):

| Method | Production callers | Test-only callers | Which test |
|---|---|---|---|
| `get_connection_channels` `:572-583` | **0** | `pubsub.rs:1323`, `:1329` | `test_remove_connection` `:1307-1332` |
| `get_connection_patterns` `:614-623` | **0** | `pubsub.rs:1324` | `test_remove_connection` |
| `get_connection_sharded_channels` `:703-714` | **0** | `pubsub.rs:1325` | `test_remove_connection` |
| `pattern_count` `:626-628` | **0** | `pubsub.rs:1215`, `:1220` | `test_psubscribe_punsubscribe` `:1204-1221` |

Nothing has to be reinvented. The real "unsubscribe from everything" path does not consult the
shard table at all: it reads the connection's own `HashSet` via
`self.state.subscriptions(spec.kind)` (`pubsub_conn_command.rs:417`). These four methods exist to
be tested. They are the shallow-module tell — an interface that grew to expose internals for
verification, because the internals were not verifiable through the real interface.

The reverse index this proposal adds makes three of them O(1)-correct *if* anyone ever wants them
back — so deleting them now is safe in both directions.

**A fifth accessor is the opposite case, and it was under-claimed in rev 1.**
`unique_pattern_count` `:871-877` — the one that builds a throwaway `HashSet` over every pattern on
the shard — has **three production callers beyond the threshold check**, each paying that O(P)
build-and-allocate on a live request path:

| Call site | Path |
|---|---|
| `shard/pubsub.rs:120` | `PUBSUB NUMPAT` — a client command |
| `shard/dispatch_search.rs:28` | search-side shard stats |
| `shard/diagnostics.rs:408` | diagnostics/metrics export |

The maintained counter fixes all three, not just the subscribe path. `PUBSUB NUMPAT` becomes a
field read. **Claimed** as part of step A/B's value, not just the recount.

### 4. LIVE: the slot-migration drain has two authorities for one count, and repairs neither side

`drain_sharded_channels_for_slot` (`core/src/pubsub.rs:668-700`) is called on slot migration
(`shard/pubsub.rs:93`, inside `handle_slot_migrated_pubsub` `:92-103`, its only caller). For each
removed channel, for each subscriber, it computes the count sent in the synthetic `SUnsubscribe`
confirmation at **`:683-687`**:

```rust
let remaining = self
    .sharded_subs
    .values()                                   // :685 - full scan of the map
    .filter(|subs| subs.contains_key(&conn_id)) // :686
    .count();                                   // :687
```

Two independent problems, both verified. **They have different preconditions, and rev 1 stated
only the narrower one** — half (b) is the broader defect:

**(a) Wrong authority — requires `num_shards >= 2`.** This `remaining` is a **shard-local** count.
The normal SUNSUBSCRIBE path returns the **connection-global** count — `state.rs:624-639` returns
`self.pubsub.sharded_subscriptions.len()` at `:636`, across all shards, which is Redis's semantics.
So the same client receives counts on two different scales depending on whether the unsubscribe was
client-initiated or migration-initiated. On a single-shard node the two scales coincide, so (a) is
invisible there.

**(b) Connection state is never repaired — bites at ANY shard count, including the default 1.**
The confirmation is rendered straight to the wire: the delivery arm at `connection.rs:700-754`
calls `pubsub_msg.to_response_with_protocol(...)` → `feed_response` and **never touches
`self.state`**. Repo-wide, the only two removals from `ConnectionState.pubsub.sharded_subscriptions`
are `state.rs:635` (client-initiated SUNSUBSCRIBE, inside `remove_subscription`) and `state.rs:668`
(`exit_pubsub`'s wholesale `PubSubState::default()` reset). The drain path is neither. So after a
drain, `ConnectionState`'s `sharded_subscriptions` set still contains the migrated-away channels —
permanently. Two consequences, both user-visible on a plain single-shard node:

- every *subsequent* SUNSUBSCRIBE confirmation on that connection reports an **inflated count**
  (the stale entries are still counted at `state.rs:636`);
- the `MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION = 10_000` budget (`pubsub.rs:279`) **never regains
  headroom** — the admission check at `state.rs:573-576` counts the stale set, so a connection that
  has ridden enough migrations can be refused subscriptions it does not hold.

**Cost — restated, because rev 1's framing overstated it by ~600×.** Rev 1 reported a harness
"drain 5 % of a shard's sharded channels". That models a **full resharding of ~819 slots**
(0.05 × 16384), not one migration event. One `ClusterMsg::SlotMigrated` drains **one slot**, i.e.
≈ S/16384 channels, and the honest single-event number is:

```
DRAIN one slot, S=  50000   ≈ 3 channels   total=   1.4ms
DRAIN one slot, S= 100000   ≈ 6 channels   total=   5.9ms
```

(Reviewer-measured; independently reproduced here analytically — the remaining-scan does
`(S/16384) × O(S)` map-value visits ≈ S²/16384, which predicts 1.5 ms and 6.1 ms.) The rev-1
aggregate numbers remain correct *as aggregates*:

```
AGGREGATE, 5% of S=   1000  notifications=    50  total=      1.2ms
AGGREGATE, 5% of S=  10000  notifications=   500  total=    104.1ms
AGGREGATE, 5% of S=  50000  notifications=  2500  total=   1921.6ms
```

So the defensible cost claim is: **a single slot migration is cheap (single-digit ms); the
aggregate cost of a full resharding is Θ(S²)** — every one of the S channels pays an O(S) scan.
This runs inline on the shard event loop during migration, i.e. during exactly the window where the
cluster is already under stress. **H4's correctness claim does not depend on any of these numbers.**

**Test coverage: degenerate, not absent** (rev 1 said "zero" — that was wrong).
`integration_pubsub.rs:1277` `test_ssubscribe_client_receives_sunsubscribe_on_slot_migration` is a
3-node cluster test that drives a real `CLUSTER SETSLOT` migration and asserts the **full**
SUNSUBSCRIBE frame, including the count: `Response::Integer(0)` at `:1394-1398`. No `FM-` row
forces it (§Spec / LOCKED clearance).

That is a **more** damning framing, not a weaker one. The test uses **one** channel, **one**
subscriber, **one** shard's worth of state, and drains the connection's only sharded subscription —
so `remaining` is 0 under *both* authorities, and the stale `ConnectionState` entry is never
observed because the test never issues a second SUNSUBSCRIBE. An asserting test exists, it asserts
the exact field that is wrong, and it passes. The defect survived *because* the coverage is
degenerate — which is precisely the failure mode a "no test exists" note would have mis-diagnosed.

This is **not** a one-line fix — repairing (b) requires a message back to the connection task, which
is a design decision, not an edit. Recorded here as an issue candidate. (a) alone is fixable once
(b) has an owner; fixing (a) without (b) would make the numbers consistent and still wrong.

### 5. Per-publish pattern scan

`publish` `:722-756` iterates every pattern registered on the shard for every published message
(`:741`, `for (pattern, compiled, _, sender) in &self.pattern_subs`). This is genuinely O(P) per
PUBLISH and it is the site the brief named. It is real, and it is the **least** of the three costs
under the measured workloads, because P is bounded by `MAX_UNIQUE_PATTERNS_PER_SHARD = 10_000`
(`pubsub.rs:288`) whereas C is bounded at 100 000 — though see §Problem 1 qualifier 3: neither
bound is enforced, both are warning thresholds only.

It is also the one site this proposal does **not** claim to make sub-linear. Glob matching against
arbitrary patterns has no index that is both correct and general; Redis, Valkey and DragonflyDB all
scan the pattern list per publish for the same reason. What the proposal *does* fix here is the
**duplicate-detection scan** on the registration side (`psubscribe` `:591-602`, scan at `:593-597` —
a linear walk of `pattern_subs` looking for `(conn_id, pattern)`), which becomes a hash lookup
through the reverse index. Claiming more than that would be dishonest.

### 6. The oracle's stated premise is stale (doc-only)

`testing/src/pubsub_oracle.rs:23-27` asserts, as the justification for its exactly-once bracket:

> "…delivers **synchronously** … into each live subscriber's **unbounded** mpsc queue. There is no
> `try_send`/drop on the regular-PUBLISH path … (The only dropping path in the codebase is the
> *cross-shard keyspace-notification* hop, which regular PUBLISH never takes.)"

At HEAD that is false. `PubSubSender::send` (`core/src/pubsub.rs:165-197`, drop branch `:168-189`)
**drops** the message when the shared byte budget is exhausted, latches an overflow flag, and still
returns `Ok(())` so the subscriber is counted. The budget is live in production: connections are built with
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

### The message fold — *hoist* `SubKind`, do not invent a third enum

Rev 1 proposed a new `SubscriptionKind` in `frogdb-core` and offered to "reuse proposal 68's
`PubSubKind` if 68 lands first". **Both halves of that were wrong**, and the corrected plan is
simpler and has fewer ordering dependencies.

- 68 is **ExecFraming**, not a kind enum. Its `PubSubKind` (`pubsub_conn_command.rs:722-734`) is a
  **nine**-variant command-identity enum (`Subscribe … Publish, SPublish, PubSub`) that 68
  explicitly rules **stays private** (68:450: *"`PubSubKind` stays private and keeps its
  `execute_multi` job"*). It is the wrong axis — "which command is this" — and cannot serve as a
  three-way subscription-kind selector.
- Inventing `SubscriptionKind` would violate this proposal's own rule ("what must not happen is two
  enums") by creating a **third**.

**The enum already exists, in the wrong crate.** `SubKind { Channel, Pattern, Sharded }` at
`server/src/connection/state.rs:291-299` is exactly the three-way distinction, is already
`SubKindSpec`'s first field (`pubsub_conn_command.rs:79`), and is already the parameter of
`admit_subscriptions` / `add_subscription` / `remove_subscription` / `subscriptions(kind)` /
`rearm_subscription_warning`. `frogdb-server` depends on `frogdb-core` (`server/Cargo.toml:116`)
and not the reverse, so the move is downhill:

> **Hoist `SubKind` from `server/src/connection/state.rs` into `frogdb-core`** (next to
> `ShardSubscriptions`), re-export or `use` it from `state.rs` so the connection side is unchanged
> at its call sites, and have the shard side consume the same type.

Then collapse the six registration variants (`shard/message.rs:284, :295, :305, :316, :340, :351`)
into two:

```rust
Register   { kind: SubKind, names: Vec<Bytes>, conn_id, sender, response_tx }
Unregister { kind: SubKind, names: Vec<Bytes>, conn_id, response_tx }
```

Six dispatch arms collapse to two; six handlers collapse to two; and — because `SubKindSpec`
already carries `kind` — the two fn-pointer fields `subscribe_msg` (`:90`) and `unsubscribe_msg`
(`:93`) plus the six closures that fill them (`:108`, `:114`, `:130`, `:136`, `:152`, `:158`)
**disappear entirely**, replaced by two constructors that read `spec.kind`. That is a strictly
larger dedup than rev 1 claimed, and it is why the step's file set is wider than rev 1 stated
(§Effort, §Risks).

**Three consequences the fold must own** (none of them optional):

1. **USDT probe names.** `PubSubMsg::probe_type_str` (`message.rs:1040-1057`) maps each variant to
   a string, and `message.rs:1003-1005` documents that surface as *"byte-for-byte identical to the
   pre-split flat variant names, which downstream USDT probe consumers depend on"*. A naive fold
   renames **six** probes (`Subscribe`, `Unsubscribe`, `PSubscribe`, `PUnsubscribe`,
   `ShardedSubscribe`, `ShardedUnsubscribe`) to two. Two acceptable answers, and the fold must pick
   one **explicitly**:
   - **(i) Preserve the names** using the idiom already in the file: `ShardMessage::DriveTick`
     (`message.rs:1020-1022`) is one variant that yields two probe strings by matching on its inner
     payload. `PubSubMsg::Register { kind: SubKind::Channel, .. } => "Subscribe"` etc. keeps all six
     names byte-stable at the cost of six hand-written arms. **Recommended** — it makes the fold a
     pure refactor with no downstream contract change.
   - **(ii) Own the rename** as a documented behaviour change: two probe names replace six, recorded
     in §Behaviour changes and in the release notes for probe consumers.
   Note (i) and proposal 85's PN11 (a variant-name derive) are **mutually exclusive for this enum**:
   a name-derive cannot express a payload-dependent string, so choosing (i) means `PubSubMsg` is
   excluded from PN11's derive exactly as `ShardMessage`'s `DriveTick` already is (85:241-242).
2. **The `lint-continuation-lock` arm-count pin** must be updated 11 → 7 in the same commit.
   §Seam-lint clearance, Gate 3.
3. **The step edits `pubsub_conn_command.rs`.** It is not read-only for this step. §Risks.

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
(`:859-863`, `:683-687`, `state.rs:636`, `:866-868`, `:871-877`). After the change it is
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
- **The `SubKind` fold** — delete it and you get the six variants back verbatim, *plus* the two
  `SubKindSpec` fn-pointer fields and their six closures. But note it is a *deduplication*, not a
  new capability: it passes the test on maintenance grounds (one axis, one expression), not on
  depth grounds. Landable last. (Note the hoist itself is not a deletion-test candidate at all —
  `SubKind` already exists and already passes; the question is only which crate owns it.)

## Testability improvement

Concrete, not aspirational:

1. **§Problem 4's defect becomes testable.** Today asserting "the count in a migration-drain
   confirmation matches the count in a subsequent client SUNSUBSCRIBE" requires reaching through
   two crates. With `remove_connection`/`unsubscribe` returning a maintained count from one
   authority, the assertion is a unit test in `core/src/pubsub.rs`.
2. **The four dead accessors stop being the test interface.** `test_remove_connection`
   (`pubsub.rs:1307-1332`) asserts entirely through `get_connection_channels` / `_patterns` /
   `_sharded_channels` (`:1323`, `:1324`, `:1325`, `:1329`), and `test_psubscribe_punsubscribe`
   (`:1204-1221`) checks `pattern_count` (`:1215`, `:1220`) — methods no production path uses. They
   verify the test surface, not the product. After the change they assert `channels_for(conn)` on
   the table that `remove_connection` actually consults. **Note this is a rewrite, not a deletion**
   — `test_remove_connection` is the only unit test of `remove_connection`, the method step A
   rewrites, so its coverage must be preserved (see H1).
3. **Threshold behaviour becomes assertable without filling the table.** Today verifying the 90 %
   warning means inserting ~90 000 subscriptions (and, for the *total* threshold, 900 000 — see
   §Problem 1 qualifier 4, which is why that path is effectively untested). With a maintained
   counter, the warning logic is a pure function of three integers and testable directly.
4. **`drain_sharded_channels_for_slot` gains its first *non-degenerate* test.** It has one test
   today (`integration_pubsub.rs:1277`), but with one channel, one subscriber and a final count of
   0 it cannot distinguish the two authorities or observe the stale connection state (§Problem 4).
   Acceptance for any change in this area:
   - a **multi-channel, multi-shard** drain test in which the drained connection retains sharded
     subscriptions on another shard, asserting the count in the drain confirmation **and** the
     count in a subsequent client SUNSUBSCRIBE on the same connection;
   - the existing `test_ssubscribe_client_receives_sunsubscribe_on_slot_migration` **pinned green,
     unchanged**, as the guard that the degenerate case did not regress.
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
- **FM tags on the `frogdb-core` files: none.** `rg 'FM-'` returns zero hits in
  `core/src/pubsub.rs`, `core/src/shard/pubsub.rs`, `core/src/shard/dispatch_pubsub.rs`,
  `core/src/shard/message.rs`. (The nearest hit in the crate is an unrelated `FM-PERSISTENCE-022`
  at `core/src/shard/worker.rs:349`.)
- **Rev 2 correction — one step-D file does carry FM tags, harmlessly.** Now that step D edits
  `server/src/connection/state.rs` (the `SubKind` hoist), the blanket "no FM tag on any touched
  file" is wrong: that file carries **12 `FM-TXN-*` tags** (`:1128`, `:1138`, `:1158`, `:1169`,
  `:1184`, `:1194`, `:1361`, `:1396`, `:1428`, `:1460`, `:1472`, `:1898`). All twelve are **inside
  `mod tests` (`:1114`+)** and all twelve force **transaction** behaviour (ASKING, MULTI/EXEC,
  WATCH) — none names a pub/sub subscription kind. Step D edits `:291-299`, 800+ lines above the
  test module. So `just lint-failure-modes` (which pairs `FM-<AREA>-NNN` rows with the tests that
  force them) is **unaffected**: no tagged test is added, removed or renamed, and no spec row needs
  amending. Restated precisely: **no FM-tagged region is edited by any step.**
  `pubsub_conn_command.rs` — step D's other server-side file — carries **zero** `FM-` tags,
  re-verified.
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
attribution is wrong — see the corrections table). A **third** gate is in scope for step D and was
missed in rev 1: `lint-continuation-lock` (`Justfile:1312-1313` →
`scripts/continuation-lock-gate.py`). `lint-gates` (`Justfile:329`) runs the compile-free subset on
every commit via lefthook and in CI's `seam-gates` job; `lint` (`Justfile:319`), `pre-commit`
(`:1381`) and `check-all` (`:1384`) all reach it. All three gates below are in that compile-free
set.

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
- **touches no `b"*-1"` literal** anywhere.

**Rev 2 correction — the third leg has to be re-argued.** Rev 1 cleared rule (a) by asserting *"this
proposal does not touch `pubsub_conn_command.rs` at all"*. That is false for step D, which edits
`:77-99` and `:102-165` (§Risks). The clearance therefore has to come from **what the edit does**,
not from a stance:

> Step D's edit to `pubsub_conn_command.rs` **deletes** two fn-pointer fields and six closures and
> **adds** two constructor expressions of the form `PubSubMsg::Register { kind: spec.kind, … }`.
> It introduces **no byte-string literal of any kind**, and in particular no member of the
> forbidden set `b"subscribe" | b"unsubscribe" | b"psubscribe" | b"punsubscribe" | b"ssubscribe" |
> b"sunsubscribe"`. The `subscribed` / `unsubscribed` fields (`:97`, `:99`), which are the fn
> pointers into `PubSubConfirmation`, are **untouched** by the fold — the label literals stay where
> they legitimately live, at `pubsub.rs:332`, `:335`, `:338`, `:341`, `:344`, `:347`.

The conclusion survives; only the argument changes. Rule (a) is preserved because the fold moves
*kind selection* across the message boundary, not *label construction*.

The one place the refactor comes near this seam is the synthetic `SUnsubscribe` built inside
`drain_sharded_channels_for_slot` (`pubsub.rs:688-690`). It already goes through
`PubSubConfirmation::SUnsubscribe` and must continue to — the fix for §Problem 4 changes the
*value* of `count`, never the construction path.

Acceptance check: `just lint-gates` green **and** `just lint` green, plus a by-hand
`grep -rEn 'b"(p|s)?(un)?subscribe"' crates/server/src/connection/pubsub_conn_command.rs` returning
nothing after step D.

### Gate 3 — `lint-continuation-lock` (`Justfile:1312-1313`, `scripts/continuation-lock-gate.py`)

**Missed in rev 1. This one does not pass by construction — step D must edit the gate.**

Mechanism: the script pins, per dispatch file, the exact number of top-level match arms and checks
**enum parity in both directions** against `message.rs`. `DISPATCH` at `:81` reads:

```python
"dispatch_pubsub.rs": ("PubSubMsg", 11),
```

Step D takes `PubSubMsg` from 11 variants to 7 and `dispatch_pubsub.rs` from 11 arms to 7. Three of
the script's rules fire at once: the count pin (`:299-306`), missing-variant parity, and
arms-for-non-existent-variants parity (`:308-334`). The gate is *doing its job* — the ratchet exists
precisely so a variant fold cannot land unnoticed.

> **Constraint (binding on implementation):** step D updates `scripts/continuation-lock-gate.py:81`
> to `("PubSubMsg", 7)` **in the same commit** as the fold, and states in the commit message which
> arms were folded into which.

Checked and clear on the harder half: **no `PubSubMsg::*` arm is pinned in `GATE` (`:96-102`),
`EXEMPT` (`:110-128`) or `GATE_GAP` (`:135-151`)**, so Rule 8b (every pinned name must still name a
real arm) is unaffected and no continuation-lock *disposition* changes. The fold moves a count, not
a safety classification. Its arms remain non-mutating registration handlers, which is why they
carry no disposition today.

Acceptance check: `just lint-continuation-lock` green after the fold, with the pin at 7 and no new
`GATE`/`EXEMPT` entries.

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
4. **Said aloud, because A+B could otherwise be misread as closing H4:** steps A and B make the
   drain's `remaining` computation **fast** — a maintained shard-local counter instead of an O(S)
   scan — **without making it right**. A shard-local count computed in O(1) is still the wrong
   authority (§Problem 4a) and still leaves `ConnectionState` unrepaired (§Problem 4b). "A+B is a
   defensible single PR" is a statement about *scope*, not about *correctness coverage*: **A+B does
   not close H4, and a PR landing A+B must not claim it does.** Only step C does.
5. **USDT probe names, step D only.** Under fold option (ii) six probe names become two — a
   documented downstream contract change on a surface `message.rs:1003-1005` calls byte-stable.
   Under the recommended option (i) nothing changes on the wire or the probe surface. The fold must
   state which option it took. See §The message fold.

## Risks and scope boundaries

### vs. proposal 68 (ExecFraming) — **rev 2: a real edge exists, and it is step-D-only**

Rev 1 claimed the two were disjoint "as long as both hold their stated stance". That claim is
**falsified**: step D cannot hold a read-only stance on `pubsub_conn_command.rs`, because the six
`PubSubMsg` registration variants are constructed in exactly one production place — the
`SubKindSpec` closures at `:108`, `:114`, `:130`, `:136`, `:152`, `:158`. Folding 6→2 means editing
them. (The only other construction site in the tree is `dispatch_pubsub.rs:155`, inside a test in a
file step D already owns.)

Restating the relationship honestly:

| Step | `pubsub_conn_command.rs` | Edge with 68 |
|---|---|---|
| A (`ChannelTable`) | untouched — verified | **none** |
| B (threshold counters) | untouched — verified | **none** |
| C (§Problem 4 fix) | untouched — verified (the repair message lands in `connection.rs` / `state.rs`) | **none** |
| D (`SubKind` hoist + 6→2 fold) | **edits `:77-99`, `:102-165`, `:392`, `:445`** | **real, file-level** |

68's own edits to that file are `:965-1005` (deleted in full), `:767-804` (nine specs gain a field)
and `:739-760` (`pubsub_spec()` gains a parameter). Step D's are 600+ lines above all of them, so a
**textual** merge conflict is unlikely; the edge is one of ownership and rebase cost, not of
overlapping hunks.

**Ordering — options and constraints for the orchestrator** (this proposal does not rule):

- **Option 1 — 68 first, then D (recommended).** 68 is a deletion-heavy change to a file D only
  adds to; rebasing D's additive edit onto a smaller file is mechanical. D also inherits a settled
  `pubsub_spec()`/spec table. Cost: D waits.
- **Option 2 — D first, then 68.** Workable, and 68's hunks are far from D's, but 68's author then
  rebases a primary-deletion diff onto a file whose top 90 lines changed shape. Slightly worse, not
  blocking.
- **Option 3 — steps A/B/C now, D deferred entirely.** A/B/C are genuinely disjoint from 68
  (verified above) and can land in **any** order relative to it, including concurrently. This is the
  option that removes the edge rather than sequencing it.

Given that D is the smallest and least valuable step, options 1 and 3 both look cheap. **The
enum-ordering dependency rev 1 worried about is gone entirely**: D hoists the *existing* `SubKind`
from `state.rs:291`, and 68 rules that its `PubSubKind` stays private (68:450), so there is no
enum to coordinate and no risk of two enums. Only the file edge remains.

### vs. proposal 90 (future, `CommandSpec::DEFAULT` mechanical sweep)

90 rewrites every `CommandSpec` static in the workspace, including the nine at
`pubsub_conn_command.rs:767-804` and `pubsub_spec()` at `:739-760`. Rev 1 cleared this "by
construction of the read-only stance on `pubsub_conn_command.rs`" — that argument is void (see
above), so it is **re-argued from the merits**, and the conclusion is unchanged:

- **no step of this proposal edits any `CommandSpec` static** — verified: A/B/C touch `frogdb-core`
  only, and D's edits to `pubsub_conn_command.rs` are confined to `:77-165` (the `SubKindSpec`
  struct and its three statics) plus the two dispatch call sites `:392`, `:445`. The spec block
  begins 600 lines lower at `:739`.
- **no step edits any `frogdb-commands` file** — verified.

So the two share a *file* (only at step D) but no *hunk* and no *concept*: 90 rewrites the
command-metadata statics, D rewrites the subscription-kind statics, and they do not read each
other. **No conflict edge**, now on evidence rather than on stance.

### vs. proposal 85 (CT3 + PN7 + **PN11**)

85's PN11 replaces the hand-written `probe_type_str` impls with a derive, and its target includes
**the same `impl PubSubMsg` block step D rewrites** (`message.rs:1040-1057`). 85 already flags the
edge from its side (85:412-433) and recommends **PN11 first**. Agreed, with one correction to the
shared picture:

- **PN11 first** is cheapest *if* step D takes fold option (ii) (own the rename): the arms are then
  generated from the two new variant names and D needs no probe edit at all.
- **If step D takes the recommended option (i)** (preserve the six probe names via a
  payload-dependent arm, the `DriveTick` idiom), then `PubSubMsg` becomes **underivable** and must
  be excluded from PN11's derive — exactly as `ShardMessage`'s `DriveTick` arms already are
  (85:241-242). PN11 then covers 10 of 11 enums instead of 11.

That is a genuine trade the two proposals should settle together: **byte-stable probe names cost
PN11 one enum**. This proposal's recommendation is to keep the names (option i) and let PN11 skip
`PubSubMsg`, because probe-name stability is an external contract while derive coverage is internal
tidiness — but the call belongs to whoever sequences 82D and 85.

Note also that both proposals touch `scripts/continuation-lock-gate.py`'s neighbourhood: 85 reads it
(85:120, read-only, must stay green) while step D **edits** its `PubSubMsg` pin. No conflict, but
85's "must stay green" assertion is only true after D updates the pin.

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
| D. Hoist `SubKind` to `frogdb-core` and fold `PubSubMsg` 6→2 (6 dispatch arms → 2, 6 handlers → 2) | see below — **six files, two crates, one script** | **M** (was `S–M`) | A; **shares a file with 68 — ordering decision required, options in §Risks** |

**Step D's file set, restated in full** (rev 1 understated this — it listed three files and declared
the connection side read-only):

| File | Edit |
|---|---|
| `core/src/shard/message.rs` | `PubSubMsg` `:284`–`:351`: six registration variants → `Register`/`Unregister`; `probe_type_str` `:1040-1057`: six arms → two (option ii) or six payload-matched arms (option i) |
| `core/src/shard/dispatch_pubsub.rs` | six arms `:10-47`, `:73-91` → two; test construction `:155` |
| `core/src/shard/pubsub.rs` | six handlers `:14-86` → two |
| `core/src/pubsub.rs` (or wherever `SubKind` lands) | receives the hoisted `SubKind` |
| `server/src/connection/state.rs` | `SubKind` `:291-299` moves out; `use`/re-export in its place |
| `server/src/connection/pubsub_conn_command.rs` | `SubKindSpec` `:77-99` drops `subscribe_msg` `:90` + `unsubscribe_msg` `:93`; the six closures `:108`/`:114`/`:130`/`:136`/`:152`/`:158` collapse to two constructors; call sites `:392`, `:445` follow |
| `scripts/continuation-lock-gate.py` | `:81` pin `("PubSubMsg", 11)` → `("PubSubMsg", 7)` |

Sizing moves from `S–M` to **M** on that basis. The *dedup* is correspondingly larger than rev 1
claimed — on top of six variants, six arms and six handlers, the fold also removes **two
`SubKindSpec` fn-pointer fields and six closures**, and deletes a whole category of "which
constructor does this kind use" indirection. The case for D is stronger than rev 1 made it; it is
just not free, and it is not disjoint from 68.

**Overall: M**, with A+B as the valuable core (they are what deliver the measured recount removal,
plus the three unclaimed `unique_pattern_count` call sites of §Problem 3) and C/D independently
schedulable. A+B alone is a defensible single PR — **but see §Behaviour changes item 4: A+B is a
performance change, not a fix for H4.**

## Independently-landable hotfixes

### H1 — delete four dead methods · **LATENT** · **claimed (scope corrected)**

`get_connection_channels` (`core/src/pubsub.rs:572-583`), `get_connection_patterns` (`:614-623`),
`get_connection_sharded_channels` (`:703-714`), `pattern_count` (`:626-628`) have zero production
callers (table in §Problem 3, re-verified in rev 2).

**Rev 1 said "delete the four fns and the three tests that exist only to call them". That would
destroy real coverage and must not be done:**

- `:1323`, `:1324`, `:1325`, `:1329` are all inside **`test_remove_connection` (`:1307-1332`) — the
  only unit test of `remove_connection`**, which is the very method step A rewrites. Deleting it
  removes the one test that would catch a reverse-index cleanup bug.
- `:1215` and `:1220` are inside **`test_psubscribe_punsubscribe` (`:1204-1221`)**, which also
  asserts psubscribe **duplicate rejection** at `:1209-1210` — the exact path §Problem 5 makes
  sub-linear.

Corrected H1: **delete the four `pub fn`s and the *assert lines* that reference them** (`:1215`,
`:1220`, `:1323`, `:1324`, `:1325`, `:1329`), and either

- **rewrite `test_remove_connection` against production accessors** — `channels(None)`, `numsub`,
  `shard_channels`, `unique_pattern_count` — preserving its "connection 1 gone, connection 2
  intact" assertion; or
- **land H1 after step A**, pointing the rewritten test at `channels_for(conn)`.

`test_psubscribe_punsubscribe` keeps its duplicate-rejection assertions either way; only its two
`pattern_count` lines change (to `unique_pattern_count`, or to a maintained counter after step A).

Because of this, **rev 1's "independent of everything above" claim is withdrawn.** H1 is
subtractive but it is coupled to step A through `test_remove_connection`; landing it standalone
requires writing the replacement test first.

### H2 — correct the oracle's stale premise · **LIVE (doc)** · **claimed**

`testing/src/pubsub_oracle.rs:23-27`. Sketch:

```diff
-//!   subscriber's **unbounded** mpsc queue. There is no `try_send`/drop on the
-//!   regular-PUBLISH path: a message is delivered to a live subscriber's queue
-//!   or the subscriber's connection is already gone. (The only dropping path in
-//!   the codebase is the *cross-shard keyspace-notification* hop, which regular
-//!   PUBLISH never takes.)
+//!   subscriber's mpsc queue. The queue is unbounded in length but bounded in
+//!   bytes: `PubSubSender::send` (core/src/pubsub.rs:165-197) drops the message
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

### H4 — §Problem 4's drain defect · **LIVE** · **NOT a hotfix, filed as an issue candidate**

Named here so it is not lost. **Two halves, with different blast radii** (rev 2 broadened this —
rev 1 presented it as a cluster-only count mismatch):

| Half | Defect | Precondition | Evidence |
|---|---|---|---|
| **(a)** Wrong authority | drain sends a **shard-local** count; every other path sends the **connection-global** one | `num_shards >= 2` (the two coincide on a single shard) | `pubsub.rs:683-687` vs `state.rs:636` |
| **(b)** State never repaired | `ConnectionState.pubsub.sharded_subscriptions` keeps the migrated-away channels **permanently** | **none — bites at any shard count, including the default 1** | delivery arm `connection.rs:700-754` renders and never touches `self.state`; the only removals repo-wide are `state.rs:635` and `:668` |

Half (b) is the one that reaches ordinary deployments: it produces **inflated counts on every
subsequent SUNSUBSCRIBE** for that connection and **permanent loss of
`MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION` headroom** at the admission check `state.rs:573-576`.

Requires a new message to the connection task (a design decision, not an edit). Recorded, not
claimed. Acceptance, if taken: the multi-channel/multi-shard drain test of §Testability item 4.

### Companion issue candidate — sharded channels escape the unique-channel threshold

`unique_channel_count` `:866-868` counts `channel_subs` only, so the
`MAX_UNIQUE_CHANNELS_PER_SHARD` warning never fires for sharded channels (§Problem 1, boxed note).
Out of scope here — fixing it is a policy choice about whether the two kinds share a ceiling.
Recorded so it is not lost.

**Security policy note:** no security-classified finding arose in this lane, and rev 2's review
raised none. Per standing policy, had one arisen it would be recorded here only, not fixed.

## Revision ledger

Rev 2, after adversarial review (verdict **AMEND**). Every item below was re-verified against the
tree at HEAD `ee5efee9` before being applied or refuted — the review was treated as fallible.

### Applied

| # | Finding | What changed |
|---|---|---|
| **B1** | Step D cannot hold a read-only stance on `pubsub_conn_command.rs` — the six `PubSubMsg` registration variants are constructed only in the `SubKindSpec` closures `:108`/`:114`/`:130`/`:136`/`:152`/`:158`, and the fold also forces `message.rs:1044-1051` | **Verified.** Files table row rewritten; step D's full file set tabulated in §Effort (six files, two crates, one script) and sized `S–M` → **M**; the 68 edge declared **step-D-only** with A/B/C confirmed genuinely disjoint; Gate 2 re-argued from what the edit *does* (introduces no label literal — conclusion survives); the 90 no-conflict claim re-argued from merits (no `CommandSpec` static, no `frogdb-commands` file — both re-verified). Dedup case strengthened accordingly |
| **B2** | The "vs 68 (PubSubKind)" framing misreads 68; `SubKind { Channel, Pattern, Sharded }` already exists at `state.rs:291` | **Verified.** 68 is ExecFraming; its `PubSubKind` `:722-734` is a nine-variant command-identity enum ruled private at 68:450. §The message fold rewritten: **hoist the existing `SubKind` into `frogdb-core`** instead of inventing `SubscriptionKind`; the enum-ordering dependency on 68 dissolves entirely (the file edge remains) |
| **B3** | "Test coverage: zero" on the drain is false | **Verified.** `integration_pubsub.rs:1277` is a 3-node test asserting the full frame incl. `Integer(0)` at `:1394-1398`. §Problem 4 and §Testability item 4 reframed to **degenerate** coverage (one channel, one subscriber, count 0 under both authorities, stale state never observed) — a more damning framing. Acceptance = multi-channel/multi-shard drain test + existing test pinned green |
| **B4** | H1's "delete the three tests" destroys real coverage | **Verified.** `:1323-1330` sit in `test_remove_connection` `:1307-1332`, the only unit test of the method step A rewrites; `:1215`/`:1220` sit in `test_psubscribe_punsubscribe`, which also asserts duplicate rejection at `:1209-1210`. H1 narrowed to "four fns + six assert lines", with a required rewrite/reorder. **"Independent of everything above" withdrawn** |
| **B5** | H1's caller table is mis-attributed | **Verified by `rg`.** Corrected to `get_connection_channels` → `:1323`,`:1329`; `_patterns` → `:1324`; `_sharded_channels` → `:1325`; `pattern_count` → `:1215`,`:1220`. Propagated into §Testability item 2 and H1 |
| **B6** | DRAIN measurement models ~819 slot migrations, not one | **Verified** (0.05 × 16384 = 819). Single-slot numbers added (1.4 ms @ S=50k, 5.9 ms @ S=100k) and independently reproduced analytically via the S²/16384 model; rev-1 figures relabelled **AGGREGATE**; honest claim restated as "one migration is cheap, a full resharding is Θ(S²)"; noted H4's correctness claim is cost-independent |
| **H4 broadening** | Half (b) bites at any shard count | **Verified.** Only removals from `ConnectionState.pubsub.sharded_subscriptions` repo-wide are `state.rs:635` and `:668`; delivery arm `connection.rs:700-754` never touches state. §Problem 4 and H4 now state (a) `num_shards>=2` and (b) **any shard count incl. default 1**, with inflated counts + permanent headroom loss at `state.rs:573-576` |
| **N1** | 946× assumes batch = 1 | **Verified** — the recount runs once per handler (`shard/pubsub.rs:24`,`:49`,`:74`), not per channel. Stated as Θ(n²/2B); batch-1 assumption made explicit |
| **N2** | 946× is a harness-total ratio | **Applied.** Honest headline restated as 70–126 µs of shard-event-loop CPU per command at the ceiling |
| **N3** | "six pairs" / "six identical variants" | **Verified.** Five pairs + one intra-method duplication; six variants = **two groups of three** (the `sender` field), hence the fold target is two, not one |
| **N4** | `handle_introspection` arm count | **Verified.** Five arms, four forming two pairs, `NumPat` unpaired |
| **N5** | Unclaimed win: `unique_pattern_count` | **Verified.** Three production callers beyond the threshold check (`shard/pubsub.rs:120` PUBSUB NUMPAT, `dispatch_search.rs:28`, `diagnostics.rs:408`), each paying an O(P) build+alloc. Now **claimed** in §Problem 3 and §Effort |
| **N6** | The 100k ceiling is warning-only | **Verified** — no admission check exists. Benchmark restated as a floor |
| **N7** | `unique_channel_count` ignores sharded channels | **Verified** (`self.channel_subs.len()`). Recorded as an out-of-scope companion issue candidate, twice (§Problem 1 box, §Hotfixes) |
| **N8** | Threshold hand-off precision | **Verified.** `MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD = 1_000_000` (`:282`) → 900k needed, never reached in the measured scenario; patterns hand off at 9k. Stated precisely |
| **N10** | A+B must not read as closing H4 | **Applied** as §Behaviour changes item 4, in the strongest available terms |
| **Off-by-one cites** | Eight line-number corrections | **All re-derived.** remaining-scan `:683-687` (§P4 + H4 now agree); `psubscribe` `:591-602`; `PubSubSender::send` `:165-197`, drop branch `:168-189`; `MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION` `:279`; `MAX_UNIQUE_PATTERNS_PER_SHARD` `:288`; `state.rs` `remove_subscription` `:624-639`; delivery arm `:700-754`; `keyspace_coordinator` `local.publish(` at `:98` (fn at `:94`) |
| **85 cross-constraint** | PN11 targets the same impl block; six probe names are on a documented byte-stable surface | **Verified** against 85 and `message.rs:1003-1005`. §The message fold now requires an explicit choice between **(i)** preserving all six names via the `DriveTick` payload-arm idiom (`message.rs:1020-1022`) — recommended — and **(ii)** owning the rename as a documented behaviour change; §Behaviour changes item 5; a `vs. proposal 85` section records the trade that option (i) costs PN11 one enum |

### Found in rev 2, beyond the review

| Finding | Detail |
|---|---|
| **The blanket FM-clearance sentence needed narrowing** | Rev 1 claimed "no FM tag on any touched file". Once step D touches `server/src/connection/state.rs` (B2's hoist), that is false — it carries 12 `FM-TXN-*` tags. All are inside `mod tests` `:1114`+ and all force transaction behaviour, 800+ lines from step D's `:291-299`; `pubsub_conn_command.rs` has none. Claim narrowed to **"no FM-tagged region is edited"**, with the evidence, in §Spec / LOCKED clearance. `just lint-failure-modes` remains unaffected |
| **A third seam lint is in scope, and it does not pass by construction** | `scripts/continuation-lock-gate.py:81` pins `("PubSubMsg", 11)` and checks enum parity in both directions. Step D's 6→2 fold takes both the variant count and the arm count to 7 and **fails `just lint-gates`** unless the pin is updated in the same commit. Added as **Gate 3** with a binding constraint, added to the header and the Files table, and flagged in §Effort. Checked and clear on the harder half: **no `PubSubMsg::*` arm is pinned in `GATE`/`EXEMPT`/`GATE_GAP`**, so no continuation-lock disposition changes and Rule 8b is unaffected |

### Refuted

| Claim | Evidence against |
|---|---|
| B2's *"second copy [of `SubKind`] at `testing/src/pubsub_oracle.rs:212`"* | **Not a copy — a name collision.** `frogdb_testing::SubKind` is a **two**-variant, payload-carrying enum (`Channel(Bytes)`, `Pattern(Bytes)`, **no `Sharded`**) documented as *"what a subscription covers"*. It answers a different question from `state.rs`'s three-variant marker and **cannot consume the hoisted enum**. Consequence for the plan: the hoist has **two** consumers (shard side + connection side), not three, and it puts a second public `SubKind` in the workspace — a naming hazard worth a `use` alias if any file ever needs both (`tests/common/pubsub_runner.rs:21` imports the testing one today; `guards.rs:1449` already spells the connection one fully-qualified) |
| B1's *"constructed in exactly one place"* (as an absolute) | True **for production**. There is one further construction at `dispatch_pubsub.rs:155`, inside a test in a file step D already owns. Does not change the conclusion; recorded so the fold's grep does not surprise anyone |
| The review's cite *"`:669` exit_pubsub"* | `exit_pubsub` is `state.rs:667-670`; the wholesale reset (`self.pubsub = PubSubState::default()`) is at **`:668`**. Corrected in §Problem 4 and H4 |
| The review's *"MUST land after 68"* as a merge-order **ruling** | The constraint is real but this proposal does not rule it — per standing policy, §Risks now **presents three options with costs** (68→D recommended; D→68 workable; A/B/C-only removes the edge) for the orchestrator to decide. Evidence on which the options rest: 68's hunks (`:739-760`, `:767-804`, `:965-1005`) and step D's (`:77-165`, `:392`, `:445`) are 600+ lines apart, so the edge is ownership and rebase cost, **not** an overlapping-hunk conflict |

### Withdrawn

| Claim (rev 1) | Why |
|---|---|
| *"`pubsub_conn_command.rs` — **READ-ONLY**"* (Files table, and the stance-based clearances built on it) | False for step D (B1). Replaced by a per-step table; A/B/C keep the read-only stance, verified |
| *"As long as both hold their stated stance the two are **disjoint** — no shared edited file"* (vs 68) | False for step D. Replaced by an explicit edge + ordering options |
| *"there is **no conflict edge** [with 90] — by construction of the read-only stance"* | The **conclusion stands**, the **argument is void**. Re-derived from the merits: no `CommandSpec` static and no `frogdb-commands` file is edited by any step |
| *"if 68 lands a shared `PubSubKind` … the message fold should reuse it"* and the whole `SubscriptionKind` proposal | Wrong axis and a third enum (B2). Replaced by hoisting the existing `SubKind` |
| *"**Test coverage: zero**"* on `drain_sharded_channels_for_slot` | False (B3). Replaced by "degenerate" |
| *"delete … the three tests that exist only to call them"* | Destroys the only unit test of `remove_connection` (B4) |
| *"Not one line, but purely subtractive and **independent of everything above**"* (H1) | Coupled to step A via `test_remove_connection` (B4) |
| *"**two seam lints in scope** — both preserved by construction"* | Three, and the third needs an explicit edit (rev-2 finding) |
| The `DRAIN 5% of S` figures **presented as per-migration cost** | Relabelled AGGREGATE; single-slot figures added (B6) |
| *"six byte-identical method pairs"* / *"six identical message variants"* / *"five arms, three of them pairwise duplicates"* | Five pairs + one intra-method; two groups of three; four arms in two pairs plus an unpaired `NumPat` (N3, N4) |

### Confirmed as-written by the review — not weakened in rev 2

H2's oracle fix (verbatim-applicable); H3's deferral into step A; **Gate 1's dead-gate trap, the
spelling-based mitigation and the *positive* acceptance check** (the review named this the
proposal's strongest contribution); the full LOCKED/FM clearance; the `TrackingTable` precedent;
`ChannelTable` passing the deletion test; the `PatternTable` hedge; step C's carve-out; and the
core thesis (recount = root cause), independently reproduced at 941×.
