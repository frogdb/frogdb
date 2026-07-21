# Proposal 11 — Make the ShardMessage category partition compiler-enforced via per-category sub-enums

## Summary (2-3 sentences)

`ShardMessage` is a flat 60-variant enum. `dispatch_message` (`event_loop.rs:217-303`) partitions
those 60 variants into 10 category groups and forwards each group to a `dispatch_*` method; each of
the 10 methods then *re-matches* its own subset and ends `_ => unreachable!()`. The partition
("every variant is handled by exactly one sub-dispatcher") is a fact stated in two independent
places — the top-level grouping and each sub-match — and their agreement is enforced only by a
runtime panic, not the compiler; this proposal wraps each category in its own exhaustive sub-enum
so a miscategorized or unhandled variant is a build error instead of an `unreachable!()` at runtime.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/core/src/shard/message.rs` | 845 | Defines the flat `ShardMessage` enum (60 variants, L105-635), `probe_type_str` exhaustive match (L639-702), and the separate `ScatterOp` enum (L707-end) |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 473 | `dispatch_message` — the top-level 10-way partition (match at L219-302), exhaustive, no wildcard |
| `frogdb-server/crates/core/src/shard/dispatch_core.rs` | 87 | 4 arms (Execute, ScatterRequest, GetVersion, ExecTransaction) + `_ => unreachable!()` (L83) |
| `frogdb-server/crates/core/src/shard/dispatch_pubsub.rs` | 111 | 11 arms + `_ => unreachable!()` (L108) |
| `frogdb-server/crates/core/src/shard/dispatch_scripting.rs` | 117 | 8 arms + `_ => unreachable!()` (L113) |
| `frogdb-server/crates/core/src/shard/dispatch_observability.rs` | 134 | 18 arms + `_ => unreachable!()` (L131) |
| `frogdb-server/crates/core/src/shard/dispatch_vll.rs` | 41 | 5 arms + `_ => unreachable!()` (L37) |
| `frogdb-server/crates/core/src/shard/dispatch_tracking.rs` | 36 | 3 arms + `_ => unreachable!()` (L33) |
| `frogdb-server/crates/core/src/shard/dispatch_blocking.rs` | 24 | 2 arms + `_ => unreachable!()` (L21) |
| `frogdb-server/crates/core/src/shard/dispatch_debug_introspection.rs` | 30 | 4 arms + `_ => unreachable!("…")` (L27) |
| `frogdb-server/crates/core/src/shard/dispatch_cluster.rs` | 27 | 2 arms + `_ => unreachable!()` (L23) |
| `frogdb-server/crates/core/src/shard/dispatch_search.rs` | 30 | 2 arms (FlushSearchIndexes, GetPubSubLimitsInfo) + `_ => unreachable!()` (L27) |
| `frogdb-server/crates/core/src/shard/mod.rs` | — | Declares the 10 `dispatch_*` modules (L33-42) |
| `frogdb-server/crates/core/src/shard/vll.rs` | — | `handle_vll_abort` (L58-60): the pure pass-through target example |

Variant count and category tally are exact (from the exhaustive `probe_type_str` match, L639-702):
**60 variants**, partitioned as core 4, pubsub 11, scripting 8, observability 18, vll 5,
debug-introspection 4, tracking 3, blocking 2, cluster 2, search 2, plus `Shutdown` handled inline
in `dispatch_message` = 59 + 1 = 60.

## Problem (concrete verified evidence)

**The category partition is asserted twice and reconciled by hand.** `dispatch_message`
(`event_loop.rs:219-302`) is one flat match that groups the 60 variants and forwards each group:

```rust
async fn dispatch_message(&mut self, msg: ShardMessage) -> bool {
    use ShardMessage::*;
    match msg {
        Execute { .. } | ScatterRequest { .. } | GetVersion { .. } | ExecTransaction { .. } => {
            self.dispatch_core(msg).await
        }
        Subscribe { .. } | Unsubscribe { .. } | PSubscribe { .. } | /* …11 total… */
        | ConnectionClosed { .. } => { self.dispatch_pubsub(msg); false }
        // …8 more category groups…
        SlotMigrated { .. } | RaftCommand { .. } => self.dispatch_cluster(msg).await,
        FlushSearchIndexes { .. } | GetPubSubLimitsInfo { .. } => { self.dispatch_search(msg); false }
        Shutdown => { /* … */ true }
    }
}
```

Then each `dispatch_*` method restates the *same* subset as its own match and closes with a
wildcard panic. Every one of the 10 sub-dispatchers ends this way — verified: `dispatch_core.rs:83`,
`dispatch_pubsub.rs:108`, `dispatch_scripting.rs:113`, `dispatch_observability.rs:131`,
`dispatch_vll.rs:37`, `dispatch_tracking.rs:33`, `dispatch_blocking.rs:21`,
`dispatch_debug_introspection.rs:27`, `dispatch_cluster.rs:23`, `dispatch_search.rs:27`. For
example `dispatch_vll.rs`:

```rust
match msg {
    ShardMessage::VllLockRequest { .. } => { /* … */ }
    ShardMessage::VllExecute { .. }     => { /* … */ }
    ShardMessage::VllAbort { txid }     => { self.handle_vll_abort(txid); }
    ShardMessage::VllContinuationLock { .. } => { /* … */ }
    ShardMessage::GetVllQueueInfo { .. }     => { /* … */ }
    _ => unreachable!(),
}
```

**The two lists must agree, and only a panic notices when they don't.** The compiler *does* force
`dispatch_message` and `probe_type_str` (both wildcard-free, so exhaustive over the 60 variants) to
mention every variant. What the compiler does **not** check is that the category a variant is
routed to in `dispatch_message` is the same category whose sub-match actually has an arm for it.
Route `MemoryStats` to `self.dispatch_vll(msg)` and it compiles cleanly — `dispatch_vll`'s
`_ => unreachable!()` swallows it into a runtime panic on the first `MemoryStats` message. A
miscategorized route is invisible until the offending message is actually sent on a running shard.

**Adding a variant is a 4-site edit inside `core` alone.** A new variant must be added to: (1) the
`ShardMessage` enum (`message.rs:105`), (2) `probe_type_str` (`message.rs:639`, compiler-forced),
(3) the `dispatch_message` partition (`event_loop.rs:219`, compiler-forced to appear *somewhere*),
and (4) the chosen sub-dispatcher's match arm (**not** compiler-forced — omit it and the top-level
group routes into a `_ => unreachable!()`). Sites 3 and 4 are the fragile pair: the compiler proves
the variant is routed, but not that it is routed to a handler that exists.

**Much of the machinery is pure pass-through.** A large share of the 60 arms are
destructure-and-forward one-liners to an identically-named `handle_*`/`collect_*` method. The
candidate example holds exactly: `dispatch_vll.rs:21` → `handle_vll_abort(txid)` →
`self.vll.abort(txid)` (`vll.rs:58-60`). The same shape recurs across `dispatch_blocking` (2/2 arms
forward to `handle_*`), `dispatch_tracking` (3/3 forward to `self.tracking.*`),
`dispatch_debug_introspection` (4/4 are `send(self.collect_*())`), and most of
`dispatch_observability` (≈15/18 forward to a `self.observability.*`/`collect_*` one-liner). Roughly
40 of the 60 arms carry no logic beyond destructure + forward + `send`; the enum-to-handler wiring
is the bulk of the code, and today none of it is compiler-linked to its category.

**One grouping is visibly ad hoc.** `dispatch_search` (`dispatch_search.rs`) handles exactly
`FlushSearchIndexes` *and* `GetPubSubLimitsInfo` — a search-index flush bundled with a pub/sub
limits query. The two have nothing in common; they are co-located because they were the leftover
variants. That is the partition drifting, with nothing structural to resist it.

## Why it is shallow/fragmented (architecture vocabulary)

**The category is a fact with no type.** "This variant belongs to the VLL category" is real,
load-bearing knowledge — it decides which of 10 methods runs. But that fact has no representation in
the type system: it lives as a hand-maintained coincidence between one arm of `dispatch_message`
and the arm set of a `dispatch_*` match. An invariant enforced by `unreachable!()` is an invariant
the compiler cannot see. By Ousterhout's framing this is a **shallow seam**: the `dispatch_*`
Interface (`fn dispatch_vll(&mut self, msg: ShardMessage)`) takes the *whole* 60-variant
`ShardMessage` and immediately narrows it back down by re-matching, so the Interface is far wider
than the Implementation behind it — the method advertises "I handle any ShardMessage" and then
panics on 55 of them.

**Two adapters for one partition.** The top-level grouping in `dispatch_message` and the per-method
sub-match are two Adapters describing the *same* partition of the variant space, wired to nothing in
common. They are kept consistent by hand, exactly the smell Proposal 01 identifies in the
scatter-gather path ("two adapters that never meet"). Here the shared fact is the category boundary.

**Locality is poor and the deletion test fails softly.** The knowledge "`VllAbort` is a VLL message"
is smeared across `event_loop.rs` (the `VllAbort { .. }` token in the vll group) and
`dispatch_vll.rs` (the `VllAbort { txid }` arm), with no reference between them. Apply the deletion
test to a sub-dispatcher arm: delete the `VllAbort` arm from `dispatch_vll` and the code still
*compiles* — the `_ => unreachable!()` absorbs it. Behavior changes only at runtime, on the first
`VllAbort`, as a panic. A seam whose broken half still compiles is not load-bearing structure; it is
a convention.

**The Leverage is low for the volume of code.** ~637 lines across 10 dispatch files, plus the
60-arm `dispatch_message` and 60-arm `probe_type_str`, exist largely to route a message to its
handler. That is a lot of surface for "call the right `handle_*`", and none of it buys
compile-time completeness of the routing — the one property that would make the surface safe to
extend.

## Proposed change (plain English)

Give each category its own enum and let `ShardMessage` become a thin two-level wrapper, so the
partition the code maintains by hand becomes the enum structure the compiler maintains for free.

1. **Introduce one sub-enum per category**, carrying that category's variants verbatim (same
   fields): `CoreMsg`, `PubSubMsg`, `ScriptingMsg`, `ObservabilityMsg`, `VllMsg`, `TrackingMsg`,
   `BlockingMsg`, `DebugIntrospectionMsg`, `ClusterMsg`, `SearchMsg`. These live in `message.rs`
   (or a small `message/` submodule per category).

2. **Reduce `ShardMessage` to a thin outer enum** wrapping the sub-enums, plus the two genuinely
   cross-cutting variants that are handled in the loop body itself:

   ```rust
   pub enum ShardMessage {
       Core(CoreMsg),
       PubSub(PubSubMsg),
       Scripting(ScriptingMsg),
       Observability(ObservabilityMsg),
       Vll(VllMsg),
       Tracking(TrackingMsg),
       Blocking(BlockingMsg),
       DebugIntrospection(DebugIntrospectionMsg),
       Cluster(ClusterMsg),
       Search(SearchMsg),
       Shutdown,
   }
   ```

3. **`dispatch_message` destructures once** — one arm per category, no variant enumeration:

   ```rust
   match msg {
       ShardMessage::Core(m)               => self.dispatch_core(m).await,
       ShardMessage::PubSub(m)             => { self.dispatch_pubsub(m); false }
       ShardMessage::Vll(m)                => self.dispatch_vll(m).await,
       // …one arm per category…
       ShardMessage::Shutdown              => { /* … */ true }
   }
   ```

4. **Each `dispatch_*` takes its own sub-enum and matches it exhaustively — the `_ => unreachable!()`
   is deleted.** `fn dispatch_vll(&mut self, msg: VllMsg)` matches `VllMsg` with no wildcard. A
   variant added to `VllMsg` without a handler arm is now `error[E0004]: non-exhaustive patterns`,
   and a variant can no longer be routed to the wrong dispatcher because a `PubSubMsg` cannot be
   passed to `dispatch_vll` — the type wouldn't check.

5. **Keep send sites ergonomic with `From` impls.** Provide `impl From<VllMsg> for ShardMessage`
   (etc.) for all 10 sub-enums, so a send site writes
   `sender.send(VllMsg::Abort { txid }.into())` — or, if `ShardSender::send` is made generic over
   `Into<ShardMessage>`, simply `sender.send(VllMsg::Abort { txid })`. This keeps the migration to a
   mechanical variant-path rewrite rather than a re-plumb.

6. **`probe_type_str` stays exhaustive** but delegates to per-sub-enum `probe_type_str` methods, so
   it too gains compiler-enforced completeness per category rather than one flat 60-arm match.

`ScatterOp` (`message.rs:707`) is out of scope — it is the scatter wire payload, a different axis,
and is addressed by Proposal 01.

## Before / After

### Before — the partition is stated twice; agreement is a runtime panic

```rust
// event_loop.rs — top-level partition (compiler forces the variant to appear SOMEWHERE)
VllLockRequest { .. } | VllExecute { .. } | VllAbort { .. }
    | VllContinuationLock { .. } | GetVllQueueInfo { .. } => self.dispatch_vll(msg).await,

// dispatch_vll.rs — the SAME subset, restated; wildcard swallows any mismatch
match msg {
    ShardMessage::VllLockRequest { .. } => { /* … */ }
    // …
    ShardMessage::VllAbort { txid } => { self.handle_vll_abort(txid); }
    // …
    _ => unreachable!(),   // fires at runtime if event_loop routes the wrong thing here
}
```

Add `VllRelease` to the enum, add it to the vll group in `dispatch_message`, forget the
`dispatch_vll` arm: **compiles clean, panics on the first `VllRelease`.**

### After — the partition IS the type; both halves are compiler-checked

```rust
// message.rs
pub enum VllMsg {
    LockRequest { txid: u64, keys: Vec<Bytes>, mode: LockMode, operation: ScatterOp, ready_tx: … },
    Execute { txid: u64, response_tx: … },
    Abort { txid: u64 },
    ContinuationLock { … },
    GetQueueInfo { response_tx: … },
}
impl From<VllMsg> for ShardMessage { fn from(m: VllMsg) -> Self { ShardMessage::Vll(m) } }

// event_loop.rs — one arm, no variant list to keep in sync
ShardMessage::Vll(m) => self.dispatch_vll(m).await,

// dispatch_vll.rs — exhaustive over VllMsg, NO wildcard
match msg {
    VllMsg::LockRequest { txid, keys, mode, operation, ready_tx } =>
        self.handle_vll_lock_request(txid, keys, mode, operation, ready_tx).await,
    VllMsg::Execute { txid, response_tx } => self.handle_vll_execute(txid, response_tx).await,
    VllMsg::Abort { txid } => self.handle_vll_abort(txid),
    VllMsg::ContinuationLock { .. } => { /* … */ }
    VllMsg::GetQueueInfo { response_tx } => { /* … */ }
    // no `_` arm — adding VllMsg::Release without an arm is a COMPILE ERROR
}
```

Add `VllMsg::Release`: `dispatch_vll` fails to compile until you add the arm, and there is no way to
misroute it because `dispatch_vll` only accepts `VllMsg`.

### Adding a NEW shard message, file/edit count

| Step | Before | After |
| --- | --- | --- |
| Declare the variant | edit `message.rs` enum | edit `message.rs` sub-enum (e.g. `VllMsg`) |
| Probe string | edit `probe_type_str` (compiler-forced) | edit sub-enum `probe_type_str` (compiler-forced) |
| Route to a category | edit `dispatch_message` group (compiler-forced to appear) | **nothing** — the wrapper variant already routes the whole sub-enum |
| Handle it | edit the `dispatch_*` match — **silently optional** | edit the `dispatch_*` match — **compiler-forced** (no wildcard) |
| Misroute failure mode | **`unreachable!()` panic at runtime** | **impossible — wrong sub-enum type won't compile** |

## Testability improvement

**Today the partition is only exercisable end-to-end.** There is no unit test that asserts "every
variant is handled by exactly one sub-dispatcher"; the guarantee is the runtime `unreachable!()`,
reachable only by booting a `ShardWorker` and sending each of the 60 message kinds. A miscategorized
route is caught, if at all, by an integration test that happens to send that specific message.

**After the change the guarantee is free and total.** The two failure modes both become compile
errors:

1. *Unhandled variant* — the wildcard-free `match msg { … }` in each `dispatch_*` makes a missing
   arm `error[E0004]`, exactly as `dispatch_server_wide` already achieves for `ServerWideOp`
   (referenced in Proposal 01).
2. *Miscategorized variant* — `dispatch_vll(msg: VllMsg)` cannot be handed a `PubSubMsg`; the
   category boundary is now a type boundary, so the mistake is `error[E0308]: mismatched types`.

No new tests are required for either — both are absorbed by the type system, and ~10 `unreachable!()`
lines (dead runtime assertions) are deleted.

## Risks / open questions

- **There is no current bug.** All 60 variants are correctly categorized today and every send site
  compiles. This is a compile-time-safety and Locality investment, not a fix. The value is realized
  the *next* time a shard message is added or a category is resplit — the honest question is whether
  that churn is frequent enough to justify a wide one-time refactor now.
- **Wide mechanical send-site migration.** `ShardMessage::` is referenced **257** times across the
  workspace; excluding `message.rs` (60, the enum + probe) and the 10 sub-dispatchers + `event_loop`
  (60 match arms), roughly **137** are construction/reference sites — **112 in `frogdb-server`**, 13
  in `crates/debug`, 7 in `core`, 5 in `telemetry`. Every one must change from
  `ShardMessage::VllAbort { txid }` to `VllMsg::Abort { txid }.into()` (or the generic-send form).
  The `From` impls keep each change to a path rewrite, and it is largely `sed`-able per category, but
  it is a genuinely large diff touching four crates. `ScatterRequest` alone (35 sites, mostly in
  `server/src/connection/scatter.rs` and `timeseries_scatter.rs`) dominates the `core`/Core category.
- **No `From` / macro exists today to lean on.** There are currently zero `impl From<…> for
  ShardMessage` and no message-generating macro; the 10 `From` impls are new (but trivial and
  uniform, one line each).
- **Enum size is unchanged (verified design fact, not a motivation).** `ShardMessage` is stored
  **inline (unboxed)** in `Envelope { message: ShardMessage, enqueued_at: Instant }`
  (`message.rs:23-28`), and the mpsc queue buffers `Envelope`s by value. Wrapping into two-level
  sub-enums is size-neutral (a tagged union of the same payloads); it neither helps nor hurts queue
  memory, so this refactor should not be sold as a size optimization. If the largest variant (the
  Execute/EvalScript-class arms with several `Vec`/`Bytes`/`oneshot` fields) is ever a concern,
  boxing a sub-enum arm becomes *easier* after this change, but that is a separate decision.
- **Naming churn.** Variant names shorten (`VllAbort` → `VllMsg::Abort`), which reads better but
  touches every probe string and any log/tracing that interpolates the variant name; keep
  `probe_type_str` outputs stable (`"VllAbort"`) if downstream USDT probe consumers depend on them.
- **`Shutdown` and the two loop-body variants.** `Shutdown` is handled directly in
  `dispatch_message` (flushes WAL, returns `true`); it should stay a top-level `ShardMessage`
  variant, not fold into a category. Confirm no other variant needs the loop's `bool` return
  (only `Shutdown` returns `true` today).

## Effort estimate

**L (large, mechanical, compiler-guided).** The structural core is small and safe — define 10
sub-enums (moving the 60 variants verbatim), reduce `ShardMessage` to 11 wrapper variants, add 10
one-line `From` impls, rewrite `dispatch_message` from a 60-variant partition to an 11-arm
destructure, and delete the 10 `_ => unreachable!()` arms while retyping each `dispatch_*` signature
to its sub-enum. The compiler then drives the rest: every one of the ~137 construction sites errors
until rewritten to the `SubMsg::Variant.into()` form. It is **L rather than M** purely because of
that send-site breadth — 137 sites across four crates (`server`, `debug`, `core`, `telemetry`), with
`ScatterRequest`'s 35 sites and the pubsub/observability families needing individual attention — and
because `probe_type_str` and any tracing that names variants must be kept output-stable. No handler
*logic* changes (the `handle_*`/`collect_*` bodies are untouched), which keeps it out of XL. Best
done as one atomic, per-category-committed sweep so the tree never sits half-migrated.
