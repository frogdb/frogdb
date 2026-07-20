# Proposal 01 — Make the Scatter-Gather declarative seam load-bearing

## Summary (2-3 sentences)

Six commands declare `strategy: ExecutionStrategy::ScatterGather { merge: MergeStrategy::… }`
in their `CommandSpec`, but nothing ever reads that declaration to decide how to scatter. The
actual routing decision is a hardcoded `match cmd_name { "MGET" => …, "MSET" => … }` string
table in `connection/routing.rs`, and the `merge` field is dead — read nowhere. This proposal
makes the spec the single source of truth for cross-shard routing so that adding a scatter
command is one declaration a missing arm turns into a compile error, mirroring the already-clean
`ServerWideOp` design.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/commands/src/string.rs` | 1 (excerpt) | Declares MGET (`OrderedArray`, L778), MSET (`AllOk`, L828) |
| `frogdb-server/crates/commands/src/basic.rs` | — | Declares DEL (`SumIntegers`, L775), EXISTS (`SumIntegers`, L839) |
| `frogdb-server/crates/commands/src/generic.rs` | — | Declares TOUCH (`SumIntegers`, L231), UNLINK (`SumIntegers`, L272) |
| `frogdb-server/crates/core/src/command.rs` | 1676 | Defines `ExecutionStrategy::ScatterGather { merge }` (L87-90) and the dead `MergeStrategy` enum (L170-189) |
| `frogdb-server/crates/server/src/connection/routing.rs` | 345 | The hardcoded string match (`route_and_execute`, L129-145) |
| `frogdb-server/crates/server/src/connection/guards.rs` | — | The *only* place `ScatterGather` is matched (L473-476, cluster-exemption) |
| `frogdb-server/crates/server/src/scatter/mod.rs` | 128 | `strategy_for_op` `ScatterOp → Box<dyn ScatterGatherStrategy>` match (L85-128) |
| `frogdb-server/crates/server/src/scatter/strategies.rs` | 393 | 6 `ScatterGatherStrategy` impls (MGet/MSet/Del/Exists/Touch/Unlink) |
| `frogdb-server/crates/server/src/scatter/broadcast.rs` | 923 | Sibling broadcast path; also defines a *trait* named `MergeStrategy` (name collision) |
| `frogdb-server/crates/server/src/scatter/executor.rs` | 193 | `ScatterGatherExecutor` (VLL-locked keyed path) |
| `frogdb-server/crates/core/src/shard/message.rs` | 852 | `ScatterOp` enum (L714) |
| `frogdb-server/crates/core/src/shard/execution.rs` | 1714 | `execute_scatter_part` giant match (L567-770) |
| `frogdb-server/crates/frogdb-macros/src/command.rs` | — | `#[command(scatter_gather)]` emits `ScatterGather { merge: Custom }` (L341-347) |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | 777 | The *contrasting* clean design: `dispatch_server_wide` exhaustive match (L172-208) + uniqueness test (L760) |

## Problem (concrete verified evidence)

**The declaration says one thing; routing does another.** Each of the six cross-shard commands
carries an explicit, typed intent in its `CommandSpec`. For example MGET
(`commands/src/string.rs:778`):

```rust
strategy: ExecutionStrategy::ScatterGather {
    merge: MergeStrategy::OrderedArray,
},
```

and DEL (`commands/src/basic.rs:775`):

```rust
strategy: ExecutionStrategy::ScatterGather { merge: MergeStrategy::SumIntegers, },
```

But `route_and_execute` in `connection/routing.rs` never asks the registry for the strategy.
When keys span shards it re-derives everything from the command *name string*
(`routing.rs:128-145`):

```rust
// Determine the scatter operation based on command name
let scatter_op = match cmd_name {
    "MGET" => Some(ScatterOp::MGet),
    "MSET" => {
        let pairs: Vec<(Bytes, Bytes)> = cmd
            .args
            .chunks(2)
            .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
            .collect();
        Some(ScatterOp::MSet { pairs })
    }
    "DEL" => Some(ScatterOp::Del),
    "EXISTS" => Some(ScatterOp::Exists),
    "TOUCH" => Some(ScatterOp::Touch),
    "UNLINK" => Some(ScatterOp::Unlink),
    _ => None,
};

match scatter_op.and_then(|op| strategy_for_op(&op)) {
    Some(strategy) => { /* … build executor, run … */ }
    None => {
        // Command doesn't support scatter-gather
        redirect::crossslot()
    }
}
```

**The `merge` field is read nowhere.** A workspace grep for `ExecutionStrategy::ScatterGather`
outside the literal spec declarations and the macro finds exactly one consumer — and it uses
`{ .. }`, throwing the field away. That is `is_cluster_exempt` in `connection/guards.rs:473-476`:

```rust
matches!(
    entry.execution_strategy(),
    ExecutionStrategy::ConnectionLevel(_)
        | ExecutionStrategy::ScatterGather { .. }
        | ExecutionStrategy::ServerWide(_)
)
```

So the *variant tag* is read once, as a single boolean ("is this command cluster-exempt?"). The
`merge: MergeStrategy` payload — the whole point of the declaration — is read by nobody. The real
merge behavior is selected instead by `strategy_for_op` (`scatter/mod.rs:85`) keyed off
`ScatterOp`, which was in turn produced by the string match. The declared
`MergeStrategy::OrderedArray` for MGET and the live `MGetStrategy::merge` are two independent
copies of the same fact with no compiler link between them.

**Adding one cross-shard command touches ~6 edit sites across 3 crates, none compiler-linked:**

1. `core/src/shard/message.rs` — add a `ScatterOp` variant (enum at L714).
2. `core/src/shard/execution.rs` — add an arm to `execute_scatter_part` (match at L573).
3. `server/src/scatter/strategies.rs` — write a new `ScatterGatherStrategy` impl.
4. `server/src/scatter/mod.rs` — add an arm to `strategy_for_op` (L85).
5. `server/src/connection/routing.rs` — add an arm to the `match cmd_name` string table (L129).
6. `commands/src/*.rs` — declare `ScatterGather { merge: … }` on the spec.

Forget step 5 and there is **no build error**. The command's keys span shards, the string match
falls through to `_ => None`, `strategy_for_op` is never consulted, and the client silently gets
`redirect::crossslot()` (a `-CROSSSLOT` error) instead of a scattered result — even though the
command's spec proudly declares it is a `ScatterGather` command. The failure is a runtime wrong
answer discoverable only by a booted multi-shard integration test that happens to exercise
cross-shard keys for that command.

## Why it is shallow/fragmented (architecture vocabulary)

**The seam is dead (deletion test).** Delete the `merge: MergeStrategy` field from
`ExecutionStrategy::ScatterGather` and the only breakage is the six literal initializers plus the
macro at `frogdb-macros/src/command.rs:343` — no *behavior* changes, because no code path reads
it. A field whose deletion removes zero behavior is not an interface; it is decoration. By
Ousterhout's test this is a classic **shallow** module: the `ScatterGather { merge }` interface
carries a `MergeStrategy` enum (six variants: `OrderedArray`, `SumIntegers`, `CollectKeys`,
`CursoredScan`, `AllOk`, `Custom`) but hides no implementation behind it — the interface is
larger than the (nonexistent) behavior it gates.

**Two adapters that never meet.** The declarative side (`ExecutionStrategy::ScatterGather` +
`MergeStrategy` enum, in `core`) and the operational side (`ScatterOp` + `ScatterGatherStrategy`
trait + `strategy_for_op`, in `server`) are two adapters purporting to satisfy the same
seam — "how does a scatter command merge?" — but they are wired to *nothing in common*. The
string match in `routing.rs` is the only bridge, and it bridges by re-typing the command name,
not by reading the spec. The `CommandSpec` premise — "const declarative facts the dispatcher
derives behavior from" (`frogdb-server/CONTEXT.md`) — is contradicted here: the dispatcher
derives scatter behavior from a string, not from the fact.

**Even the vocabulary is duplicated.** `core/src/command.rs:170` defines
`pub enum MergeStrategy` (the dead declarative one) while `scatter/broadcast.rs:45` defines
`pub trait MergeStrategy` (the live one, for the *broadcast* path). Same name, different crates,
zero relationship — a symptom of the same fact being modeled twice with no single seam.

**Locality is poor.** The knowledge "MGET is a cross-shard command that merges as an ordered
array" is smeared across `string.rs` (spec), `routing.rs` (string arm), `message.rs`
(`ScatterOp::MGet`), `mod.rs` (`strategy_for_op` arm), `strategies.rs` (`MGetStrategy`), and
`execution.rs` (`execute_scatter_part` arm). None reference each other; consistency is maintained
by hand.

**The contrast proves it is fixable.** `ServerWideOp` (`core/src/command.rs:109`) models the
*same shape* of problem — "fan a command across all shards" — but correctly. The command declares
`ExecutionStrategy::ServerWide(ServerWideOp::DbSize)` and `dispatch.rs:514-525` reads that
declaration to route. The exhaustive `match op { … }` in `dispatch_server_wide`
(`dispatch.rs:172-208`) means **adding a `ServerWideOp` variant without a handler arm is a
compile error** (documented at `dispatch.rs:158-162`). A spec-level unit test,
`server_wide_ops_are_unique_per_command` (`dispatch.rs:760`), catches the opposite mistake. That
is a deep seam: one declaration, compiler-enforced completeness. The scatter path is the shallow
version of the exact same idea.

## Proposed change (plain English)

Make routing derive the scatter operation from the spec, and give the spec a payload rich enough
that no second table is needed.

1. **Replace the dead `MergeStrategy` enum payload with a typed scatter identity**, mirroring
   `ServerWideOp`. Introduce `ScatterGatherOp` (a flat, `Copy` identity enum: `MGet`, `MSet`,
   `Del`, `Exists`, `Touch`, `Unlink`) in `core`, and change the strategy to
   `ExecutionStrategy::ScatterGather(ScatterGatherOp)`. This is the *single seam* at which a
   scatter command is declared.
2. **Delete the `match cmd_name` string table** in `routing.rs`. `route_and_execute` reads the
   registry entry's strategy; if it is `ScatterGather(op)`, it hands `op` (plus `cmd.args`) to a
   single `dispatch_scatter(op, args)` method whose body is an **exhaustive `match op`** — exactly
   like `dispatch_server_wide`. A missing arm is now a compile error, not a silent `-CROSSSLOT`.
3. **Fold `ScatterOp`-selection into that one match.** The `strategy_for_op` indirection in
   `scatter/mod.rs` (which re-dispatches on `ScatterOp`) collapses: the exhaustive `match op`
   directly constructs the `ScatterGatherStrategy` (e.g. `MGetStrategy`) and builds any
   arg-derived payload (the MSET pair-chunking that currently lives inline in `routing.rs:131-138`
   moves into `MSetStrategy` construction or the match arm).
4. **Keep the keyed `ScatterOp` enum** (`message.rs`) — it is still the shard-worker wire
   message. But it is no longer *routing's* selection key; it becomes an implementation detail of
   each `ScatterGatherStrategy::scatter_op()`, which already exists (`strategies.rs:74`).

Net: the count of edit sites for a new scatter command drops from ~6 (uncoordinated) to a smaller
set that is *compiler-linked* — declare the spec op, add the exhaustive-match arm (forced by the
compiler), write the strategy. The `ScatterOp` wire variant + `execute_scatter_part` arm remain
(they are genuinely the shard-side implementation), but a forgotten routing arm can no longer
compile.

## Before / After

### Before — routing re-derives from the command name string (`routing.rs:128-173`, real code)

```rust
// Determine the scatter operation based on command name
let scatter_op = match cmd_name {
    "MGET" => Some(ScatterOp::MGet),
    "MSET" => {
        let pairs: Vec<(Bytes, Bytes)> = cmd
            .args
            .chunks(2)
            .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
            .collect();
        Some(ScatterOp::MSet { pairs })
    }
    "DEL" => Some(ScatterOp::Del),
    "EXISTS" => Some(ScatterOp::Exists),
    "TOUCH" => Some(ScatterOp::Touch),
    "UNLINK" => Some(ScatterOp::Unlink),
    _ => None,
};

match scatter_op.and_then(|op| strategy_for_op(&op)) {
    Some(strategy) => {
        let executor = ScatterGatherExecutor::new( /* … */ );
        executor.execute(strategy.as_ref(), &cmd.args).await
    }
    None => {
        // Command doesn't support scatter-gather
        redirect::crossslot()
    }
}
```

The spec's declared `merge: MergeStrategy::OrderedArray` (MGET) is ignored; `cmd_name == "MGET"`
is what actually drives the decision. **Adding `SMISMEMBER` as a scatter command and forgetting
this arm compiles fine and silently `-CROSSSLOT`s.**

### After — routing reads the spec; completeness is compiler-enforced (illustrative sketch)

`route_and_execute` stops string-matching and reads the strategy:

```rust
// Keys span shards and cross-slot is allowed. Derive the scatter op from the
// command's declared strategy — the single source of truth — not its name.
let op = match handler.spec().strategy {
    ExecutionStrategy::ScatterGather(op) => op,
    // Not a scatter command: no cross-shard plan exists.
    _ => return redirect::crossslot(),
};
return self.dispatch_scatter(op, &cmd.args).await;
```

```rust
/// The single dispatch point for keyed cross-shard commands. Exhaustive: adding
/// a `ScatterGatherOp` variant without an arm here is a COMPILE ERROR, so a new
/// scatter command can never silently fall through to `-CROSSSLOT`.
async fn dispatch_scatter(&self, op: ScatterGatherOp, args: &[Bytes]) -> Response {
    let strategy: Box<dyn ScatterGatherStrategy> = match op {
        ScatterGatherOp::MGet   => Box::new(MGetStrategy),
        ScatterGatherOp::MSet   => Box::new(MSetStrategy::from_args(args)), // pair-chunking moves here
        ScatterGatherOp::Del    => Box::new(DelStrategy),
        ScatterGatherOp::Exists => Box::new(ExistsStrategy),
        ScatterGatherOp::Touch  => Box::new(TouchStrategy),
        ScatterGatherOp::Unlink => Box::new(UnlinkStrategy),
    };
    let executor = ScatterGatherExecutor::new(/* … unchanged … */);
    executor.execute(strategy.as_ref(), args).await
}
```

And the spec collapses to one load-bearing token (was `ScatterGather { merge: OrderedArray }`):

```rust
strategy: ExecutionStrategy::ScatterGather(ScatterGatherOp::MGet),
```

### Adding a NEW scatter command (e.g. `SMISMEMBER`-style), file/edit count

| Step | Before | After |
| --- | --- | --- |
| Declare on `CommandSpec` | edit `commands/src/*.rs` (pick a `merge` variant that is never read) | edit `commands/src/*.rs` (`ScatterGather(ScatterGatherOp::New)`) |
| Add routing selection | **edit `routing.rs` string match** — *silently optional* | add `ScatterGatherOp::New` variant → **compiler forces** the `dispatch_scatter` arm |
| Select the merge strategy | edit `scatter/mod.rs` `strategy_for_op` | folded into the forced `dispatch_scatter` arm (no separate table) |
| Write the strategy | `scatter/strategies.rs` | `scatter/strategies.rs` (unchanged) |
| Add wire op + shard exec | `message.rs` + `execution.rs` | `message.rs` + `execution.rs` (unchanged) |
| **Forgotten-arm failure mode** | **silent `-CROSSSLOT` at runtime** | **`error[E0004]: non-exhaustive patterns` at build** |

Before: ~6 files, 3 crates, and the one arm that matters most (routing) is the one with no safety
net. After: the routing decision is the one the compiler guarantees you cannot forget, and the
`strategy_for_op` table is deleted (one fewer uncoordinated match).

## Testability improvement

**Today, the routing decision is only exercisable end-to-end.** The `ScatterGatherStrategy` impls
*are* unit-tested in isolation (`scatter/strategies.rs:304-393`: `test_mget_partition`,
`test_mget_merge_preserves_order`, `test_del_merge_sums`, `test_mset_partition_distributes_pairs`)
— those test partition/merge without a socket. But there is **no test that a command's declared
strategy actually routes to the right scatter path**, because that link is a `cmd_name` string
match buried inside `route_and_execute`, which needs a booted multi-shard `ConnectionHandler` with
live `shard_senders`, a timeout, a metrics recorder, and (under `turmoil`) a chaos config — i.e. a
full TCP server with keys deliberately placed on different shards. A forgotten arm is caught, if
at all, only by such an integration test.

**After the change, two things become plain unit tests**, mirroring the ServerWide design:

1. **A registry-level exhaustiveness/uniqueness test**, a direct copy of
   `server_wide_ops_are_unique_per_command` (`dispatch.rs:760`):

   ```rust
   #[test]
   fn scatter_gather_ops_are_unique_per_command() {
       let mut registry = CommandRegistry::new();
       crate::register_commands(&mut registry);
       let mut seen: Vec<(ScatterGatherOp, String)> = Vec::new();
       for (name, entry) in registry.iter() {
           if let ExecutionStrategy::ScatterGather(op) = entry.execution_strategy() {
               assert!(!seen.iter().any(|(o, _)| *o == op),
                   "ScatterGatherOp::{op:?} declared by two commands");
               seen.push((op, name.to_string()));
           }
       }
   }
   ```

2. **The completeness half is free** — the exhaustive `match op` in `dispatch_scatter` makes a
   missing handler a compile error, exactly as documented for `dispatch_server_wide`
   (`dispatch.rs:158-162`). No test needed for the case that today silently `-CROSSSLOT`s.

The socket-free strategy tests in `strategies.rs` stay as-is; they just stop being the *only*
thing standing between a spec typo and a wrong answer.

## Risks / open questions

- **`is_cluster_exempt` (`guards.rs:473`) reads the variant tag.** Changing `ScatterGather { merge }`
  to `ScatterGather(op)` keeps the `{ .. }` / `(_)` match working, but the migration must update
  that arm. Low risk (one line), noted for completeness — this is the one genuinely load-bearing
  read of the variant today.
- **`MergeStrategy` enum deletion.** Removing the dead `core::command::MergeStrategy` enum
  (L170-189) is the clean end state, but confirm nothing outside the six specs + macro references
  it (grep suggests not). Keeping it as `#[deprecated]` for one step is an option.
- **The macro path** (`frogdb-macros/src/command.rs:341-347`) emits
  `ScatterGather { merge: Custom }` for `#[command(scatter_gather)]`. No current command uses that
  attribute for the six scatter commands (they hand-write the spec), but the macro must be updated
  in lockstep or it will fail to compile against the new variant shape.
- **`ScatterOp` stays.** This proposal does *not* merge `ScatterGatherOp` (routing identity) with
  `ScatterOp` (shard wire message) — the broadcast/server-wide `ScatterOp` variants
  (`Keys`, `DbSize`, `FtSearch`, …) are a superset and travel a different path. Conflating them is
  a larger, separate refactor; keeping them distinct is deliberate and lower-risk.
- **Behavioral parity.** The MSET pair-chunking currently inline in `routing.rs:131-138` must move
  verbatim into `MSetStrategy` construction; a subtle change there would alter MSET semantics.
  Covered by existing MSET tests but worth a careful diff.

## Effort estimate

**M.** The mechanical core is small and compiler-guided: add `ScatterGatherOp` (~6 variants),
change one `ExecutionStrategy` variant, rewrite ~45 lines of `routing.rs` into a `dispatch_scatter`
match, delete `strategy_for_op`, and update 6 spec declarations + the macro. The compiler drives
the migration (every changed site errors until fixed). It is not S because it spans 3 crates
(`core`, `commands`, `server`) and touches the cluster-exemption read and the proc-macro, and
because the MSET arg-derivation move needs care to preserve semantics. No shard-side
(`execution.rs`) or `ScatterGatherStrategy` impl changes are required, which keeps it out of L.
