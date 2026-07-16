# Proposal: Command Write Outcome as a Return Value

Status: implemented
Date: 2026-07-16

## Problem

`CommandContext` (`frogdb-server/crates/core/src/command.rs:928`) is one struct doing two jobs. It
is the command's **input** — the store handle, shard routing (`shard_senders`, `shard_id`,
`num_shards`), and the cluster/replication/registry references a handler reads — and it is
simultaneously a mutable **out-buffer**: eight deposit fields a handler writes into as a side
channel for everything its `Response` cannot carry. The interface conflates "what the command is
given" with "what the command produced", and the produced half has no type of its own.

The out-buffer half, all on the same struct:

| Deposit field | command.rs | What it carries |
|---|---|---|
| `dirty_delta` | :994 | logical changes, for `rdb_changes_since_last_save` and the WATCH version bump |
| `lazyfreed_delta` | :1000 | objects freed by UNLINK / FLUSHALL ASYNC |
| `keyspace_hits` | :1010 | `LookupSpec::Reported` hit count |
| `keyspace_misses` | :1015 | `LookupSpec::Reported` miss count |
| `write_was_noop` | :1025 | "I verified I changed nothing" — suppresses the whole write-effect pipeline |
| `hll_wal_delta` | :1036 | dense-PFADD register delta, routed to a WAL `Merge` operand |
| `keyspace_events` | :1046 | `EventSpec::Dynamic` events, drained to the notifications effect |
| `script_writes` | :1057 | effective writes recorded by a script's `redis.call`s |

Handlers deposit through methods (`notify_event` command.rs:1108, `take_keyspace_events` :1117,
`record_lookup` :1133) or by direct field assignment. Nothing about the `execute` **interface**
says a command produces these — the signature is `execute(&mut ctx, args) -> Result<Response, _>`,
so the compiler sees only the `Response`. Every other output is a convention the seam must know to
go looking for.

### The seam drains field-by-field, and the discipline is the only guardrail

`execute_command_inner` (`core/src/shard/execution.rs:40`) runs the handler and then hand-rolls an
**eight-way tuple destructure** to pull the deposits back off the context
(`execution.rs:117-142`): it names each field, `.take()`s the two owned ones, and reassembles them.
Suppression is enforced *after* that, purely by seam discipline — `write_was_noop` is honored by the
gate at `execution.rs:183`:

```rust
let meta = if is_write && !write_was_noop {
    Some(WriteCommandMeta { handler, dirty_delta, hll_wal_delta, keyspace_events })
} else {
    None // no-op write: deposits (incl. keyspace_events) silently discarded
};
```

The contract "a `write_was_noop` handler that *also* deposited a keyspace event relies on the seam
throwing the event away" is documented on the field (`command.rs:1043-1045`) and enforced nowhere in
the type system — only by that one `if`. This is the classic shape where **the bug hides in how the
pure function is called**: the handler is pure-ish, but the correctness of no-op suppression,
HLL-delta routing, and event draining lives entirely in the extraction discipline at the call site.

### The extraction discipline is duplicated across every path that runs a handler

The deposit fields are not drained in one place. Each site that calls `handler.execute` re-writes
its own destructure, and each is free to forget a field:

| Drain site | Where | Drains |
|---|---|---|
| single command **and** MULTI/EXEC | `execution.rs:117-142` (`execute_command_inner`) | all 8; transaction reuses it via `execution.rs:360` |
| local script sub-command | `scripting/gate.rs:433-441` (`ScriptInvoker::run_local`) | `write_was_noop`, `dirty_delta`, `hll_wal_delta`, `keyspace_events` |
| cross-shard script sub-command | `shard/scripting.rs:205-215` (`execute_script_sub_command`) | same four |

Add a ninth deposit field tomorrow and **the compiler will not object** at any of these three sites:
each destructure lists the fields it happens to know about, so a new one is simply not read — a
notification dropped, a WAL delta lost, on whichever path the author forgot. Adding a field is
exactly the change these three copies are worst at absorbing.

(Note: the scatter path — `execute_scatter_part` at `execution.rs:450` — is **not** a deposit-drain
site. `scatter_mset`/`scatter_del`/`scatter_del`… hand-roll their writes and effects without ever
building a `CommandContext`, so they neither deposit nor drain. That is its own smear, out of scope
here; the drain discipline problem is the three sites above.)

## Design

Give the produced half of `CommandContext` a type. The eight deposit fields collapse into one
`CommandEffects` struct that lives as a single field on the context; the seam drains it as **one
value**, and every consumer destructures that value exhaustively — so adding a field is a
compile error at every drain site instead of a silent drop.

```rust
// core/src/command.rs — the produced half, as a module of its own
#[derive(Default)]
pub struct CommandEffects {
    pub dirty_delta: i64,
    pub lazyfreed_delta: u64,
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub write_was_noop: bool,
    pub hll_wal_delta: Option<SmallVec<[(u16, u8); 8]>>,
    pub keyspace_events: KeyspaceEventDeposits,
    pub script_writes: Vec<ScriptWriteRecord>,
}

pub struct CommandContext<'a> {
    // INPUT half only: store, shard_senders, shard_id, num_shards, conn_id,
    // protocol_version, replication_tracker, cluster_state, … (unchanged)
    pub effects: CommandEffects,   // the OUT-buffer, now one named thing
}
```

The deposit **methods** are unchanged in signature — `notify_event`, `record_lookup`, and direct
`ctx.effects.dirty_delta += …` all still target the context — so the ~296 `impl Command` blocks in
`crates/commands` (354 repo-wide) do not churn. What changes is the seam:

```rust
// execution.rs — the eight-tuple destructure becomes one move…
let effects = std::mem::take(&mut ctx.effects);   // drain as a value
// …consumed exhaustively so a new field cannot be silently ignored:
let CommandEffects {
    dirty_delta, lazyfreed_delta, keyspace_hits, keyspace_misses,
    write_was_noop, hll_wal_delta, keyspace_events, script_writes,
} = effects;
```

`CommandEffects` also owns the suppression rule, so it is stated once instead of re-derived at each
gate:

```rust
impl CommandEffects {
    /// The write-effect payload, or `None` when the write declared itself a
    /// no-op — the single home of the rule documented at command.rs:1043.
    /// Adding an effect field forces this method to be revisited (it moves).
    pub fn into_write_meta(self, handler: Arc<dyn Command>) -> Option<WriteCommandMeta> {
        if self.write_was_noop { return None; }
        Some(WriteCommandMeta {
            handler,
            dirty_delta: self.dirty_delta,
            hll_wal_delta: self.hll_wal_delta,
            keyspace_events: self.keyspace_events,
        })
    }
}
```

`WriteCommandMeta` (`execution.rs:19-33`) and `WRITE_EFFECT_ORDER`
(`core/src/shard/post_execution.rs:156`) then consume the drained value from **one** construction
point per path — the three drain sites all call `effects.into_write_meta(handler)` (or, for the
script sub-command paths, the `ScriptWriteRecord` sibling) instead of open-coding the
`is_write && !write_was_noop` gate.

### Options weighed

- **Option A — `execute` returns `(Response, CommandEffects)`.** Deepest coupling of output to the
  interface: the compiler sees the effects at every call site. But every one of the 354 `impl
  Command` blocks changes signature, and every handler's early `return Ok(resp)` must now name an
  effects value. Maximum churn for a benefit (compile-forced draining) that Option B already gets.
  **Rejected.**
- **Option B — consolidate deposits into `ctx.effects: CommandEffects`, drain as a value
  (recommended).** The input/output conflation is resolved at the *field* level, not the signature
  level: `execute` keeps `(&mut ctx, args) -> Result<Response, _>`, handlers keep depositing through
  the same methods, and the drain becomes one `mem::take` + one exhaustive destructure. The
  exhaustive `let CommandEffects { … } = …` at each of the three sites is what makes a new field a
  compile error there — the property Option A buys with 354 edits, bought here with ~3. `into_write_meta`
  gives the no-op rule a single home.
- **Option C — `execute` returns `WriteOutcome { response, effects }` typed, ctx becomes
  input-only.** The clean end-state: the context carries no mutable out-buffer at all, and the
  interface's return type names exactly what a command produces. This is Option A with a nicer name
  and the same 354-block cost. Worth doing *eventually* — Option B is the seam that makes it a
  mechanical follow-up (the `CommandEffects` type already exists; the flip is moving it from a field
  to a return position), but not worth blocking on now.

**Recommendation: Option B.** It resolves the conflation and closes the silent-drop hole with
almost no handler churn, and it is the strict prerequisite for Option C if the full return-value
shape is ever wanted.

## Why this is the right depth

- **Locality.** Everything a command *produces* lives in one type in one place, next to the methods
  that write it, instead of being eight sibling fields interleaved with the input references on a
  30-field context. "What can a handler output?" is answered by reading `CommandEffects`, not by
  grepping the context for which fields the seam happens to drain.
- **Leverage.** One exhaustive destructure per drain site replaces three independent field-by-field
  copies. The no-op suppression rule — today an `if` re-stated at `execution.rs:183` and implicitly
  at the two script sites — becomes `into_write_meta`, one method every path calls. A ninth effect
  is added to the struct once; the three sites stop compiling until they handle it. That compile
  break *is* the leverage the eight-tuple destructure cannot give.
- **Deletion test.** The change deletes the eight-field destructure at `execution.rs:117-142`, the
  four-field destructures at `gate.rs:433-441` and `scripting.rs:205-215`, and the open-coded
  `is_write && !write_was_noop` gate — replaced by a `mem::take` and a shared method. Net removal of
  duplicated extraction discipline is the signature of moving an out-buffer behind a type.
- **Deepens, does not fork.** This is proposal 01's move applied to command *outputs*.
  01-declarative-command-spec made a `CommandSpec` the single source of a command's *metadata*
  (keys, events, WAL strategy); this makes `CommandEffects` the single source of a command's
  *effects*. 03-unified-post-execution-pipeline already funnels those effects through one
  `WRITE_EFFECT_ORDER`; this proposal hands that pipeline a typed value at its mouth instead of eight
  loose fields, so `WriteCommandMeta` is built in one place per path. No new effect channel, no
  parallel out-buffer.

## Testing impact

- **Compile-time regression.** The exhaustive `let CommandEffects { … } =` at each drain site is the
  test: a new field that any path forgets to handle fails to build. This is behavior the eight-tuple
  destructure never had.
- **Suppression pinned once.** A unit test on `CommandEffects::into_write_meta` covers "no-op write
  drops its keyspace_events and hll_wal_delta" in one place, replacing the reliance on three call
  sites independently getting the gate right. Regression for the exact contract at command.rs:1043.
- **Existing suites carry over.** Keyspace-notification, client-tracking, PFADD-dense-WAL,
  UNLINK-lazyfree, and MULTI/EXEC integration tests already exercise every deposit end-to-end; they
  are the safety net that the field consolidation changed no runtime behavior. The scripting suites
  (local + cross-shard `redis.call` write propagation) cover the two script drain sites.
- **No new behavioral tests required** — this is a pure interface deepening; the effect *values* and
  their routing are unchanged. Any new test is a characterization of the seam, not of a feature.

## Risks / open questions

- **`ctx.effects.` prefix churn.** Handlers that assign fields directly (e.g. `ctx.dirty_delta = 1`,
  `ctx.write_was_noop = true`) become `ctx.effects.dirty_delta = 1`. This is a mechanical rename
  across the command crates — a `sed`/`awk` job, not a design change — but it touches many files. An
  alternative is `Deref`-style forwarding methods on the context to keep `ctx.set_dirty(1)`, trading
  a bit of adapter boilerplate for zero handler churn; decide during step 1.
- **`take_keyspace_events`/`hll_wal_delta.take()` semantics.** These `.take()` owned fields today so
  the context can be reused; `mem::take(&mut ctx.effects)` subsumes both by resetting the whole
  sub-struct to `Default`. Verify no path reads a deposit field *after* the drain (grep found none —
  every site drains last, immediately before dropping the context).
- **Script sub-command records stay bespoke.** `gate.rs` and `scripting.rs` build a
  `ScriptWriteRecord`, not a `WriteCommandMeta`, from the drained effects. `CommandEffects` should
  offer both constructors (`into_write_meta`, `into_script_record`) so neither site open-codes the
  gate; the two record types remain distinct (a script record carries `args`), which is fine.
- **The scatter smear is untouched.** `execute_scatter_part` still hand-rolls effects without a
  `CommandContext`, so it neither benefits from nor is broken by this change. Converging scatter onto
  `CommandEffects` would be a genuine deepening but is a separate proposal (it needs scatter to route
  through `handler.execute`, which today it deliberately does not).
- **Option C timing.** If the input-only context (Option C) is wanted, it is cleanest to land it
  before more code accretes against `ctx.effects` as a mutable field. Flag the decision now; the
  cost is 354 signature edits whenever it happens, and Option B does not make that cheaper — it only
  makes it mechanical.
