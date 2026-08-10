# Proposal 45 — VLL key ownership moves inside `VllShardState`; diagnostics collapse to one call

*Round 38. Candidates TX1 (phase 1) + TX13 (phase 2) of the txn+vll+scripting lane. One
proposal because phase 2 depends on phase 1: the ownership map changes what the diagnostics
can truthfully report.*

## Summary

`VllShardState` hands a dequeued op's lock keys **out** to the host worker and then requires the
host to hand the same keys **back** (`release_after_execution(txid, &keys)`), tracking outstanding
ops as a bare `usize` that `saturating_sub`s on release — an interface whose central invariant
("release exactly once, with exactly the keys you were given") the module cannot check and the
compiler cannot enforce, with the whole failure mode written down in the spec instead
([FM-VLL-005](../../hardening/specs/vll-failure-modes.md#fm-vll-005--a-granted-op-panics-while-executing)).
Phase 1 moves that fact inside: `executing: Option<ExecutingOp>` replaces the counter, `release(txid)`
looks the keys up, and **two of the four misuses become unrepresentable while the other two become
detectable** (accounting in §1). Phase 2 then collapses six diagnostic entry points plus two
`LockTable` diag-only methods into a single `diagnostics()` call.

**No live operator-visible defect is claimed.** An earlier draft of this proposal asserted that
`DEBUG VLL` under-reports the executing txid. That claim is **withdrawn**: the shard event loop is
sequential, so a `GetVllQueueInfo` can never be served while the loop is inside `handle_vll_execute`
(§3). What is real is smaller and still worth fixing: `PendingOpState::Executing` is written to a
value nobody can read, `VllQueueInfo.executing_txid` is consequently always `None`, and the render
branch behind it is dead. Phase 1 turns that dead field into a **leak detector** — after the change
`executing_txid` is `Some(n)` **if and only if** a release leaked, which is exactly FM-VLL-005's
`NOT observable` clause made observable.

## Files involved (verified paths + current line counts)

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/vll/src/shard.rs` | 1159 | **Owner of both phases.** `VllShardState` struct (L68–81, `executing_ops: usize` at L79); `with_max_queue_depth` (L91–101, `executing_ops: 0` at L98); `dequeue_for_execution` (L226–238); `release_after_execution` (L250–257); `is_drained` (L334–336); the five public introspection methods (L415–463); `DequeuedOp` (L479–483) and the three pub-field snapshot structs (L486–508). |
| `frogdb-server/crates/vll/src/lock_table.rs` | 343 | `LockTable::declare` (L51) / `try_grant` (L90) / `release` (L131) — the intent store phase 1's map shadows — plus the two diag-only methods `iter_keys` (L150–154) and `lock_state_string` (L157–173). |
| `frogdb-server/crates/vll/src/queue.rs` | 299 | `VllPendingOp` (owns `keys: Vec<Bytes>`, L20–33) and `TransactionQueue::dequeue` (L122–124, `self.pending.remove(&txid)`) — the removal that strands the `Executing` state write. |
| `frogdb-server/crates/vll/src/lib.rs` | 28 | `pub use shard::{…}` (L22–26) — phase 2 **adds** `VllShardDiagnostics` here; see the export note in phase 2. |
| `frogdb-server/crates/core/src/shard/vll.rs` | 290 | **The only production caller.** `handle_vll_execute` (L40–75, doc comment at L39): `dequeue_for_execution` at L45, the panic guard at L50–58, `release_after_execution(op.txid, &op.keys)` at L72. Stale `executing_ops` prose at L52. |
| `frogdb-server/crates/core/src/shard/dispatch_vll.rs` | 38 | The `VllMsg` match — `VllExecute` at L18, `GetVllQueueInfo` at L32. Both arms of one sequential dispatch; this is the file that makes §3's severity downgrade true. |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | — | The `tokio::select!` message arm (L128–170): one message at a time, awaited to completion. |
| `frogdb-server/crates/core/src/shard/diagnostics.rs` | 598 | **The only production diagnostics consumer.** `collect_vll_queue_info` (L167–204) and `collect_lock_table_info` (L207–231) — between them they make five separate calls into `VllShardState`. |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | `can_execute_during_lock` (L854–861) — the **non**-diagnostic reader of `continuation_lock_owner()`. |
| `frogdb-server/crates/core/src/shard/panic_guard.rs` | 564 | FM-VLL-005's only forcing test, `a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_serving` (tag at L483, fn at L495), which asserts through `collect_lock_table_info()` (L534) — so it is a phase-2 consumer too. Stale `executing_ops` prose at L34 (module doc) and L491 (test doc). |
| `frogdb-server/crates/core/src/shard/types.rs` | 1498 | `VllQueueInfo` (L980–994, `#[derive(Debug, Clone, Default)]` — **no `Serialize`**); `executing_txid: Option<u64>` at L987. |
| `frogdb-server/crates/server/src/connection/debug_handler.rs` | — | `gather_vll` (L75–97): single-shard `Err(_) => Vec::new()` at **L86**; the multi-shard path goes through `ScatterGather::gather_all` (`scatter/broadcast.rs:285`), which silently omits shards that do not answer inside one shared deadline. |
| `frogdb-server/crates/server/src/connection/debug_conn_command.rs` | 1445 | `format_vll_response` (L535–…) — the **sole** consumer of `executing_txid` (L550–551) and the `all_empty` short-circuit at L537–543. |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | LOCKED spec. **Six** edit sites: L3 (Status), L20–23 (Scope), L69 (FM-VLL-003 NOT observable), L105 + L106 (FM-VLL-005), L108 (FM-VLL-005 `Forced by`). Enumerated below. |

## Problem (concrete verified evidence)

### 1. The key list is an out-parameter the caller must not lose

`dequeue_for_execution` moves the op's keys out of the queue and into a caller-owned struct
(`shard.rs:226-238`, `DequeuedOp` at `shard.rs:479-483`):

```rust
pub fn dequeue_for_execution(&mut self, txid: u64) -> Option<DequeuedOp<O>> {
    let tx_queue = self.tx_queue.as_mut()?;
    let mut op = tx_queue.dequeue(txid)?;          // removed from the queue outright
    op.state = PendingOpState::Executing;
    self.executing_ops += 1;                       // ...but only a COUNT stays behind
    Some(DequeuedOp { txid: op.txid, keys: op.keys, operation: op.operation })
}
```

and the release requires them back (`shard.rs:250-257`):

```rust
pub fn release_after_execution(&mut self, txid: u64, keys: &[Bytes]) {
    self.executing_ops = self.executing_ops.saturating_sub(1);
    if let Some(lock_table) = self.lock_table.as_mut() { lock_table.release(keys, txid); }
    self.try_advance_pending_locks();
    self.try_grant_pending_continuation();
}
```

Between those two calls the shard's most safety-critical fact — *which keys is txid still holding*
— lives **outside** the state machine, in a value the host can drop, truncate, reorder, or pass with
a mismatched `txid`. Nothing in the interface prevents any of it:

| # | Misuse | What the module does today | Consequence | After phase 1 |
| --- | --- | --- | --- | --- |
| 1 | Never call `release_after_execution` | nothing | intents leak; `executing_ops` stays ≥1 so `is_drained()` (`shard.rs:334-336`) is false **forever** → every later continuation-lock request parks and times out (FM-VLL-003), every SCA request is refused (FM-VLL-001/004) | **unchanged** under variant (a) — still caller-side; structurally impossible under variant (b) |
| 2 | Call it twice | `saturating_sub` floors at 0; the second `lock_table.release` is a no-op | the count silently under-reports. *Theoretical on this host*: erasing a **different** op's outstanding-ness needs ≥2 ops executing concurrently, which the sequential event loop (§3) cannot produce. Under a hypothetical concurrent host it would mean premature "drained" and a continuation lock granted over a live op. | **detectable** — the take returns `None`; `debug_assert!` + a `bool` return say so |
| 3 | Call it with a short / reordered / edited key slice | releases only those keys | the omitted keys stay locked with no owner | **unrepresentable** — there is no key argument |
| 4 | Call it with *another* op's key slice | releases keys that op still needs | cross-op corruption of the lock table | **unrepresentable** — same reason |
| 5 | Call it with the wrong `txid` | `LockTable::release` (`lock_table.rs:131-140`) is a total no-op for unknown `(key, txid)` | silent; nothing is released, nothing complains | **detectable** — the stored entry's txid does not match |

**2 unrepresentable (3, 4), 2 detectable (2, 5), 1 unchanged (1).** Rows 3 and 4 stop existing as
states because the key argument stops existing; rows 2 and 5 stay representable but stop being
silent.

The doc comment states the contract in prose and the `saturating_sub` is explicitly a damage limiter
for its violation (`shard.rs:247-249`: *"Pairs 1:1 with `dequeue_for_execution`; `saturating_sub`
keeps an unpaired call from wrapping the outstanding-op count"*). A saturating operator defending
against your own caller is the tell: the interface is shallow at exactly the point where it should
be deep.

The spec agrees, at length. The scope section (`vll-failure-modes.md:20-23`) carves out an explicit
exception to the file's own scope rule for this one obligation:

> One caller-side obligation is in scope even though it lives outside those two files: the shard
> worker must pair every `dequeue_for_execution` with a `release_after_execution` … **The lock table
> cannot enforce this from the inside** — a caller that never returns leaves the entry held forever.

That sentence is a design defect written down as a specification. It is only true because the keys
left the module.

**Verified caller census.** `release_after_execution` has exactly **one** production caller
(`frogdb-server/crates/core/src/shard/vll.rs:72`) and **eight** test callers, all inside
`vll/src/shard.rs` (L546, 591, 666, 709, 792, 836, 888, 904). `dequeue_for_execution` mirrors it
exactly: one production caller (`core/src/shard/vll.rs:45`), eight tests (L545, 590, 665, 708, 779,
835, 887, 903). So the *cost* of narrowing the interface is one production edit — and the whole
`FM-VLL-005` structural argument exists to protect that single call site.

**`DequeuedOp` is unnameable outside the crate.** `mod shard` is private and `lib.rs:22-26` does not
re-export `DequeuedOp`, so no downstream crate can write its type — `handle_vll_execute` only ever
binds it by inference. Changing its shape (dropping `keys`, or switching to `Arc<[Bytes]>`) is
therefore invisible to every consumer that does not destructure it, which is none.

### 2. The one caller pays for the leak with a hand-written panic guard

Because the release is the host's obligation, the host must also make it unwind-safe itself
(`core/src/shard/vll.rs:50-56`, comment abridged):

> Panic isolation (c2-07). The release below is the whole reason this site needs its own guard
> rather than relying on the outer net: an unwind past it leaks the op's key locks *and* leaves
> `executing_ops` incremented, which permanently blocks every later request on those keys and any
> parked continuation lock.

A guard at the call site is the right fix *given* the interface. With the keys owned by the state
machine, a much stronger option opens up (a drop-guard or a `run_dequeued` closure inside
`frogdb-vll`) — see phase 1 variant (b).

### 3. `PendingOpState::Executing` is written to a value nobody can read — a dead variant, not an under-report

The mechanical facts are verified end to end:

- `TransactionQueue::dequeue` is `self.pending.remove(&txid)` (`queue.rs:122-124`) — the op is gone
  from the queue.
- `dequeue_for_execution` sets `op.state = PendingOpState::Executing` (`shard.rs:229`) on that
  already-removed value, which is then moved field-by-field into `DequeuedOp` — and `DequeuedOp`
  (`shard.rs:479-483`) **has no `state` field**. The write is dead on arrival.
- `iter_pending_ops` (`shard.rs:425-437`) iterates `self.tx_queue` only, so it can never yield a
  snapshot whose `state` is `Executing`.
- Therefore `diagnostics.rs:175`'s branch
  `if snap.state == crate::vll::PendingOpState::Executing { info.executing_txid = Some(snap.txid); }`
  is unreachable, `VllQueueInfo.executing_txid` (`types.rs:987`) is **always `None`**, and
  `debug_conn_command.rs:550-551`'s ` executing_txid:{}` suffix never renders.
- `grep executing_txid` finds **four** hits repo-wide (`types.rs:987`, `diagnostics.rs:176`,
  `debug_conn_command.rs:550,551`) and **zero** assertions. Nothing tests it, which is why 100% on
  the frogdb-vll mutation gate did not surface it: the field is only ever written by dead code, so
  there is no behaviour for a mutant to break.

**What this is *not*.** An earlier draft called this a live `DEBUG VLL` under-report — an operator
seeing `queue_depth:0` and no executing txid on a stuck shard. That scenario is impossible, for
three independent reasons, each verified:

1. **The host loop is sequential.** `event_loop.rs:128-170` takes one message off `message_rx` and
   `await`s `dispatch_message` to completion before taking the next. `VllExecute` and
   `GetVllQueueInfo` are two arms of the same `dispatch_vll` match (`dispatch_vll.rs:18` and `:32`),
   and `handle_vll_execute` (`core/src/shard/vll.rs:40-75`) dequeues, awaits execution, and releases
   inside one dispatch. The `executing` set is non-empty **only** while the loop is inside
   `handle_vll_execute` — exactly the window in which it cannot serve a diagnostics message.
2. **A blocked shard is dropped from the reply, not rendered wrong.** Single-shard `DEBUG VLL <id>`
   maps a timeout to `Err(_) => Vec::new()` (`debug_handler.rs:86`); the all-shard path
   (`ScatterGather::gather_all`, `scatter/broadcast.rs:285-330`) collects survivors under one shared
   deadline and silently omits the rest. Either way the operator sees no line for that shard, not a
   misleading one.
3. **Mid-execution the txid is visible anyway.** Locks are released *after* execution, so during the
   window the op's intents are still in the lock table. `format_vll_response`'s `all_empty`
   short-circuit (`debug_conn_command.rs:537-543`) tests `queue_depth == 0 && continuation_lock ==
   None && intent_table.is_empty()`, so a non-empty intent table defeats it and the `intents:` block
   renders `key:… txids:[N] …` with the executing txid in it.

**What it actually is.** A dead enum variant, a dead struct field, and a dead render branch — plus
one latent piece of value. After phase 1, `executing_txid` is populated from a fact the state
machine owns, and because of reason (1) above it is `None` in every *correctly-operating*
`DEBUG VLL`. It becomes `Some(n)` **if and only if** a release leaked. That converts FM-VLL-005's
`NOT observable` clause — *"`executing_ops` staying incremented"* (`vll-failure-modes.md:105`) —
into something an operator can actually observe. Severity: **dead code plus a leak detector**, not a
bug fix.

**Bonus dead variant.** `PendingOpState::Done` (`types.rs:21-22`) is never constructed anywhere in
the workspace — grep finds writes of `Pending` (`queue.rs:47`), `Ready` (`queue.rs:55`) and
`Executing` (`shard.rs:229`) only. Once `Executing`'s write moves to the `executing` slot, the enum
has two variants that exist purely to be `Debug`-formatted. Both should go (see phase 2), and
`PendingOpState` is re-exported twice (`vll/src/lib.rs:28`, `core/src/lib.rs:166`), so the deletion
is a three-line sweep.

### 4. Six diagnostic entry points + two `LockTable` methods, two production consumers

Verified surface (the lane's "7 introspection entry points"):

| # | Entry point | Defined | Production readers |
| --- | --- | --- | --- |
| 1 | `VllShardState::queue_depth()` | `shard.rs:420` | `diagnostics.rs:170` |
| 2 | `VllShardState::iter_pending_ops()` | `shard.rs:425` | `diagnostics.rs:174` |
| 3 | `VllShardState::continuation_lock_snapshot()` | `shard.rs:440` | `diagnostics.rs:187`, `:220` |
| 4 | `VllShardState::intent_snapshots()` | `shard.rs:451` | `diagnostics.rs:195`, `:210` |
| 5 | `VllShardState::continuation_lock_owner()` | `shard.rs:415` | `worker.rs:856` — **control path, not diagnostics** |
| 6 | `LockTable::iter_keys()` | `lock_table.rs:150` | `shard.rs:456` only |
| 7 | `LockTable::lock_state_string()` | `lock_table.rs:157` | `shard.rs:460` only |

plus three all-`pub`-field snapshot structs (`PendingOpSnapshot` L486, `ContinuationLockSnapshot`
L496, `IntentSnapshot` L504), all re-exported from `lib.rs:22-26`.

Two production functions consume the whole group, and both live in one file:
`collect_vll_queue_info` (`diagnostics.rs:167-204`, feeds `DEBUG VLL`) and `collect_lock_table_info`
(`diagnostics.rs:207-231`, feeds `DEBUG LOCKTABLE`). They overlap: `collect_lock_table_info` is a
strict subset of `collect_vll_queue_info` minus the queue half, and both re-walk
`intent_snapshots()` → `VllKeyIntentInfo` with **byte-identical** mapping code (L195-201 vs
L210-217). Meanwhile `iter_keys`/`lock_state_string` are `pub` on `LockTable` purely so `shard.rs`
two lines away can build `IntentSnapshot` — a crate-internal detail promoted to public API by
accident of module layout.

**Depth reading.** Each of these methods is a thin projection of one private field. A caller wanting
a coherent picture of the shard must learn six names, call them in an order nobody documents, and
know that they are not atomic with respect to each other (they are today, only because the caller
holds `&self` on a single-threaded worker — an invariant the interface never states). That is the
textbook shallow interface: nearly as much surface as implementation, and no leverage — the caller
does the assembly.

## Proposed change

### Phase 1 — key ownership moves inside `VllShardState`

Replace the counter with the slot the counter was standing in for:

```rust
/// An op handed to the host by `dequeue_for_execution` that has not yet
/// reported back. The state machine keeps its keys: the host is not trusted
/// to return them, and cannot.
#[derive(Debug)]
struct ExecutingOp {
    txid: u64,
    keys: Arc<[Bytes]>,
}

pub struct VllShardState<O: Debug> {
    lock_table: LockTable,                 // 51 unwraps these two
    tx_queue: TransactionQueue<O>,
    continuation_lock: Option<ContinuationLock>,
    pending_continuation_release: Option<oneshot::Receiver<()>>,
    pending_continuation: Option<PendingContinuation>,
    executing: Option<ExecutingOp>,
}
```

- `dequeue_for_execution(txid)` stores the op in `executing` before handing it out.
- `release_after_execution(txid)` — **one argument** — takes the slot, releases exactly those keys
  through `LockTable::release`, then runs the existing `try_advance_pending_locks()` +
  `try_grant_pending_continuation()` drain points. A txid that does not match the stored entry
  releases nothing (unchanged observable behaviour), but is now *detectable*: the method returns
  `bool` and carries a `debug_assert!`.
- `is_drained()` becomes `self.executing.is_none() && self.tx_queue.is_empty()` — `saturating_sub`
  and the entire wrap-around hazard it guards disappear, because there is no arithmetic left.

#### `Option` vs `HashMap` — answering the representation question

The earlier draft proposed `HashMap<u64, Vec<Bytes>>`. **`Option` is the right choice**, and the
reason is the proposal's own criterion — it makes one more state unrepresentable:

- **The host cannot have two ops executing.** §3(1): the shard loop dispatches one message at a
  time and `handle_vll_execute` completes within one dispatch. A map's second entry is a state the
  system cannot reach.
- **`VllShardState` is not host-agnostic, despite the crate's posture.** `traits.rs`'s module doc
  says the inversion exists "so the VLL crate can drive the cross-shard protocol without knowing
  about the host's concrete `ShardMessage` enum" — that is the **coordinator** half. `VllShardState`
  is generic only over the op payload `O`, and its interface already bakes in the sequential loop:
  `request_continuation_lock` is documented *"**This call never waits.** It runs on the host's shard
  event loop, and that loop is exactly what drains the queue"* (`shard.rs:288-290`, on
  `request_continuation_lock` at `:302`). A type that
  already assumes one host thread should not pretend otherwise in one field.
- **`is_drained()` becomes a boolean over a boolean** rather than a set-emptiness test — which is
  the exact question FM-VLL-003 asks (`vll-failure-modes.md:70`: *"queue empty **and** no dequeued
  op outstanding"*).
- **It matches the diagnostics type.** `VllQueueInfo.executing_txid` is already `Option<u64>`
  (`types.rs:987`); the map shape would have forced either a lossy `Vec → Option` projection or a
  `VllQueueInfo` field change with a `DEBUG VLL` output change behind it.

The one cost: `dequeue_for_execution` must define an answer for a *second* dequeue while one is
outstanding. Specify it as **return `None` + `debug_assert!`** — observably identical to today's
unknown-txid path (`handle_vll_execute` sends `PartialResult::default()`), so it needs no new spec
row; note the choice in FM-VLL-005's Invariant so the next reader does not "fix" it. If the lane
later wants a genuinely concurrent shard host, that is the moment to widen `Option` to a map, and
the change is local to three method bodies.

#### `Arc<[Bytes]>`, not a `Vec` clone

`handle_vll_execute` genuinely uses the key slice twice — `execute_scatter_part(&op.keys, …)`
(`execution.rs:713-717`, `keys: &[Bytes]`) and `scatter_error_reply(&op.operation, &op.keys, err)`
(`dispatch_core.rs:193-196`, `keys: &[bytes::Bytes]`). Because `execute_scatter_part` takes
`&mut self`, the host cannot hold a borrow into `self.vll` across it, so `DequeuedOp` must keep
owned keys.

Use `Arc<[Bytes]>` for both the slot and `DequeuedOp.keys`. **Deref coercion feeds both consumers
unchanged** — `&Arc<[Bytes]>` coerces to `&[Bytes]`, so neither signature moves, and `DequeuedOp` is
not exported (§1) so nothing else can notice. Cost accounting against a `Vec` clone:

| | allocations | per-key work |
| --- | --- | --- |
| `Vec<Bytes>` clone | 1 (the clone) | N `Bytes` refcount increments |
| `Arc<[Bytes]>` | 1 (the `Vec → Arc<[Bytes]>` conversion at dequeue) | 1 refcount increment for the second handle |

Same allocation count, strictly less per-key work, and no signature churn. The earlier draft's
"deliberately not proposed now (premature)" was unearned — `Arc` is the cheaper option at equal
diff size. (A follow-up could push `Arc<[Bytes]>` all the way into `VllPendingOp.keys`
(`queue.rs:24`) so the conversion happens once at enqueue instead of once at dequeue; not required
here.)

Two variants, both compatible:

- **(a) minimal** — signature becomes `release_after_execution(&mut self, txid: u64) -> bool`; the
  host's one call site loses its second argument. Smallest diff, ships the whole misuse-proofing.
- **(b) stronger, recommended if the host refactor is acceptable** — additionally expose
  `run_dequeued(txid, f)` (or return a drop-guard) so the release cannot be *forgotten* either, not
  just mis-argued. This is what would let `core/src/shard/vll.rs`'s bespoke panic-guard comment
  (L50-56) shrink to "the guard lives in frogdb-vll", and turns FM-VLL-005's invariant from a
  caller obligation into a module property. Note the interaction with sibling **52** below before
  choosing (b).

**Rejected alternative:** a `by_txid: HashMap<u64, Vec<Bytes>>` reverse index inside `LockTable`
instead. It also removes the out-parameter, but it does **not** replace `executing_ops` — the drain
predicate needs "dequeued but not yet released", which is not the same set as "holds intents"
(a queued-but-ungranted op holds intents too). The `VllShardState` slot does both jobs with one
field, so it is the better shape.

### Phase 2 — one `diagnostics()` call (lands after phase 1)

```rust
pub struct VllShardDiagnostics<'a, O> {
    pub queue_depth: usize,
    pub executing_txid: Option<u64>,                        // NEW — phase 1 makes this real
    pub pending_ops: Vec<PendingOpSnapshot<'a, O>>,
    pub continuation_lock: Option<ContinuationLockSnapshot>,
    pub intents: Vec<IntentSnapshot>,
}

impl<O: Debug> VllShardState<O> {
    /// The complete introspection surface, consistent as of one instant.
    pub fn diagnostics(&self) -> VllShardDiagnostics<'_, O> { … }
}
```

Consequences, all verified against the census above:

- `queue_depth`, `iter_pending_ops`, `continuation_lock_snapshot`, `intent_snapshots` become
  private (their only production readers are `diagnostics.rs`, which now makes one call).
- `LockTable::iter_keys` and `lock_state_string` drop to `pub(crate)` or private — their only
  non-test callers are `shard.rs:456` and `:460`, inside the same crate. Their unit tests
  (`lock_table.rs:190-341`) are in-module and keep working unchanged.
- **Exports go up by one, not down.** `lib.rs:22-26` gains `VllShardDiagnostics`;
  `PendingOpSnapshot`, `ContinuationLockSnapshot` and `IntentSnapshot` all stay exported because
  they are field types of the new struct. The win is **one entry point instead of six**, not a
  smaller type list — the caller learns one name and gets the whole picture, atomically, which is
  the depth claim. Say so in the PR rather than advertising a shrink that does not happen.
- `diagnostics.rs`'s two collectors become one `let d = self.vll.diagnostics();` plus two
  projections, and the duplicated `IntentSnapshot → VllKeyIntentInfo` mapping (L195-201 ≡ L210-217)
  collapses to one helper.
- `executing_txid` is populated from `d.executing_txid` and the dead `state == Executing` branch
  (`diagnostics.rs:175`) is deleted. `PendingOpState::Executing` then has no writer and no reader
  beyond `format!("{:?}", …)`; delete it together with `PendingOpState::Done` (§3), which already
  has neither. That removes the `shard.rs:229` write, the variants, and touches the two re-export
  lines (`vll/src/lib.rs:28`, `core/src/lib.rs:166`).
- Document the new meaning of `executing_txid` **at the field**: on this host it is `Some` only if a
  release leaked, and that is the point.

**`continuation_lock_owner()` — keep it `pub`, or migrate its six test callers.** It is not
diagnostics: `worker.rs:856` uses it as the `can_execute_during_lock` gate. Pushing the comparison
in (`VllShardState::may_execute(conn_id) -> bool`) is a real depth win for the *production* reader —
but the accessor cannot then go private without breaking six `frogdb-core` tests that assert on the
owner directly, and `may_execute(conn_id) -> bool` cannot express any of them (they check
`== Some(N)` / `== None`, not "may this conn run"):

| Test call site | Assertion |
| --- | --- |
| `core/src/shard/dispatch_core.rs:276` | `assert_eq!(worker.vll.continuation_lock_owner(), Some(owner))` |
| `core/src/shard/dispatch_core.rs:533` | `assert_eq!(…, Some(100))` |
| `core/src/shard/vll.rs:189` | `assert_eq!(…, Some(99))` |
| `core/src/shard/vll.rs:224` | `assert_eq!(…, Some(100))` |
| `core/src/shard/vll.rs:257` | `assert_eq!(…, …)` |
| `core/src/shard/vll.rs:269` | `assert_eq!(…, None)` |

Pick one and write it down: **(i)** add `may_execute` and leave `continuation_lock_owner` `pub`
(smallest, recommended — the accessor is one line and the tests keep their precision), or **(ii)**
add `may_execute`, privatise the accessor, and migrate all six to
`worker.vll.diagnostics().continuation_lock.map(|l| l.conn_id)`. Option (ii) also re-points
FM-VLL-003's `NOT observable` row, which names `continuation_lock_owner()` by name
(`vll-failure-modes.md:69`). The in-crate `frogdb-vll` tests (twelve call sites in `shard.rs`) are
unaffected either way — they are in the same module.

## Before / After

```rust
// BEFORE — core/src/shard/vll.rs:45..72
let Some(op) = self.vll.dequeue_for_execution(txid) else { … };
let outcome = panic_guard::caught(self.execute_scatter_part(&op.keys, &op.operation, 0)).await;
…
self.vll.release_after_execution(op.txid, &op.keys);   // ← the keys round-trip

// AFTER (variant a) — &op.keys still compiles: Arc<[Bytes]> derefs to [Bytes]
self.vll.release_after_execution(op.txid);             // keys never left the module

// AFTER (variant b)
let result = self.vll.run_dequeued(txid, |keys, operation| { … }).await;  // release is structural
```

```rust
// BEFORE — diagnostics.rs:167..231, five calls across two collectors
queue_depth: self.vll.queue_depth(),
for snap in self.vll.iter_pending_ops() { … }
if let Some(lock) = self.vll.continuation_lock_snapshot() { … }
for snap in self.vll.intent_snapshots() { … }
// … and again in collect_lock_table_info: intent_snapshots() + continuation_lock_snapshot()

// AFTER — one call, one instant, one mapping helper
let d = self.vll.diagnostics();
```

## Testability improvement

**Phase 1 removes a bug class instead of testing for it.** The five misuse rows in §1 are not
currently testable at all from `frogdb-vll` — they are *caller* mistakes in `frogdb-core`, so a
`frogdb-vll` test cannot construct them and a `frogdb-core` test would have to fake a wrong call
deliberately. After phase 1 rows 3 and 4 cease to exist as states (there is no key list to get
wrong) and rows 2 and 5 become observable no-ops the module can assert on. Row 1 — *never*
releasing — is either still caller-side (variant a) or structurally impossible (variant b).

**A `frogdb-vll`-local FM-VLL-005 forcing test is writable *today*, and should land first.**
The earlier draft argued that phase 1 is what "relocates FM-VLL-005's evidence into the gated
crate". That argument is **withdrawn** — it is not true. `is_drained` is private but the test module
is `#[cfg(test)] mod tests` **inside `shard.rs`** (L511 onward), a child of the same module, so it
can call private methods; and `executing_ops` already keeps `is_drained()` false after a dropped
`DequeuedOp`. Nothing about phase 1 is required. Write it now:

```rust
// FM-VLL-005
#[tokio::test(start_paused = true)]
async fn a_dropped_dequeued_op_leaves_the_shard_undrained_until_it_is_released() {
    let mut state: VllShardState<()> = VllShardState::default();
    let (rt, rr) = channels();
    state.enqueue_lock_request(1, vec![Bytes::from_static(b"k")], LockMode::Write, (), rt);
    assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

    let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
    assert_eq!(state.queue_depth(), 0, "the op has left the queue");
    assert!(!state.is_drained(), "but the shard is not drained: it still holds the op's locks");
    assert!(!state.intent_snapshots().is_empty(), "and the intent is still in the lock table");

    state.release_after_execution(dequeued.txid, &dequeued.keys);
    assert!(state.is_drained());
    assert!(state.intent_snapshots().is_empty());
}
```

This is the **first** assertion on `is_drained` anywhere — grep finds only two callers, both
production (`shard.rs:314`, `:364`) — and the first FM-VLL-005 evidence in the gated crate.
`scripts/failure-modes.py` enforces both directions, so the same change must add the name to
FM-VLL-005's `Forced by` cell (`vll-failure-modes.md:108`, currently naming only
`a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_serving`). Under variant (b), a second
test forces the same row with a panicking closure inside `run_dequeued`.

**Phase 1 makes the executing op assertable through the public interface.** `executing_txid` gives
the first test that can distinguish "queue empty" from "shard drained" without reaching for a
private method — the very distinction FM-VLL-003 turns on (`vll-failure-modes.md:69`). Today
`continuation_lock_parks_while_a_dequeued_op_is_still_executing` (`shard.rs:770-795`, fn at `:771`,
tag at `:769`) proves it only indirectly, by observing that the parked request stays parked.

**Phase 2 makes diagnostics a single assertion.** `diagnostic_snapshots_reflect_state`
(`shard.rs:1144-1159`, fn at `:1145`) currently makes five separate calls and asserts on each; it
becomes one call and one struct comparison, and gains a real assertion for `executing_txid`. The
dead `executing_txid` render path gets its first-ever test.

## Risks / scope boundaries vs siblings

**Only two of the four round-38 vll proposals touch `frogdb-vll/src/shard.rs`: this one and 51.**
46 touches `coordinator.rs`/`types.rs`/`lib.rs` in that crate and no line of `shard.rs`; 52 lives
entirely in `frogdb-core`. The shared surface across all four is the **spec**, not the code. File
ownership, to keep the merge honest:

| Proposal | Owns (must not be edited by the others) | Overlap with 45 |
| --- | --- | --- |
| **45** (this) | `shard.rs` struct fields L68–81 + ctor L91–101 + `dequeue_for_execution` L226–238 + `release_after_execution` L250–257 + `is_drained` L334–336 + introspection block L415–463 + `DequeuedOp` L479–483; `lock_table.rs` L150–173 visibility; `core/src/shard/diagnostics.rs` L167–231; `core/src/shard/vll.rs` L52 + L72; `vll-failure-modes.md` L3, L20–23, :69, :105–106, :108 | — |
| **46** vll-acquire-error-unify | `vll/src/coordinator.rs` (both error enums), `vll/src/types.rs`, `vll/src/lib.rs:19`, `server/src/scatter/executor.rs:141-193`, `server/src/connection/scripting/eval.rs:268-281`; in the spec, FM-VLL-001–004's **`Observable` / `Outcome variant`** fields plus the preamble L14–18 / L26–30 | **No code overlap** (46 touches no line of `shard.rs`), but **spec overlap is real**: both edit FM-VLL-003 — 45 the `NOT observable` field (**L69**), 46 the `Observable` (**L68**) and `Outcome variant` (**L71**). Adjacent lines in one row. 46's spec diff is the smaller one and 46's own boundary section asks to go first: **land 46 first, rebase 45 onto it.** |
| **51** txn-slot-vll-state-small | `txn/src/state.rs:16-115` (promotion lattice) **and** `vll/src/shard.rs:68-114` (fields L68–81, `Default` L83–87, `with_max_queue_depth` L91–101, `ensure_initialized` L103–114) plus its nine unwrap sites (incl. L252–254 and L335) + `vll/src/queue.rs:79-83` + `vll-failure-modes.md:46` | **CONFLICT (textual, same struct).** *Authored — read `51-txn-slot-vll-state-small.md` before scheduling.* 51 rewrites the same field block and constructor 45 edits, and every `self.lock_table.as_mut()` / `self.tx_queue.as_ref()` site — including the ones inside `release_after_execution` and `is_drained`. Both changes are additive in intent (45 replaces one field, 51 unwraps two others) but the diffs will not auto-merge. **Land 51's TX12 half first** — 51 states this ordering explicitly and accepts it from its side ("Do TX12 first"), and this proposal's struct sketch above is already written against 51's post-state (plain `LockTable` / `TransactionQueue`). 51 also owns the FM-VLL-001 Invariant edit at `vll-failure-modes.md:46`, which names `ensure_initialized` — **45 must not touch that row.** |
| **52** vll-unknown-txid-refusal | `core/src/shard/vll.rs:40-75` (`handle_vll_execute`'s `None` arm → typed refusal), new FM-VLL-006 row | **CONFLICT (same function, adjacent lines) — provisional: 52 is being authored and is not on disk yet, so verify against its text before scheduling.** 52 rewrites the `dequeue_for_execution` → `None` branch at L45–48; 45 rewrites the release at L72 (variant a) or the whole body (variant b). **Land 52 first under variant (a)** — the two edits are then ~25 lines apart and merge cleanly. Under **variant (b)** the function is restructured around `run_dequeued`, which swallows 52's `else` arm entirely; in that case 45 and 52 must be a single change, or (b) deferred to a follow-up issue. This is the main reason variant (a) is the safe default. Note also that phase 1's second-dequeue refusal (see the `Option` section) lands in the *same* arm 52 owns. |

**Recommended order:** 46 → 51 (TX12) → 52 → 45. Only the last three edges are load-bearing; 46 is
first purely because its spec diff is smallest.

**Locked-area landing steps (frogdb-vll, gate 0.90):**

1. **Spec-first is not required for phase 1** — no observable behaviour changes (same locks
   released, same drain points, same `ShardReadyResult`s). But **six spec edits are required as
   part of the same change**, because the rows describe the mechanism by name:
   - **`:3` (Status)** — reads *"Phase 1 mutation gate passed (frogdb-vll 100% vs 90% gate)"*. If the
     re-gate lands below 100% this line is stale; update it with the new number, or drop the
     parenthetical and let `just mutants-gate` be the record.
   - **`:20-23` (Scope)** — the "caller-side obligation … the lock table cannot enforce this from the
     inside" carve-out becomes **false** and must be rewritten to say the state machine owns the keys
     and releases by txid (variant b: delete the carve-out entirely).
   - **`:69` (FM-VLL-003 NOT observable)** — names `continuation_lock_owner()` and
     `continuation_lock_snapshot()`; **phase 2** makes the latter private, so the row must name
     `diagnostics().continuation_lock` (and, under option (ii) above, `may_execute()`). Coordinate
     with 46, which edits L68/L71 of the same row.
   - **`:105` (FM-VLL-005 NOT observable)** — `executing_ops` staying incremented → the `executing`
     slot still holding the txid, *and* the new observable: `DEBUG VLL` reports `executing_txid` only
     when a release leaked.
   - **`:106` (FM-VLL-005 Invariant)** — `release_after_execution` no longer takes keys; record the
     second-dequeue refusal; under variant (b), the pairing is enforced by the module, not by
     `handle_vll_execute`'s guard.
   - **`:108` (FM-VLL-005 Forced by)** — add the new `frogdb-vll` test name (see Testability).
     `scripts/failure-modes.py` requires the tag and the row to agree in **both** directions.
   - Do **not** touch `:46` (FM-VLL-001) — that row belongs to sibling 51.
2. `just lint-failure-modes` after every spec edit (it is part of `just lint`).
3. New forcing tests go in **`frogdb-vll`**, not `frogdb-core` — but note the leading test above can
   and should land *before* phase 1, independently.
4. `just mutants-diff frogdb-vll` before pushing (push discipline); full `just mutants frogdb-vll`
   + `just mutants-gate frogdb-vll 0.90` for the re-gate. Phase 2's method deletions *reduce* the
   mutable surface, so the score should hold or improve; phase 1 adds `Option` take/insert
   operations that need real assertions behind them.
5. **`frogdb-txn` is untouched** by both phases — no `frogdb-txn` re-gate needed, even though the
   locked *area* nominally spans both crates.

**Other risks:**

- **Stale prose sweep.** `executing_ops` is named in four live places outside `shard.rs` and all
  four must move with the field: `core/src/shard/vll.rs:52` (the panic-guard comment),
  `core/src/shard/panic_guard.rs:34` (module doc, VLL bullet), `panic_guard.rs:491` (the FM-VLL-005
  test's doc comment), and `.scratch/hardening-2/c3-arm-dispositions.md:43-44` (the C3 arm
  disposition, which spells out `is_drained()` as `executing_ops == 0`). Two further hits are in
  `.scratch/**/issues/done/` (`hardening-2/…/07`, `hardening/…/02`) — those are historical records
  of what was true when they were filed; leave them.
- **`abort(txid)` is not a release path for executing ops.** `abort` (`shard.rs:264-279`) dequeues
  from `tx_queue` and returns early if the txid is not there — so an op that is already *executing*
  cannot be aborted, today or after this change. Phase 1 does not change that, and should not: the
  host is mid-`execute_scatter_part` and the keys must stay held. Worth a one-line comment on the
  `executing` slot so a future reader does not "fix" it.
- **`DEBUG VLL` output changes — but only on a leak.** Once `executing_txid` is populated, the
  ` executing_txid:{}` suffix can render. Because of §3(1) it renders only when a release has
  leaked, so no correct-operation golden shifts. Verified there are **zero** assertions on
  `executing_txid` repo-wide today. Still worth a line in the PR description and, if `DEBUG VLL` is
  documented on the website, a docs pass.
- **Phase 2 is a `frogdb-vll` public-API break.** Pre-production, and the only external consumer is
  `frogdb-core` (verified: `frogdb-vll` has three dependent crates — `core`, `server`,
  `shard-harness` — and grep finds zero uses of `PendingOpSnapshot`/`IntentSnapshot`/
  `ContinuationLockSnapshot`/`VllShardState` outside `core`), so this is a two-file change, not a
  sweep.

## Effort estimate

- **Leading FM-VLL-005 test: XS.** One `frogdb-vll` test + one `Forced by` cell. Lands before
  anything else, needs no spec-first ruling, and gives the gate its first FM-VLL-005 evidence.
- **Phase 1 (variant a): S–M.** One struct field (+ one small private struct), three method bodies
  (`dequeue_for_execution`, `release_after_execution`, `is_drained`), one production call site, eight
  in-crate test call sites (mechanical: drop the second argument), the four-site prose sweep, five
  spec-row edits, plus the `Option` refusal assertion. The mutation re-gate is the long pole, not the
  diff.
- **Phase 1 (variant b): M.** Adds a closure/guard API and restructures `handle_vll_execute` around
  it, which collides with sibling 52 — see the table. Recommend filing as a follow-up issue rather
  than bundling.
- **Phase 2: M.** One new struct + one method in `frogdb-vll`, six methods narrowed, two `LockTable`
  methods de-`pub`ed, the two `diagnostics.rs` collectors rewritten around one call with their
  duplicated mapping merged, `lib.rs` exports adjusted (+1 net), two `PendingOpState` variants
  deleted across two re-export lines, the `executing_txid` path made live and given its first test,
  and the FM-VLL-003 row re-worded. Blocked on phase 1.

### Independently-landable slice

**Not a hotfix — there is no live operator-visible defect (§3).** What *is* independently landable is
the dead-code half of phase 1, which turns `executing_txid` into a leak detector:

1. `shard.rs:79` — `executing_ops: usize` → `executing_txid: Option<u64>`; `shard.rs:98` —
   `executing_ops: 0` → `executing_txid: None`; increment/decrement (L232, L251) become
   `= Some(txid)` / `= None`; `is_drained()` (L335) reads `.is_none()`.
2. `shard.rs` — add `pub fn executing_txid(&self) -> Option<u64>`.
3. `diagnostics.rs:174-177` — populate `info.executing_txid` from it; delete the unreachable
   `state == Executing` branch. Delete `PendingOpState::Executing` and `::Done` and the
   `shard.rs:229` write.
4. One `frogdb-vll` test asserting `executing_txid()` is `Some` between dequeue and release, and one
   `frogdb-core` test asserting `collect_vll_queue_info().executing_txid` is `Some` in the same
   window (constructed directly on the worker, not through the message loop).
5. The four-site prose sweep listed under *Other risks*.

**Conflict note (corrected).** This slice edits `shard.rs:79` and `:98`, both **inside** 51's rewrite
region (fields L68–81, constructor L91–101), so it **does** conflict with 51 — contrary to what 51's
own boundary table currently says. The conflict is trivial-git-conflict scale (two adjacent line
changes in hunks 51 rewrites wholesale), not a redesign: land it after 51's TX12, or expect a
five-minute rebase. It keeps the `(txid, &keys)` release signature, so it does **not** conflict with
52. It still touches a locked crate, so it carries `just mutants-gate frogdb-vll 0.90`. Its spec
edits are `:105` (the `executing_ops` → `executing_txid` rename plus the new observable) and `:108`
(the new test name); the Scope carve-out at L20-23 and the Invariant at L106 stay as they are,
because the `(txid, &keys)` release contract is unchanged. Those are phase 1's edits.

**Cheaper fallback, if the lane wants zero locked-crate work:** delete `VllQueueInfo.executing_txid`
(`types.rs:987`), the `diagnostics.rs:175-177` branch, and the `debug_conn_command.rs:550-551` render
— all three files are unlocked. That removes the dead code without buying the leak detector.
Recommend the slice above instead; the detector is what gives FM-VLL-005 an observable.
