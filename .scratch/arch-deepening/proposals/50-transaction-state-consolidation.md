# Proposal 50 — `ConnectionState`'s transaction surface collapses into `TransactionState`; the `asking` flag moves in with it

*Round 38. Candidate TX10 of the txn+vll+scripting lane. **Carries an ADR-0002 seam question that
the go/no-go gate must answer — see [Decision point](#decision-point--requires-sign-off-at-the-gate).
The implementer must not decide it.***

## Summary (2-3 sentences)

`ConnectionState` exposes twelve transaction methods (`state.rs:678-770`), nine of which are
one-line forwards to `TransactionState` and add nothing a caller must know — they fail the deletion
test outright, and five of them sit in a *three*-hop chain (`ConnCommandState` trait → `ConnectionState`
→ `TransactionState`). The three that are not pure forwards are impure for exactly one reason: they
each clear `self.asking`, a flag whose lifecycle is split across six sites in the same file
(`state.rs:743-770, 797-828`) and whose central rule — *inside an open MULTI the flag is read
without clearing* — is implemented in `ConnectionState::take_asking` by asking the transaction
whether it is open (`state.rs:812`). The proposal is to move `asking` into `TransactionState`, which
already owns `is_open` and already carries the flag into `TxnSummary`, and then expose the
transaction object directly instead of twelve delegations — **but whether `asking` may live in
`frogdb-txn` at all is an ADR-0002 seam decision this proposal deliberately leaves open.**

## Files involved (verified paths + current line counts)

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/server/src/connection/state.rs` | 1961 | **Primary owner.** The twelve transaction delegations (L673-770); the `asking` field declaration (L472-476) and initialiser (L512); the three non-transaction `asking` methods (L797-828); `reset()` (L1078-1092), which clears `asking` via `clear_transaction()` at L1083. Also the six in-file `asking` unit tests (L1128-1203) — three of them FM-TXN-015's — plus `reset_clears_cluster_flags` (L1919-1931), which drives `set_asking`/`take_asking` and is **outside** that block. |
| `frogdb-server/crates/txn/src/state.rs` | 624 | **LOCKED crate (`frogdb-txn`, gate 0.90).** `TxnSummary::asking` (L139-142); `TransactionState` struct + private fields (L161-180); `is_open` (L184-186); `take(&mut self, asking: bool)` (L275-305); `discard` (L309-317); `clear` (L320-322). The `asking` field and its three accessors land here under option 1. |
| `frogdb-server/crates/core/src/conn_command.rs` | 1172 | The `ConnCommandState` seam. `set_asking` (L388); the five transaction methods `begin_multi`/`is_in_multi`/`watch_key`/`unwatch`/`discard` (L439-469). **Hard constraint: `frogdb-core` cannot name `TransactionState` — `frogdb-txn` depends on `frogdb-core` (`txn/Cargo.toml`), so the reverse edge would be a cycle.** These five stay plain-data whatever the gate decides. |
| `frogdb-server/crates/server/src/connection/auth_conn_command.rs` | 672 | The `ConnCommandState` impl: `set_asking` (L82-84) and the five transaction forwards (L160-181) — the middle hop of the three-hop chain. |
| `frogdb-server/crates/server/src/connection/guards.rs` | 1886 | Heaviest caller. `abort_transaction` ×4 (L533, 557, 568, 617), `fold_transaction_keys` (L626), `push_queued_command` (L629), `in_transaction` (L517). Also the two **non-transactional** `asking` consumers: `validate_cluster_slots_inner`'s take-and-restore (L737-744) and `validate_watch_slots`' peek (L792). |
| `frogdb-server/crates/txn/src/exec.rs` | 412 | `execute_transaction` destructures `TxnSummary::asking` (L127-135) and hands it to the host at L188, L214, L233 — **the flag is already `frogdb-txn` data at EXEC time.** |
| `frogdb-server/crates/txn/src/host.rs` | 145 | `TxnHost::validate_queued_batch(..., asking: bool)` (L85) and `watched_slots_still_local(..., asking: bool)` (L101) — the existing plain-data crossings ADR-0002 prescribes. |
| `frogdb-server/crates/server/src/connection/transaction.rs` | 243 | `handle_exec` calls `take_transaction()` (L40). **Sibling 68 owns L78-106 of this file** — see the boundaries table. |
| `frogdb-server/crates/server/src/connection/transaction_conn_command.rs` | 667 | `handle_watch` drives the trait's `watch_key` (L341) and `is_in_multi` (L275); `dispatch_transaction_command` reads `in_transaction` (L421). |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | 1177 | `in_transaction` + `abort_transaction` in the queue-time poisoning arm (L477-482). Sibling 68 owns L201-217. |
| `frogdb-server/crates/server/src/connection/lifecycle.rs` | 749 | The only readers of `queued_commands` (L245) and `watched_key_iter` (L268) — DEBUG MEMORY accounting. |
| `frogdb-server/crates/server/src/connection.rs` | — | QUIT calls `clear_transaction()` (L394). |
| `frogdb-server/crates/server/src/connection/connection_state_conn_command.rs` | 488 | The `ASKING` command executor: `state.set_asking()` (L186). |
| `frogdb-server/crates/server/src/connection/pubsub_conn_command.rs` | 1128 | A **third** non-transactional `asking` consumer: `take_asking()` for keyed SUBSCRIBE routing (L343). Sibling 68 owns `exec_pubsub_in_transaction` (L965). |
| `.scratch/hardening/specs/txn-failure-modes.md` | 650 | LOCKED spec. Rows touching `asking`: FM-TXN-003 (L62-73), FM-TXN-004 (L74-85), FM-TXN-014 (L194-204), FM-TXN-015 (L206-217), FM-TXN-047 (L590-601). Rows naming a **deleted delegation** but *not* `asking`: FM-TXN-002 (L50-60, `:57`), FM-TXN-005 (L86-96, `:93`), FM-TXN-021 (L278-288, `:284`). Rows naming a deleted-delegation *test name* in `Forced by`: FM-TXN-013 (`:191`), FM-TXN-020 (`:275`) — names **kept**. Seven-row edit set enumerated below. |
| `scripts/failure-modes.py` | — | `NEXTEST_CRATES` (L64-77, includes **both** `frogdb-txn` and `frogdb-server`); `SOURCE_ROOTS` (L50, all of `frogdb-server/crates`); `resolve()` (L397-414, load-bearing docstring at **L404-405**). These three facts are what make the `Forced by` lists edit-free — see the discrepancy note. |

## Problem (concrete verified evidence)

### 1. Twelve delegations; nine are pure forwards

Verified census of the `// Transactions (MULTI / EXEC / DISCARD / WATCH)` section
(`state.rs:673-770`), with every production caller outside `state.rs`:

| # | `ConnectionState` method | Body | Production callers (outside `state.rs`) |
| --- | --- | --- | --- |
| 1 | `in_transaction` (L678-680) | `self.transaction.is_open()` | `dispatch.rs:477`, `guards.rs:517`, `transaction_conn_command.rs:421`, `auth_conn_command.rs:165` |
| 2 | `queued_commands` (L684-686) | `self.transaction.queued_commands()` | `lifecycle.rs:245` |
| 3 | `watched_key_iter` (L689-691) | `self.transaction.watched_key_iter()` | `lifecycle.rs:268` |
| 4 | `begin_transaction` (L695-697) | `self.transaction.begin()` | `auth_conn_command.rs:161` only |
| 5 | `push_queued_command` (L701-703) | `self.transaction.push_queued_command(cmd)` | `guards.rs:629` |
| 6 | `abort_transaction` (L707-709) | `self.transaction.abort(error)` | `dispatch.rs:482`, `guards.rs:533/557/568/617` |
| 7 | `fold_transaction_keys` (L715-722) | `self.transaction.fold_keys(..)` | `guards.rs:626` |
| 8 | `watch_key` (L726-729) | `self.transaction.watch_key(..)` | `auth_conn_command.rs:169` |
| 9 | `unwatch_all` (L732-734) | `self.transaction.unwatch_all()` | `auth_conn_command.rs:173` only |
| 10 | `take_transaction` (L743-747) | `transaction.take(self.asking)?` **+ `self.asking = false`** | `transaction.rs:40` |
| 11 | `discard_transaction` (L758-762) | `transaction.discard()?` **+ `self.asking = false`** | `auth_conn_command.rs:177` only |
| 12 | `clear_transaction` (L767-770) | `transaction.clear()` **+ `self.asking = false`** | `connection.rs:394` (QUIT), `state.rs:1083` (RESET) |

Rows 1-9 are one statement with the arguments passed straight through. Their doc comments are
copies of `TransactionState`'s own — L693-694 is verbatim `txn/state.rs:199-200`; L699-700 is
verbatim `:213-214`; L705-706 is verbatim `:221-222`; L724-725 shortens `:244-257` to a `see`
link. **Deletion test**: delete rows 1-9 and their callers say `state.transaction_mut().begin()`
instead of `state.begin_transaction()`. No complexity reappears anywhere, because there was none to
reappear — the module is not hiding a fact, it is renaming one. Nine of twelve entries on the
interface buy the caller zero leverage.

### 2. Five of them are the middle hop of a three-hop chain

`ConnCommandState` (`core/src/conn_command.rs:439-469`) re-declares the same transitions again, and
`auth_conn_command.rs:160-181` implements each as a second forward:

```rust
// auth_conn_command.rs:160-174 — hop 2 of 3
fn begin_multi(&mut self) -> bool { ConnectionState::begin_transaction(self).is_ok() }
fn is_in_multi(&self) -> bool     { ConnectionState::in_transaction(self) }
fn watch_key(&mut self, key: Bytes, shard_id: usize, version: u64, live_at_watch: bool) {
    ConnectionState::watch_key(self, key, shard_id, version, live_at_watch);
}
fn unwatch(&mut self)             { ConnectionState::unwatch_all(self); }
```

So `handle_watch`'s `state.watch_key(..)` (`transaction_conn_command.rs:341`, where `state` is
`&mut dyn ConnCommandState`) travels trait → `ConnectionState` → `TransactionState` to reach one
`HashMap::entry().or_insert()`. Two of the twelve (`begin_transaction`, `unwatch_all`) have **no
caller at all** except this hop; a third (`discard_transaction`) has only this hop plus `state.rs`'s
own tests.

The trait hop is *not* redundant — it is the `ConnCommandState` seam, and it must stay plain-data
because `frogdb-core` cannot depend on `frogdb-txn` (cycle). The `ConnectionState` hop is the one
with nothing behind it.

### 3. The `asking` lifecycle is split across six sites, and its rule lives on the wrong side

Verified — every site in `state.rs` that touches the field:

| # | Site | Line(s) | What it does |
| --- | --- | --- | --- |
| 1 | `take_transaction` | L744-745 | reads `self.asking` into `TransactionState::take`, then clears |
| 2 | `discard_transaction` | L760 | clears |
| 3 | `clear_transaction` | L769 | clears (QUIT/RESET) |
| 4 | `set_asking` | L797-799 | sets (the `ASKING` command) |
| 5 | `take_asking` | L811-816 | **read-and-clear — unless `self.in_transaction()`, then read only** |
| 6 | `is_asking` | L826-828 | peeks without consuming (WATCH's slot probe) |

Site 5 is the whole design:

```rust
pub fn take_asking(&mut self) -> bool {
    if self.in_transaction() {        // ← server-side code querying transaction state
        return self.asking;           //    to decide a flag's lifecycle
    }
    std::mem::replace(&mut self.asking, false)
}
```

The stickiness rule is a *transaction* rule. Implementing it here means `ConnectionState` must ask
`TransactionState` a question (delegation 1) in order to run it, and the three transitions that end a
block (sites 1-3) must each remember to clear a field they do not own. Three separate places encode
"a transaction ending ends the stickiness"; nothing in the type system connects them. The three
`asking`-clearing sites are precisely the three delegations that are not pure forwards — the impurity
and the split are the same defect.

Meanwhile the fact *already* travels into `frogdb-txn` at EXEC: `TxnSummary::asking`
(`txn/state.rs:139-142`) is filled by `TransactionState::take(asking)` (`:275`, `:302`), destructured
by `execute_transaction` (`exec.rs:127-135`), and handed to the host at `exec.rs:188/214/233`. The
`take(asking: bool)` parameter exists for one reason: the state machine that owns the block's
lifetime does not own the flag that is scoped to it.

### 4. FM-TXN-015's unit forcing tests sit outside the crate that is gated

`cargo mutants -p frogdb-txn` runs only that package's own tests (CLAUDE.md: *"Put the forcing test
in the mutated crate"*). Verified locations of FM-TXN-015's five `Forced by` entries:

| Test | File | Crate | Counts toward `frogdb-txn` 0.90? |
| --- | --- | --- | --- |
| `asking_is_one_shot` | `server/src/connection/state.rs:1130` | frogdb-server | **no** |
| `asking_is_sticky_inside_multi_and_consumed_by_exec` | `server/src/connection/state.rs:1143` | frogdb-server | **no** |
| `asking_absent_inside_multi_stays_absent` | `server/src/connection/state.rs:1161` | frogdb-server | **no** |
| `test_multi_exec_on_import_target_with_asking_serves_the_batch` | `server/tests/cluster_migration.rs:3743` | frogdb-server | no (integration, correctly so) |
| `batch_on_import_target_without_asking_is_moved` | `server/src/slot_migration/tests.rs:466` | frogdb-server | no (router, correctly so) |

Three more `asking` rows are in the same position: FM-TXN-004's `asking_cleared_by_discard`
(`state.rs:1174`), FM-TXN-014's `asking_cleared_by_clear_transaction` (`state.rs:1197`), and
FM-TXN-003's `asking_survives_discard_without_multi` (`state.rs:1187`) — the row whose Invariant
(`:69`) names that test *as* the invariant.

So the row whose Invariant is stated in terms of `TransactionState::take` and `TxnSummary::asking`
contributes **nothing** to the mutation gate on the crate that owns them. This is the same structural
defect proposal 45 documents for FM-VLL-005, in the sibling locked crate.

### 5. One spec row is already false — in two independent clauses

`txn-failure-modes.md:81` (FM-TXN-004, Invariant), in full:

> `TransactionState::discard` resets queue, watch set, slot accumulator, `exec_abort` and `asking`
> in one move, and reports the discarded queue length to the metrics recorder under the
> `discarded` label.

Both halves misattribute:

1. **`asking`.** `TransactionState::discard` (`txn/state.rs:309-317`) does
   `*self = TransactionState::default()` — which cannot reset `asking`, because `TransactionState`
   has no such field. The clear happens one level up, in `ConnectionState::discard_transaction`
   (`state.rs:760`).
2. **The metric.** `discard` *returns* `TxnMetrics { queued_count, start_time }`; it reports
   nothing. Emission is the DISCARD executor's, in
   `transaction_conn_command.rs:205-218` — `match state.discard() { Some(metrics) => …
   frogdb_txn::record_transaction_metrics(recorder, "discarded", …) }`. The row credits the
   producer of the numbers with the act of publishing them.

The row describes the design this proposal proposes, not the code that exists. That is a small but
real signal about where the flag belongs; it is also an independently-landable docs fix (below) —
**and it is already being landed separately by the orchestrator, with both clauses corrected.**

## Proposed change

### Part A — `asking` moves into `TransactionState` *(gated on the seam decision)*

```rust
pub struct TransactionState {
    queue: Option<Vec<ParsedCommand>>,
    watches: HashMap<Bytes, (usize, u64, bool)>,
    slots: TxnSlotAccumulator,
    exec_abort: bool,
    queued_errors: Vec<String>,
    start_time: Option<std::time::Instant>,
    /// The cluster ASKING flag. One-shot outside a block; **sticky inside one**,
    /// because the EXEC-time batch re-validation is its last reader. Stored here
    /// rather than on the connection so the one type that knows whether a block
    /// is open is the one type that decides when the flag is consumed.
    asking: bool,
}

impl TransactionState {
    pub fn set_asking(&mut self) { self.asking = true; }
    /// Read-and-clear, except inside an open transaction, where it reads only.
    pub fn take_asking(&mut self) -> bool {
        if self.is_open() { return self.asking; }
        std::mem::replace(&mut self.asking, false)
    }
    pub fn is_asking(&self) -> bool { self.asking }
}
```

Consequences, each a deletion:

- `take(&mut self, asking: bool)` → `take(&mut self)`. The parameter and every `t.take(false)` /
  `t.take(true)` in the in-crate tests (`txn/state.rs:341, 350, 364, 380, 397, 412, 425, 548, 556,
  579, 620`) lose an argument that only ever existed to ferry a field across a crate line.
- `discard` and `clear` already do `*self = TransactionState::default()`, so `asking` clears for
  free — the three explicit `self.asking = false` statements (`state.rs:745, 760, 769`) are deleted,
  and FM-TXN-004's Invariant (§5) becomes true.
- Site 5's `if self.in_transaction()` stops being a cross-type query and becomes `self.is_open()` on
  the same struct.
- `ConnectionState::asking` (L472-476) and its initialiser (L512) are deleted.

### Part B — expose the transaction object instead of twelve delegations

```rust
impl ConnectionState {
    /// The connection's transaction state machine. `TransactionState`'s own
    /// fields stay private; this replaces twelve forwards that added nothing.
    pub fn transaction(&self) -> &TransactionState { &self.transaction }
    pub fn transaction_mut(&mut self) -> &mut TransactionState { &mut self.transaction }
}
```

All twelve go away. Call-site edits are mechanical and total **20 production lines** (`guards.rs` ×7
— L517/533/557/568/617/626/629; `auth_conn_command.rs` ×5 — L161/165/169/173/177; `dispatch.rs` ×2 —
L477/482; `lifecycle.rs` ×2 — L245/268; `transaction.rs` ×1 — L40; `connection.rs` ×1 — L394;
`transaction_conn_command.rs` ×1 — L421; `state.rs:1083` ×1), plus the in-file tests. Under option 2
(rows 1-9 only) the same census gives **16** — rows 10-12's four call lines
(`transaction.rs:40`, `auth_conn_command.rs:177`, `connection.rs:394`, `state.rs:1083`) survive.

**What deliberately stays:**

- The five `ConnCommandState` methods (`conn_command.rs:450-469`). `frogdb-core` cannot name
  `TransactionState`. Their impls in `auth_conn_command.rs` shorten from
  `ConnectionState::begin_transaction(self).is_ok()` to
  `self.transaction_mut().begin().is_ok()` — three hops become two, which is the achievable win.
- `ConnCommandState::set_asking` (`conn_command.rs:388`) for the same reason.
- `TxnSummary::asking` and `TxnHost`'s two `asking: bool` parameters. These are the plain-data
  crossings ADR-0002 asks for and nothing here changes them.

### Decision point — REQUIRES SIGN-OFF AT THE GATE

**ADR-0002 records the decision** (`adr/0002-txn-orchestration-behind-txnhost-seam.md`):

> We extracted the connection-side orchestration — transaction state, the EXEC algorithm,
> outcome/metric mapping — into `frogdb-txn` behind an object-safe `TxnHost` trait;
> `ConnectionHandler` implements the trait and **everything touching connection dispatch, the
> registry, `SlotValidator`, or TLS stays server-side.**

and names the standing cost:

> The cost is the trait indirection: **new transaction behavior needs a seam decision (algorithm
> side or host side); when in doubt, plain-data signatures through the trait.**

**Relationship to the ADR: this consolidates an existing crossing; it does not create a new one.**
`asking` already lives on both sides — set and stored server-side, carried as `TxnSummary::asking`
and consumed through two `TxnHost` methods. Part A moves the *storage* of an already-shared fact; it
adds no new type to the seam and **leaves the `TxnHost` seam byte-identical** (`host.rs:85, :101`
keep their `asking: bool` parameters). The parameter it deletes — `TransactionState::take(asking)` —
is not on the seam at all; see Strengthens #2. What is genuinely in question is whether `frogdb-txn`
may be the home of a flag that is also live outside any transaction.

**The question: does moving `asking` into `TransactionState` strengthen or weaken the ADR-0002 seam?**

*Strengthens:*

1. The stickiness rule is a transaction rule. Today it is implemented in `frogdb-server`
   (`state.rs:811-816`) and can only run by querying transaction state. ADR-0002 put "transaction
   state" in `frogdb-txn`; the rule is transaction state that stayed behind.
2. It is a **locality** win, not a seam win — stated that way deliberately, because the seam
   framing self-undercuts. `take(asking)` → `take()` deletes a parameter from `TransactionState`,
   which is *not* at the `TxnHost` seam; it is the algorithm side's own data. ADR-0002's
   "plain-data signatures through the trait" governs `TxnHost`, and `TxnHost`'s two `asking: bool`
   parameters (`host.rs:85, :101`) are **untouched** by this proposal. So the argument is: the
   field and the rule that governs it come to live in the same struct. It buys no seam
   simplification, and claiming otherwise would be arguing against the ADR's own vocabulary.
3. It relocates FM-TXN-015's evidence into the gated crate (§4), which is what the ADR's mutation
   argument exists for.
4. It makes FM-TXN-004's Invariant true as *originally written* (§5) — but this argument is now
   **weaker than it looks**, because the orchestrator's hotfix is correcting `:81` to describe the
   code that exists. After that lands, option 1 does not make a false row true; it edits a true row
   back toward the wording the spec author reached for first. That is evidence about intent, not a
   defect fixed. Weigh it as a hint, not a win.

*Weakens:*

1. **The sharpest cost: non-transactional code paths acquire a transaction-state mutation.**
   `asking` is a **cluster session flag**, not only a transaction one. It is set by the `ASKING`
   command (`connection_state_conn_command.rs:186`) and read by three consumers, **two of which have
   nothing to do with MULTI**: keyed-command routing (`guards.rs:737`) and keyed SUBSCRIBE
   routing (`pubsub_conn_command.rs:343`). After part A those two lines read
   `self.state.transaction_mut().take_asking()` on connections that never issue MULTI — the
   ordinary single-key routing path now takes a `&mut` on the transaction state machine to
   answer a routing question. Worse, `ConnCommandState::set_asking`'s impl
   (`auth_conn_command.rs:82-84`) becomes `self.transaction_mut().set_asking()`: the trait method
   the `ASKING` *command* runs through is then, by its implementation, a transaction-state
   mutation. **This is the strongest argument for option 3** — option 3 keeps those three lines
   reading a `ClusterSessionFlags` that is honestly named for what they are doing.
   Counter-consideration: the storage move is invisible at the three call sites' *semantics* (the
   flag's read-and-clear behaviour is byte-identical), so the cost is naming and reachability,
   not behaviour. The gate weighs naming against the §4 mutation-weight win.
2. A connection that never issues MULTI stores its ASKING flag inside a type called
   `TransactionState`, in a crate whose `description` reads *"Connection-side MULTI/EXEC
   transaction orchestration and state"* (`txn/Cargo.toml:7`) — verbatim. Option 1 must update
   that line as part of the change, or the manifest starts lying too.
3. ADR-0002 explicitly keeps `SlotValidator`-touching code server-side. `asking` is an input to
   every slot verdict. Storage is not logic — but the ADR's rule is about which facts live where.
4. Redis pairs the two flags: `clearClientConnectionState` clears `CLIENT_ASKING | CLIENT_READONLY`
   together, which `ConnectionState::reset` mirrors (`state.rs:1078-1092`). After part A the pair is
   split across two crates and RESET clears one via `clear_transaction()` and the other inline —
   an asymmetry a future reader must be told about.

**Three options for the gate. The implementer picks none of them.**

| Option | Shape | ADR-0002 posture | Locked-crate impact |
| --- | --- | --- | --- |
| **1** (lane's direction) | Part A + Part B as written. `asking` and its three accessors live on `TransactionState`. | Consolidates the crossing on the algorithm side. | `frogdb-txn` edited → spec edits + 0.90 re-gate. |
| **2** (minimal) | Part B only. `asking` stays on `ConnectionState`; delegations 10-12 survive as the three real methods, 1-9 are deleted. | Leaves the crossing exactly where the ADR left it. | No `frogdb-txn` code edit, no re-gate — but **two spec Invariant re-wordings are still required** (`:93`, `:284`; see step 1). "Zero spec edits" is wrong. |
| **3** (third home) | Part B, plus a `ClusterSessionFlags { asking, readonly }` module in `frogdb-server` owning the stickiness rule via `take_asking(in_transaction: bool)`. | Deepens without crossing the seam; keeps the Redis flag pairing intact for RESET. | **None** to `frogdb-txn`; the full spec edit set of step 1 applies, with FM-TXN-004/015 re-worded to name the new owner rather than `TransactionState`. |

Option 3 is the honest counter-proposal to option 1: it fixes the six-site split and the pure-forward
problem without asking `frogdb-txn` to hold a routing flag, at the cost of passing `in_transaction`
in as an argument (i.e. keeping one query, but making it explicit and plain-data — which is exactly
the ADR's stated fallback). **Options 1 and 3 are a genuine seam choice, not a style preference; the
gate must rule.** Option 2 is available as the no-decision fallback and is strictly a subset of both.

## Before / After (option 1)

```rust
// BEFORE — state.rs:743-747 + :758-762 + :767-770 + :811-816
pub fn take_transaction(&mut self) -> Option<TxnSummary> {
    let summary = self.transaction.take(self.asking)?;
    self.asking = false;
    Some(summary)
}
pub fn discard_transaction(&mut self) -> Option<TxnMetrics> {
    let metrics = self.transaction.discard()?;
    self.asking = false;
    Some(metrics)
}
pub fn clear_transaction(&mut self) { self.transaction.clear(); self.asking = false; }
pub fn take_asking(&mut self) -> bool {
    if self.in_transaction() { return self.asking; }
    std::mem::replace(&mut self.asking, false)
}

// AFTER — all four are gone from ConnectionState; callers say:
self.state.transaction_mut().take()        // transaction.rs:40
self.state.transaction_mut().discard()     // auth_conn_command.rs:177
self.state.transaction_mut().clear()       // connection.rs:394, state.rs:1083
self.state.transaction_mut().take_asking() // guards.rs:737, pubsub_conn_command.rs:343
```

## Testability improvement

**FM-TXN-015's unit evidence moves into the gated crate.** The three `asking` unit tests
(`state.rs:1130/1143/1161`) construct a whole `ConnectionState` today — `state()` at the top of that
test module — to exercise one `bool` and one `Option<Vec<_>>`. Under option 1 they are
`TransactionState::default()` tests in `frogdb-txn`, alongside the eleven that already live there
(`txn/state.rs:336-623`), and for the first time they count toward the 0.90 gate. Same for
FM-TXN-004's `asking_cleared_by_discard` (`state.rs:1174`), FM-TXN-014's
`asking_cleared_by_clear_transaction` (`state.rs:1197`) and FM-TXN-003's
`asking_survives_discard_without_multi` (`state.rs:1187`) — six unit tests in all.
`reset_clears_cluster_flags` (`state.rs:1919-1931`) does **not** move: it asserts the ASKING/READONLY
*pair*, so it stays in `frogdb-server` and is merely rewritten (`s.set_asking()` →
`s.transaction_mut().set_asking()`).

**The stickiness rule becomes assertable at one seam.** Today "a transaction ending ends the
stickiness" is forced by three separate tests exercising three separate `ConnectionState` methods,
each of which could regress independently. With `discard`/`clear` implemented as
`*self = TransactionState::default()`, a single test that asserts *every* field is defaulted covers
all three; a new lifecycle transition that forgets `asking` becomes impossible rather than untested.

**Nine deletions shrink the mutable surface.** `cargo mutants` generates mutants for the pure
forwards in `frogdb-server` today — they are not in the gated crate, so they do not affect the score,
but they do cost `just mutants-diff` time on every touch of `state.rs`. Deleting them removes the
cost with no loss of coverage.

**Test-shape check.** Part B does not weaken encapsulation: `TransactionState`'s fields stay private
(`txn/state.rs:161-180` — the struct's own doc comment says *"Fields are private: the connection
drives the state through the named transitions below"*), and its interface is unchanged. Callers and
tests keep crossing the same seam; there is simply one fewer name to learn at it.

## Risks / scope boundaries vs siblings

| Proposal | Owns (must not be edited by the others) | Overlap with 50 |
| --- | --- | --- |
| **50** (this) | `server/src/connection/state.rs` L472-476, L512, L673-770, L797-828, L1083, L1128-1203, **L1375/L1413/L1445** (the `transaction.fold_shard` test callers), **L1398 + L1430** (the two `take_transaction_*` tests — bodies rewritten, **names kept**, step 2), **L1919-1931** (`reset_clears_cluster_flags`, which calls `set_asking()` then `take_asking()` and must be rewritten under option 1); `txn/src/state.rs` struct fields L161-180 + `take`/`discard`/`clear` L269-322 (+ the new accessors); `auth_conn_command.rs` L82-84 + L158-181; the individual delegation call lines in `guards.rs`/`dispatch.rs`/`lifecycle.rs`/`transaction.rs`/`connection.rs`/`transaction_conn_command.rs` | — |
| **45** vll-key-ownership-diagnostics | `vll/src/shard.rs`, `vll/src/lock_table.rs`, `core/src/shard/{vll,diagnostics}.rs` | **None.** Different crate, no shared file. |
| **46** vll-acquire-error-unify | `vll/src/types.rs`, `server/src/scatter/executor.rs:155-157`, `server/src/connection/scripting/eval.rs:268-281` | **None.** |
| **51** txn-slot-vll-state-small | `txn/src/state.rs` **L47-116** (`TransactionTarget` L16-38 + the `TxnSlotAccumulator::fold_shard`/`note_slot` promotion lattice) + `vll/src/shard.rs` field block/constructor | **Same file, disjoint regions. → LAND 51 FIRST.** 51 owns the `TxnSlotAccumulator` block; 50 owns the `TransactionState` block (L161-322). No signature changes on 51's side, so this is a rebase, not a redesign — but **both re-gate `frogdb-txn` 0.90**, and 51 states the ordering twice from its own side (its boundaries row for 50, and its closing "land the small one first" note): 51 is S, mechanical, and needs no gate sign-off, whereas 50 is M and blocked on an ADR ruling. **Accepted and restated here: land 51, then rebase 50 onto plain `TxnSlotAccumulator`, then run the `frogdb-txn` gate once.** 51 also owns FM-TXN-019/020/042; 50 must not touch those rows (50's step-2 caveat about `take_transaction_folds_cross_shard_watch_set_to_multi` is a *test-name* ruling, not a row edit — the row text is untouched by both). *Informational:* 51's boundaries row describes 50 as a "13-method pass-through"; the verified census is **twelve** (`state.rs:673-770`). Harmless in 51, but do not propagate the count. |
| **52** vll-unknown-txid-refusal | `core/src/shard/vll.rs:40-75`, new FM-VLL-006 row | **None.** ⚠ **No file for 52 exists on disk** (`.scratch/arch-deepening/proposals/` has no `52-*.md`); this row records the *claimed* scope from the lane plan. Different crate and no shared file, so a disjointness claim is safe even unverified. |
| **68** exec-framing-datum (SV8) | `server/src/connection/transaction.rs` **L78-106**; `server/src/connection/dispatch.rs` **L201-217**; `pubsub_conn_command.rs` `exec_pubsub_in_transaction` (**L965**+); `ExecFraming` datum on `CommandSpec` in `frogdb-commands` | **Three shared files, claimed-disjoint line regions — but the contract rests on an unwritten document.** ⚠ **No `68-*.md` exists on disk**, so 68's claimed regions (`transaction.rs:78-106`, `dispatch.rs:201-217`) are **unverifiable from this side**. What *is* verified is 50's own touch points and that they fall outside those ranges: `transaction.rs:40` (the `take_transaction` call), `dispatch.rs:477/482` (the abort arm), `pubsub_conn_command.rs:343` (the `take_asking` call). **File ownership rule (proposed, needs 68's assent once it is written): 68 owns the EXEC framing/dispatch bodies; 50 owns only the state-accessor call lines.** Land in either order; if both are in flight, the second rebases three one-line edits. If 68 turns out to claim `transaction.rs:40` or `dispatch.rs:477-482`, this row is void and the two must be sequenced explicitly. |

**Locked-area landing steps (`frogdb-txn`, gate 0.90) — options 1 and 3 for steps 4-7; step 1
applies in part to *every* option, including 2:**

1. **Spec-first is not required** — no observable behaviour changes. `set_asking` / `take_asking` /
   `is_asking` keep byte-identical semantics; the sticky-inside-MULTI exception is preserved verbatim;
   `TxnSummary::asking` still reaches `validate_queued_batch` unchanged. But **Invariant-row edits are
   required in the same change**, because **seven** rows name a mechanism this proposal deletes.
   Verified against the spec, with the options each row binds:

   | Row | Line | Names | Deleted by |
   | --- | --- | --- | --- |
   | FM-TXN-002 | `:57` (Invariant) | *"`handle_exec` short-circuits on `take_transaction() == None`"* | options **1, 3** (part B deletes delegation 10) |
   | FM-TXN-004 | `:81` (Invariant) | `TransactionState::discard` resetting `asking` + reporting the metric | already false (§5); the orchestrator's hotfix corrects both clauses. Option 1 then edits it a **second** time so clause 1 becomes true as written; option 3 re-words it to name `ClusterSessionFlags` |
   | FM-TXN-005 | `:93` (Invariant) | *"`queue_command` calls `abort_transaction`"* — `guards.rs:544`'s `queue_command` calling `ConnectionState::abort_transaction` (delegation 6) | **ALL options, including 2** |
   | FM-TXN-014 | `:201` (Invariant) | *"`clear_transaction` is the QUIT/RESET seam"* | options **1, 3** (delegation 12) |
   | FM-TXN-015 | `:213` (Invariant) | *"`take_transaction` moves it into `TxnSummary::asking`"* → `TransactionState::take` reads its own field | options **1, 3** |
   | FM-TXN-021 | `:284` (**NOT observable**) | *"the flag leaking into `fold_transaction_keys`"* (delegation 7) | **ALL options, including 2** |
   | FM-TXN-047 | `:597` (Invariant) | *"`take_transaction` runs once, at EXEC entry"* | options **1, 3** |

   `:69` (FM-TXN-003, Invariant) names only a test — **no edit**.

   **Consequence for the gate's choice: option 2 is not spec-free.** `:93` and `:284` name
   `abort_transaction` and `fold_transaction_keys`, both of which part B deletes under every
   option. Neither edit is *lint*-visible — `just lint-failure-modes` checks `Forced by` lists
   against test tags, never Invariant/NOT-observable prose, so option 2 still needs no re-gate and
   no re-run beyond the usual. That is exactly why the edits must be made deliberately: a stale
   method name in a LOCKED spec is precisely the defect §5 condemns, and precisely the defect the
   two-directional lint cannot catch. Doing part B and skipping these two rows would *create* two
   more §5s while fixing one.
2. **`Forced by` lists need no edits — verified, and this contradicts the lane note.** `resolve()`
   (`scripts/failure-modes.py:397-414`) matches a bare trailing segment explicitly *"so that moving a
   test between modules is not a spec edit"* (docstring L404-405); the listing spans `NEXTEST_CRATES`
   (L64-77), which contains both `frogdb-txn` and `frogdb-server`; and `scan_tags` walks
   `SOURCE_ROOTS = frogdb-server/crates` (L50), so the `// FM-TXN-015` tags travel with the tests.
   Moving a test between the two crates without renaming it is invisible to the lint.

   **The one real caveat: two `Forced by` entries embed the deleted method name in the *test* name.**
   - `take_transaction_folds_cross_shard_watch_set_to_multi` — spec `:275` (FM-TXN-020),
     test at `state.rs:1398`.
   - `take_transaction_unwatch_drops_stale_cross_shard_watch_fold` — spec `:191` (FM-TXN-013),
     test at `state.rs:1430`.

   Both tests' *bodies* must be rewritten under part B (`take_transaction()` →
   `transaction_mut().take()`). **KEEP BOTH NAMES.** Renaming them to match the new call would turn
   a body edit into a `Forced by` edit in two rows — the one thing step 2 claims is avoidable — and
   would break `just lint-failure-modes` if the spec side were missed. Sibling 51 makes the same
   ruling for `accumulator_shard_fold_none_single_multi` and cites `state.rs:1398` from its own
   side; the rule is consistent across the lane. A slightly stale test name is cheaper than a
   locked-spec edit.
3. `just lint-failure-modes` after every spec edit (it is part of `just lint`; two-directional).
4. New/moved forcing tests land in **`frogdb-txn`** (§4) — that is the point of option 1.
5. **Record the ruling in ADR-0002** (`adr/0002-txn-orchestration-behind-txnhost-seam.md`, 22 lines
   of prose with no section structure — so this is an appended paragraph, not a new heading). The
   ADR's standing cost clause says *"new transaction behavior needs a seam decision"*; the gate is
   making one, and the next reader who asks "why does a cluster routing flag live in `frogdb-txn`?"
   must find the answer there rather than in this proposal. **Option 3 needs the addendum too** —
   it is equally a ruling, in the other direction. Only option 2 skips it.
6. **Update `txn/Cargo.toml:7`** (option 1 only). The `description` reads *"Connection-side
   MULTI/EXEC transaction orchestration and state"*; after part A the crate also holds a flag live
   outside any transaction. One line, but it is the manifest a reader hits first.
7. `just mutants-diff frogdb-txn` before pushing (push discipline); full `just mutants frogdb-txn` +
   `just mutants-gate frogdb-txn 0.90` for the re-gate. The crate held 100% at lock (ADR-0002), and
   part A adds three trivial accessors whose forcing tests arrive with them; the risk is that
   `take_asking`'s two arms need both directions asserted — the moved tests already do that.
8. **`frogdb-vll` is untouched**, so no vll re-gate, even though the locked *area* spans both crates.

**Other risks:**

- **`transaction_mut()` widens what a caller can reach.** `ConnectionState` currently exposes twelve
  named transitions; part B exposes the whole `TransactionState` interface — **thirteen** public
  methods today (`is_open`, `queued_commands`, `watched_key_iter`, `begin`, `push_queued_command`,
  `abort`, `fold_keys`, `fold_shard`, `watch_key`, `unwatch_all`, `take`, `discard`, `clear`;
  `txn/state.rs:184-322`), **sixteen** after part A adds `set_asking` / `take_asking` /
  `is_asking` — to any holder of `&mut ConnectionState`. All are already-public and
  invariant-preserving, and the struct's fields stay private, so nothing new is *mutable*; but the
  count of reachable names goes up before it goes down. Call it out in review rather than
  pretending it is a pure subtraction.
- **`TransactionState::fold_shard` is production-dead, and part B makes it reachable everywhere.**
  Verified: the only callers of the public `fold_shard` (`txn/state.rs:240-242`) are tests —
  `txn/state.rs:362, 410` in-crate and `server/src/connection/state.rs:1375, 1413, 1445` in
  `state.rs`'s own `#[cfg(test)] mod tests` (L1114+). No production line calls it; `take`'s
  watch-fold reaches the accumulator directly (`txn/state.rs:286`, via the private
  `TxnSlotAccumulator::fold_shard` at L74). So part B promotes a method with **zero** production
  callers from "reachable only through a `ConnectionState` that never forwards it" to "reachable
  from every `&mut ConnectionState` holder" — the widening above, at its worst instance. It is a
  deletion candidate in its own right (its three `state.rs` test callers would move to constructing
  a `TransactionState` directly). **Sibling 51 owns the `fold_shard` / `note_slot` factoring
  (`txn/state.rs:47-116`) — do not duplicate that work here**; note the observation, and if 51
  lands first the deletion question is answered on 51's side.
- **The `ASKING`/`READONLY` split (option 1 only).** `reset()` (`state.rs:1078-1092`) clears both;
  after part A it clears `readonly` inline and `asking` transitively via `clear()`. The existing
  comment at L1082 (*"Clears the MULTI-sticky ASKING flag along with the queue"*) must be updated, and
  a one-line note added at the `readonly` field explaining why its twin lives elsewhere.
- **Merge-order churn.** 50 edits single lines in six files that four other proposals also touch.
  Nothing conflicts semantically; the cost is rebase noise. The boundaries table is the contract.
- **No live bug is fixed here.** This is a locality/leverage change plus a mutation-weight relocation.
  The FM-TXN-004 docs defect that *was* found is already being landed separately (see below), so it
  is no longer part of 50's value. If the gate is short on capacity, option 2 is what remains — and
  it still carries the two `:93`/`:284` spec re-wordings, so "cheap" is not "free".

## Effort estimate

- **Option 1 (parts A + B): M.** One field moved plus three accessors in `frogdb-txn`; twelve methods
  deleted from `ConnectionState`; **20** production call-site lines rewritten; six unit tests moved
  between crates plus `reset_clears_cluster_flags` (`state.rs:1919-1931`) and the two
  `take_transaction_*` test bodies rewritten in place; eleven in-crate `take(bool)` call sites
  de-argumented; **seven** spec row edits (step 1); the ADR-0002 addendum; one `Cargo.toml` line.
  The `frogdb-txn` re-gate is the long pole, not the diff.
- **Option 2 (part B only): S.** Nine deletions, **16** call-site lines (rows 1-9), zero locked-crate
  *code* edits, no re-gate — but **two spec Invariant re-wordings** (`:93`, `:284`), not zero.
- **Option 3 (part B + `ClusterSessionFlags`): M.** Part B (all twenty call-site lines), plus one new
  server-side module owning both flags and the stickiness rule, plus the step-1 spec edits re-worded
  to name the new owner, plus the ADR-0002 addendum. No `frogdb-txn` edit, so no re-gate — but the
  ASKING tests stay outside the gated crate, which forfeits the §4 win.

### Independently-landable hotfix — **being landed separately by the orchestrator**

**FM-TXN-004's Invariant is factually wrong today** (§5), in **both** of its clauses. Review
confirmed the finding and widened it; the orchestrator is landing the corrected row outside this
proposal, so **50 does not carry this fix** — it is recorded here only so the gate can see what has
already been subtracted from 50's scope.

The correction to `.scratch/hardening/specs/txn-failure-modes.md:81` covers:

1. **The asking attribution.** `TransactionState::discard` (`txn/state.rs:309-317`) cannot reset
   `asking` — the struct has no such field. The clearing site is
   `ConnectionState::discard_transaction` (`state.rs:760`), and the row must name it.
2. **The metrics clause.** *"…and reports the discarded queue length to the metrics recorder under
   the `discarded` label"* also misattributes: `discard` **returns** `TxnMetrics`; the DISCARD
   executor is what emits, at `transaction_conn_command.rs:205-218`
   (`frogdb_txn::record_transaction_metrics(recorder, "discarded", …)`).

Docs-only: no code change, no test change, no mutation re-gate, and the `Forced by` list is
untouched, so `just lint-failure-modes` stays green. It is worth landing regardless of how the gate
rules, because a locked spec that misdescribes its own mechanism is exactly what the two-directional
lint cannot catch.

**Interaction with the gate's ruling.** If the gate picks option 1, `:81` is edited a *second* time
so clause 1 says `TransactionState::discard` and means it (clause 2's fix stands under every
option — the metric emission does not move). If the gate picks option 3, clause 1 is re-worded again
to name `ClusterSessionFlags`. Either way the hotfix is the correct intermediate state, not wasted
work: it makes the row true about the code that exists today.
