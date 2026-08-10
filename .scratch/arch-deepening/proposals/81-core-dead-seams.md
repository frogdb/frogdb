# Proposal 81 — Two dead seams in the shard: a connection channel nobody sends on, and a wait queue that unlinks entries six ways

Round 38 · lane: protocol / net / core · candidates **PN2 + PN3** · effort **S** (PN2)
+ **S** (PN3), independently landable · **no locked crate edited** (`frogdb-core`,
`frogdb-server`, `frogdb-shard-harness`), **zero `FM-` tags in any edited region**

**Verified at HEAD `54baa2bb`** (worktree `arch-round-38-99`, branch `main`). Every file:line
below was re-derived at this SHA; nothing is inherited from the lane brief. Concurrent authors
are editing `frogdb-server/crates/protocol/src/response.rs` (proposal 80) and several proposal
`.md` files; neither is in this file set.

**Four brief claims are corrected, and one of the corrections turns a "latent cleanup" into a
LIVE correctness bug:**

| Brief claim | Correction at HEAD |
|---|---|
| PN2: "`ShardWorker::handle_new_connection`" lives in `connection.rs` | It lives in **`core/src/shard/persistence.rs:11-24`** — an `impl ShardWorker` block bolted onto the top of the *persistence bridge* file, above that file's own "One decision lives here" doc banner (`:26-35`). `connection.rs` (18 lines) holds only the `NewConnection` struct. |
| PN2: "~9 harness sites" | **25 channel-construction sites across 26 code files** (24 test/harness + the one production site, `init.rs:222`), plus a 27th file (`server/util.rs:14`, the now-unreferenced `NEW_CONN_CHANNEL_CAPACITY = 256`) and a **28th, the published architecture page**, which documents the seam as live. Still mechanical, still **S**, but the blast radius is 3× the brief's number and it collides with three sibling proposals (§Boundaries). |
| PN3: `pop_oldest_xreadgroup_waiter` is a "line-for-line copy of `pop_oldest_waiter_of_kind`, only predicate differs" | True, **and the shared body is wrong**. Both copies — and the dead third, `pop_oldest_waiter` — skip the popped key when unlinking (`:261`, `:457`, `:545`), which leaks a stale slot index whenever a waiter names the same key twice (`BLPOP k k 0`). After the slot is recycled, the leak **serves a blocking client an element from a key it never asked for**. See §Problem 4. **LIVE.** |
| PN3 is "**Latent**" | Half right. The three dead methods are latent. The duplicated pop body is **LIVE** (above), and the repo already treats duplicate keys as in-contract: `redis-regression/tests/list_tcl.rs:1454` (`tcl_blpop_with_same_key_multiple_times_issue_801`) exercises exactly the shape that triggers it, and passes only because the recycled slot happens to go back to a waiter on the same keys (§Problem 4). |

Two further findings the brief did not name: the architecture page describes a message-passing
seam that does not exist (§Problem 3), and `ShardWaitQueue`'s `pub use` at
`core/src/lib.rs:149` is what suppresses `dead_code` on all three dead methods (§Problem 5).

## Summary

Two independent deletions, one theme: **`frogdb-core`'s shard module exports interfaces wider
than its implementations, and the width is what hides the defects.**

- **PN2 — `NewConnection` is a seam with no traffic.** `ShardWorker::handle_new_connection`
  (`persistence.rs:11-24`) is a `tracing::debug!` and two comments saying the real work happens
  elsewhere. Its `select!` arm (`event_loop.rs:122-124`) is one of seven arms in the hottest loop
  in the system and it can never fire, because **`AcceptorContext.new_conn_senders`
  (`acceptor.rs:110`) is constructed, cloned per acceptor, and never `send`-ed on** — verified by
  a repo-wide grep. The acceptor spawns the connection task directly (`acceptor.rs:358-360`).
  What remains is a **required** builder field (`builder.rs:337-339`,
  `MissingField("new_conn_rx")`), so 24 test and harness sites must mint a dummy channel to build
  a worker at all, a per-shard 256-slot channel allocated at boot (`init.rs:222`,
  `util.rs:14`), a `Vec` threaded through `init.rs` → `mod.rs` → `subsystems.rs` → `shards.rs`,
  and a published architecture page that documents the whole thing as the connection-assignment
  mechanism. Deleting it removes an **interface that carries no decision** and, with it, the
  builder's ability to fail for a reason that no longer exists.
- **PN3 — `ShardWaitQueue` has six copies of one operation and three methods nobody calls.**
  Three public methods have **zero callers workspace-wide**: `pop_oldest_waiter` (`:246`),
  `has_waiters` (`:400`), `has_xreadgroup_waiters` (`:509`) — 75 lines. `pop_oldest_xreadgroup_waiter`
  (`:526`) is `pop_oldest_waiter_of_kind` (`:430`) with one predicate changed. And underneath both,
  the *same* operation — "unlink entry `idx` from every index" — is written out **six times**
  (`:222-238`, `:260-289`, `:313-337`, `:370-394`, `:456-485`, `:545-570`). The three copies that
  are written the "clever" way, skipping the key already handled, are **exactly the three that are
  wrong** (§Problem 4). One private `remove_entry_at(idx, keys)` makes the operation have one
  spelling, and the bug cannot be written a seventh time.

Neither change alters production behaviour except where §Problem 4 says the current behaviour is
wrong, and that fix is carved out as a two-line hotfix that does not wait on the refactor.

## Files involved

### PN2 — `NewConnection`

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/core/src/shard/connection.rs` | 18 | **Deleted whole.** `NewConnection` struct `:2-9` + hand-written `Debug` `:11-18`. |
| `frogdb-server/crates/core/src/shard/persistence.rs` | 908 | **Primary.** Delete the `use` `:8` and the grafted-on `impl ShardWorker` block `:11-24` (`handle_new_connection`). The file's real subject starts at its own banner `:26-35`. **13 `FM-PERSISTENCE-*` tags live at `:607`–`:883`, all ≥583 lines below the deleted block** — see §Spec clearance. |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 1229 | **Primary.** The `select!` arm `:119-124` (arm 6 of 7) and the fairness-comment mention `:48`. Test scaffolding `:632`, `:637`, `:1169`, `:1176`. |
| `frogdb-server/crates/core/src/shard/builder.rs` | 523 | **Primary.** `use` `:23`; doc example `:86`; field `:99`; init `:133`; `with_new_conn_rx` `:166-170`; the required-field check `:337-339`; struct-literal field `:439`; the `# Panics` bullet `:486`; test `:507`, `:510`. **Owned concurrently by proposal 66 — see §Boundaries.** |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | **Primary.** `use` `:24`; field `:117`; **four** public constructors take `new_conn_rx` and forward it: `new` `:385-399`, `with_eviction` `:402-425`, `with_fake_persistence` `:434-459`, `with_persistence` `:463-495`. Test `:988`. |
| `frogdb-server/crates/core/src/shard/mod.rs` | 96 | **Primary.** `mod connection;` `:30`; `pub use connection::NewConnection;` `:71`; the module doc's builder example `:19`. |
| `frogdb-server/crates/server/src/acceptor.rs` | 584 | **Primary.** `use` `:4`; the dead field `new_conn_senders` `:109-110` on `AcceptorContext`; the test-fixture init `:405`, `:418`. The live connection path is `:358-360` (`spawn(conn_future)`), which never consults the field. |
| `frogdb-server/crates/server/src/server/init.rs` | 669 | **Primary.** `InitResult` fields `:52-53`; the boot loop `:217-218`, `:222`, `:226-227`; the return literal `:467-468`; the `use` of the capacity constant `:27`. **Owned concurrently by 63/64.** |
| `frogdb-server/crates/server/src/server/util.rs` | — | **Primary.** `NEW_CONN_CHANNEL_CAPACITY: usize = 256` `:14` — sole consumer is `init.rs:222`. |
| `frogdb-server/crates/server/src/server/mod.rs` | 598 | **Primary.** `Server` field `:94`; `ShardSpawnContext` init `:380`; `Server` init `:469`. **Owned concurrently by 63/64.** |
| `frogdb-server/crates/server/src/server/shards.rs` | 367 | **Primary.** `ShardSpawnContext.new_conn_receivers` `:24`; the `.zip(ctx.new_conn_receivers.into_iter())` at `:86` collapses back to a plain `enumerate()` over `shard_receivers`. |
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **Primary, one line.** `new_conn_senders: std::mem::take(&mut self.new_conn_senders)` `:558`. **Owned concurrently by 63/64/74** — 64 explicitly plans to move this exact expression into `build_acceptor_ctx`. |
| `frogdb-server/crates/shard-harness/src/harness.rs` | 399 | **Primary.** `_conn_txs` field `:53` (a `Vec` whose doc says it is "held open so shard queues never close"), channel `:80-81`, builder call `:84`, struct init `:94`. **Owned concurrently by 66.** |
| 10 further `frogdb-core` files with dummy-channel ceremony only | — | `vll.rs:121,127`; `blocking.rs:1595,1598,1603`; `dispatch_pubsub.rs:128,134,137`; `diagnostics.rs:502,508`; `panic_guard.rs:343,350`; `post_execution.rs:972,977,992`; `rollback.rs:147,547,550`; `execution.rs:1359`; `dispatch_core.rs:251`; `eviction.rs:519,543,552`. Each drops one `let (_tx, rx) = mpsc::channel(16);` and one argument. |
| 5 `frogdb-shard-harness` test files | — | `eviction_spill_failure.rs:61,88,95`; `shard_driver.rs:34,38,43,104,109`; `rendering_incrbyfloat.rs:30,32,37`; `scenario_s6.rs:17,39,44,49`; `script_timeout_effects.rs:70,77,84`. |
| `website/src/content/docs/architecture/architecture.md` | — | **Doc defect, claimed.** `:39` (the message-type bullet), `:326` (the component-interaction table row), `:453` (the sequence-diagram arrow). No `docs-spec` source names `NewConnection`, so this page is the authority and it is wrong. §Problem 3. |

### PN3 — `ShardWaitQueue`

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/core/src/shard/wait_queue.rs` | 931 | **Primary and effectively sole.** `ShardWaitQueue` `:51-81`; the six unlink copies `:222-238`, `:260-289`, `:313-337`, `:370-394`, `:456-485`, `:545-570`; the three dead methods `:243-292`, `:399-405`, `:503-520`; the duplicated pop `:522-573`; `entry_matches_kind` `:490-501`; test module `:672-931` (**11 tests**, listed in §Testability). **Zero `FM-` tags in the file.** |
| `frogdb-server/crates/core/src/shard/blocking.rs` | 2090 | **Read-only for PN3** (its only PN3-visible lines are call sites that keep compiling). `drive_satisfaction_body` `:271-...`, the `has_waiters_for_kind` loop guard `:292`, the pop `:306`; `drain_stream_waiters_with_error` `:493-505` and `drain_stream_waiters_wrongtype` `:512-519`, both looping on `pop_oldest_xreadgroup_waiter` (`:494`, `:513`). One `FM-CLUSTER-038` tag at `:2065` — **not in any region PN3 edits**. Also PN2's dummy-channel file. |
| `frogdb-server/crates/core/src/shard/diagnostics.rs` | — | **Read-only evidence.** `BlockedKeys::set(.., blocked_keys_count(), ..)` `:418-422` — the metric the index leak inflates (§Problem 4). |
| `frogdb-server/crates/core/src/lib.rs` | — | **Read-only evidence.** `ShardWaitQueue`, `WaitEntry` re-exported at `:149-150`; this `pub` re-export is why `dead_code` never fires on the three dead methods (§Problem 5). |
| `frogdb-server/crates/redis-regression/tests/list_tcl.rs` | — | **Read-only evidence.** `tcl_blpop_with_same_key_multiple_times_issue_801` `:1454-1497` — the existing duplicate-key contract test, which the leak survives (§Problem 4). |
| `frogdb-server/crates/server/src/connection/blocking.rs` | — | **Read-only evidence.** `handle_blocking_wait` `:34-74` passes `keys.to_vec()` verbatim (`:101`) with **no dedupe**; `cleanup_wait` `:134-156` sends `UnregisterWait` **only** on `Timeout`/`Unblocked` (`:150`), so a served waiter never triggers the retain-based cleanup that would have repaired the leak. |
| `frogdb-server/crates/commands/src/blocking.rs` | 905 | **Read-only evidence.** Seven multi-key blocking commands hand `keys.to_vec()` through unchanged: `:96`, `:178`, `:436`, `:521`, `:606`, `:750`, plus XREAD. No duplicate-key rejection anywhere. |
| `frogdb-server/crates/core/tests/common/mock_streams.rs` | — | **Read-only, disambiguation.** Its `MockStreamWaitQueue::has_waiters(&self)` `:27` is a **different type**; the three `queue.has_waiters()` calls in `core/tests/concurrency.rs` (`:1537`, `:1581`, `:1635`) bind to it, **not** to `ShardWaitQueue::has_waiters(&self, key)`. This is the one trap in the dead-method claim and it is checked. |

## Problem

### 1. PN2 — a seven-arm `select!` with one arm that is provably unreachable

`event_loop.rs:119-124`:

```rust
// 6. Handle new connections — rare relative to steady-state
// message traffic, so prioritizing it ahead of dispatch costs
// nothing and keeps CLIENT accept latency low.
Some(new_conn) = self.new_conn_rx.recv() => {
    self.handle_new_connection(new_conn).await;
}
```

The comment is a **priority argument about a branch that cannot fire**, sitting in the one loop
whose branch order the tree treats as a real fairness decision — `:40-55` carries 16 lines of
`biased;` rationale, and `:48` cites `new_conn_rx` by name as one of the two arms that "can be
always ready under load". Neither half is true. The handler it guards, in full
(`persistence.rs:11-24`):

```rust
impl ShardWorker {
    /// Handle a new connection assigned to this shard.
    pub(crate) async fn handle_new_connection(&self, new_conn: NewConnection) {
        tracing::debug!(shard_id = self.shard_id(), conn_id = new_conn.conn_id,
                        addr = %new_conn.addr, "New connection assigned to shard");

        // Connection handling is spawned as a separate task
        // The actual connection loop is implemented in the server crate
    }
}
```

The producer side is dead on arrival. `AcceptorContext.new_conn_senders` (`acceptor.rs:110`) is
built once at `subsystems.rs:558`, cloned into every acceptor, and **never sent on**: a repo-wide
grep for the identifier returns exactly eight sites — one declaration (`acceptor.rs:110`), one
test-fixture init (`:418`), the `InitResult` declaration and boot loop (`init.rs:52`, `:217`,
`:226`, `:467`), the `Server` field (`mod.rs:94`, `:469`) and the `mem::take` (`subsystems.rs:558`)
— and **no `.send(`**. The acceptor's real connection path is `acceptor.rs:358-360`, which spawns
the connection future directly and never consults the field.

So the seam costs, today, without buying anything:

| Cost | Site |
|---|---|
| A required builder field whose absence is a build error | `builder.rs:337-339` — `MissingField("new_conn_rx")` |
| **24** dummy `mpsc::channel` constructions in tests/harnesses | the 25 sites listed in §Files, minus the one production site |
| `num_shards` × 256-slot bounded channels allocated at boot, never used | `init.rs:222` + `util.rs:14` |
| A `Vec<Sender>` and a `Vec<Receiver>` threaded through four server files | `init.rs` → `mod.rs` → `subsystems.rs` / `shards.rs` |
| Four `ShardWorker` constructors carrying a parameter they discard | `worker.rs:385`, `:402`, `:434`, `:463` |
| A public architecture page documenting it as live | §Problem 3 |

Applying the **deletion test**: delete `NewConnection`, `handle_new_connection`, the arm, the
field, the two `Vec`s and the capacity constant, and **nothing reappears anywhere** — no
complexity migrates to a caller, because there is no caller. That is the definition of a
pass-through, at module scale.

### 2. PN2 — the harness pays a per-worker tax to satisfy a field the worker ignores

`shard-harness/src/harness.rs` keeps a whole field for it (`:53`):

```rust
/// Held open so shard queues never close under the workers.
_conn_txs: Vec<mpsc::Sender<NewConnection>>,
```

The doc comment describes a real hazard for `message_rx` and asserts it for a channel whose
receiver is never polled to completion by anything. Every one of the five `shard-harness` test
files repeats the same three lines (channel, builder call, keep-alive binding), and ten further
`frogdb-core` test modules repeat two of them. This is **locality** damage of the cheapest kind:
a fact with zero information content, restated at 25 sites, that a reader of any one of them must
chase to `event_loop.rs:122` before learning it means nothing.

### 3. PN2 — the published architecture page documents a mechanism that does not exist

`website/src/content/docs/architecture/architecture.md` states the seam three times:

- `:39` — under **Message-Passing Over Shared State**: "`NewConnection` for connection assignment
  from the acceptor".
- `:326` — the component-interaction table: "**Acceptor -> ShardWorker** | New connections sent
  via `NewConnection { socket, addr, conn_id }` struct".
- `:453` — the request-flow sequence diagram: `Acceptor->>Handler: NewConnection (shard 2)`.

No file under `website/docs-spec/specs/` mentions `NewConnection`, so this content page is its own
source of truth and there is no spec edit to sequence ahead of it. Adjacent and **not claimed
here**: the same section asserts each shard owns "All connections pinned to it" (`:31`), which the
dead seam makes doubtful — connections are handled by spawned tasks that route per key, not pinned
per shard. That sentence predates this proposal, is a broader claim than the three lines above,
and is recorded as a follow-up rather than rewritten in a deletion commit. **Latent** (a
documentation defect, not a wire defect), claimed as hotfix **H3**.

### 4. PN3 — the popped key is skipped when unlinking, and duplicate keys make that a LIVE bug

`ShardWaitQueue` indexes each waiter three ways: `waiters_by_key: HashMap<Bytes, VecDeque<usize>>`
(`:53`), `entries: Vec<Option<WaitEntry>>` (`:55`) with a `free_slots` stack (`:57`), and
`conn_entries` (`:59`). `register` pushes the slot index **once per key in `entry.keys`**, with no
dedupe (`:197-202`):

```rust
for key in &keys {
    self.waiters_by_key.entry(key.clone()).or_default().push_back(slot_idx);
}
```

Duplicate keys reach it. `handle_blocking_wait` passes `keys.to_vec()` through
(`server/src/connection/blocking.rs:101`), and every multi-key blocking command builds its key
list verbatim (`commands/src/blocking.rs:96`, `:178`, `:436`, `:521`, `:606`, `:750`). `BLPOP k k
0` is legal Redis and the repo already has a contract test for it
(`list_tcl.rs:1454`). So `waiters_by_key["k"]` legitimately holds the same index twice.

Now compare the two families of removal:

| Family | Sites | Unlink strategy | Duplicate-safe? |
|---|---|---|---|
| retain-based | `unregister` `:222-238`, `collect_expired` `:313-337`, `drain_waiters_for_slot` `:370-394` | `for key in &entry.keys { waiters.retain(\|&i\| i != idx) }` — every key, every occurrence | **Yes** |
| pop-based | `pop_oldest_waiter` `:260-289`, `pop_oldest_waiter_of_kind` `:456-485`, `pop_oldest_xreadgroup_waiter` `:545-570` | one `VecDeque::remove(pos)` for the popped key, then `retain` over `entry.keys.iter().filter(\|k\| *k != key)` — **the popped key is deliberately excluded** | **No** |

`VecDeque::remove(found_pos)` drops **one** occurrence. The `filter(|k| *k != key)` at `:261`,
`:457` and `:545` then guarantees the other occurrence is never cleaned. The trailing
"remove the key if its deque is now empty" guard (`:481-485`, `:566-570`) does not fire, because
the deque is not empty — it holds a stale index.

**Stage 1 (leak, always).** After serving `BLPOP k k 0`, `waiters_by_key["k"] == [idx]` with
`entries[idx] == None` and `idx` pushed onto `free_slots` (`:477`). Nothing removes it:
`unregister` is only reached via `UnregisterWait`, which `cleanup_wait`
(`server/src/connection/blocking.rs:148-152`) sends **only** on `Timeout`/`Unblocked`, never on a
successful serve. The map entry survives for the shard's lifetime, and
`blocked_keys_count()` (`:581`) over-reports it into the **`BlockedKeys` gauge**
(`diagnostics.rs:418-422`).

**Stage 2 (mis-delivery, once the slot is recycled).** `free_slots` is a LIFO stack; the very next
`register` reuses `idx` (`:185-188`). The stale entry in `waiters_by_key["k"]` now points at a
**live, different** waiter:

1. Client A: `BLPOP k k 0` on an empty `k` → slot `0`; `waiters_by_key["k"] == [0, 0]`.
2. `LPUSH k v1` → `pop_oldest_waiter_of_kind("k", List)` serves A. `waiters_by_key["k"] == [0]`
   (stale), `free_slots == [0]`.
3. Client B: `BLPOP j 0` → `register` pops slot `0`; `entries[0] = B`;
   `waiters_by_key["j"] == [0]`; `waiters_by_key["k"]` **still** `== [0]`.
4. `LPUSH k v2` → `has_waiters_for_kind("k", List)` (`:412`) resolves index `0` to **B**, whose op
   is `BLPop`, and returns `true`. `pop_oldest_waiter_of_kind("k", List)` pops B and
   `strat.satisfy` pops the element from **`k`**.
5. **B, who issued `BLPOP j 0`, receives `["k", "v2"]`** — a key it never named, carrying an
   element that a legitimate `k` waiter should have had.

That is cross-key mis-delivery plus a FIFO-fairness violation, reachable from two ordinary client
sessions. The same defect exists on the XREADGROUP path (`:545`), reached from
`drain_stream_waiters_with_error` / `_wrongtype` (`blocking.rs:494`, `:513`) whenever an
`XREADGROUP … STREAMS s s $ $` waiter is drained.

**Why the existing regression test does not catch it.** `tcl_blpop_with_same_key_multiple_times_issue_801`
(`list_tcl.rs:1454-1497`) issues `BLPOP list1 list2 list2 list1 0` twice on the **same connection**.
The second `register` does recycle the leaked slot — but back to a waiter naming the *same* four
keys, so every stale index happens to point at a waiter that legitimately belongs there. The test
passes and the queue is left corrupt. That is the signature of a bug the tests cannot see because
the invariant it violates is not named anywhere.

### 5. PN3 — three dead methods, and why the compiler is silent

| Method | Site | Callers (workspace-wide) |
|---|---|---|
| `pop_oldest_waiter` | `:243-292` (50 lines) | **0** |
| `has_waiters` | `:399-405` (7 lines) | **0** — the three `queue.has_waiters()` calls in `core/tests/concurrency.rs` (`:1537`, `:1581`, `:1635`) are the **zero-argument** method on `MockStreamWaitQueue` (`core/tests/common/mock_streams.rs:27`), a different type |
| `has_xreadgroup_waiters` | `:503-520` (18 lines) | **0** — its own doc comment (`:505-508`) describes a caller ("Used by the drain-on-delete path") that calls the *pop* instead |

75 dead lines, all `pub`, none warned about: `dead_code` does not fire on public items reachable
from the crate root, and `core/src/lib.rs:149` re-exports `ShardWaitQueue` at the top level. The
re-export buys nothing — grepping the workspace for `ShardWaitQueue` outside `frogdb-core` returns
**three doc-comment mentions and no type use** (`server/tests/common/{quiescence_probe.rs:251,
invariants.rs:113, workload_runner.rs:54}`). The **interface is wider than any consumer**, and the
width is precisely what makes deadness invisible.

### 6. PN3 — one operation, six spellings

Six blocks unlink one entry from the three indices. They differ in whether they filter the popped
key, whether they consult `conn_entries` before or after, and whether they clean the primary key's
deque at the end:

| Site | Function | Lines | Filters the popped key? |
|---|---|---|---|
| `:222-238` | `unregister` | 17 | n/a (no popped key) |
| `:260-289` | `pop_oldest_waiter` (dead) | 30 | **yes — wrong** |
| `:313-337` | `collect_expired` | 25 | n/a |
| `:370-394` | `drain_waiters_for_slot` | 25 | n/a |
| `:456-485` | `pop_oldest_waiter_of_kind` | 30 | **yes — wrong, LIVE** |
| `:545-570` | `pop_oldest_xreadgroup_waiter` | 26 | **yes — wrong, LIVE** |

There is no place where "what it means to remove a waiter" is written once, so there was no place
for the three pop copies to disagree with the three retain copies *visibly*. The **leverage** of a
single `remove_entry_at` is not the ~90 lines it saves; it is that the class of bug in §Problem 4
stops being expressible.

## Proposed change

### PN2 — delete the seam, and let the builder stop being able to fail for it

A pure deletion, in one commit, in this order (each step compiles):

1. **`frogdb-core`.** Delete `shard/connection.rs`, `mod connection;` (`mod.rs:30`) and the
   `pub use` (`mod.rs:71`). Delete `handle_new_connection` and its `use` (`persistence.rs:8`,
   `:11-24`) — `persistence.rs` becomes a file about persistence. Delete the `select!` arm
   (`event_loop.rs:119-124`) and correct the fairness comment (`:48`) to name only `message_rx`,
   which is now the sole always-ready arm — a **more** accurate statement of the `biased;`
   rationale, not a weaker one. Delete the builder field, setter, required-field check and
   struct-literal field (`builder.rs:23`, `:86`, `:99`, `:133`, `:166-170`, `:337-339`, `:439`,
   `:486`); `ShardWorker.new_conn_rx` (`worker.rs:117`) and the parameter from all four
   constructors (`:385`, `:402`, `:434`, `:463`).
2. **`frogdb-server`.** Delete `AcceptorContext.new_conn_senders` (`acceptor.rs:109-110`, `:418`),
   `InitResult`'s two `Vec`s (`init.rs:52-53`, `:217-218`, `:226-227`, `:467-468`), the channel
   construction and its capacity constant (`init.rs:222`, `util.rs:14`, and the `use` at `:27`),
   the `Server` field (`mod.rs:94`, `:469`), the `ShardSpawnContext` field (`mod.rs:380`,
   `shards.rs:24`) — `shards.rs:83-88`'s `.zip(...)` collapses to `enumerate()` — and the
   `mem::take` (`subsystems.rs:558`).
3. **Harnesses.** Delete 24 `let (_tx, rx) = mpsc::channel(..)` bindings, 24 `.with_new_conn_rx`
   / positional arguments, `harness.rs`'s `_conn_txs` field and its `NewConnection` import.
   Mechanical; `sed`-able per file.
4. **Docs.** Remove the three `NewConnection` claims from
   `website/src/content/docs/architecture/architecture.md` (`:39`, `:326`, `:453`). §Problem 3
   establishes there is no `docs-spec` source to edit first.

#### Depth and locality

The shard event loop's **interface** shrinks from seven inputs to six while its
**implementation** is untouched — the loop gets *deeper* by the only measure that matters, since
one of the seven inputs was pure interface. `ShardWorkerBuilder` loses one of its four required
fields, so `MissingField("new_conn_rx")` becomes an unrepresentable failure rather than an
untested one. And "how does a connection reach a shard?" acquires exactly one answer
(`acceptor.rs:358-360`, spawn a task that routes by key) instead of one true answer and one
documented-but-false one.

### PN3 — one unlink, one pop, and three deletions

**(a) Delete the three dead methods.** `pop_oldest_waiter` (`:243-292`), `has_waiters`
(`:399-405`), `has_xreadgroup_waiters` (`:503-520`). 75 lines, zero callers, no behaviour change.
Narrow `core/src/lib.rs:149`'s re-export at the same time or leave it — see §Risks; the deletion
does not depend on it.

**(b) One private unlink.** The six copies collapse to one method whose whole job is the
index invariant:

```rust
/// Unlink `idx` from every index. `entry.keys` may name the same key more than
/// once (`BLPOP k k 0`), so this retains over *all* of them — including one the
/// caller has already partially removed. Idempotent per key.
fn remove_entry_at(&mut self, idx: usize, keys: &[Bytes]) {
    for key in keys {
        if let Some(w) = self.waiters_by_key.get_mut(key) {
            w.retain(|&i| i != idx);
            if w.is_empty() { self.waiters_by_key.remove(key); }
        }
    }
    // conn_entries cleanup … free_slots.push(idx); self.waiter_count -= 1;
}
```

Note the shape of the fix: the correct behaviour is the **simpler** one. The three buggy copies
were buggy because they tried to be clever about a key they had already touched; retaining over
every key is both shorter and right, and `retain` on an already-clean deque is a no-op.

**(c) One pop, parameterized by predicate.** `pop_oldest_waiter_of_kind` and
`pop_oldest_xreadgroup_waiter` become one private `pop_oldest_matching(&mut self, key: &Bytes,
pred: impl Fn(&WaitEntry) -> bool) -> Option<WaitEntry>` — find position, `VecDeque::remove`,
`entries[idx].take()`, `remove_entry_at`. The two public entry points stay, as two-line callers:

```rust
pub fn pop_oldest_waiter_of_kind(&mut self, key: &Bytes, kind: WaiterKind) -> Option<WaitEntry> {
    self.pop_oldest_matching(key, |e| Self::entry_matches_kind(e, kind))
}
pub fn pop_oldest_xreadgroup_waiter(&mut self, key: &Bytes) -> Option<WaitEntry> {
    self.pop_oldest_matching(key, |e| matches!(e.op, BlockingOp::XReadGroup { .. }))
}
```

Both public signatures are unchanged, so `blocking.rs:306`, `:494` and `:513` compile untouched
and no test moves. The predicates stay where they are readable: `entry_matches_kind` (`:490-501`)
is unchanged, and the XREADGROUP predicate is the same `matches!` it is today (`:533`), now
written once instead of twice.

**(d) `has_waiters_for_kind` (`:412`) stays.** Two live callers (`blocking.rs:292`, plus 8 test
assertions across `blocking.rs` and `execution.rs:1453`, `:1472`) and no duplication. Named
explicitly because it sits between two methods this proposal deletes and a reviewer will ask.

#### Deletion test, applied honestly

- **`remove_entry_at`** — delete it and the unlink body reappears at **six sites**, three of which
  must independently re-derive the duplicate-key rule that §Problem 4 shows nobody got right.
  **Earns its keep.** This is the load-bearing number.
- **`pop_oldest_matching`** — delete it and two 50-line functions reappear that differ by one
  closure. **Earns its keep**, modestly; the real win is that it has exactly one `remove_entry_at`
  call instead of two.
- **The three dead methods** — delete them and *nothing* reappears. **Pure deletion.**
- **The `pub use` at `core/src/lib.rs:149`** — delete it and callers must write
  `frogdb_core::shard::ShardWaitQueue`. There are none. Recorded, not claimed (§Risks).

## Testability improvement

**PN3 gains a nameable invariant, which is the whole point.** `wait_queue.rs` has **11 tests**
(`:672-931`): three on `dump`, four on `drain_waiters_for_slot`, four on the kind-aware pop
(`:809`, `:833`, `:866`, `:892`). **None** covers `unregister`, `collect_expired`, `register`'s
limit checks, or slot reuse — and none covers duplicate keys, which is why §Problem 4 shipped.

Once `remove_entry_at` exists, the invariant it maintains can be *stated* and then asserted from
one place:

```rust
#[cfg(test)]
fn index_invariants_hold(&self) -> bool {
    // (1) every index in waiters_by_key points at a live entry;
    // (2) that entry's `keys` contains the map key;
    // (3) occurrences of idx under key == occurrences of key in entry.keys;
    // (4) waiter_count == entries.iter().flatten().count();
    // (5) free_slots ∩ live-entry indices == ∅.
}
```

Clause (3) is exactly what §Problem 4 violates, and it is checkable in ~20 lines. Asserted after
every mutation in a handful of table-driven tests — register/pop/unregister/expire/drain, with a
duplicate-key case in each — it covers **all six** former unlink copies through one predicate.
That is leverage the current shape cannot offer: with six bodies, an invariant test has to be
written six times, which is why it was written zero times.

Three regression tests ride along with hotfix **H1**, and they are worth writing *before* the
refactor so they fail against today's tree:

1. **Unit** (`wait_queue.rs`): register `BLPOP k k`, `pop_oldest_waiter_of_kind`, assert
   `blocked_keys_count() == 0` and `waiters_by_key` is empty. Fails today.
2. **Unit**: the §Problem 4 five-step sequence — serve a duplicate-key waiter, register a waiter
   on a *different* key, pop on the first key, assert `None`. Fails today (returns the wrong
   waiter).
3. **Integration** (`redis-regression/tests/list_tcl.rs`, beside `…issue_801`): client A `BLPOP k
   k 0` served, then client B `BLPOP j 0`, then `LPUSH k v`; assert B stays blocked and `LRANGE k`
   still holds `v`. This is the client-visible form and is what a reviewer should ask for.

**PN2's testability gain is smaller and stated as such.** It does not unlock anything: it removes
a 3-line ceremony from 24 test setups and makes one builder failure mode unrepresentable. Worth
having; not a justification on its own. PN2's justification is §Problem 1's deletion test.

## Spec / LOCKED clearance — explicit

- **Locked crates.** The four locked pairs are `frogdb-txn`+`frogdb-vll`,
  `frogdb-persistence`+`frogdb-recovery`, `frogdb-replication`+`frogdb-replication-runtime`,
  `frogdb-cluster`+`frogdb-cluster-runtime` (ADRs 0002–0004). This proposal edits **`frogdb-core`,
  `frogdb-server`, `frogdb-shard-harness`** and one website page. **None is locked; none carries a
  mutation gate**, so no `just mutants-gate` is owed. `just mutants-diff frogdb-core` is not a
  push requirement here, but PN3 changes a live pop path and running it costs little.
- **`FM-` tags, grepped file by file across the whole edited set:**
  - `core/src/shard/wait_queue.rs` — **zero**.
  - `core/src/shard/persistence.rs` — **13 tags**, `FM-PERSISTENCE-{001,002,005,007,008,009,012,014}`
    at `:607`, `:628`, `:639`, `:697`, `:721`, `:745`, `:761`, `:782`, `:811`, `:833`, `:860`,
    `:879`, `:883`. **All at least 583 lines below the deleted block (`:8`, `:11-24`)**, in the
    persistence bridge, which PN2 does not touch. The deletion removes a stowaway `impl` from the
    top of the file and nothing else.
  - `core/src/shard/blocking.rs` — **one tag**, `FM-CLUSTER-038` at `:2065`, inside the test
    module. PN3 does not edit `blocking.rs` (its call sites keep their signatures); PN2 edits only
    its dummy-channel scaffolding at `:1595-1603`.
  - `core/src/shard/{connection,event_loop,builder,worker,mod}.rs`,
    `server/src/{acceptor.rs, server/*.rs}` — **zero tags**.
  - No `FM-…` row is added, moved, or retargeted, so `just lint-failure-modes` (which enforces
    spec↔test agreement both ways) has nothing to reconcile.
- **Governing failure-mode rows for the wait-queue path, cited as required.**
  - **`blocking-failure-modes.md`** (`FM-BLOCKING-001`…`005`, `:38`–`:86`) is **explicitly scoped
    away from this code**: its Scope paragraph (`:8-14`) says the rows cover the *connection-side*
    path (`server/src/connection/blocking.rs`, `…/blocking/coordinator.rs`) and that "the
    shard-side wait queue (registration, FIFO `pop_oldest_*`, the acknowledged `UnregisterWait`
    handshake, restore-on-send-failure) lives in `frogdb-core` and **gets its own spec**". **That
    spec does not exist.** So `wait_queue.rs` is governed by no locked row today — which is the
    structural reason §Problem 4 had nowhere to be caught, and a data point for whoever writes
    that spec: clause (3) of §Testability's invariant is a row.
  - **`FM-CLUSTER-038`** (`cluster-failure-modes.md:615`, in a **LOCKED** spec, `Status: LOCKED
    (2026-08-05)`) is the one locked row whose mechanism runs through a function PN3 edits:
    `drain_waiters_for_slot` (`wait_queue.rs:346`), reached from `blocking.rs:123`. Its invariant
    names `core/src/shard/blocking.rs:118`, not `wait_queue.rs`, and its observable is "every
    blocked client on the migrated slot is woken with `MOVED` or `CLUSTERDOWN`" — i.e. *which
    entries are drained*, which PN3 preserves exactly (`drain_waiters_for_slot` is already
    retain-based and duplicate-safe; only its unlink body moves behind `remove_entry_at`). Its five
    forcing tests — `migration_event_with_a_known_target_notifies_the_owning_shard`,
    `…with_an_unknown_target_still_wakes_blocked_clients`,
    `…routes_to_slot_modulo_num_shards`, `…reports_a_closed_shard_channel`
    (`cluster-runtime/src/migration_events.rs:168`ff) and
    `slot_migrated_without_a_known_target_replies_clusterdown` (`blocking.rs:2067`) — must pass
    unchanged; **no spec edit is owed**, because the row's text describes behaviour this change
    does not touch. If a reviewer wants belt-and-braces, `just mutants-diff frogdb-cluster` is
    unaffected by construction (the crate is not edited).
- **Seam lints** (`agents/seam-lints.md`; `lint-gates`, `Justfile:329`):
  - **`lint-clock-seam`** — the allowlist (`scripts/clock-seam.py:75-140`) is an *exact-count* map
    and contains **no `frogdb-core` shard file**. The gate bans `SystemTime::now` /
    `std::time::Instant::now`; the only clock read in the touched code is
    `tokio::time::Instant::now()` (`blocking.rs:291`), which is the compliant clock and is neither
    added nor removed. **Unaffected.**
  - **`lint-metrics-chokepoint`** — PN3 touches no emission site; `BlockedKeys::set`
    (`diagnostics.rs:418`) is already a typed handle from `define_metrics!` and is read-only
    evidence here. **Unaffected.** (Note H1 changes the *value* that gauge reports, in the
    direction of accuracy — see §Behaviour changes.)
  - The remaining twelve gates (INFO sections, redirect replies, pub/sub confirmations, failover
    atomicity, float formatting, typed-store unwraps, keyspace-notify routing, the script gate,
    durable acks, figment `.nested()`, error sanitisation, continuation locks) have no surface in
    a wait-queue index or a dead connection channel. **Unaffected.**
  - **`lint-turmoil-features`** — PN2 deletes no `cfg` and adds none; the `select!` arm and the
    builder field are unconditional.
- **Vocabulary** (`CONTEXT-MAP.md` → `frogdb-server/CONTEXT.md`). Prose here uses **shard**,
  **shard worker**, **wait queue**, **waiter**, **slot**, **Primary/Replica**; no `master`/`slave`
  is introduced. The architecture-page edit (H3) removes text and adds none.

## Behaviour changes

PN2 is behaviour-preserving with no wire surface. PN3's refactor is behaviour-preserving; the
**hotfix** it enables is not, and that is the point:

| Change | Before | After | Risk |
|---|---|---|---|
| Blocking pop on a key a served waiter named twice | stale index left in `waiters_by_key`; after slot reuse, a waiter on a *different* key can be served from this one | index removed; only genuine waiters are eligible | The corrected behaviour is what `list_tcl.rs:1454` already asserts for the single-connection case. |
| `BlockedKeys` gauge (`diagnostics.rs:418`) | over-reports by one key per duplicate-key serve, monotonically, for the shard's lifetime | reports live blocked keys | Accuracy improvement. Per the standing observability rule, a misleading gauge is not acceptable; no dashboard alerts on it today. |

## Risks / scope boundaries

### vs proposal 80 (PN1, `Response`/`WireResponse`/`InternalAction` fold) — disjoint, with one correction

80 §Boundaries already rules this pair disjoint and it is right, but its guess at 81's file set
needs one correction so neither author works from a stale map: **81 does not touch
`server/tests/common/`** (the three files there mention `ShardWaitQueue` only in doc comments —
`quiescence_probe.rs:251`, `invariants.rs:113`, `workload_runner.rs:54` — and PN3 changes no
public signature, so none needs editing). 81's `frogdb-protocol` contact is **read-only**:
`wait_queue.rs:4` imports `ProtocolVersion` and `Response` and stores them in `WaitEntry` (`:21`,
`:32`); neither is constructed, matched, or converted anywhere PN3 edits. 80's edits are confined
to `protocol/src/response.rs` plus `core/src/scripting/bindings.rs` and (in its H1)
`core/src/shard/execution.rs:626` — **zero file overlap** with 81. Either order.

### vs proposal 66 (SV4, `ShardWorkerBuilder`) — **hard conflict, 81 should land first**

This is the real coordination cost of PN2, and it is not a merge nuisance — it changes 66's plan.
Shared files: `core/src/shard/builder.rs`, `worker.rs`, `mod.rs`, `shard-harness/src/harness.rs`,
`server/src/server/shards.rs`. And 66's proposed production wiring **keeps** the seam: its SV4-c
sketch writes `.with_new_conn_rx(conn_rx)` into the new builder call, and its §7 quotes the
harness's four setters including `.with_new_conn_rx` as the baseline it is improving on.

**Recommendation: PN2 before 66.** Deleting a required field is a two-line edit inside 66's
untouched-yet code; re-plumbing it through a new `ShardWiring` value object and *then* deleting it
is strictly more work, and 66's headline ("the builder can express 10 of the 24 setters") is
computed against a required-field list that PN2 shortens by one. If 66 lands first, PN2's
`builder.rs` hunk moves but does not grow. Either way the two authors must agree on order before
either touches `builder.rs`; this is the one place in round 38 where "no ordering constraint" is
not true. **66 is otherwise unaffected by PN3** — its only `wait_queue.rs` interest is
`new()`/`with_limits` (`:116-121`), which PN3 does not touch, and it lists that file **read-only**
in its own table.

### vs proposals 63 and 64 (`Server` bundles / subsystem lifecycle) — 81 shrinks both

- **63** partitions `Server`'s 47 fields into bundles and names `new_conn_senders` in its flat
  remainder (`63:122`, `:300`, `:332`) and `new_conn_receivers` in its `ShardSpawnContext` group
  (`63:353`). PN2 deletes both fields.
- **64** plans `build_acceptor_ctx` around exactly this line: "`std::mem::take(&mut
  self.new_conn_senders)` (`:558`) — that stays, and moves *into* the constructor where a test can
  observe it" (`64:462`), and lists it again among the seven take-sites (`64:669`). PN2 deletes it.

Neither is a blocker — a deleted field is the easiest possible merge for a proposal that is
*moving* fields — but both authors must be told, because both currently carry a line item for a
field that will not exist. **Recommendation: PN2 before 63/64**, for the same reason as 66.
`subsystems.rs` is a one-line deletion for PN2 and a whole-file restructure for 64; going the other
way means 64 relocates a line and PN2 then deletes it from its new home.

### vs future proposal 84 (PN6, `BlockingOp`/`Direction` dedupe) — adjacent, not absorbed

84 will fold `frogdb-protocol`'s `BlockingOp`/`Direction` (`protocol/src/response.rs:475`, `:500`)
into `frogdb-types`' copies and delete the hand converter in `server/connection/util.rs`. PN3
*reads* `crate::types::BlockingOp` in two predicates — `entry_matches_kind` (`:490-501`) and the
XREADGROUP `matches!` (`:533`) — and `blocking_op_name` (`:658-670`) matches all nine variants.
**PN3 does not touch, move, or re-shape either enum**; it only relocates two existing `matches!`
expressions. If 84 lands first, PN3's predicates compile unchanged (same variant names). If PN3
lands first, 84 finds **one** XREADGROUP predicate instead of two — strictly easier. **No ordering
constraint; PN3 does not absorb PN6.**

### vs future proposal 88 (PN12, blocking-serve wake effects) — same file, disjoint concern, edge declared

88 will route the served-wake path's write effects through `WRITE_EFFECT_ORDER` — its subject is
`blocking.rs`'s `ListSatisfaction::satisfy`, `bump_version_for_key` (`:348`), the inline notify
(`:369`) and `pending_serve_propagations` (`:360`). PN3's pops feed that path: `blocking.rs:306`
hands a `WaitEntry` to `strat.satisfy`, whose written keys are 88's subject. **The edge is real
and it is one-way: PN3 changes *which entry* is popped (correctly, per H1), 88 changes *what
happens to the store* after one is popped.** They meet at `drive_satisfaction_body` and touch
disjoint lines of it. Two things 88's author should have from here:

1. §Problem 4's mis-delivery means that **today, an effect-tracking fix could be applied to a
   wake that served the wrong client**. H1 should land first so 88 is built on a pop that is
   correct.
2. `drain_stream_waiters_with_error` / `_wrongtype` (`blocking.rs:493-519`) reply with errors and
   mutate nothing, so they are outside 88's write-effect scope even though PN3 unifies their pop.

**88 is not absorbed here.** PN3 adds no effect, removes none, and does not touch `satisfy`,
`bump_version_for_key`, `pending_serve_propagations`, or `Restore`.

### Other risks

- **PN2 — the `select!` after deletion.** Six arms remain; `message_rx` stays last, preserving the
  documented `biased;` invariant that nothing may follow the one perpetually-ready arm. The five
  maintenance arms are periodic ticks. No arm becomes newly reachable or newly starvable, and the
  fairness comment at `:48` gets *more* accurate (it currently names a channel that can never be
  ready). Low.
- **PN2 — `mem::take` semantics.** `subsystems.rs:558` moves a `Vec` out of `Server` by
  replacement, not `Option::take`; deleting the line removes the only reason `build_acceptor_ctx`
  needs `&mut Server` **for this field**. Six other `Option::take` sites (`64:669`) still require
  it, so the signature does not change. Stated because it is the first thing 64's author will ask.
- **PN3 — `pop_oldest_matching`'s closure and the borrow checker.** The `position` scan borrows
  `self.waiters_by_key` and `self.entries` immutably while the predicate runs; today's code
  handles this with a scoped block (`:436-445`). A `impl Fn(&WaitEntry) -> bool` taken by value
  keeps that shape. If it fights the borrow checker, the fallback is a `WaiterFilter` enum
  (`Kind(WaiterKind)` | `XReadGroup`) matched inside — same result, no closure. Named so an
  implementer does not conclude the design is wrong at the first `E0502`.
- **PN3 — `core/src/lib.rs:149` re-export.** Narrowing it (dropping `ShardWaitQueue`/`WaitEntry`
  from the crate-root list, keeping `frogdb_core::shard::…`) would make `dead_code` load-bearing
  again and is the change that prevents §Problem 5 recurring. It is **not claimed**: that list is
  a broad public-surface decision spanning ~40 names, and bundling it into a wait-queue fix would
  make a clean deletion contentious. Recorded as a follow-up issue.
- **PN3 — `find`-style first-match semantics are unchanged.** `pop_oldest_matching` preserves
  `position()`-order (registration FIFO within a kind), which `test_pop_of_kind_preserves_fifo_within_kind`
  (`:892`) pins. No fairness change.
- **Security.** Nothing in this proposal is a security fix, and none is proposed. The unbounded
  `waiters_by_key` growth in §Problem 4 is a resource leak reachable by an authenticated client
  issuing duplicate-key blocking commands; per the standing policy it is **recorded here and
  parked**, not written up as a security item and not used to argue urgency. The functional fix
  (H1) closes it as a side effect.

## Effort

| Part | Effort | Notes |
|---|---|---|
| **PN2** — delete the `NewConnection` seam | **S** | ~150 lines deleted across **26 code files** + 1 website page; zero lines added. Every hunk is a deletion or an argument drop. The size is not the code — it is the three-way sibling coordination (66, 63, 64) that must be settled first. |
| **PN3** — 3 dead deletions + `remove_entry_at` + `pop_oldest_matching` | **S** | **1 file**, ~90 lines net deleted. No public signature changes, so `blocking.rs` and all 11 existing tests compile untouched. Add the invariant helper + 3 regression tests (~60 lines) and it is a comfortable **S**. |

The two parts share **one** file (`core/src/shard/blocking.rs`, and only PN2's test scaffolding at
`:1595-1603` versus PN3's read-only call sites), so they can land in either order and in either
sequence with 80, 84 or 88. Against 66/63/64, **81 first** (§Boundaries).

## Independently-landable hotfixes

**H1 — a blocking client can be served an element from a key it never named (LIVE, claimed).**
`wait_queue.rs:457` and `:545`. §Problem 4. Two one-line changes, no refactor required:

```diff
 // pop_oldest_waiter_of_kind, :457
-        let other_keys: Vec<Bytes> = entry.keys.iter().filter(|k| *k != key).cloned().collect();
+        // Include `key`: a waiter may name it more than once (`BLPOP k k 0`) and the
+        // `remove(found_pos)` above dropped only one of its deque entries. Leaving the
+        // other behind leaks a stale slot index that, once the slot is recycled, makes
+        // an unrelated waiter eligible for this key.
+        let other_keys: Vec<Bytes> = entry.keys.clone();
```

— and the character-identical change at `:545` in `pop_oldest_xreadgroup_waiter`. `retain` over
`key` is a no-op for the occurrence already removed, and the trailing empty-deque cleanup
(`:481-485`, `:566-570`) then correctly finds nothing left. Ships with the three tests in
§Testability, at least one of which must be the client-visible integration form. The dead third
copy (`:261`) carries the same defect and is simply deleted by PN3(a) rather than fixed.

**H2 — `AcceptorContext.new_conn_senders` is a dead field on a hot-path struct (LATENT, claimed
as part of PN2, but landable alone).** `acceptor.rs:109-110` plus its test init `:418`. The struct
is documented as "cheaply cloned … for each `Acceptor::bind` call" (`:89-92`); it clones a
`Vec<Sender>` per acceptor that nothing reads. Deleting just this field and the `subsystems.rs:558`
line is a self-contained 4-line commit that does not require touching `frogdb-core` — useful if
the 66/63/64 ordering conversation stalls. **XS.**

**H3 — the architecture page documents a seam that does not exist (LATENT, claimed).**
`website/src/content/docs/architecture/architecture.md:39`, `:326`, `:453`. §Problem 3. No
`docs-spec` source names `NewConnection`, so the content page is edited directly — the one case in
this round where the spec-first rule does not apply, and it is checked rather than assumed. Lands
with PN2 (deleting a mechanism and its documentation in one commit) or alone. **XS.**

**H4 — "All connections pinned to it" (LATENT, not claimed).**
`website/src/content/docs/architecture/architecture.md:31`, in the same section as H3. Connections
are served by spawned tasks that route per key (`acceptor.rs:358-360`); nothing pins a connection
to a shard, and the dead `NewConnection` seam is presumably where that claim came from. This is a
broader statement than H3's three lines and rewriting it correctly needs the shard-ownership model
stated properly. **Filed as a follow-up issue, not folded into a deletion commit.**

**H5 — the `frogdb-core` shard-side wait-queue failure-mode spec does not exist (process gap, not
claimed).** `blocking-failure-modes.md:8-14` promises it ("gets its own spec") and nothing
delivers it. §Problem 4 is precisely the class of defect such a spec catches: the invariant it
violates — *every index in `waiters_by_key` resolves to a live entry that names that key, with
matching multiplicity* — is one row. Filed as an issue against the hardening campaign, with
§Testability's five clauses as its starting draft.
