# Concurrency / distributed-systems design review — `specs/txn.md`, `specs/vll.md`, `specs/blocking.md`

Read-only audit. Lens: deadlock/livelock, atomicity holes, blocking-waiter correctness, timeout
semantics (wall-clock as liveness bound = fine, as correctness input = not), cluster/replication
interaction, and divergence from CRDB / FDB / Calvin / the VLL paper where the spec's choice is
weaker. Code was consulted to avoid manufactured findings; file:line references are given where a
claim rests on the implementation rather than on the spec text.

## Verdicts

| Spec | Verdict |
|---|---|
| `specs/txn.md` | Strong on the connection-side lifecycle; **1 CRITICAL** (script writes escape both the slot gate and the pre-dispatch gauntlet, while the row title asserts the opposite), 1 HIGH, 4 ADVISORY. |
| `specs/vll.md` | Weakest of the three. **4 HIGH** — mutual-abort livelock, unbounded lock-hold time, the atomicity-critical phase-2/3 unwind left unrowed, panic isolation without a rollback story — plus 2 ADVISORY. |
| `specs/blocking.md` | The serve-vs-timeout race is exemplary; the *rest of the failure surface is missing*. **3 HIGH** — an internal contradiction across three rows, zero cluster/replication rows, and an enforced-in-code admission limit that no row forces. |

---

## CRITICAL

### C1 — `txn.md` FM-TXN-030 (with FM-TXN-007): a script's writes bypass both gates, and the row claims they do not

FM-TXN-030 is titled *"Scatter and script batches are slot-validated like any other"* and its
Invariant states that "the keyless fast path is only reachable when no command in the batch names a
key." Its own `Bug refs` cell then concedes: *"a script's **undeclared** runtime writes are still
unvalidated."* FM-TXN-007 concedes the parallel hole for authorization: *"the same gate is not
applied to writes a Lua script issues from inside a transaction"* — so `NOPERM`, `NOREPLICAS`
(`min-replicas-to-write`), and the replication self-fence are all enforced against a queued `SET`
and not against `EVAL "redis.call('SET', …)"` in the same `MULTI`.

Two distinct defects hide behind one bug ref:

1. **Cluster**: an `EVAL` that writes a key it did not declare in `KEYS` produces exactly the orphan
   write FM-TXN-009 calls *"the orphan-write shape this campaign exists to prevent"* — applied and
   replicated on a node that does not own the slot.
2. **Authorization**: an ACL-restricted user can reach a denied key or command through `EVAL` inside
   `MULTI`, which the non-scripted path refuses with `-NOPERM` plus an `ACL LOG` entry.

A LOCKED spec asserting a guarantee its own bug ref retracts is worse than no row: the mutation gate
measures against the row, so a mutant that deletes the (nonexistent) script-write check has nothing
to survive.

**Modern practice.** Enforcement belongs at the *write seam* every mutation must traverse, not at the
command's declared key spec — the repo already has the machinery for this (`agents/seam-lints.md`,
"every X must go through Y"). Redis Cluster refuses undeclared-key script access outright rather than
serving it; Calvin and CRDB both require the write set to be declared (or the transaction aborts) so
that routing and admission decisions are provably complete.

**Recommended change.** Add `FM-TXN-051 — a script's runtime write outside the declared key set`,
with `Observable` = the write is refused at the shard write seam (slot + ACL + write-admission
checked there, not at queue time), and promote `.scratch/replication-cluster-rework/issues/03` to a
forcing test. Until the fix lands, mark FM-TXN-030's and FM-TXN-007's affected clauses
`Status: KNOWN-VIOLATED` and soften the titles, so the LOCKED contract does not assert a property the
system lacks. A seam lint over the shard's mutation entry point makes the class structurally
non-recurring.

---

## HIGH

### H1 — `vll.md` FM-VLL-001/002/004: no-wait refusal without priority ⇒ mutual-abort livelock

FM-VLL-002 refuses a second continuation request outright (`-BUSY … retry`) and explicitly forbids
queueing it. FM-VLL-001/004 refuse SCA work the same way. Nothing in any of the three rows bounds
retry, orders contenders, or asserts progress.

Concretely: `acquire_continuation` (`frogdb-server/crates/vll/src/coordinator.rs:371-392`) dispatches
the lock request to **every** shard before awaiting **any** ready reply. Two cross-shard scripts over
overlapping shard sets can therefore each win a different shard, each receive `ShardBusy` from the
other's, each release everything, and each retry into the same interleaving — indefinitely. No
liveness property in the spec excludes this.

This also makes the coordinator's own doc comment misleading: *"`shards` must be sorted in ascending
order to prevent deadlocks"* (`coordinator.rs:329,354`). Callers do sort
(`connection/scripting/eval.rs:78`), but because acquisition is pipelined rather than serialized,
sorted order is *not* what buys deadlock freedom here — the no-wait refusal is. What sorted order
would buy (ordered *waiting*) is precisely the property the design gave up, and the residual hazard
it traded into is livelock, which is unrowed.

**Modern practice.** CRDB resolves lock conflicts with **wound-wait** keyed on transaction priority
so exactly one contender always survives a conflict; Calvin/VLL avoid the question entirely by
sequencing lock requests globally once, after which a transaction never aborts for contention. Pure
no-wait-plus-retry is the textbook livelock configuration.

**Recommended change.** Add `FM-VLL-006 — two continuation requests collide`, pinning a
starvation-free rule: lowest `txid` wins and the higher-`txid` holder yields (wound-wait), or allow a
parked queue of depth 1 ordered by `txid`. Force it with a test that two colliding continuations over
the same shard set both terminate, one committing. Fix the doc comments to state the real invariant.

### H2 — `vll.md`: the continuation lock has no bounded hold time and no rowed kill path

`acquire_continuation_and_run` bounds *acquisition* with `DEFAULT_LOCK_ACQUISITION_TIMEOUT` (4 s) but
leaves `run` unbounded (`coordinator.rs:341-349`). A cross-shard script that loops holds
shard-exclusive locks on every participant for as long as it runs, and FM-VLL-001/004 then refuse
*every* SCA request on those shards for the whole duration — a node-wide availability event with no
rowed escape. `SCRIPT KILL` exists (`connection/scripting/script.rs:106`) but no VLL row describes
its interaction with a held continuation lock.

Worse, the kill path is an atomicity question, not just a liveness one: a cross-shard script killed
mid-flight has already applied sub-command writes on sibling shards, and neither `vll.md` nor
`txn.md` states whether those are rolled back. FM-VLL-005 declines the question (`Outcome variant:
n/a — the failure is in execution, not acquisition`).

**Modern practice.** Exclusive cross-shard state is held under a **revocable lease**, not an
open-ended guard: FDB caps transactions at a hard 5 s and fails them past it; CRDB ties lock validity
to transaction heartbeats so a dead or wedged holder's locks are reclaimable by a third party.

**Recommended change.** Add a row for `SCRIPT KILL` / `FUNCTION KILL` vs. a held continuation lock,
stating (a) whether the lock is revoked or waits for the script to unwind, and (b) the observable
state of already-applied cross-shard sub-command writes. If they are not rolled back, say so in a
`NOT observable`-style clause and file it — a killed cross-shard script that leaves half its writes
behind is a cross-shard partial application.

### H3 — `vll.md` scope note (lines 25-30): the cross-shard atomicity core is explicitly unrowed

> "Not yet rowed: the scatter phases themselves (lock-request dispatch failure, phase-2/3 partial
> failure unwind, gather timeouts) … their observable is a generic `-ERR VLL lock acquisition
> failed`."

Phase-2/3 partial-failure unwind *is* the atomicity contract of the whole subsystem: an op whose
execute succeeded on shard A and failed or timed out on shard B. Deferring it because "the modes they
distinguish are internal" inverts the priority — the client-visible question is not *which* shard
unwound but *whether anything was applied*, and the generic error answers neither.

Two specific problems follow:

- **A gather timeout resolves an op's outcome, not merely a wait.** A wall-clock deadline that
  abandons a granted, already-executing op is exactly the wall-clock-as-correctness input this team
  rejects. It is a different animal from `BLPOP`'s timeout (which bounds a wait and consumes
  nothing).
- **`-ERR VLL lock acquisition failed` is returned for a possibly-applied op.** `txn.md` FM-TXN-032
  gets the analogous case right, keeping `-ERR shard unavailable` ("never accepted") distinct from
  `-ERR shard dropped request` ("accepted, fate unknown") and calling the distinction load-bearing.
  The VLL path collapses both into one message that reads like a pre-execution refusal.

**Recommended change.** Row the three phases before the next lock refresh. Split the observable so a
possibly-applied outcome is distinguishable from a definitely-not-applied one, mirroring FM-TXN-032.
The three existing tests (`phase1_dispatch_failure_aborts_shards_already_holding_intents`, etc.) are
already there — they need rows, not new code.

### H4 — `vll.md` FM-VLL-005: panic isolation without a rollback or invariant-recovery story

The row pins that locks are released and the shard "answers the next message normally" after an op
panics mid-`execute_scatter_part`. It never states what happened to the writes the op had already
applied — on this shard or on the sibling shards of a cross-shard op. The client gets an
error-shaped `PartialResult` while some effects may stand: a cross-shard partial application
reported as a failure.

Independently, catching an unwind and continuing to serve means continuing against data structures
whose invariants may have been broken mid-mutation (the store is not `UnwindSafe` in any meaningful
sense). The row's caveat — "isolation is a structural backstop, never a licence to leave the
panicking arithmetic unfixed" — addresses the *process* discipline, not this hazard.

**Modern practice.** For a replicated state machine, a panic mid-apply is handled fail-stop: abort the
shard and recover deterministically from the WAL, or make the apply path undo-able. Resuming a shard
whose in-memory state may be torn trades a loud crash for silent corruption — and corruption then
replicates.

**Recommended change.** Extend `NOT observable` with "the panicking op's partial writes surviving,"
and either restart the shard from its WAL on `site="vll_execute"` or route `execute_scatter_part`
through an undo-able write path. If neither is affordable yet, the row should say plainly that the
shard's post-panic state is trusted, so the risk is a visible spec property rather than an implicit
one.

### H5 — `blocking.md` FM-BLOCKING-004 contradicts FM-BLOCKING-002 and FM-BLOCKING-003

FM-BLOCKING-004 specifies `Response::Null` when the shard drops the response channel — confirmed by
its own forcing test (`connection/blocking/coordinator.rs:173-180`,
`channel_drop_yields_null_response`). But:

- FM-BLOCKING-002 lists as **NOT observable**: "one nil shape for both op families (the wrong-shape
  bug `into_response` exists to fix)".
- FM-BLOCKING-003 lists as **NOT observable**: "a bare `Response::Null` standing in for the
  array-shaped nil".

So a `BLPOP` client gets `$-1` where every other nil path on the same command gives `*-1`. The
semantic problem is larger than the shape: **shard death is reported as a timeout.** The client
cannot distinguish "no data arrived within your timeout" from "the shard serving your key is gone,"
so the natural client behaviour is a silent retry loop against a dead shard. `txn.md` FM-TXN-032
takes the opposite and correct position for the same underlying event.

**Recommended change.** Reply `-ERR shard unavailable` on channel closure (matching FM-TXN-032's
vocabulary), or at absolute minimum the op-aware nil. Reconcile the three rows either way — as
written, no implementation can satisfy all of 002, 003 and 004.

### H6 — `blocking.md`: no rows for slot migration, promotion, or client disconnect

`txn.md` devotes eight rows to cluster interaction (FM-TXN-022 … FM-TXN-030, FM-TXN-048/049).
`blocking.md` has none, despite blocking waits being the longest-lived connection state in the
system — precisely the state most likely to span a topology change. Three gaps:

1. **Demotion.** A waiter parked on a primary that is demoted must not be served from replicated
   data — serving it would pop an element on a replica, i.e. a replica-side write and a divergence
   between the replica's store and its own replication stream. The `WAIT` command already handles
   the analogous case (`connection/blocking.rs:330-335`, `WAIT_ROLE_CHANGED_ERR`, cross-referenced to
   FM-REPLICATION-040) — and `WAIT` is listed in `blocking.md`'s own scope line yet has no row here
   at all.
2. **Slot migration.** A waiter parked on a key whose slot departs must be resolved (`-MOVED` or an
   unblock), never served locally from a slot this node no longer owns — the blocking analogue of
   FM-TXN-049.
3. **Client disconnect.** `cleanup_wait` is explicitly in scope, but no row covers a disconnect while
   parked, so nothing forces the shard-side unregistration that prevents a leaked wait entry.

**Modern practice.** Long-lived registrations are invalidated by epoch/lease change, not by hoping
the topology holds still. Redis's `disconnectAllBlockedClients` on role change exists for exactly
hazard 1 and is the direct precedent.

**Recommended change.** Three rows — migration, demotion, disconnect — each with an explicit "must
not serve" clause, plus one row for `WAIT`'s role-change reply so the scope line stops promising
coverage the spec does not deliver.

### H7 — `blocking.md`: the admission limit is enforced in code and rowed nowhere

`ShardWaitQueue` enforces real bounds — 10 000 waiters per key, 50 000 blocked connections
(`frogdb-server/crates/core/src/shard/wait_queue.rs:117-155`) — returning `-ERR max blocked
connections limit reached` / `-ERR max waiters per key limit reached`. `register_wait` is named in
`blocking.md`'s scope, but no row covers refusal-at-registration.

Unrowed means unforced: a mutant that removes the bound (turning a DoS-resistant queue into an
unbounded one) survives the gate, and the client-visible error text is unpinned.

Note this is an *admission* decision, so it deserves the timeout-semantics scrutiny too — but it is
correctly a resource bound, not a clock-derived one, so the design is sound. Only the spec coverage
is missing.

**Recommended change.** Add `FM-BLOCKING-006 — registration refused at the waiter limit`, pinning
both error texts and, as `NOT observable`, a refused registration that nonetheless leaves an entry in
the wait queue.

### H8 — `txn.md` FM-TXN-023/024/027: the presence probe is a TOCTOU against the shard round-trip

The migration verdict is taken from "one snapshot for the whole batch, taken once per EXEC"
(FM-TXN-023), after which the batch is sent to the shard as a *separate* operation. Between probe and
apply the migration can complete, so FM-TXN-027's permissive arm ("every key still local → commit")
can commit on the ex-owner. FM-TXN-022's bug refs already name this
(`.scratch/replication-cluster-rework/issues/02`, "residual commit/apply window"), so the team knows —
but no row states it as a `NOT observable`, which leaves the LOCKED spec reading as though the
redirect gate closes the hole completely.

**Modern practice.** CRDB does not re-check routing at admission and hope; it carries the range
lease/epoch into the request and **re-validates at apply time**, so a lease that moved during the
request causes rejection at the point of application rather than a stale-but-plausible admission
decision.

**Recommended change.** Carry the routing epoch into the shard message and have the shard refuse to
apply on epoch mismatch (or fuse presence-probe and execute into one shard-side operation). Row it
either way: FM-TXN-027 should state the residual window explicitly rather than delegating it to a bug
ref on a neighbouring row.

---

## ADVISORY

### A1 — `txn.md` FM-TXN-050 cites a clause that does not exist

FM-TXN-050 twice refers to `live_at_watch` and to "FM-TXN-033's gap-4 clause (an already-stale watch
never aborts)". FM-TXN-033 contains neither the phrase "gap-4" nor any mention of `live_at_watch` or
of stale watches. The dangling reference lands on the single most subtle question in the WATCH
contract — whether a watch on a key that was absent or already expired at `WATCH` time can ever
abort — where Redis's answer (a `WATCH` on a missing key **does** abort if the key is subsequently
created) is not obviously what "an already-stale watch never aborts" describes.

**Recommended change.** Fold the rule into FM-TXN-033 explicitly, stating the outcome for (i) key
absent at `WATCH`, later created; (ii) key present but logically expired at `WATCH`; (iii) key live at
`WATCH`, later expired. Then have FM-TXN-050 reference the named clause.

### A2 — `txn.md` FM-TXN-040 vs FM-TXN-049: is the *watch* verdict retaken after the pause barrier?

FM-TXN-040 correctly pins "exactly two `validate_queued_batch` calls when the barrier blocked … the
first verdict is never reused across a wait." FM-TXN-049 requires every watched key's slot to be
re-checked at EXEC. Neither row says whether the *watch-set* re-check is also retaken after the park.
A `CLIENT PAUSE` is exactly the window in which a slot changes hands (that is what the pause is for),
so a watched slot can depart while the transaction is parked, and the CAS would then be decided
against this node's stale copy — the identical defect FM-TXN-040 exists to prevent, one gate over.

**Recommended change.** Restate FM-TXN-040's invariant as "each verdict covers the queue *and* the
watch set" and add a forcing test that moves a watched slot during the pause.

### A3 — `txn.md` FM-TXN-039 is an unlisted Redis deviation

Redis `EXEC` is atomic including `FLUSHDB`/`KEYS`/`SCAN`; FrogDB runs them after the shard batch,
outside the atomicity envelope, and returns a single array frame that looks atomic to the client. The
row is admirably honest ("a claim of atomicity" is listed as NOT observable), but the *Redis
deviations* table — the spec's own single source of truth for drift, and the thing a compatibility
reviewer reads — omits it.

**Recommended change.** Add the row to the deviations table with the same rationale.

### A4 — `txn.md` FM-TXN-019/021: the cross-shard refusal is framed as inherent when it is deferred

The rationale is "there is no cross-shard rollback story," while single-key ops *do* get cross-shard
atomicity from VLL. But a `MULTI` batch's full key set is known at `EXEC` — which is exactly the
declare-all-locks-up-front precondition that VLL and Calvin require — and the cross-shard continuation
machinery already exists and is used by scripts. The missing piece is per-shard undo, not a structural
impossibility. Meanwhile FM-TXN-021 makes standalone FrogDB refuse a transaction that standalone Redis
accepts, which is a real portability cost for a shape users hit constantly.

**Recommended change.** Reword the rationale to name the missing piece (per-shard undo / rollback on
the batch path) and file it, rather than implying the architecture forbids what its own primitives
already support for scripts.

### A5 — `vll.md` FM-VLL-003: layered timeouts as an ownership rule

The row and its liveness note rest on `CONTINUATION_DRAIN_TIMEOUT` (2 s) sitting under
`DEFAULT_LOCK_ACQUISITION_TIMEOUT` (4 s) "so the shard, not the coordinator, resolves the request and
cleans up after it." Nested-timeout margins are a fragile basis for a cleanup-ownership rule: they
degrade under scheduling delay, and (per H2) the shard's event loop can itself be occupied by a long
script, which is precisely when the margin is needed.

The design is in fact saved by something better, which the row already contains but underweights:
`grant_continuation` "installs neither if the requester has already given up (`ready_tx` closed)" —
a structural guard that makes a lost race harmless regardless of which timer fires first.

**Recommended change.** Promote the `ready_tx`-closed guard to *the* invariant of the row and demote
the 2 s < 4 s relationship to a tuning note. Better still, derive the shard deadline from the
coordinator's rather than hard-coding both, so the ordering cannot drift apart in a later edit.

### A6 — `vll.md` FM-VLL-002: release-by-`Drop` makes exclusivity depend on an unrowed precondition

"every failure path drops `release_txs` … `acquire_continuation_and_run` owns the guard for the whole
run, so the release happens whether the work returns or panics." Panic is covered; **cancellation is
not**. If the connection-side future is dropped mid-`run`, the guard drops and the locks release —
while the shard, which received the script message over a channel, keeps executing it and its
cross-shard sub-commands. Another continuation could then be granted over a shard that is still
running the previous one's work: silent isolation loss, no test.

Today the exposure is small: connections are spawned as detached tasks
(`frogdb-server/crates/server/src/acceptor.rs:358-360`) with no `JoinHandle` abort in the accept loop,
so mid-command cancellation is not a routine path. But nothing *enforces* that — a future `CLIENT
KILL` implementation, a graceful-shutdown abort, or any `tokio::time::timeout` wrapper added around
command dispatch would break it silently, and the spec gives no reviewer a reason to notice.

**Recommended change.** Either make release explicit and shard-acknowledged (a release token matched
against the granted `txid`, so a stale release cannot free a lock the shard has since regranted), or
state the cancellation precondition in the row's Invariant and pin it with a test that aborts the
task mid-`run` and asserts the shard still refuses a second continuation.

---

## Where these specs already match or exceed best practice

1. **`blocking.md` FM-BLOCKING-005 — the serve-vs-timeout reconciliation.** The shard's serial mailbox
   as the single serialization point, an *acked* `UnregisterWait` handshake, and a **borrowed** rather
   than consumed `response_rx` so an `AlreadyServed` value is still drainable. This is the textbook
   fix for the lost-wakeup / lost-element class, and critically it resolves the race **by authority
   rather than by clock** — the timeout selects a candidate outcome, the shard's ack decides it.
   Exactly the discipline this team asks for.
2. **`blocking.md` FM-BLOCKING-001 — `biased` select with the response first,** so a value that becomes
   ready in the same poll as an elapsed deadline still wins, with `biased_response_beats_elapsed_deadline`
   pinning the tie-break that a live socket would only hit by luck. Same discipline applied again in
   `resolve_wait_race`, where the comment explains *why* the role change outranks `CLIENT UNBLOCK`
   rather than leaving it to `select!`'s random choice.
3. **`txn.md` FM-TXN-026 — the presence probe fails closed.** "An unknown answer is a refusal, never
   an optimistic serve." The correct default for a routing decision under uncertainty.
4. **`txn.md` FM-TXN-032 — refusing to collapse "never accepted" and "accepted, fate unknown"** into
   one message. Most systems lose this distinction; keeping two wire messages is the honest
   representation of an ambiguous commit, and it is the model H3 and H5 should be held to.
5. **`txn.md` FM-TXN-047 — EXEC consumes the transaction on every exit path,** so no redirect or error
   leaves a re-`EXEC`able queue a client could double-apply. The `debug_assert`ing spec-carrier that
   can never fabricate a `+OK` is a nice structural touch.
6. **`txn.md` FM-TXN-040 — re-validating topology after the pause barrier, and pinning the *count* of
   verdicts** (exactly two when blocked, exactly one when not). Asserting the number of checks, not
   just the outcome, is what makes the mutation gate able to see the difference.
7. **`vll.md` FM-VLL-003's inversion of the drain wait.** Parking the request instead of awaiting
   inside the shard's own event loop removed a genuine self-deadlock — the wait prevented the very
   drain it waited for. The liveness note documenting the old failure is exactly the right artifact to
   leave behind.
8. **`vll.md` FM-VLL-004's drain barrier.** "The queue can only shrink while a request is parked, so
   the drain terminates" is a real termination argument rather than an appeal to timing, and it
   correctly identifies that admitting new SCA work would starve the drain into a guaranteed timeout.
9. **`txn.md` FM-TXN-050 — first-wins re-`WATCH`,** matching Redis `watchForKey()` and `CLIENT_DIRTY_CAS`.
   The CAS decision is version-based throughout; wall-clock never enters the WATCH path.
10. **Both `txn.md` and `vll.md` are unusually rigorous about `NOT observable`** — naming the specific
    wrong behaviour a mutant would produce, not just the right one. That is what makes the mutation
    gate meaningful, and it is the reason the gaps above are worth closing rather than tolerating.
