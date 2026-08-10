# Proposal 88 — The blocking-serve path is a fourth write-effect authority

**Lane**: PN12 (core / shard effects)
**Status**: proposal, unimplemented
**Locked-adjacent**: yes — touches txn WATCH semantics (gate 0.90) and persistence WAL staging
(gate 0.85). Behavior changes here are **spec-first**.
**Sibling edges**: 83 lands first (orchestrator ruling); 81 H1 lands first (one-way); 84 confirmed
disjoint.

---

## Summary

`post_execution.rs` claims, in its own module doc, that post-write effect ordering "lives in exactly
one place":

> Here the ordering lives in exactly one place: [`WRITE_EFFECT_ORDER`], iterated by
> [`ShardWorker::run_write_effects`].
> — `post_execution.rs:12`

That claim is true about *ordering* and false about *coverage*. Three pipeline entry points
(`run_write_effects`, `run_scatter_effects`, `run_internal_removal_effects`) route store mutations
through the nine-effect order. A **fourth** mutation site — the blocking-serve path in
`blocking.rs` — mutates the store and then hand-applies a **three-effect subset** inline:

| `WRITE_EFFECT_ORDER` index | effect | applied on a served wake? |
|---|---|---|
| 0 | `VersionIncrement` | **partial** — `bump_version_for_key(key)` at `blocking.rs:348`, **source key only** |
| 1 | `TrackingInvalidation` | **no** |
| 2 | `KeyspaceNotifications` | yes — inline loop at `blocking.rs:369` |
| 3 | `DirtyCounter` | **no** |
| 4 | `WaiterSatisfaction` | n/a — this *is* the satisfaction step (cascade = own recursion, `:374`) |
| 5 | `KeysizesFlush` | **no** |
| 6 | `WalPersistence` | **no** |
| 7 | `SearchIndex` | **no** (owed nothing — see claim (d)) |
| 8 | `ReplicationBroadcast` | deferred — `pending_serve_propagations` at `:360`, drained at `post_execution.rs:420` |

For **BLPOP / BLMPOP / BZPOPMIN / BZPOPMAX / BLMOVE-source**, the subset is survivable: every key
the wake mutates is already in the *waking write's* declared key set, so effects 0–3 (already run at
the time the wake fires) covered it in the same or a prior position, and effects 5–8 (still ahead)
cover it on the way out. The hand-written version bump and notification loop are redundant belt-and-
braces, not load-bearing.

For **BLMOVE / BRPOPLPUSH**, the subset is a hole. `ListSatisfaction::satisfy` mutates the
**destination** key directly (`blocking.rs:~800`), and dest is **not in the waking write's key
set** — the waking write is `LPUSH src v`, whose `handler.keys(args)` is `[src]`. Every effect that
derives its work from declared keys therefore skips dest entirely.

Three of the four suspected misses are **LIVE**, one is **REFUTED**, and a fifth (not in the lane
brief) is **LATENT**. The worst is (c): a crash after a served `BRPOPLPUSH` and before the next
command that happens to declare dest as a write key loses the element from **both** src and dest —
acknowledged-write loss in exactly the reliable-queue pattern `BRPOPLPUSH` exists to serve.

The proposed change makes the serve path a *client* of the pipeline rather than a fourth author:
satisfaction returns the set of keys it wrote, and a new `run_served_wake_effects` under
`EffectScope::ServedWake` walks `WRITE_EFFECT_ORDER` deciding, **per effect**, one of three things —
apply now, union into the outer run's pending key set, or skip with a stated reason. Effect position
relative to `WaiterSatisfaction` becomes data, not a comment, and a future tenth effect is a compile
error until someone classifies it.

**One correction to the lane brief**: `pending_serve_propagations` **cannot be deleted**. It is not a
hand copy of `ReplicationBroadcast`; it is a *cross-position deferral* that exists because the served
pop must broadcast **after** the waking push, or replicas apply pop-before-push and diverge. It also
carries a documented panic-guard reset. The deletion test lands on the three hand effects, not on
this vector. See §Proposed change / deletion test.

---

## Files involved

| File | Lines | Role in this proposal |
|---|---:|---|
| `frogdb-server/crates/core/src/shard/blocking.rs` | 2090 | The fourth authority. `drive_satisfaction` `:234`, `drive_satisfaction_body` `:271`, the `Done` arm + three hand effects `:327-388`, `apply_restore` `:426`, `BLMove` satisfaction arm `:754-840`, `StreamSatisfaction::satisfy` `:1089-1185` |
| `frogdb-server/crates/core/src/shard/post_execution.rs` | 1907 | The interface. Module doc single-ownership claim `:12`, `EffectScope` `:211-242`, `WriteEffectKind` `:249-268`, `WRITE_EFFECT_ORDER` `:282-292`, `run_write_effects` `:305`, `run_scatter_effects` `:512`, `run_internal_removal_effects` `:561`, `invalidate_written_keys` `:671`, `satisfy_waiters_for_command` `:701`, served-pop drain `:420` |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | `SlotVersions` `:56-96`, `pending_serve_propagations` field `:175`, `bump_version_for_key` `:618`, `get_key_version` `:632`, `check_watches` `:648-664`, panic reset `:902` |
| `frogdb-server/crates/core/src/shard/persistence.rs` | 908 | `execute_wal_action` `:106`, `WalTarget::write_set` `:138-146` (reads `store.get_hot(key)`), `persist_records` `:247` |
| `frogdb-server/crates/core/src/shard/panic_guard.rs` | 564 | `:18-42` documents `pending_serve_propagations` as command-scoped state dropped on the panic path |
| `frogdb-server/crates/core/src/shard/wait_queue.rs` | 931 | 81's H1 site (`:457`, `:545`); `drain_stream_waiters_*` `:493-519` ruled non-mutating by 81 |
| `frogdb-server/crates/core/src/shard/keyspace_notify.rs` | 141 | `emit_keyspace_notification` `:28` — the routing seam blocking.rs already honors |
| `frogdb-server/crates/core/src/shard/execution.rs` | — | pipeline callers `:470`, `:503`, `:675`, `:687`, `:998`, `:1042`, `:1076`, `:1135` |
| `frogdb-server/crates/core/src/shard/eviction.rs`, `event_loop.rs` | — | `run_internal_removal_effects` callers `:375` / `:338` (83's territory) |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | `FM-PERSISTENCE-019` `:291` — the row whose Invariant claim (c) breaks |
| `.scratch/hardening/specs/txn-failure-modes.md` | — | `FM-TXN-033` `:422` (WATCH abort); scope paragraph `:10-15` disclaims shard-side |
| `.scratch/hardening/specs/blocking-failure-modes.md` | — | **not LOCKED**; scope forward-declares the missing shard-side spec; rows 001–005 only |

Verified at HEAD: `blocking.rs` contains **no** call to `invalidate`, `record_read`, `persist`,
`reindex`, or `increment_dirty` anywhere in the file.

---

## Problem — four chains, graded separately

Shared premise for (a)–(d), established once:

1. `SlotValidator::same_shard` (`server/src/connection/routing.rs:106-121`) is what enforces
   `requires_same_slot`. It checks **same shard**, not same slot. `shard_for_key = slot %
   num_shards` (`partition.rs:32-36`).
   ⇒ A BLMOVE src/dest pair shares a shard and, with `num_shards == 1`, shares nothing else. Slot
   equality is not implied and in practice essentially never holds.
2. Every effect at indices 0–3 and 5–8 derives its work from `record.handler.keys(record.args)` —
   the *declared* keys of the **waking write**, i.e. `[src]`. Nothing anywhere in the pipeline learns
   about dest.
3. The wake fires from `WaiterSatisfaction`, index **4**. Effects 0–3 have already run; 5–8 have
   not.

### (a) WATCH on the destination — **LIVE**

Chain:

- `bump_version_for_key(key)` (`blocking.rs:348`, guarded by `strat.bumps_version()`) bumps the
  version of **`key`** — the source, the key whose waiters were being driven. `worker.rs:618`.
- Versions are slot-granular: `SlotVersions` (`worker.rs:56-96`) stamps per slot plus a shard-wide
  epoch; `get_key_version(k) = version_for(slot_for_key(k))` (`worker.rs:632`).
- `check_watches` (`worker.rs:648-664`) compares each watched key's recorded stamp against
  `get_key_version` at EXEC time.
- `slot_for_key(dest) != slot_for_key(src)` in the general case (premise 1). The dest slot's stamp
  never moved. The shard epoch never moved.
- ⇒ EXEC sees an unchanged watch and commits.

Repro (two sessions plus the pusher; dest either absent or an existing list — both work):

```
A: WATCH dest
A: (reads dest, decides its transaction based on that read)
B: BLMOVE src dest LEFT RIGHT 0        # blocks
C: LPUSH src v                         # serves B; element lands in dest
A: MULTI ; <ops predicated on dest> ; EXEC
   → EXPECTED: nil (abort)   ACTUAL: array (commits)
```

`FM-TXN-033` (`txn-failure-modes.md:422`) states the contract this breaks — a watched key whose
version moved under another client's write must abort. But that spec's scope paragraph (`:10-15`)
explicitly disclaims the shard side: "The shard-side engine (WATCH version check, rollback,
replication framing) lives in `frogdb-core` and gets its own spec." That spec does not exist.

**Verdict: LIVE, and it is a spec-ownership gap, not merely a row violation.** The row states the
contract; no row owns the site that breaks it.

Two things this claim does **not** say. The transaction is not corrupted — A's own writes still apply
atomically. And BLPOP-family wakes are fine: source and watched key coincide, and the waking `LPUSH`
already bumped that slot at effect index 0 before the wake even fired (the `:348` bump is redundant
there — see §Proposed change).

### (b) RESP3 client-side-caching invalidation on the destination — **LIVE**

Chain:

- `invalidate_written_keys` (`post_execution.rs:671-688`) is the sole invalidation seam for writes;
  it iterates `record.handler.keys(record.args)` and calls `invalidate_keys_all_modes`, which covers
  both default key-based tracking and BCAST prefix matching.
- Effect index 1. Runs **before** the wake. Key set = `[src]`.
- `blocking.rs` calls nothing tracking-related (grep: zero `invalidate` occurrences in the file).
- ⇒ A client that cached `dest` keeps a stale entry indefinitely — until some *later, unrelated*
  command declares dest as a write key.

Exposure is wider in BCAST mode than in default mode: default-mode tracking requires the client to
have read dest (so `record_read` registered it), whereas a BCAST client subscribed to dest's prefix
expects notification without ever having read it, and gets none.

**Verdict: LIVE.** Source-key invalidation is correct today only incidentally — the waking write
declares it.

### (c) WAL staging of the destination write — **LIVE**, and worse than the brief supposed

Chain:

- `WalPersistence` (`post_execution.rs:385-405`) resolves the command's `WalStrategy` into
  `WalAction`s and calls `persist_records`. Every `WalAction` variant (`Persist`,
  `DeleteIfMissing`, `PersistOrDelete`, `PersistIfExists`, `MergeHllDelta`, `ClearShard`) is
  parameterized by a **declared key**.
- `WalTarget::write_set` (`persistence.rs:138-146`) resolves a key to bytes by reading
  `self.store.get_hot(key)` **at persist time**. The WAL is a snapshot-of-value log, not an
  operation log.
- Effect index 6. Runs **after** the wake — so a dest entry added to the key set at index 4 *would*
  be picked up and would snapshot the post-move value correctly. Nothing adds it.
- Fallback path: RocksDB checkpointing (`persistence/src/snapshot/rocks_coordinator.rs:158-208`,
  driven from `server/src/server/init.rs:345-375`) quiesces shards and drains the flush engine. It
  can only capture what entered the WAL. A write that never staged is not in the checkpoint either.
- ⇒ Crash window: from the moment the served `BRPOPLPUSH`/`BLMOVE` reply is sent until some later
  command declares dest as a write key. On recovery, src has already been persisted **without** the
  element (the waking `LPUSH`'s own WAL action snapshots src post-pop — `write_set` reads the *live*
  store, and the pop already happened at index 4, before index 6). Dest never got it.
  **The element is gone from both keys.**

That last step is the sharp edge and is worth stating plainly: the ordering that makes (c) a
*silent* loss rather than a *duplicated* element is the same index-4-before-index-6 ordering that
makes the source side correct. The pop is visible to the WAL; the push is not.

This directly contradicts the stated Invariant of `FM-PERSISTENCE-019`
(`persistence-failure-modes.md:291`, "every write acknowledged before the cut is in the cut"):

> The `WalPersistence` effect enqueues a write's WAL entry before `ReplicationBroadcast`
> acknowledges it, so the drain message is behind every acknowledged write by construction.

The argument is valid but rests on an unstated precondition — *every acknowledged mutation goes
through `WalPersistence`*. The serve path acknowledges (the reply is sent at `blocking.rs:~330`,
inside effect 4) without going through it.

Relation to proposal 83 (§on `run_internal_removal_effects`): 83 found the same root cause —
WAL work derived from declared keys — running in the opposite direction. 83's lazy-expiry path
removes a key the WAL is never told to *delete*; 88's serve path adds a value the WAL is never told
to *put*. 83 fixes its case by **routing** the removal through the existing
`run_internal_removal_effects`, which already resolves a full key set. 88 cannot borrow that
routing: the serve happens *mid-run*, at index 4 of an in-flight `run_write_effects`, not as a
standalone event. Hence the union-into-the-current-run design in §Proposed change rather than a
second nested pipeline run.

**Verdict: LIVE. Highest-severity finding in this proposal.**

### (d) Search reindex on the destination — **REFUTED**

Chain, and it terminates immediately:

- `IndexKind` (`command_spec.rs:332`) has exactly two variants: `Hash` and `Json`. Lists are not
  an indexable kind. There is no list index to become stale.
- `ReindexSpec::None` (`:355`) is the default, and `LMOVE`'s own spec
  (`commands/src/list.rs:870-885`) declares it. The *immediate* (non-blocking) path owes the search
  index nothing either — so the serve path is symmetric with it, not divergent from it.
- BLMOVE's WRONGTYPE guard on dest (`blocking.rs:762-770`) means dest is provably absent or a list
  at the moment of the move. It can never be a hash or JSON document that an index tracks.

**Verdict: REFUTED.** No FM row, no test, no work. The `SearchIndex` arm of the new classification
records `Skip("lists are not an indexable IndexKind")` and that is a permanent, provable answer —
unless a future `IndexKind::List` lands, at which point the exhaustive match forces reconsideration.
That forcing is itself a reason to encode the skip in code rather than drop the effect silently.

### (e) Dirty counter — **LATENT** (not in the lane brief; found during the sweep)

`update_dirty_counter` (`post_execution.rs:690-699`) is effect index 3 — already run when the wake
fires, and its delta was computed for the waking write alone. A served pop/move therefore never
advances `rdb_changes_since_last_save`.

Redis increments `server.dirty` on the serve. Divergence is observable through `INFO
persistence`'s `rdb_changes_since_last_save` and, indirectly, through `save`-point scheduling: a
workload consisting mostly of served wakes under-counts its own mutation rate and delays an
auto-BGSAVE that Redis would have triggered.

**Verdict: LATENT.** Real divergence; no correctness invariant currently names it; no data loss.
Graded latent because the observable is a counter, not a value, and because a fix is free once the
served key set exists. Do not file this as an independent defect — it rides along with the fix.

---

## Proposed change

### Vocabulary

Introduce one type and one pipeline entry point.

**`ServedWrites`** — what a satisfaction produced, returned by value from
`Satisfaction::Done` instead of being partially hand-applied:

- `keys: SmallVec<[Bytes; 2]>` — every key the satisfaction *mutated*. For BLPOP-family, `[src]`.
  For BLMOVE, `[src, dest]`. For a cascade, each hop contributes its own pair as the recursion
  unwinds.
- `propagate: Option<Command>` — unchanged in meaning; retained (see deletion test).
- `events: Vec<(Bytes, &'static str, NotifyClass)>` — unchanged.
- `bumps_version: bool` — folded from `strat.bumps_version()`.

**`EffectScope::ServedWake`** — a fifth variant alongside `Command`, `Transaction`, `ScatterPart`,
and (post-83) `InternalRemoval { propagation }`. It carries the `ServedWrites` and the identity of
the outer run it is nested inside.

**`run_served_wake_effects(&mut self, served: &ServedWrites, outer: &mut PendingKeys)`** — the
fourth entry point, and the one that ends the fourth *authority*. It walks `WRITE_EFFECT_ORDER` and
for each `WriteEffectKind` produces exactly one of:

| disposition | meaning | effects |
|---|---|---|
| `ApplyNow` | index < 4: this effect already ran for the outer command; the served keys need it applied again, now | `VersionIncrement`, `TrackingInvalidation`, `KeyspaceNotifications`, `DirtyCounter` |
| `UnionIntoOuter` | index > 4: this effect has not run yet; add the served keys to the set the outer run will process | `KeysizesFlush`, `WalPersistence`, `SearchIndex`, `ReplicationBroadcast` |
| `Skip(&'static str)` | structurally not owed, with the reason recorded at the site | `WaiterSatisfaction` — "the cascade is the driver's own recursion; the depth cap depends on it" |

The match is exhaustive over `WriteEffectKind`. **A tenth effect does not compile until someone
classifies it for the serve path.** That is the whole architectural point: today, adding an effect
to `WRITE_EFFECT_ORDER` silently does nothing on served wakes, and nothing anywhere says so.

`SearchIndex` is `UnionIntoOuter` rather than `Skip`, despite claim (d) being refuted, because the
union is free and correct-by-construction: a key set that contains no indexable value produces no
reindex work. Encoding (d)'s answer as a code-level `Skip` would be a hand-maintained claim about
value types that the reindex machinery already decides correctly. The `Skip` reason for (d) belongs
in the *proposal* and in the FM spec's "NOT observable" column, not as a branch.

Note the ordering subtlety in `ApplyNow`: the four sub-index-4 effects are applied to the served
keys **in `WRITE_EFFECT_ORDER` order among themselves**, not in whatever order the old hand code
happened to use. Today the hand code runs version-bump → propagation-push → notifications, which is
0 → 8 → 2 — out of order relative to the canonical sequence. Nothing currently observes the
difference (`MUST_PRECEDE` at `post_execution.rs:754` does not constrain 2 before 8), but the
proposed form makes it moot rather than accidentally-fine.

### What gets deleted

The three hand effects in the `Done` arm of `drive_satisfaction_body` (`blocking.rs:327-388`):

- `self.bump_version_for_key(key)` at `:348` — subsumed by `ApplyNow(VersionIncrement)` over
  `served.keys`, which additionally covers dest (fixes claim (a)).
- the `for (key, name, class) in &events` loop at `:369` — subsumed by
  `ApplyNow(KeyspaceNotifications)`, still routing through `emit_keyspace_notification` so
  `lint-keyspace-notify-routing` stays satisfied.
- `strat.bumps_version()` as a call-site branch — folded into `ServedWrites`.

`apply_restore` and the `Err(_)` arm are untouched. The restore path must remain a pure store
rollback with **no** effects applied, which is what it is today (the `Restore::Move` arm at `:454-483`
pops dest, calls `cleanup_empty_list`, and pushes back onto src, leaving the store byte-identical to
pre-wake). Under the new design this becomes structurally clearer, not just conventionally true:
effects are driven from a `ServedWrites` that the `Err` arm never produces.

### Deletion test — `pending_serve_propagations`

The lane brief proposed deleting `pending_serve_propagations` along with the hand effects. **It must
survive.** Applying the deletion test properly:

*If the vector were removed and served propagations broadcast inline at `blocking.rs:360`, what
breaks?* Replicas receive `LMOVE src dest` (or `LPOP src`) **before** `LPUSH src v` — the push that
made the pop possible. The replica applies a pop against a list that does not yet contain the
element, then applies the push, and diverges permanently. The vector exists precisely to move the
served propagation from effect position 4 to effect position 8.

Pinned by:

- `post_execution.rs:1736-1907` — the served-pop broadcast tests, including the drained-to-empty
  assertions at `:1846` and `:1903`.
- `blocking.rs:1898` `blmove_cascade_records_ordered_propagations` — ordering across a cascade.
- `blocking.rs:1854` `served_blpop_records_pending_propagation` and `:1873`
  `doomed_waiter_records_no_propagation` — the reply-first-then-commit pin: a waiter whose channel
  is gone records **nothing**, which is only expressible because recording is separate from
  broadcasting.
- `panic_guard.rs:18-42` and `worker.rs:902` — the vector is documented command-scoped state that
  must be cleared on the panic path.

So the deletion test's answer is: the vector **is** load-bearing and stays. What changes is its
type-level story — `UnionIntoOuter(ReplicationBroadcast)` is the *same* deferral idea generalized
from "propagations" to "any post-index-4 effect", and the vector becomes one field of a
`PendingServedEffects` record rather than a lone special case. If the implementation prefers to
leave it as a standalone `Vec` and only add sibling fields for the WAL/keysizes/tracking key set,
that is acceptable — the architectural win is the exhaustive classification, not the struct shape.
Whichever shape lands, `worker.rs:902`'s panic reset must clear the whole record, and the
`is_empty()` assertions at `post_execution.rs:1846`/`:1903` must be extended to the new fields or
they will pass vacuously.

### Ordering invariants that must survive verbatim

1. **Reply-first-then-commit.** The reply is sent, and only on `Ok(())` are effects recorded. Pinned
   by `blocking.rs:1873` `doomed_waiter_records_no_propagation`.
2. **Restore-on-send-failure.** On `Err(_)`, `apply_restore` returns the store to its pre-wake
   state and **no** effect is applied. Pinned by `blocking.rs:1669`
   `push_after_receiver_dropped_does_not_lose_element`, `:1711`
   `blmpop_restore_preserves_all_elements_in_order`, `:1767`
   `bzpopmin_restore_preserves_member_and_score`.

Both survive by construction: `ServedWrites` is produced *inside* the `Ok(())` arm, and
`run_served_wake_effects` is only reachable from a `ServedWrites` value.

---

## Testability improvement

One red-green pair per surviving LIVE claim, each written to fail at HEAD.

**(a) WATCH-on-dest** — `frogdb-core`, integration-style over `ShardWorker`:
`served_blmove_bumps_destination_watch_version`. WATCH dest (record the version via
`get_key_version`), block a BLMOVE, push to src, assert `check_watches` now reports the dest watch
as invalidated. Red at HEAD: the dest slot's stamp is unchanged. Companion end-to-end test in
`frogdb-server` asserting EXEC returns nil, exercising the three-connection repro above.

The `frogdb-core` unit test is the one that matters for the gate discussion below; the server test
is the one that matters for the user-visible contract. Write both.

**(b) tracking invalidation** — `served_blmove_invalidates_destination_for_tracking_clients`, with
a default-mode variant (client read dest first) and a BCAST-mode variant (client subscribed to
dest's prefix, never read it). Red at HEAD in both modes: zero invalidation messages.

**(c) WAL staging** — `served_blmove_stages_destination_write_to_wal`. Assert at the WAL/`WalTarget`
level that a record naming dest is staged with the moved element. Red at HEAD: no dest record
exists. Then the crash-recovery test that actually states the failure mode:
`crash_after_served_brpoplpush_recovers_element_in_destination` — serve, cut before any subsequent
dest-declaring write, recover, assert the element is present in dest and absent from src. Red at
HEAD: absent from both.

**(e) dirty counter** — folded into (a)'s test as an additional assertion
(`rdb_changes_since_last_save` advanced), not a separate test.

**Structural test** — `served_wake_classifies_every_write_effect`: iterate `WRITE_EFFECT_ORDER`,
assert each kind maps to a disposition, assert the `ApplyNow` set is exactly indices 0–3 and
`UnionIntoOuter` exactly 5–8. This is the regression barrier for the *architectural* claim and the
thing that makes a future tenth effect a visible decision. It belongs next to
`order_satisfies_all_declared_constraints` (`post_execution.rs`, near `:754`).

### Spec-first plan (FM rows)

Behavior changes in locked-adjacent territory need rows before code.

**Ownership problem, stated first.** Neither locked spec owns the fix site.
`txn-failure-modes.md` (LOCKED) disclaims the shard side at `:10-15`.
`blocking-failure-modes.md` is **not** LOCKED, is scoped connection-side, and *forward-declares the
missing spec*: "the shard-side wait queue … lives in `frogdb-core` and gets its own spec."

**Recommendation (primary)**: amend `blocking-failure-modes.md`'s scope paragraph to claim the
shard-side serve path and add rows there. It is unlocked (no gate ceremony to amend), it already
points at the gap, and `just lint-failure-modes` derives the area from the filename, so rows become
`FM-BLOCKING-006…` with no tooling change. Existing rows stop at 005.

**Alternative**: a new `.scratch/hardening/specs/wake-failure-modes.md` (area `WAKE`). Cleaner
naming, but it creates a spec whose area no locked crate set owns, which is how the current gap
arose in the first place. Prefer amending unless the orchestrator wants a new area.

Rows to add:

| Row | Spec | Trigger | Observable | Forced by |
|---|---|---|---|---|
| `FM-BLOCKING-006` | blocking (amended scope) | Blocked BLMOVE served by a push to src while another client WATCHes dest | EXEC aborts (nil) | `served_blmove_bumps_destination_watch_version` + server-side EXEC test |
| `FM-BLOCKING-007` | blocking | Served BLMOVE with a tracking client caching dest (default and BCAST) | Invalidation push for dest | `served_blmove_invalidates_destination_for_tracking_clients` (×2 modes) |
| `FM-BLOCKING-008` | blocking | Served BLMOVE, then crash before any later dest-declaring write | Element present in dest after recovery, absent from src | `crash_after_served_brpoplpush_recovers_element_in_destination` |

Plus **one amendment to the LOCKED persistence spec**: `FM-PERSISTENCE-019`'s Invariant text states
an ordering argument with an unstated precondition. Amend it to state the precondition explicitly —
"…**and every acknowledged mutation is routed through `WalPersistence`**" — and cross-reference
`FM-BLOCKING-008` as the row that forces the precondition. That is a text amendment to a LOCKED
spec, so it needs the orchestrator's sign-off; it does not require a new persistence row, because
019's Observable is unchanged.

Each new row needs the full `REQUIRED_FIELDS` set (Trigger / Observable / NOT observable /
Invariant / Outcome variant / Forced by / Bug refs) or `just lint-failure-modes` rejects it. The
"NOT observable" column for 006 should record claim (d)'s refutation: no search-index effect is owed
because lists are not an `IndexKind`.

### Mutation-gate implications — be honest

**No mutation gate covers the fix site.** Every line changed lives in `frogdb-core`, which is in
none of the four locked crate sets (txn: `frogdb-txn` + `frogdb-vll`; persistence:
`frogdb-persistence` + `frogdb-recovery`; replication; cluster). `cargo mutants -p <crate>` runs only
that package's own tests, so:

- Tests added in `frogdb-core` contribute **zero** to the 0.90 txn gate and **zero** to the 0.85
  persistence gate.
- `just mutants-diff frogdb-persistence` before pushing this change will report **no diff** — the
  change does not touch that crate. That is a true negative, not clearance.
- The push-discipline value of a mutants run here is nil. Do not let a green `mutants-diff` be read
  as evidence that (a) or (c) is covered.

Consequence for review: the safety story for this change rests entirely on the named forcing tests
plus `just lint-failure-modes`' bidirectional spec↔test check (which does cover `frogdb-core` —
it is in `NEXTEST_CRATES`). Reviewers should weight the crash-recovery test in (c) accordingly; it
is the only thing standing between this fix and a silent regression.

A separate, larger question this surfaces — and one deliberately **not** proposed here — is whether
`frogdb-core`'s shard-effect pipeline should be a locked area at all. It hosts the WATCH version
check and the WAL staging decision for every write in the system while belonging to no gate. Filing
that as a lane candidate is the orchestrator's call.

---

## Risks and scope boundaries

### Sibling 83 (`83-lazy-expiry-authority.md`, commit 05a7ecb5) — 83 lands first

Accepted; this document is written assuming 83's `ExpiryReport` routing is already merged.

Textual conflict surface in `post_execution.rs`: `:241` (`EffectScope` variants — 83 adds
`InternalRemoval { propagation }`, 88 adds `ServedWake`), `:330`/`:358`/`:392`/`:426`/`:454` (effect
arms), and tests `:736-866`. All are additive-in-the-same-region conflicts, mechanical to resolve
once 83 is in.

**The semantic interplay 83 flagged, addressed.** 83 gains full `WaiterSatisfaction` behavior *by
routing* — its lazy-expiry removal goes through `run_internal_removal_effects`, which includes
effect index 4, so an expiry-triggered removal can now wake waiters. 88 *redefines what satisfaction
does* — after 88, waking a waiter also drags a served-key set through effects 0–3 and 5–8.

Composed, these mean an expiry-driven removal can now transitively cause a WAL write to a BLMOVE
destination. That is correct and desirable (it is exactly claim (c)'s fix applying to a second
trigger), but it is a **new** path that neither proposal's tests cover alone. Add one composition
test when the second of the two lands:
`expiry_triggered_wake_of_blmove_stages_destination_write`. Whoever lands second owns it.

**If the 83-first assumption breaks** and 88 lands first: `run_served_wake_effects` still works
(it hooks the satisfaction path, not the removal path), but 83 will then inherit a
`WaiterSatisfaction` that has grown a key-set side channel, and 83's routing change becomes strictly
larger than 83 currently scopes it. Flag to the orchestrator rather than reorder silently.

### Sibling 81 (`81-core-dead-seams.md`, commit f73bdd8f) — one-way edge, 81 H1 first

**Hard constraint, restated.** 81's H1 fixes a wait-queue unlink bug (`wait_queue.rs:457`, `:545`).
Until it lands, a wake can serve the **wrong client**. Applying 88's effect tracking on top of that
makes the situation *worse*, not better: today a mis-served wake produces a wrong reply and a
partially-tracked mutation; after 88 it would produce a wrong reply plus a fully-durable,
fully-replicated, fully-invalidated mutation attributed to the wrong waiter. **88 must not land
before 81 H1.**

**Scope exclusion, honored.** 81 ruled `drain_stream_waiters_*` (`wait_queue.rs:493-519`) mutate
nothing. 88 does not touch them. Confirmed independently: those functions dequeue and reply; they do
not call into the store.

### Sibling 84 (`84-blocking-op-dedupe.md`, commit ddc4b184) — boundary **CONFIRMED**

84's only edit inside a satisfaction arm is the XRead arm at `blocking.rs:1114-1122`. 84 claims it is
outside 88's write-effect scope by construction. **Verified and confirmed** — that arm constructs
its `Done` with `events: Vec::new()`, `restore: Restore::None`, `propagate: None`, and mutates
nothing in the store. It produces an empty `ServedWrites` under the proposed design, and
`run_served_wake_effects` over an empty key set is a no-op for every disposition. No dispute.

One adjacent observation, **not** a dispute with 84 and **not** claimed by 88: the sibling
`XReadGroup` arm (`blocking.rs:~1140-1185`) *does* mutate — group last-delivered-id and the PEL via
`read_group_entries` — with `bumps_version() == false` and a KNOWN GAP comment about propagation
already in the code. Under 88's design that arm would need a `ServedWrites` naming the stream key,
which would newly stage a WAL write for the consumer-group state change. That is a real and probably
correct consequence, but consumer-group durability is its own subject with its own existing gap
comment. **Out of scope for 88**; call it out to the orchestrator as a lane candidate rather than
folding it in.

### `FM-CLUSTER-038`

The only FM tag in `blocking.rs` is `FM-CLUSTER-038` at `:2065`, on
`slot_migrated_without_a_known_target_replies_clusterdown` — and it is in the **test module**, not
the main body. Its observable (blocked clients woken with MOVED/CLUSTERDOWN on slot migration, via
`drain_waiters_for_slot`) is unaffected: those wakes reply with an error and mutate nothing, so they
produce no `ServedWrites` and never reach `run_served_wake_effects`. Both 81 and 84 cite this tag;
all three proposals preserve it.

### Seam lints

- `lint-keyspace-notify-routing` — satisfied today (blocking.rs routes through
  `emit_keyspace_notification`) and after the change (the `ApplyNow(KeyspaceNotifications)`
  disposition calls the same seam).
- `lint-metrics-chokepoint` — the typed handles in the `Done` arm (`BlockedSatisfiedTotal::inc`
  and siblings) are untouched.
- `lint-durable-ack` — worth a re-run after (c)'s fix, since the change adds a durable write to a
  path the gate models.
- Generally: routing *more* work through the canonical pipeline is the compliant direction for every
  chokepoint gate in the family. No lint is expected to newly fire; `just lint-gates` is cheap and
  should be run anyway.

### Residual risks

- **Double-application on the source key.** After the change, source-key effects could be applied
  twice — once by the outer command (indices 0–3, before the wake) and once by `ApplyNow` over
  `served.keys`. For `VersionIncrement` and `TrackingInvalidation` this is harmless (idempotent-ish:
  an extra bump can only cause a spurious WATCH abort, which is the safe direction; an extra
  invalidation is a redundant push). For `DirtyCounter` it **double-counts**, which is wrong — Redis
  counts the push and the serve as two dirty events, so counting both is arguably right, but it must
  be a decided answer with a test, not a side effect. Decide it when implementing; the honest
  default is to match Redis (count both).
- **Cascade depth.** Each cascade hop contributes its own served keys. The depth cap
  (`blocking.rs:1615` `blmove_fanout_stops_at_depth_cap`) bounds the set, so unbounded key-set
  growth is not a risk, but the union must accumulate across hops rather than overwrite.
- **`Skip(WaiterSatisfaction)` is load-bearing.** If someone "fixes" that skip into an `ApplyNow`,
  the driver's own recursion and the pipeline's satisfaction step both fire and the depth cap no
  longer bounds anything. The reason string at the site is the defense; the structural test asserts
  the disposition.

---

## Effort

**M/L** — matching the lane brief's estimate, with the balance toward L.

- `ServedWrites` + `EffectScope::ServedWake` + `run_served_wake_effects` + threading the key set into
  the in-flight run: **M**. Mechanical, but touches the shape of `run_write_effects`' local state.
- Deleting the three hand effects and re-pointing the existing blocking tests: **S**.
- The three FM rows + spec scope amendment + the LOCKED `FM-PERSISTENCE-019` text amendment: **S**,
  but gated on orchestrator sign-off for the locked-spec edit.
- The forcing tests, especially (c)'s crash-recovery test: **M**. Crash-window tests in this repo are
  the expensive part.
- Merge sequencing behind 83 and 81 H1: schedule risk, not effort.

---

## Hotfix candidates

**Claimed (LIVE, safe to land ahead of the full change):**

- **H1 — dest version bump.** In the `Done` arm, bump the version for every key the satisfaction
  wrote, not just the source. Concretely: have the BLMove arm report dest, and bump both. Fixes claim
  (a). One-directional risk — an extra bump can only cause a *spurious abort*, never a missed one, so
  the failure mode of getting it wrong is conservative. Needs `FM-BLOCKING-006` and its forcing test;
  do not land bare.

**Claimed with a warning (LIVE, but the fix must be complete):**

- **H2 — dest tracking invalidation.** Fixes claim (b). Safe in isolation *only if* both tracking
  modes are handled — invalidating for default-mode clients while leaving BCAST subscribers stale is
  worse than the status quo, because it makes the gap intermittent and mode-dependent, which is
  exactly the shape of bug that survives a decade. Land via `invalidate_keys_all_modes`, never via a
  hand-rolled key-based-only call. Needs `FM-BLOCKING-007`.

**NOT a hotfix — do not land alone:**

- **A3 — dest WAL staging.** Claim (c) is the highest-severity finding and the *most* dangerous to
  patch narrowly. A minimal patch (append a dest `WalAction` at the serve site) writes to the WAL
  from **inside effect index 4**, i.e. out of order with respect to `WalPersistence` at index 6 and
  outside the ordering argument `FM-PERSISTENCE-019` relies on. The result is a WAL whose records are
  no longer in effect order, in a **LOCKED area (0.85 gate)**, with no spec row and no crash test.
  It would very plausibly trade a known loss for an unknown ordering corruption. **Fix (c) only via
  the `UnionIntoOuter(WalPersistence)` route, with `FM-BLOCKING-008` and the crash-recovery test
  landed together.** This is the same shape as 83's H3 anti-hotfix and deserves the same treatment.

**LATENT — no hotfix, ride along:**

- **(e) dirty counter.** Free once the served key set exists; not worth an independent patch, and an
  independent patch would have to answer the double-counting question (see Residual risks) with no
  surrounding structure to answer it in.

**REFUTED — nothing to fix:**

- **(d) search reindex.** No hotfix, no row, no test. Recorded in `FM-BLOCKING-006`'s "NOT
  observable" column so the refutation is durable and a future reader does not re-litigate it.

**Security findings**: none identified in this lane. Per standing policy, any security finding would
be classification-only — filed and parked, never turned into a fix proposal.
