# Proposal 88 — The blocking-serve path is a fourth write-effect authority

**Lane**: PN12 (core / shard effects)
**Status**: proposal (rev 2 — revised per adversarial review), unimplemented
**Locked-adjacent**: yes — touches txn WATCH semantics (gate 0.90) and persistence WAL staging
(gate 0.85). Behavior changes here are **spec-first**.
**Sibling edges**: 83 rev 2 lands first (orchestrator ruling); 81 H1 lands first (one-way); 84
confirmed disjoint.

**Rev 2 changes** (all driven by the review, each re-verified against HEAD before rewriting):
`KeysizesFlush` is **already correct** and its disposition is `Skip`, not `UnionIntoOuter` —
rev 1's premise 2 was false and its structural test would have encoded the bug (§Problem premise 2´,
§Proposed change). Claim (a) is **standalone-only** (§(a)). The mechanism is rebuilt around the
already-existing `pending_serve_propagations` vector instead of a union into a borrowed slice
(§Proposed change / the channel). `EffectScope::ServedWake` is **dropped** — 88 no longer widens
`EffectScope`, which dissolves the textual conflict 83 rev 2 flagged (§Sibling 83). The XReadGroup
aside was **wrong** and is corrected (§Sibling 84). The FM plan is rebuilt: the persistence spec
already reaches into `frogdb-core`, `FM-PERSISTENCE-019`'s *Observable* is falsified today, and
`blocking-failure-modes.md`'s frogdb-core carve-out needs amending (§Spec-first plan). Claim (c)
severity raised (§(c)). Effort raised to **L**.

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
| 5 | `KeysizesFlush` | **yes — via the store-side buffer, not via declared keys.** `get_mut` queues a refresh for whatever key was actually mutated (`store/hashmap.rs:1320-1329`), and `MUST_BE_ADJACENT` pins 4→5 (`post_execution.rs:771-772`) so the flush at `:383` catches it |
| 6 | `WalPersistence` | **no** |
| 7 | `SearchIndex` | **no** (owed nothing — see claim (d)) |
| 8 | `ReplicationBroadcast` | deferred — `pending_serve_propagations` at `:360`, drained at `post_execution.rs:420` |

For **BLPOP / BLMPOP / BZPOPMIN / BZPOPMAX / BLMOVE-source**, the subset is survivable: every key
the wake mutates is already in the *waking write's* declared key set, so effects 0–3 (already run at
the time the wake fires) covered it in the same or a prior position, and effects 6–8 (still ahead)
cover it on the way out. The hand-written version bump and notification loop are redundant belt-and-
braces, not load-bearing.

For **BLMOVE / BRPOPLPUSH**, the subset is a hole. `ListSatisfaction::satisfy` mutates the
**destination** key directly (`blocking.rs:788-797`), and dest is **not in the waking write's key
set** — the waking write is `LPUSH src v`, whose `handler.keys(args)` is `[src]`. Every effect that
derives its work *from declared keys* therefore skips dest entirely. Index 5 is the one exception,
and the reason it is an exception is the whole shape of the fix: it derives its work from what the
store was actually asked to mutate, not from what the command declared.

Three of the four suspected misses are **LIVE**, one is **REFUTED**, and a fifth (not in the lane
brief) is **LATENT**. The worst is (c): a crash after a served `BRPOPLPUSH` and before the next
command that happens to declare dest as a write key loses the element from **both** src and dest —
acknowledged-write loss in exactly the reliable-queue pattern `BRPOPLPUSH` exists to serve, at any
durability setting, with a replica-divergence amplifier (§(c)).

The proposed change makes the serve path a *client* of the pipeline rather than a fourth author.
The leverage point is a datum the serve path **already produces and already owns**: the
`SynthesizedCommand` it records in `pending_serve_propagations` for replication. That command
(`LMOVE src dest LEFT RIGHT`) carries, through the ordinary `Command` interface, everything the
remaining effects need — `keys()` gives `[src, dest]`, `wal_actions()` gives
`[PersistOrDelete(src), Persist(dest)]`, `spec().reindex` gives the reindex policy. Today exactly
one consumer reads that vector (the broadcast at index 8). The fix is to give it the other three.
Effect position relative to `WaiterSatisfaction` becomes data, not a comment, and a future tenth
effect — or a future satisfaction arm — is a compile error until someone classifies it.

**One correction to the lane brief**: `pending_serve_propagations` **cannot be deleted**. It is not a
hand copy of `ReplicationBroadcast`; it is a *cross-position deferral* that exists because the served
pop must broadcast **after** the waking push, or replicas apply pop-before-push and diverge. Under
this proposal it stops being a replication side-vector and becomes the serve path's *interface* to
the pipeline. The deletion test lands on the three hand effects, not on this vector. See
§Proposed change / deletion test.

---

## Files involved

| File | Lines | Role in this proposal |
|---|---:|---|
| `frogdb-server/crates/core/src/shard/blocking.rs` | 2090 | The fourth authority. `drive_satisfaction` `:255-258` (**deleted by 83 rev 2**), `drive_satisfaction_body` `:271`, the `Done` arm + three hand effects `:327-388`, cascade recursion `:374`, `apply_restore` `:426`, `BLMove` satisfaction arm `:754-838` (dest mutation `:788-797`, synthesized `LMOVE` `:828-838`), `StreamSatisfaction::satisfy` `:1089-1181` |
| `frogdb-server/crates/core/src/shard/post_execution.rs` | 1907 | The interface. Module doc single-ownership claim `:12`, `WriteSummary` `:114-132`, `EffectScope` `:211-242`, `WriteEffectKind` `:249-268`, `WRITE_EFFECT_ORDER` `:282-292`, `run_write_effects` `:305`, `WaiterSatisfaction` arm `:377-381`, `KeysizesFlush` arm `:382-384`, `WalPersistence` arm `:385-405`, served-pop drain `:413-420`, `run_scatter_effects` `:512`, `run_internal_removal_effects` `:561`, `invalidate_written_keys` `:671`, `satisfy_waiters_for_command` `:701`, `MUST_PRECEDE`/`MUST_BE_ADJACENT` `:753-772` |
| `frogdb-server/crates/core/src/store/hashmap.rs` | — | **New in rev 2, and it refutes rev 1.** `get_mut` `:1298`, the unconditional keysizes-refresh queue `:1320-1329` — the store-side seam that makes index 5 already correct |
| `frogdb-server/crates/core/src/command.rs` | — | `WalStrategy::MoveKeys` `:680-689` → `[PersistOrDelete(args[0]), Persist(args[1])]`; the exact actions claim (c) needs |
| `frogdb-server/crates/commands/src/list.rs` | — | `LmoveCommand::spec` `:867-889`: `keys: KeySpec::FirstTwo` `:872`, `wal: WalStrategy::MoveKeys` `:875`, `requires_same_slot: false` `:884` |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | `SlotVersions` `:56-96`, `pending_serve_propagations` field `:175`, `bump_versions_for` `:613`, `bump_version_for_key` `:618`, `get_key_version` `:632`, `check_watches` `:648-664`, panic reset `:902` |
| `frogdb-server/crates/core/src/shard/types.rs` | — | `invalidate_keys` `:501` (key-based only — the H2 trap) vs `invalidate_keys_all_modes` `:509` |
| `frogdb-server/crates/core/src/shard/persistence.rs` | 908 | `execute_wal_action` `:106`, `WalTarget::write_set` `:138-146` (reads `store.get_hot(key)`), `persist_records` `:247`. **In the persistence spec's declared Scope already** |
| `frogdb-server/crates/core/src/shard/panic_guard.rs` | 564 | `:18-42` documents `pending_serve_propagations` as command-scoped state dropped on the panic path |
| `frogdb-server/crates/server/src/connection/guards.rs` | — | **New in rev 2.** `validate_cluster_slots` `:690`, `validate_cluster_slots_inner` `:701` with its cluster gate `:703`, the `SlotValidator::same_slot` CROSSSLOT rejection `:726-731` — why claim (a) is standalone-only |
| `frogdb-server/crates/server/src/connection/routing.rs` | — | `SlotValidator::same_shard` `:106`, `requires_same_slot` consult `:119` (cross-*shard* only) |
| `frogdb-server/crates/core/src/shard/wait_queue.rs` | 931 | 81's H1 site (`:457`, `:545`); `drain_stream_waiters_*` `:493-519` ruled non-mutating by 81 |
| `frogdb-server/crates/core/src/shard/keyspace_notify.rs` | 141 | `emit_keyspace_notification` `:28` — the routing seam blocking.rs already honors |
| `scripts/failure-modes.py` | — | `NEXTEST_CRATES` `:64-77` (**includes `frogdb-core`**), area-from-filename `:161` |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | Scope `:10-15` (**already names `frogdb-core/src/shard/persistence.rs`**); `FM-PERSISTENCE-019` `:291-302`, Observable `:296`, Invariant `:298` |
| `.scratch/hardening/specs/txn-failure-modes.md` | — | `FM-TXN-033` `:422` (WATCH abort); scope `:10-14` forward-declares the missing shard-side spec |
| `.scratch/hardening/specs/blocking-failure-modes.md` | — | **not LOCKED**; scope `:8-14` forward-declares the same missing spec; **frogdb-core carve-out `:22-30`**; rows 001–005 only |

Verified at HEAD: `blocking.rs` contains **no** call to `invalidate`, `record_read`, `persist`,
`reindex`, or `increment_dirty` anywhere in the file.

---

## Problem — four chains, graded separately

Shared premise for (a)–(d), established once. **Premise 2 is restated from rev 1; the review was
right that rev 1's version was false and load-bearing.**

1. In **standalone** mode, `SlotValidator::same_shard` (`routing.rs:106`) is what routes multi-key
   commands. It checks **same shard**, not same slot, and `requires_same_slot` (`:119`) is consulted
   only when the keys span *shards*. `shard_for_key = slot % num_shards` (`partition.rs:32-36`).
   ⇒ A standalone BLMOVE src/dest pair with `num_shards == 1` shares a shard and shares nothing
   else. Slot equality is not implied and in practice essentially never holds.
   In **cluster** mode this premise does not hold at all — see (a).
2. ~~Every effect at indices 0–3 and 5–8 derives its work from `record.handler.keys(record.args)`.~~
   **FALSE, and rev 1 built a wrong disposition and a wrong test on it.**
   **2´.** Effects at indices 0–3 and 6–8 derive their work from
   `record.handler.keys(record.args)` — the *declared* keys of the **waking write**, i.e. `[src]`.
   Effect **5** (`KeysizesFlush`) does not: its arm is `self.store.flush_keysizes_refreshes()`
   (`post_execution.rs:383`), which takes **no key set**. It drains a buffer the store filled
   itself — `HashMapStore::get_mut` pushes a histogram snapshot for whatever key it was called on,
   unconditionally (`store/hashmap.rs:1320-1329`). The BLMove arm reaches dest through `get_mut`
   (`blocking.rs:792`, after `store.set(dest, Value::list())` at `:789` when dest is absent), so
   dest **is** in that buffer. And the flush cannot be missed: `MUST_BE_ADJACENT` declares
   `(WaiterSatisfaction, KeysizesFlush)` (`post_execution.rs:771-772`) and
   `order_satisfies_all_declared_constraints` (`:780`) fails the build if 5 stops immediately
   following 4. That adjacency constraint exists *precisely* to catch waiter-driven `get_mut`
   mutations — it is the one place where somebody already solved this problem, and the right
   architectural reading of it is as the **model** for the fix, not as another instance of the bug.
3. The wake fires from `WaiterSatisfaction`, index **4**. Effects 0–3 have already run; 5–8 have
   not.

### (a) WATCH on the destination — **LIVE, standalone mode only**

Chain:

- `bump_version_for_key(key)` (`blocking.rs:348`, guarded by `strat.bumps_version()`) bumps the
  version of **`key`** — the source, the key whose waiters were being driven. `worker.rs:618`.
- Versions are slot-granular: `SlotVersions` (`worker.rs:56-96`) stamps per slot plus a shard-wide
  epoch; `get_key_version(k) = version_for(slot_for_key(k))` (`worker.rs:632`).
- `check_watches` (`worker.rs:648-664`) compares each watched key's recorded stamp against
  `get_key_version` at EXEC time.
- `slot_for_key(dest) != slot_for_key(src)` in the general standalone case (premise 1). The dest
  slot's stamp never moved. The shard epoch never moved.
- ⇒ EXEC sees an unchanged watch and commits.

**Mode scoping — rev 2, per review.** This is a **standalone-only** defect, and the review is right
that rev 1 implied otherwise by leaning on `requires_same_slot`.

- In **cluster** mode a cross-slot `BLMOVE` never reaches the serve path. `validate_cluster_slots`
  (`guards.rs:690`) → `validate_cluster_slots_inner` (`:701`) runs the strict
  `SlotValidator::same_slot(&keys)` CROSSSLOT check at `:726-731` and replies `CROSSSLOT` before
  execution. BLMOVE is not cluster-exempt (`is_cluster_exempt` `:662` covers pub/sub, txn, admin,
  auth, conn-state, replication, persistence families).
- A **same-slot** cluster BLMOVE (hash-tagged) is also fine for the opposite reason:
  `slot_for_key(dest) == slot_for_key(src)`, so the source bump at `:348` already moves dest's
  stamp.
- The gate is `self.cluster.slot_migration.as_ref()?` at `guards.rs:703` — cluster-mode only, so
  standalone falls straight through to `routing.rs`, where LMOVE's
  `requires_same_slot: false` (`list.rs:884`) is never even consulted, because both keys share the
  single shard and `same_shard` returns `Ok` at `:106-110`.

⇒ **`requires_same_slot` was never the lever.** The lever is that standalone mode has no
slot-equality gate at all, and versions are slot-granular.

Repro (standalone; two sessions plus the pusher; dest either absent or an existing list — both
work):

```
A: WATCH dest
A: (reads dest, decides its transaction based on that read)
B: BLMOVE src dest LEFT RIGHT 0        # blocks
C: LPUSH src v                         # serves B; element lands in dest
A: MULTI ; <ops predicated on dest> ; EXEC
   → EXPECTED: nil (abort)   ACTUAL: array (commits)
```

`FM-TXN-033` (`txn-failure-modes.md:422`) states the contract this breaks — a watched key whose
version moved under another client's write must abort. But that spec's scope paragraph (`:10-14`)
explicitly disclaims the shard side: "The shard-side engine (WATCH version check, rollback,
replication framing) lives in `frogdb-core` and gets its own spec." That spec does not exist.

**Verdict: LIVE in standalone; structurally impossible in cluster mode.** It is a spec-ownership
gap as much as a row violation: the row states the contract; no row owns the site that breaks it.

Two things this claim does **not** say. The transaction is not corrupted — A's own writes still apply
atomically. And BLPOP-family wakes are fine: source and watched key coincide, and the waking `LPUSH`
already bumped that slot at effect index 0 before the wake even fired (the `:348` bump is redundant
there — see §Proposed change).

### (b) RESP3 client-side-caching invalidation on the destination — **LIVE**

Chain:

- `invalidate_written_keys` (`post_execution.rs:671-688`) is the sole invalidation seam for writes;
  it iterates `record.handler.keys(record.args)` and calls `invalidate_keys_all_modes`
  (`types.rs:509`), which covers both default key-based tracking and BCAST prefix matching.
- Effect index 1 (arm at `post_execution.rs:352`, taking `summary.conn_id`). Runs **before** the
  wake. Key set = `[src]`.
- `blocking.rs` calls nothing tracking-related (grep: zero `invalidate` occurrences in the file).
- ⇒ A client that cached `dest` keeps a stale entry indefinitely — until some *later, unrelated*
  command declares dest as a write key.

Exposure is wider in BCAST mode than in default mode: default-mode tracking requires the client to
have read dest (so `record_read` registered it), whereas a BCAST client subscribed to dest's prefix
expects notification without ever having read it, and gets none.

Unlike (a), this is **not** mode-scoped: it is a cross-*key* gap, not a cross-slot one, so a
same-slot cluster BLMOVE is affected identically.

**Verdict: LIVE.** Source-key invalidation is correct today only incidentally — the waking write
declares it.

### (c) WAL staging of the destination write — **LIVE**, highest severity, and worse than rev 1 said

Chain:

- `WalPersistence` (`post_execution.rs:385-405`) resolves the command's `WalStrategy` into
  `WalAction`s and calls `persist`/`persist_records`. Every `WalAction` variant (`Persist`,
  `DeleteIfMissing`, `PersistOrDelete`, `PersistIfExists`, `MergeHllDelta`, `ClearShard`) is
  parameterized by a **declared key**.
- `WalTarget::write_set` (`persistence.rs:138-146`) resolves a key to bytes by reading
  `self.store.get_hot(key)` **at persist time**. The WAL is a snapshot-of-value log, not an
  operation log.
- Effect index 6. Runs **after** the wake — so a dest action added at index 4 *would* be picked up
  and would snapshot the post-move value correctly. Nothing adds it.
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

**Severity, raised in rev 2 per review. Three amplifiers rev 1 understated or missed:**

1. **Durability-config independent.** There is no `appendfsync`-style setting that closes this. The
   write is not *late* to the WAL; it never enters the WAL. `Durability::AlwaysSync` would still
   lose it, because the sync applies to records that were staged and no record was staged.
2. **Replica divergence, not just local loss.** The served BLMOVE **does** propagate: the
   synthesized `LMOVE` is pushed to `pending_serve_propagations` (`blocking.rs:360`) and broadcast
   at index 8 (`post_execution.rs:413-420`). So replicas apply the move and **hold the element in
   dest correctly**. After a primary crash and WAL replay, the primary comes back *without* it —
   the primary is now behind its own replicas on a write it already acknowledged and already
   shipped.
3. **The divergence is then propagated onto the replicas.** The recovered primary's next
   `FULLRESYNC` cuts a checkpoint from its own (lossy) state and ships it. The replicas that had
   the element right are overwritten with the loss. A defect that starts as one node's crash
   recovery ends as cluster-wide data loss of an acknowledged write.

The window is **unbounded** in the sense that matters: it is not a fixed flush interval, it is "until
some future, unrelated command happens to declare dest as a write key" — a queue whose consumer
uses `BRPOPLPUSH` and whose processing loop never writes the dest key by name may never close it.

This directly contradicts the stated **Observable** of `FM-PERSISTENCE-019`
(`persistence-failure-modes.md:296`) — not merely its Invariant, which is what rev 1 claimed:

> The restored checkpoint contains every write the server had already replied to when the cut began

The served BLMOVE was replied to. The dest half is not in the cut. The row's Invariant (`:298`)
supplies the argument —

> The `WalPersistence` effect enqueues a write's WAL entry before `ReplicationBroadcast`
> acknowledges it, so the drain message is behind every acknowledged write by construction

— and the argument is valid but rests on an unstated precondition: *every acknowledged mutation goes
through `WalPersistence`*. The serve path acknowledges (the reply is sent at `blocking.rs:333`,
inside effect 4) without going through it. So this is a **falsified row**, which is a stronger
obligation than rev 1's "text amendment" framing — see §Spec-first plan.

Relation to proposal 83 (§on `run_internal_removal_effects`): 83 found the same root cause —
WAL work derived from declared keys — running in the opposite direction. 83's lazy-expiry path
removes a key the WAL is never told to *delete*; 88's serve path adds a value the WAL is never told
to *put*. 83 fixes its case by **routing** the removal through the existing
`run_internal_removal_effects`, which already resolves a full key set. 88 cannot borrow that
routing: the serve happens *mid-run*, at index 4 of an in-flight `run_write_effects`, not as a
standalone event. Hence the in-run served-command channel in §Proposed change rather than a second
nested pipeline run.

**Verdict: LIVE. Highest-severity finding in this proposal.**

### (d) Search reindex on the destination — **REFUTED**

Chain, and it terminates immediately:

- `IndexKind` (`command_spec.rs:332`) has exactly two variants: `Hash` and `Json`. Lists are not
  an indexable kind. There is no list index to become stale.
- `ReindexSpec::None` (`:355`) is the default, and `LMOVE`'s own spec
  (`commands/src/list.rs:886`) declares it. The *immediate* (non-blocking) path owes the search
  index nothing either — so the serve path is symmetric with it, not divergent from it.
- BLMOVE's WRONGTYPE guard on dest (`blocking.rs:762-772`) means dest is provably absent or a list
  at the moment of the move. It can never be a hash or JSON document that an index tracks.

**Verdict: REFUTED.** No FM row, no test, no work. Under the revised mechanism the answer is not
even a hand-maintained claim: the `SearchIndex` effect resolves `spec().reindex` off the *same*
synthesized `LMOVE` the other effects use, reads `ReindexSpec::None`, and does nothing. If an
`IndexKind::List` ever lands and `LMOVE`'s spec changes, the served path inherits the change for
free. This is the argument for driving the fix through the `Command` interface rather than through a
bare key set.

### (e) Dirty counter — **LATENT** (not in the lane brief; found during the sweep)

`update_dirty_counter` (`post_execution.rs:374-376`) is effect index 3 — already run when the wake
fires, and its delta was computed for the waking write alone. A served pop/move therefore never
advances `rdb_changes_since_last_save`.

Redis increments `server.dirty` on the serve. Divergence is observable through `INFO
persistence`'s `rdb_changes_since_last_save` and, indirectly, through `save`-point scheduling: a
workload consisting mostly of served wakes under-counts its own mutation rate and delays an
auto-BGSAVE that Redis would have triggered.

**Verdict: LATENT.** Real divergence; no correctness invariant currently names it; no data loss.
Graded latent because the observable is a counter, not a value, and because a fix is free once the
served-command channel exists. Do not file this as an independent defect — it rides along with the
fix.

---

## Proposed change

### The channel (rewritten in rev 2)

Rev 1 proposed `UnionIntoOuter`: adding served keys to "the outer run's pending key set". The review
correctly killed this — **there is no such set**. `WriteSummary::writes` is
`&'a [WriteRecord<'a>]` (`post_execution.rs:120`), a *borrowed slice*. Nothing in
`run_write_effects` owns a mutable key collection that a union could land in, and manufacturing one
would mean allocating synthetic `WriteRecord`s with borrowed `args` that outlive nothing.

The constructive fix is already sitting in the file. `pending_serve_propagations`
(`worker.rs:175`) is an **owned `Vec<SynthesizedCommand>`** that:

- is pushed by the serve path at `blocking.rs:360`, inside the committed-delivery arm;
- accumulates across the whole BLMove wake cascade in apply order (the push happens *before* the
  recursion at `:374`, by design and by test);
- survives to index 8, where `std::mem::take` drains it (`post_execution.rs:420`);
- is already command-scoped state cleared on the panic path (`panic_guard.rs:18-42`,
  `worker.rs:902`).

And the command it carries is not a stub. For BLMove the arm records
`SynthesizedCommand { name: "LMOVE", args: [src, dest, src_dir, dest_dir] }`
(`blocking.rs:828-838`). Resolving `"LMOVE"` through the registry yields a handler whose spec
(`list.rs:867-889`) declares:

| spec field | value | what it gives the served-wake effects |
|---|---|---|
| `keys: KeySpec::FirstTwo` (`:872`) | `[src, dest]` | the key set `VersionIncrement` and `TrackingInvalidation` need |
| `wal: WalStrategy::MoveKeys` (`:875`) | `[PersistOrDelete(args[0]), Persist(args[1])]` (`command.rs:680-689`) | **exactly** persist-src-or-delete + persist-dest — claim (c)'s fix, verbatim, with no new `WalAction` and no new strategy |
| `reindex: ReindexSpec::None` (`:886`) | nothing | claim (d)'s answer, derived rather than asserted |
| `event: EventSpec::Dynamic` (`:882`) | — | the notifications the hand loop at `:369` already emits |

So: **one owned vector, one resolution step, four effects fixed through one channel.** The vector
stops being "pending propagations" and becomes what it always structurally was — *the record of what
the wake did, expressed in the same vocabulary the pipeline already consumes*. Rename it
`pending_served_commands` to say so.

This is the module-boundary win. The serve path's job shrinks to "describe your mutation as a
command"; deciding what effects a command owes stays entirely in `post_execution.rs`. The serve path
loses the ability to be a fourth authority, because it no longer applies anything.

### Vocabulary

Two types, no new `EffectScope` variant.

**`ServedEffect`** — replaces the bare `propagate: Option<SynthesizedCommand>` field on
`Satisfaction::Done`:

```rust
enum ServedEffect {
    /// This satisfaction mutated the store; here is the command that reproduces it.
    Command(SynthesizedCommand),
    /// This satisfaction mutated nothing the pipeline owes effects for, because <reason>.
    NoneBecause(&'static str),
}
```

The point is exhaustiveness at the *authoring* site. Today an arm that writes `propagate: None` is
indistinguishable from an arm that mutates but forgot; `ServedEffect::NoneBecause` forces the author
to state which. The two current `None` arms both get honest reasons — see §Sibling 84 for
`XReadGroup`, whose reason is non-obvious and which rev 1 got wrong.

**`ServedWrites`** — the pipeline-side accumulation, built at index 4 from the commands recorded
during that index's satisfaction pass:

- `commands: Vec<SynthesizedCommand>` — the drained `pending_served_commands`. Each resolves to a
  handler; all key sets, WAL actions and reindex policies derive from it.
- `conn_id: u64` — **new in rev 2, per review.** `TrackingInvalidation` cannot be applied without
  it (`invalidate_written_keys(writes, conn_id)`, `post_execution.rs:352`). The value is the
  *waking write's* `summary.conn_id`: the mutation is caused by the pushing client, and the served
  client is a reader of dest, not its writer. That is a decision, not an inference — it needs the
  assertion named in §Testability.
- `dirty_delta: i64` — **new in rev 2, per review.** `update_dirty_counter(delta)`
  (`post_execution.rs:375`) takes a number, not a key set. One per served satisfaction, matching
  Redis's `server.dirty++` on the serve. Accumulated across cascade hops.

`EffectScope::ServedWake` from rev 1 is **dropped.** It was never dispatched on: the served effects
run *inside* an existing `run_write_effects` call, under whatever scope the outer command already
carries. Adding a variant that no `match` arm distinguishes is cost without leverage — and, per
§Sibling 83, it was the entire textual conflict with 83 rev 2.

### Per-effect disposition — corrected

`run_served_wake_effects(&mut self, served: &ServedWrites)` and the index-6/7/8 arms between them
give every `WriteEffectKind` exactly one disposition. The match is exhaustive over
`WriteEffectKind`.

| disposition | meaning | effects |
|---|---|---|
| `ApplyNow` | index < 4: this effect already ran for the outer command and cannot see the served keys; apply it again, now, over the served commands | `VersionIncrement`, `TrackingInvalidation`, `KeyspaceNotifications`, `DirtyCounter` |
| `Skip(&'static str)` | structurally not owed, reason recorded at the site | `WaiterSatisfaction` — "the cascade is the driver's own recursion; the depth cap depends on it"; **`KeysizesFlush`** — "the store queues its own refresh per `get_mut` (`hashmap.rs:1320-1329`); `MUST_BE_ADJACENT(4,5)` guarantees the flush at index 5 sees them" |
| `IncludeServed` | index > 5: the effect has not run yet; its arm iterates `served.commands` **in addition to** `summary.writes` | `WalPersistence`, `SearchIndex`, `ReplicationBroadcast` |

**`KeysizesFlush` is `Skip`, not `UnionIntoOuter` — rev 1 was wrong and the review caught it.**
Rev 1's disposition would have been a category error twice over: the effect takes no key set to
union into, and the behavior it would have "fixed" is already correct (premise 2´). Worse, rev 1's
structural test asserted `UnionIntoOuter` is *exactly* indices 5–8 — which would have **encoded the
bug as a regression barrier**, making the correct classification the thing that fails CI. Rev 2's
structural test asserts the actual classification per kind and deliberately does **not** assert a
positional rule (see §Testability), because the positional rule is false: index 5 already works by a
different mechanism, and pretending otherwise is what produced the error.

`SearchIndex` is `IncludeServed` rather than `Skip`, despite claim (d) being refuted, because the
inclusion is free and correct-by-construction: the arm resolves `spec().reindex` off the served
command and finds `ReindexSpec::None`. Encoding (d)'s answer as a code-level `Skip` would be a
hand-maintained claim about value types that the reindex machinery already decides correctly. The
`Skip` reason for (d) belongs in the *proposal* and in the FM spec's "NOT observable" column, not as
a branch.

**A tenth effect does not compile until someone classifies it for the serve path.** That is the
architectural point: today, adding an effect to `WRITE_EFFECT_ORDER` silently does nothing on served
wakes, and nothing anywhere says so. `ServedEffect::NoneBecause` is the same forcing property from
the other direction: a new satisfaction arm cannot silently mutate without effects.

Note the ordering subtlety in `ApplyNow`: the four sub-index-4 effects are applied to the served
commands **in `WRITE_EFFECT_ORDER` order among themselves**, not in whatever order the old hand code
happened to use. Today the hand code runs version-bump → propagation-push → notifications, which is
0 → 8 → 2 — out of order relative to the canonical sequence. Nothing currently observes the
difference (`MUST_PRECEDE` at `post_execution.rs:753` does not constrain 2 before 8), but the
proposed form makes it moot rather than accidentally-fine.

### Where it hooks

Immediately after the `WaiterSatisfaction` arm's loop (`post_execution.rs:377-381`), still inside
`run_write_effects`, still `async`-legal, still before the `KeysizesFlush` arm at `:382` — so the
`MUST_BE_ADJACENT(4,5)` relation is untouched and index 5 keeps working the way it already does.
The `ApplyNow` half runs there (all four effects are synchronous). The `IncludeServed` half needs no
hook at all: arms 6/7/8 simply also iterate `served.commands`, and arm 8 already does.

This site is deliberately *not* in `blocking.rs`. See §Sibling 83 — the wrapper rev 1 would have
hung it under is deleted by 83 rev 2.

### What gets deleted

The three hand effects in the `Done` arm of `drive_satisfaction_body` (`blocking.rs:327-388`):

- `self.bump_version_for_key(key)` at `:348` — subsumed by `ApplyNow(VersionIncrement)` over the
  served command's `keys()`, which additionally covers dest (fixes claim (a)).
- the `for (key, name, class) in &events` loop at `:369` — subsumed by
  `ApplyNow(KeyspaceNotifications)`, still routing through `emit_keyspace_notification` so
  `lint-keyspace-notify-routing` stays satisfied.
- `strat.bumps_version()` as a call-site branch — folded into the served command's own spec.

After these three go, `blocking.rs` applies **zero** write effects. It records a `ServedEffect` and
returns. That is the deletion the proposal is really about, and it is checkable by grep: no
`bump_version`, no `emit_keyspace_notification`, no effect application anywhere in the file.

`apply_restore` and the `Err(_)` arm are untouched. The restore path must remain a pure store
rollback with **no** effects applied, which is what it is today (the `Restore::Move` arm at
`:454-483` pops dest, calls `cleanup_empty_list`, and pushes back onto src, leaving the store
byte-identical to pre-wake). Under the new design this becomes structurally clearer, not just
conventionally true: effects derive from a `ServedEffect::Command` that the `Err` arm never records.

### Deletion test — `pending_served_commands`

The lane brief proposed deleting `pending_serve_propagations` along with the hand effects. **It must
survive, and after this change it is more load-bearing, not less.** Applying the deletion test
properly:

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
*role*. It stops being a replication side-channel with one consumer and becomes the serve path's
single interface to the pipeline, with four. The rename (`pending_serve_propagations` →
`pending_served_commands`) is the whole API change; the `std::mem::take` at
`post_execution.rs:420` moves to index 4 (into `ServedWrites`) and arm 8 reads from
`served.commands` instead.

Two obligations that rev 1 stated and rev 2 keeps: `worker.rs:902`'s panic reset must clear the
whole record, and the `is_empty()` assertions at `post_execution.rs:1846`/`:1903` must be re-pointed
at whatever now holds the drained state or they will pass vacuously.

### Ordering invariants that must survive verbatim

1. **Reply-first-then-commit.** The reply is sent, and only on `Ok(())` is a `ServedEffect::Command`
   recorded. Pinned by `blocking.rs:1873` `doomed_waiter_records_no_propagation`.
2. **Restore-on-send-failure.** On `Err(_)`, `apply_restore` returns the store to its pre-wake
   state and **no** effect is applied. Pinned by `blocking.rs:1669`
   `push_after_receiver_dropped_does_not_lose_element`, `:1711`
   `blmpop_restore_preserves_all_elements_in_order`, `:1767`
   `bzpopmin_restore_preserves_member_and_score`.
3. **`MUST_BE_ADJACENT(WaiterSatisfaction, KeysizesFlush)`** (`post_execution.rs:771-772`). The
   `ApplyNow` pass is inserted between them, which does not break adjacency in
   `WRITE_EFFECT_ORDER` — the constraint is on the *slice*, checked by
   `order_satisfies_all_declared_constraints` (`:780`), not on wall-clock interleaving. But the
   inserted pass must not call `get_mut` on anything, or it would queue refreshes the flush then
   picks up. It does not: `ApplyNow` covers version, tracking, notifications and a counter — none
   of which touch the store.

Invariants 1 and 2 survive by construction: a `ServedEffect::Command` is produced *inside* the
`Ok(())` arm, and `run_served_wake_effects` is only reachable from a recorded command. Invariant 3
is the new one rev 2 adds, and it is the reason the hook site is where it is.

---

## Testability improvement

One red-green pair per surviving LIVE claim, each written to fail at HEAD.

**(a) WATCH-on-dest** — `frogdb-core`, integration-style over `ShardWorker`:
`served_blmove_bumps_destination_watch_version`. WATCH dest (record the version via
`get_key_version`), block a BLMOVE, push to src, assert `check_watches` now reports the dest watch
as invalidated. Red at HEAD: the dest slot's stamp is unchanged. **Standalone configuration
explicitly** — a cluster variant would be vacuous (guards.rs rejects the cross-slot BLMOVE) and a
same-slot variant is green at HEAD. Companion end-to-end test in `frogdb-server` asserting EXEC
returns nil, exercising the three-connection repro above.

The `frogdb-core` unit test is the one that matters for the gate and carve-out discussion below; the
server test is the one that matters for the user-visible contract. Write both.

**(b) tracking invalidation** — `served_blmove_invalidates_destination_for_tracking_clients`, with
a default-mode variant (client read dest first) and a BCAST-mode variant (client subscribed to
dest's prefix, never read it). Red at HEAD in both modes: zero invalidation messages. Prefer writing
these in `frogdb-server`, where RESP3 invalidation pushes are directly observable — that also keeps
this row clear of the `frogdb-core` carve-out (§Spec-first plan / B8).

Plus one assertion pinning the `conn_id` decision:
`served_blmove_invalidation_is_attributed_to_the_waking_connection` — the pushing client does not
receive an invalidation for its own write; the blocked client does. This is the decision
`ServedWrites::conn_id` encodes, and it must not be inferred from an implementation detail.

**(c) WAL staging** — `served_blmove_stages_destination_write_to_wal`. Assert at the
`WalTarget`/`WalAction` level that resolving the served `LMOVE` yields
`[PersistOrDelete(src), Persist(dest)]` and that a record naming dest is staged with the moved
element. Red at HEAD: no dest record exists. Then the crash-recovery test that actually states the
failure mode: `crash_after_served_brpoplpush_recovers_element_in_destination` — serve, cut before
any subsequent dest-declaring write, recover, assert the element is present in dest and absent from
src. Red at HEAD: absent from both.

Given the severity amplifiers, add one more:
`recovered_primary_does_not_diverge_from_replica_after_served_blmove` — serve, verify the replica
holds dest, crash and recover the primary, assert primary and replica agree. Red at HEAD: they do
not. This is the test that states amplifier 2/3 rather than leaving it as prose.

**(e) dirty counter** — folded into (a)'s test as an additional assertion
(`rdb_changes_since_last_save` advanced), not a separate test.

**Structural test** — `served_wake_classifies_every_write_effect`: iterate `WRITE_EFFECT_ORDER` and
assert each kind maps to its stated disposition **by kind, not by position**:

| kind | expected disposition |
|---|---|
| `VersionIncrement`, `TrackingInvalidation`, `KeyspaceNotifications`, `DirtyCounter` | `ApplyNow` |
| `WaiterSatisfaction` | `Skip` |
| `KeysizesFlush` | `Skip` |
| `WalPersistence`, `SearchIndex`, `ReplicationBroadcast` | `IncludeServed` |

**Explicitly do not assert "`ApplyNow` == indices 0–3 and the rest == 5–8".** Rev 1's version of
this test did, and that positional rule is false at index 5 — it would have frozen the wrong answer
into CI. What the test *should* additionally assert is the property that makes the positional
intuition safe where it does hold: every `ApplyNow` kind has a position `< 4` and every
`IncludeServed` kind has a position `> 4`, with `Skip` unconstrained. That is the real invariant;
index 5's `Skip` is exactly why the weaker form is the correct one.

A companion test pins the store-side mechanism the index-5 `Skip` rests on, so the skip is not a
comment: `served_blmove_refreshes_destination_keysizes` — serve a BLMOVE into a fresh dest, assert
the keysize histogram and memory accounting for dest are correct after the run. **Green at HEAD**
(this is the behavior rev 1 misdiagnosed), and its job is to *stay* green — it is the regression
barrier for the reason the skip is legal.

Both belong next to `order_satisfies_all_declared_constraints` (`post_execution.rs:780`).

A second exhaustiveness test on the other side: `every_satisfaction_arm_states_its_served_effect` —
construct each `BlockingOp` arm, assert `Satisfaction::Done` carries a `ServedEffect` and that a
`NoneBecause` reason is non-empty. Cheap, and it is what stops the next `XReadGroup`-shaped arm from
mutating silently.

### Spec-first plan (FM rows) — rebuilt in rev 2

Rev 1 opened with "neither locked spec owns the fix site." That is **wrong for persistence**, and the
correction changes the whole plan.

**What the persistence spec already owns.** `persistence-failure-modes.md`'s Scope (`:10-15`)
reads:

> Scope: the shard-to-storage path — `frogdb-core/src/shard/persistence.rs` (the `WalTarget` seam
> and `persist_records`), `frogdb-persistence/src/wal/` …

It **already reaches into `frogdb-core`**. It is LOCKED, and it has been claiming a `frogdb-core`
file since it was locked. So the ownership gap for claim (c) is much narrower than rev 1 said: the
spec's declared scope stops at `shard/persistence.rs` and does not extend to
`shard/post_execution.rs` (where `WalPersistence` decides *what* to persist) or `shard/blocking.rs`
(where the serve path bypasses that decision).

**The real gap is mutation-gate coverage, not spec scope.** `cargo mutants -p frogdb-persistence`
runs only that package's own tests and mutates only that package's own code. So *nothing in the
`frogdb-core` half of the persistence spec's own declared scope is gate-covered today* — including
`shard/persistence.rs`, which the spec explicitly claims. That is a pre-existing hole that 88 merely
illuminates; it is not created by this proposal, and it is not closable by anything 88 does.

**`FM-PERSISTENCE-019` is falsified today, not merely under-stated.** Its Observable (`:296`) —
"The restored checkpoint contains every write the server had already replied to when the cut began"
— is directly contradicted by claim (c). This makes the amendment a **persistence-row change**, and
persistence is LOCKED (gate 0.85). **It needs the orchestrator's sign-off before any code lands.**
Concretely:

1. Amend `FM-PERSISTENCE-019`'s Invariant (`:298`) to state the precondition the argument silently
   assumes — "…**and every acknowledged mutation is routed through `WalPersistence`**" — and
   cross-reference the new row below.
2. Add **`FM-PERSISTENCE-0NN` — a served blocking wake's write is in the cut**, forced by
   `crash_after_served_brpoplpush_recovers_element_in_destination` and
   `served_blmove_stages_destination_write_to_wal`. This row belongs in *persistence*, not blocking:
   its Observable is a property of restored storage, which is precisely what the persistence spec's
   "How to read a row" defines the Observable column to mean (`:23`). And the persistence spec has
   no `frogdb-core` carve-out, so it can cite these tests as written (see B8 below).

**Rows (a) and (b)** are connection-observable, not storage-observable, and go to blocking:

| Row | Spec | Trigger | Observable | Forced by |
|---|---|---|---|---|
| `FM-BLOCKING-006` | blocking (scope amended) | **Standalone**: blocked BLMOVE served by a push to src while another client WATCHes dest | EXEC aborts (nil) | server-side EXEC test + `served_blmove_bumps_destination_watch_version` (needs B8 amendment) |
| `FM-BLOCKING-007` | blocking (scope amended) | Served BLMOVE with a tracking client caching dest (default and BCAST) | Invalidation push for dest | `served_blmove_invalidates_destination_for_tracking_clients` (×2 modes), `served_blmove_invalidation_is_attributed_to_the_waking_connection` |
| `FM-PERSISTENCE-0NN` | **persistence (LOCKED)** | Served BLMOVE/BRPOPLPUSH, then a crash or a checkpoint cut before any later dest-declaring write | Element present in dest after recovery, absent from src; recovered primary agrees with its replica | `crash_after_served_brpoplpush_recovers_element_in_destination`, `served_blmove_stages_destination_write_to_wal`, `recovered_primary_does_not_diverge_from_replica_after_served_blmove` |

Each new row needs the full `REQUIRED_FIELDS` set (Trigger / Observable / NOT observable /
Invariant / Outcome variant / Forced by / Bug refs) or `just lint-failure-modes` rejects it. The
"NOT observable" column for 006 should record claim (d)'s refutation (no search-index effect is owed
because lists are not an `IndexKind`) and the cluster-mode scoping (a cross-slot BLMOVE is rejected
at `guards.rs:726-731`, so there is no cluster instance of this mode).

**B8 — `blocking-failure-modes.md` carves `frogdb-core` out of `Forced by`.** Its paragraph at
`:22-30` says test names resolve against `cargo nextest list -p frogdb-txn -p frogdb-vll -p
frogdb-server`, and that "that listing profile does **not** build `frogdb-core`,
`frogdb-shard-harness`, or anything behind `--features turmoil`/`--features shuttle`", so such tests
"are deliberately not cited in `Forced by` rows". 88's (a) forcing test lives in `frogdb-core`, so
this paragraph must be amended too.

The amendment is a **correction, not a widening**, and that is worth saying to whoever signs it off:

- `scripts/failure-modes.py:64-77` lists `frogdb-core` in `NEXTEST_CRATES`. The tool — which is the
  actual enforcer of both directions — has been resolving `frogdb-core` tests all along.
- 89 `// FM-…` tags already live under `frogdb-server/crates/core/` (`command_spec.rs:1609`,
  `:1621`, `:1632`, `:1654`, `:1667`, `:1679`, `:1689`, and others), cited by cluster and
  persistence rows.

So the fix is narrow: **strike the `frogdb-core` clause only.** The `frogdb-shard-harness` clause
and both feature-gated bullets (`--features shuttle` concurrency model, `--features turmoil`
whole-history checkers) stay — those crates/features genuinely are outside `NEXTEST_CRATES` and the
default feature resolution, and the two bullets remain accurate load-bearing-but-uncited guards.

Also amend `blocking-failure-modes.md`'s Scope (`:8-14`) to claim the shard-side serve path, so rows
006/007 are in-scope where they sit. It is unlocked (no gate ceremony), it already forward-declares
this exact gap, and `just lint-failure-modes` derives the area from the filename
(`scripts/failure-modes.py:161`), so rows become `FM-BLOCKING-006…` with no tooling change. Existing
rows stop at 005.

### The forward-declared shard-side spec — resolution of rev 1's open question

Rev 1 ended this section by asking whether `frogdb-core`'s shard-effect pipeline should become a
locked area. **Answer: no.** `frogdb-core` is far too large for a mutation gate to be meaningful or
affordable; a fifth locked area at crate granularity would be a gate nobody can pass and therefore a
gate nobody runs. Two real options remain, and both are about *spec* ownership, not gates:

**Option 1 — extend the persistence spec's Scope wording (cheap).** One sentence: extend `:10-15`
from `frogdb-core/src/shard/persistence.rs` to also name `shard/post_execution.rs`'s
`WalPersistence` effect and the serve path that must route through it. Cost: one LOCKED-spec edit,
one sign-off, no new file, unblocks 88 immediately. Leaves the txn-side forward-declaration
(`txn-failure-modes.md:10-14`, the WATCH version check) still dangling, so claim (a) still has no
owning row except the blocking spec's amended scope.

**Option 2 — write the forward-declared shard-side spec (honest).** Create
`.scratch/hardening/specs/shard-failure-modes.md` (area `SHARD`) covering the shard-side engine:
WATCH version check, the effect pipeline, the wait queue, rollback, replication framing. This
discharges **two** standing promises at once — `txn-failure-modes.md:10-14` and
`blocking-failure-modes.md:8-14` both say this spec exists and it does not — and it is where rows
006, 007 and the (c) row all naturally belong. Cost: a whole spec, not three rows; and it creates a
spec whose area no locked crate set owns. That last objection is weaker than rev 1 made it sound —
persistence is LOCKED and already reaches into `frogdb-core`, so area↔crate-set is already not 1:1.

**Recommendation: Option 1 for this proposal, Option 2 filed as the follow-up lane candidate.**
Rationale: Option 2 is the correct end state, but bundling a new failure-mode spec into 88 triples
its scope and puts the (c) fix — the one with acknowledged-write loss and replica divergence —
behind a documentation project. Option 1 is one sentence and one sign-off, and it does not
foreclose Option 2. If the orchestrator would rather not touch a LOCKED spec's Scope for a single
proposal, then Option 2 is the alternative and 88 waits on it; say which, because the answer gates
the code.

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

Note the asymmetry this creates with the spec plan above: `FM-PERSISTENCE-0NN` will be a row in a
gated spec whose forcing tests are *not* gate-covered. That is not a reason to put the row somewhere
else — the row belongs where its Observable belongs — but it must be stated in the row's Invariant
field so a future reader of the persistence mutation report does not mistake a passing gate for
coverage of this mode.

Consequently the safety story for this change rests entirely on the named forcing tests plus
`just lint-failure-modes`' bidirectional spec↔test check (which does cover `frogdb-core` — it is in
`NEXTEST_CRATES`, `scripts/failure-modes.py:64-77`). Reviewers should weight the crash-recovery and
replica-divergence tests in (c) accordingly; they are the only things standing between this fix and
a silent regression.

---

## Risks and scope boundaries

### Sibling 83 (`83-lazy-expiry-authority.md` **rev 2**, commit `d94dc24f`) — re-derived in rev 2

Rev 1 was written against 83 rev 1 (commit `05a7ecb5`) and is **superseded**. Rev 2 of 83 changes
three things that matter to 88, and the review is right that both proposals were designing the same
mechanism twice.

**1. The hook site rev 1 implied is gone.** 83 rev 2 §B.2 **deletes `drive_satisfaction`'s trailing
drain (`blocking.rs:255-258`) and collapses the wrapper into `drive_satisfaction_body`**. That
wrapper is the natural "once per whole wake chain, after the cascade has unwound" seam, and it is
the seam a `blocking.rs`-side effect hook would have wanted.

88 rev 2 does not need it. The hook is in `post_execution.rs`, immediately after the
`WaiterSatisfaction` arm (`:377-381`) — a site 83 rev 2 leaves untouched, and one that is strictly
better for 88's purposes for a reason 83 rev 2 supplies: 83's own call-graph analysis establishes
that `try_satisfy_{list,zset,stream}_waiters` have **exactly one non-test caller in the workspace**,
`post_execution.rs:721-723`. So in production the entire wake chain — cascade included — unwinds
before control returns to the `WaiterSatisfaction` arm. `pending_served_commands` is therefore
complete at exactly the point 88 reads it. 88's design depends on that fact; 83 rev 2 is where it is
proved. Cite it rather than re-deriving it.

**2. The `EffectScope` collision is dissolved, not resolved.** 83 rev 2 §B.3 explicitly names a
"**Textual conflict with proposal 88**, which also widens `EffectScope` (`:241`)". 88 rev 2 **drops
`EffectScope::ServedWake`** (§Vocabulary): the served effects run inside an existing
`run_write_effects` call under the outer command's own scope, so there is no discriminant to add.
83's widening — `InternalRemoval { propagation, version: VersionPolicy }`, pending its own
orchestrator ruling — now stands alone at `post_execution.rs:241`. **There is no longer a shared
`EffectScope` decision to coordinate.** This is the concrete deliverable of the re-derivation: one
of the two proposals stops widening the enum, and it should be 88, because 88 never dispatched on
its variant.

**3. Landing order: 83 first still stands, for a new reason.** Rev 1's reason was "88 assumes 83's
`ExpiryReport` routing is merged." The real reason after rev 2 is interleaving. Today the wrapper's
`apply_lazy_purge_effects()` runs *between* the cascade unwinding and the return to index 4, so 88's
`ApplyNow` pass would observe served commands recorded across a lazy purge whose own effects have
not yet been applied — an interleaving 88 would have to reason about and test. After 83 rev 2, purge
effects drain at the pipeline exit (§B.3, behind the `in_effect_run` barrier), strictly *after* 88's
pass. The interleaving disappears. **Land 83 first and 88 inherits a clean seam; land 88 first and
88 owns an ordering question that 83 is about to delete.**

**Residual composition hazard — new, and neither proposal covers it alone.** 83 rev 2's tail drain
is a fixpoint loop that calls `run_internal_removal_effects`, which includes effect index 4, which
can wake a `BLMOVE` — 83's own proof-obligation test constructs exactly this ("removing key A wakes
a `BLMOVE` whose destination key B is itself past its deadline"). A served `LMOVE` recorded during
that exit drain lands in `pending_served_commands` **after** 88's `ApplyNow` pass has already run
for the outer command. Its version bump, tracking invalidation and dirty delta would be dropped.

The fix is small but must be deliberate: `run_served_wake_effects` has to be a function the fixpoint
loop also calls each pass, not a statement inlined after the `WaiterSatisfaction` arm. Write it as a
method from the start. Composition test — `expiry_triggered_wake_of_blmove_stages_destination_write`
plus `expiry_triggered_wake_of_blmove_bumps_destination_watch_version` — **owned by whichever
proposal lands second.**

### Sibling 81 (`81-core-dead-seams.md`, commit `f73bdd8f`) — one-way edge, 81 H1 first

**Hard constraint, restated.** 81's H1 fixes a wait-queue unlink bug (`wait_queue.rs:457`, `:545`).
Until it lands, a wake can serve the **wrong client**. Applying 88's effect tracking on top of that
makes the situation *worse*, not better: today a mis-served wake produces a wrong reply and a
partially-tracked mutation; after 88 it would produce a wrong reply plus a fully-durable,
fully-replicated, fully-invalidated mutation attributed to the wrong waiter. **88 must not land
before 81 H1.**

**Scope exclusion, honored.** 81 ruled `drain_stream_waiters_*` (`wait_queue.rs:493-519`) mutate
nothing. 88 does not touch them. Confirmed independently: those functions dequeue and reply; they do
not call into the store.

### Sibling 84 (`84-blocking-op-dedupe.md`, commit `ddc4b184`) — boundary **CONFIRMED**

84's only edit inside a satisfaction arm is the XRead arm at `blocking.rs:1096-1125`. 84 claims it is
outside 88's write-effect scope by construction. **Verified and confirmed** — that arm constructs
its `Done` with `events: Vec::new()`, `restore: Restore::None`, `propagate: None`, and mutates
nothing in the store (`store.get(key)` + `read_after`, both read-only, `:1101-1106`). Under the
proposed design it records `ServedEffect::NoneBecause("XREAD is a pure read; XADD already
notified")`, and the served-wake pass over an empty command list is a no-op. No dispute.

**Correction to rev 1 — the `XReadGroup` aside was wrong.** Rev 1 asserted that the sibling
`XReadGroup` arm would "newly stage a WAL write for the consumer-group state change" under 88's
design, and treated the arm's `KNOWN GAP` comment as evidence of a durability hole. Both halves are
false, and the review is right to strike them:

- **Consumer-group state is already durable.** `read_group_entries` (`blocking.rs:1150`) advances
  last-delivered-id and the PEL *inside the stored `Value`* — groups live in the stream (the arm
  reaches them via `store.get(key).as_stream().get_group(group)`, `:1136-1140`). The stream key is
  declared by the waking `XADD`, so it is in `record.handler.keys(args)`, so `WalPersistence` at
  index 6 stages it — and `WalTarget::write_set` snapshots `store.get_hot(key)` **at persist time**
  (`persistence.rs:138-146`), i.e. *after* the index-4 wake mutated the groups. The advanced group
  state is in the snapshot. There is no WAL gap here at all. This is the same
  index-4-before-index-6 ordering that makes the source side of claim (c) correct — applied to a
  key that *is* declared, it produces the right answer.
- **The `KNOWN GAP` comment at `:1160-1172` is about replication, not WAL.** Its own words: "…
  advances consumer-group state … that is **NOT reproduced on the replica** — the waking XADD is
  broadcast but the group advancement is not … reproducing it means synthesizing an
  XREADGROUP/XCLAIM against the replica's group". That is a replication-framing gap, tracked as an
  issue-02 follow-up, and 88 neither fixes it nor worsens it.

What 88 *does* owe this arm is honesty about it. Under `ServedEffect`, the arm records
`NoneBecause("group state lives inside the declared stream key, so effects 0-3 and 6-8 already
cover it; replica reproduction is a separate documented gap")`. That is a better artifact than the
current bare `propagate: None`: it distinguishes "nothing owed" from "owed and deferred", and it
means a future reader does not have to re-derive the durability argument the way rev 1 failed to.
**Out of scope for 88 beyond that one string.** Do not fold the replication gap in.

### `FM-CLUSTER-038`

The only FM tag in `blocking.rs` is `FM-CLUSTER-038` at `:2065`, on
`slot_migrated_without_a_known_target_replies_clusterdown` — and it is in the **test module**, not
the main body. Its observable (blocked clients woken with MOVED/CLUSTERDOWN on slot migration, via
`drain_waiters_for_slot`) is unaffected: those wakes reply with an error and mutate nothing, so they
record `ServedEffect::NoneBecause(...)` and contribute no commands. Both 81 and 84 cite this tag;
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

- **Double-application on the source key.** After the change, source-key effects are applied twice —
  once by the outer command (indices 0–3, before the wake) and once by `ApplyNow` over the served
  command's keys. For `VersionIncrement` this is unobservable: `get_key_version` has exactly two
  readers, `check_watches` (`worker.rs:648-664`) and the test harness (`harness.rs:182`), and both
  only ever compare a stamp for *inequality* against a recorded value — a monotone counter bumped
  twice compares unequal exactly as one bumped once. For `TrackingInvalidation` it is a redundant
  push. For `DirtyCounter` it **double-counts**, which must be a decided answer with a test, not a
  side effect — Redis counts the push and the serve as two dirty events, so counting both is
  arguably right. The honest default is to match Redis (count both) and assert it.
- **Cascade depth.** Each cascade hop contributes its own served command. The depth cap
  (`blocking.rs:1615` `blmove_fanout_stops_at_depth_cap`) bounds the vector, so unbounded growth is
  not a risk, but `ServedWrites::dirty_delta` must accumulate across hops rather than overwrite, and
  the commands must stay in apply order (the existing `blmove_cascade_records_ordered_propagations`
  pin already covers ordering).
- **Registry resolution cost.** Every served wake now resolves a command name through the registry
  at index 4. This is a hash lookup on a hot-ish path. If it measures, cache the resolved handler on
  `SynthesizedCommand` at record time — but do not pre-optimize it into the design.
- **`Skip(WaiterSatisfaction)` is load-bearing.** If someone "fixes" that skip into an `ApplyNow`,
  the driver's own recursion and the pipeline's satisfaction step both fire and the depth cap no
  longer bounds anything. The reason string at the site is the defense; the structural test asserts
  the disposition.
- **`Skip(KeysizesFlush)` is load-bearing in the opposite direction.** Its legality depends on
  `hashmap.rs:1320-1329` queueing unconditionally and on `MUST_BE_ADJACENT(4,5)` holding. Both are
  asserted (`order_satisfies_all_declared_constraints`, plus the new
  `served_blmove_refreshes_destination_keysizes`), but the reason string must name both or the next
  reader repeats rev 1's error in the other direction and "fixes" a working effect.

---

## Effort

**L** — raised from rev 1's "M/L". The rev-2 mechanism is smaller in the pipeline but larger in the
spec and test surface, and the LOCKED-spec dependency is a real gate.

- `ServedEffect` + `ServedWrites` + `run_served_wake_effects` + re-pointing the `pending_served_commands`
  drain from index 8 to index 4 and threading it into arms 6/7/8: **M**. Smaller than rev 1's design
  (no `EffectScope` variant, no union into a borrowed slice) but it touches every satisfaction arm's
  `Done` construction.
- Deleting the three hand effects and re-pointing the existing blocking tests: **S**.
- Spec work: two blocking rows + blocking Scope amendment + the **B8 carve-out amendment** + the
  LOCKED `FM-PERSISTENCE-019` Invariant amendment + a **new LOCKED persistence row** + the Option-1
  Scope extension: **M**, and **gated on orchestrator sign-off** for three separate LOCKED-spec
  edits. This is the item that moved the estimate.
- The forcing tests: **M/L**. (c) needs a crash-recovery test *and* a primary/replica divergence
  test; crash-window and replication tests in this repo are the expensive part.
- Merge sequencing behind 83 rev 2 and 81 H1, plus the composition test if 88 lands second:
  schedule risk plus **S**.

---

## Hotfix candidates

**Claimed (LIVE, safe to land ahead of the full change):**

- **H1 — dest version bump. ENDORSED.** In the `Done` arm, bump the version for every key the
  satisfaction wrote, not just the source. Concretely: have the BLMove arm report dest, and bump
  both. Fixes claim (a). The double-bump on the source is **unobservable**: `get_key_version` has
  exactly two readers — `check_watches` (`worker.rs:648-664`) and the test harness
  (`harness.rs:182`) — and both only compare a recorded stamp for inequality, so a counter bumped
  twice is indistinguishable from one bumped once. One-directional risk beyond that: an extra bump
  can only cause a *spurious abort*, never a missed one. Needs `FM-BLOCKING-006` and its forcing
  test; do not land bare. Scope the row to standalone (§(a)).

**Claimed with a warning (LIVE, but the fix must be complete):**

- **H2 — dest tracking invalidation. ENDORSED, warning intact.** Fixes claim (b). Safe in isolation
  *only if* both tracking modes are handled — invalidating for default-mode clients while leaving
  BCAST subscribers stale is worse than the status quo, because it makes the gap intermittent and
  mode-dependent, which is exactly the shape of bug that survives a decade. **The hand-rolled trap
  is real and is one line away**: `types.rs:501` `invalidate_keys` is the key-based-only path and
  `:509` `invalidate_keys_all_modes` is the correct one, and they differ by a suffix. Land via
  `invalidate_keys_all_modes`, never via `invalidate_keys`. Needs `FM-BLOCKING-007`.

**NOT a hotfix — do not land alone:**

- **A3 — dest WAL staging. CONFIRMED anti-hotfix, and the structural reason is decisive.** Claim
  (c) is the highest-severity finding and the *most* dangerous to patch narrowly. The blocking-serve
  path is **synchronous recursion** (`drive_satisfaction_body` calls itself at `blocking.rs:374`,
  depth-capped at `:277`); the persist call is `.await` (`post_execution.rs:398-403`). A narrow
  patch that appends a dest `WalAction` at the serve site **cannot answer where the await point
  goes**: making the serve site async forces `Box::pin` on the cascade and infects
  `satisfy_waiters` / `satisfy_waiters_for_command` / every `try_satisfy_*` with `async` — the exact
  cost 83 rev 2 spent its §B rejecting. Deferring instead means inventing a second deferral vector,
  at which point the patch has reimplemented `pending_served_commands` badly.

  On top of that, a serve-site WAL write happens **inside effect index 4**, out of order with
  respect to `WalPersistence` at index 6 and outside the ordering argument `FM-PERSISTENCE-019`
  relies on — in a **LOCKED area (0.85 gate)**, with no spec row and no crash test. It would very
  plausibly trade a known loss for an unknown ordering corruption. **Fix (c) only via the
  `IncludeServed(WalPersistence)` route, with the new persistence row and both (c) tests landed
  together.** Same shape as 83's H3 anti-hotfix; same treatment.

**LATENT — no hotfix, ride along:**

- **(e) dirty counter.** Free once the served-command channel exists; not worth an independent
  patch, and an independent patch would have to answer the double-counting question (see Residual
  risks) with no surrounding structure to answer it in.

**REFUTED — nothing to fix:**

- **(d) search reindex.** No hotfix, no row, no test. Recorded in `FM-BLOCKING-006`'s "NOT
  observable" column so the refutation is durable and a future reader does not re-litigate it.

**Security findings**: none identified in this lane. Per standing policy, any security finding would
be classification-only — filed and parked, never turned into a fix proposal.
