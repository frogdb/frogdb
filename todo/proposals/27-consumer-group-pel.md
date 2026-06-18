# Proposal: Consumer-Group PEL Claim

Status: proposed
Date: 2026-06-17

## Problem

A stream consumer group owns one non-trivial invariant: every entry in its **PEL** (Pending Entries
List) is attributed to exactly one consumer, and each consumer's `pending_count` equals the number
of PEL entries it owns. Stated as one line:

> `sum(consumer.pending_count for consumer in consumers) == pending.len()`, and each `pending`
> entry's `consumer` field names a `consumers` key whose count includes it.

That invariant is maintained by **reassignment logic that is duplicated, byte-near-identically,
across `XCLAIM` and `XAUTOCLAIM`**, and both copies live in the *command* layer, reaching directly
into `ConsumerGroup`'s `pub pending` and `pub consumers` maps. The type that owns the invariant
(`ConsumerGroup`) does not own the operation that preserves it. The invariant is therefore an
emergent property of two distant call sites agreeing on a five-step dance — decrement the old
consumer, insert/fetch the entry, reassign its `consumer`, adjust `delivery_count`/`delivery_time`,
increment the new consumer and `touch()`. Miss one step in one copy and the count silently rots; no
type, test, or compiler will object.

This is the project's encapsulation principle inverted: behaviour that belongs **on the type** is
spread across callers that reach into the type's internals. The interface a `XCLAIM`/`XAUTOCLAIM`
author must understand (which two maps, in which order, with which saturating arithmetic) is as large
as the implementation it is supposed to hide — the signature of a shallow seam.

**Deletion test.** Delete the reassignment block from one command and paste nothing in its place:
the command still compiles and still returns the claimed entries (the response is built in a separate
read-only pass), but it stops decrementing the old consumer / incrementing the new one. The PEL
counts drift, `XINFO GROUPS`/`XINFO CONSUMERS` report wrong `pending`, and the corruption surfaces
arbitrarily later. The fact that a copy can be deleted *and the command still mostly works* is the
proof the invariant is not co-located with the data it governs.

There is a second, sharper signal: the two copies are **not** actually identical, and one of the
divergences is a live count-corruption bug in `XAUTOCLAIM`'s "entry was deleted from the stream"
path (see Correctness flags). Two copies that look the same but quietly differ are exactly what a
single owning method would have made impossible.

This proposal owns the **PEL-claim contract**: a single `ConsumerGroup::claim_pending` method (plus a
`ClaimOpts` value capturing the `IDLE`/`TIME`/`RETRYCOUNT`/`JUSTID` rules in one place) that both
commands call, so the count invariant has exactly one home.

## Current state

`frogdb_core` re-exports `frogdb_types::*` (`core/src/lib.rs:7`), so the command layer's
`frogdb_core::{ConsumerGroup, PendingEntry, Consumer}` are the types defined in
`types/src/types/stream.rs`. Every field touched below is `pub`.

### The type that owns the invariant (`types/src/types/stream.rs:276-364`)

```rust
// stream.rs:276 — the PEL entry
pub struct PendingEntry {
    pub consumer: Bytes,          // which consumer owns it
    pub delivery_time: Instant,   // monotonic; idle_ms() = elapsed
    pub delivery_count: u32,      // starts at 1 (see PendingEntry::new, :287-293)
}

// stream.rs:303 — the consumer, holding the count this proposal protects
pub struct Consumer {
    pub name: Bytes,
    pub pending_count: usize,     // MUST equal #PEL entries with consumer == name
    pub last_seen: Instant,       // touch() updates it (:341)
    pub active_time: Option<Instant>,
}

// stream.rs:353 — the group; both maps are pub and reached into from commands
pub struct ConsumerGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub pending: BTreeMap<StreamId, PendingEntry>,   // :359
    pub consumers: BTreeMap<Bytes, Consumer>,        // :361
    pub entries_read: Option<u64>,
}
```

The group already has *some* count-correct PEL methods — `add_pending` (`stream.rs:419-427`,
increments + `touch()`), `ack` (`stream.rs:432-444`, removes + `saturating_sub`), and
`remove_all_pel_refs` (`stream.rs:~890`, removes + `saturating_sub`). The reassignment ("claim")
operation is the one PEL mutation that was *not* given a method, so it leaked into the commands.

### XCLAIM — the claim block (`commands/src/stream/pending.rs:267-300`)

```rust
for id in &ids_to_claim {
    // Remove from old consumer's count
    if let Some(pe) = group.pending.get(id) {
        let old_consumer_name = pe.consumer.clone();
        if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
            old_consumer.pending_count = old_consumer.pending_count.saturating_sub(1);
        }
    }
    // Update or insert pending entry  (entry().or_insert_with → supports FORCE-creating a PEL entry)
    let pe = group
        .pending
        .entry(*id)
        .or_insert_with(|| frogdb_core::PendingEntry::new(consumer_name.clone()));
    pe.consumer = consumer_name.clone();
    if let Some(idle_ms) = idle {
        pe.delivery_time =
            std::time::Instant::now() - std::time::Duration::from_millis(idle_ms);
    }
    if let Some(_time_ms) = time {
        pe.delivery_time = std::time::Instant::now();   // NB: _time_ms is parsed then discarded
    }
    if let Some(rc) = retrycount {
        pe.delivery_count = rc;
    } else if !justid {
        pe.delivery_count += 1;
    }
    // Update new consumer's count
    if let Some(new_consumer) = group.consumers.get_mut(&consumer_name) {
        new_consumer.pending_count += 1;
        new_consumer.touch();
    }
}
```

XCLAIM parses the full option set — `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID`
(`pending.rs:166-225`).

### XAUTOCLAIM — the same block (`commands/src/stream/pending.rs:459-481`)

```rust
for id in &to_claim {
    // Remove from old consumer's count
    if let Some(pe) = group.pending.get(id) {
        let old_consumer_name = pe.consumer.clone();
        if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
            old_consumer.pending_count = old_consumer.pending_count.saturating_sub(1);
        }
    }
    // Update pending entry  (get_mut → only updates an entry that already exists)
    if let Some(pe) = group.pending.get_mut(id) {
        pe.consumer = consumer_name.clone();
        if !justid {
            pe.delivery_count += 1;
        }
        pe.delivery_time = std::time::Instant::now();
    }
    // Update new consumer's count
    if let Some(new_consumer) = group.consumers.get_mut(&consumer_name) {
        new_consumer.pending_count += 1;
        new_consumer.touch();
    }
}
// Remove deleted entries from PEL
for id in &deleted_ids {
    group.pending.remove(id);          // :486 — bare remove, NO count decrement (bug, see flags)
}
```

The old-consumer decrement (`:268-274` ≡ `:460-466`) and the new-consumer increment (`:295-299` ≡
`:477-481`) are byte-for-byte identical between the two commands. The middle — entry insert/update
and `delivery_*` adjustment — is the **same operation expressed two ways**, and that is where the
copies quietly diverge:

| Aspect | XCLAIM (`:277-293`) | XAUTOCLAIM (`:469-475`) |
|---|---|---|
| Entry lookup | `pending.entry(id).or_insert_with(PendingEntry::new)` — **creates** if absent (FORCE) | `pending.get_mut(id)` — **update only**; entries always pre-exist (came from `pending.range`) |
| `delivery_time` | `IDLE` → `now - idle_ms`; `TIME` → `now`; else **unchanged** | always `now` |
| `delivery_count` | `RETRYCOUNT` → `rc`; else if `!JUSTID` → `+= 1` | if `!JUSTID` → `+= 1` |
| Options accepted | `IDLE`/`TIME`/`RETRYCOUNT`/`JUSTID` | `JUSTID` only (`pending.rs:368-389`) |
| Deleted-from-stream entries | n/a (claims explicit IDs) | reassigns **then** bare-`remove` → **count corruption** (`:459-486`) |

XAUTOCLAIM is the strict-subset case of XCLAIM: no `IDLE`/`TIME`/`RETRYCOUNT`, `delivery_time`
always advances to `now`. A single method parameterised by `ClaimOpts` covers both — XAUTOCLAIM just
passes `ClaimOpts { idle: None, time: Some(_now-marker), retrycount: None, justid }`, or more cleanly
a `ClaimOpts::touch_now(justid)` constructor that sets `delivery_time = now`.

## Proposed design

Move the claim into a method on the type that owns the maps. The command states *intent* ("claim
this id for this consumer with these options"); the group keeps its own counts correct.

### The seam — `ConsumerGroup::claim_pending` + `ClaimOpts`

```rust
/// How a claim adjusts a pending entry's delivery bookkeeping.
/// One place for the IDLE / TIME / RETRYCOUNT / JUSTID rules that today live
/// inline in two commands.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClaimOpts {
    /// XCLAIM IDLE <ms>: set delivery_time so idle_ms() == this.
    pub idle: Option<u64>,
    /// XCLAIM TIME <unix-ms>: set the last-delivery time. See risks — with a
    /// monotonic Instant clock this degrades to "now" (matching today's code).
    pub time: Option<u64>,
    /// XCLAIM RETRYCOUNT <n>: force delivery_count to this value.
    pub retrycount: Option<u32>,
    /// JUSTID: do NOT bump delivery_count.
    pub justid: bool,
}

impl ClaimOpts {
    /// XAUTOCLAIM and the default XCLAIM behaviour: stamp delivery_time = now.
    pub fn touch_now(justid: bool) -> Self {
        Self { idle: None, time: Some(0 /* sentinel: now */), retrycount: None, justid }
    }

    /// The delivery_time / delivery_count rules, captured ONCE.
    fn apply(&self, pe: &mut PendingEntry) {
        if let Some(idle_ms) = self.idle {
            pe.delivery_time = Instant::now() - Duration::from_millis(idle_ms);
        } else if self.time.is_some() {
            // TIME currently maps to "now" (Instant is monotonic, not wall-clock).
            pe.delivery_time = Instant::now();
        }
        if let Some(rc) = self.retrycount {
            pe.delivery_count = rc;
        } else if !self.justid {
            pe.delivery_count = pe.delivery_count.saturating_add(1);
        }
    }
}

impl ConsumerGroup {
    /// Reassign (or, for FORCE, create) a pending entry so `consumer` owns it,
    /// keeping per-consumer `pending_count` correct. This is the ONLY place the
    /// claim half of the PEL invariant lives.
    pub fn claim_pending(&mut self, id: StreamId, consumer: &Bytes, opts: ClaimOpts) {
        if let Some(pe) = self.pending.get(&id) {
            let old = pe.consumer.clone();
            if let Some(c) = self.consumers.get_mut(&old) {
                c.pending_count = c.pending_count.saturating_sub(1);
            }
        }
        let pe = self
            .pending
            .entry(id)
            .or_insert_with(|| PendingEntry::new(consumer.clone()));
        pe.consumer = consumer.clone();
        opts.apply(pe);
        let c = self
            .consumers
            .entry(consumer.clone())
            .or_insert_with(|| Consumer::new(consumer.clone())); // NB: not or_default — Consumer has no Default
        c.pending_count += 1;
        c.touch();
    }

    /// XAUTOCLAIM only: a PEL entry whose underlying stream message is gone.
    /// Evict it count-correctly (decrement its current owner) WITHOUT claiming
    /// it — replaces XAUTOCLAIM's bare `pending.remove` (pending.rs:486).
    /// Mirrors the existing remove_all_pel_refs / ack pattern.
    pub fn drop_missing_pending(&mut self, id: &StreamId) {
        if let Some(pe) = self.pending.remove(id)
            && let Some(c) = self.consumers.get_mut(&pe.consumer)
        {
            c.pending_count = c.pending_count.saturating_sub(1);
        }
    }
}
```

### Before / after — XCLAIM

Before (`pending.rs:267-300`): 34 lines reaching into both maps. After:

```rust
group.get_or_create_consumer(consumer_name.clone());
let opts = ClaimOpts { idle, time, retrycount, justid };
for id in &ids_to_claim {
    group.claim_pending(*id, &consumer_name, opts);
}
```

### Before / after — XAUTOCLAIM

Before (`pending.rs:459-487`): the reassignment loop **plus** the bare-remove deleted-entry loop —
the two together being where the count silently corrupts. After, the two cases are explicit and each
goes through a count-correct method:

```rust
group.get_or_create_consumer(consumer_name.clone());
let opts = ClaimOpts::touch_now(justid);
for id in &to_claim {
    if existing_ids.contains(id) {
        group.claim_pending(*id, &consumer_name, opts);  // live entry: reassign
    } else {
        group.drop_missing_pending(id);                  // gone from stream: evict, don't claim
    }
}
```

This also fixes the deleted-entry bug as a side effect of routing every PEL mutation through a method
(the response loop already filters `deleted_ids` out, so it is unaffected — `pending.rs:490-508`).

### Why this is the right depth

- **Encapsulation (behaviour on the type).** `claim_pending` lives next to `add_pending`/`ack`/
  `delete_consumer`, which already own their slice of the same invariant. The command stops naming
  `group.pending`/`group.consumers` at all — it speaks claims, not map mutations.
- **Locality.** "Old consumer −1, new consumer +1, exactly one owner per entry" stops being a
  property maintained by two distant copies and becomes a property of one function. The `IDLE`/
  `TIME`/`RETRYCOUNT`/`JUSTID` rules — today duplicated and *already divergent* — live once in
  `ClaimOpts::apply`.
- **Testability.** `claim_pending` is unit-testable on a bare `ConsumerGroup` — construct a group,
  add pending entries, claim, assert the count invariant — with no `CommandContext`, store, dispatch,
  or socket. The window of "which command did I forget to update" closes: there is one place.
- **Net deletion.** The migration removes ~50 duplicated lines across the two commands and the
  latent deleted-entry bug, and adds one small, owned method. If the new method could not delete the
  copies, its shape would be wrong.
- **Not a new layer.** This deepens an existing seam — `ConsumerGroup` already half-owns the PEL
  via `add_pending`/`ack`. `claim_pending` is the missing third mutation, not a wrapper callers may
  bypass; after migration the raw `group.pending.entry(...)` / `group.consumers.get_mut(...)` claim
  reach-ins are gone.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Behaviour-preserving **except** the
XAUTOCLAIM deleted-entry count fix in Phase 2, which is the point.

1. **Phase 0 — add the method, no callers.** Add `ClaimOpts`, `ClaimOpts::apply`/`touch_now`,
   `ConsumerGroup::claim_pending`, and `drop_missing_pending` to `types/src/types/stream.rs`. Unit-
   test them directly against a hand-built `ConsumerGroup` (claim across consumers, FORCE-create,
   JUSTID, RETRYCOUNT, idle, self-reclaim, evict-missing). `just check frogdb-types`.
2. **Phase 1 — migrate XCLAIM.** Replace `pending.rs:267-300` with the `claim_pending` loop. Pure
   behaviour-preserving: same option semantics, same FORCE-create via `entry().or_insert_with`.
   `just test frogdb-server xclaim`.
3. **Phase 2 — migrate XAUTOCLAIM + fix the deleted-entry bug.** Replace `pending.rs:459-487` with
   the `existing_ids` branch calling `claim_pending` / `drop_missing_pending`. This deletes the bare-
   `remove` loop and stops reassigning soon-to-be-deleted entries to the new consumer. Add the
   regression test below; it fails before this phase. `just test frogdb-server xautoclaim`.
4. **Phase 3 — lint gate.** Add a grep gate to `just lint`: outside `types/src/types/stream.rs`, no
   `\.pending\.entry\(` and no `\.consumers\.get_mut\(` in the stream command crate — PEL claims must
   go through the method. (The remaining read-only `group.pending.get(id).idle_ms()` filter in the
   first pass, `pending.rs:243`/`:405`, stays; it is a read, not a count mutation — optionally
   promote it to a `group.pending_idle_ms(id)` helper in a follow-up.)

## Testing impact

- **`claim_pending` unit tests (new, fast).** On a bare `ConsumerGroup`: (a) claim an entry from
  consumer A to B → A.count−1, B.count+1, `pe.consumer == B`; (b) self-reclaim A→A → count net 0;
  (c) FORCE-create (id absent) → new entry, `delivery_count == 1`, only the new consumer counted;
  (d) JUSTID → `delivery_count` unchanged; (e) RETRYCOUNT → exact value; (f) IDLE → `idle_ms()`
  reflects it; after every case assert the invariant
  `consumers.values().map(pending_count).sum() == pending.len()`.
- **PEL count-invariant property test (fails today).** Drive a random sequence of XADD /
  XREADGROUP / XCLAIM / XAUTOCLAIM / XACK against a group, then assert the sum invariant holds. With
  XAUTOCLAIM over entries that were XDEL'd between read and claim, this fails pre-Phase-2 (the new
  consumer over-counts by the number of deleted entries) and holds after.
- **XAUTOCLAIM deleted-entry regression (the bug pin).** XADD an entry, XREADGROUP it (PEL entry for
  consumer A), XDEL the underlying entry, then XAUTOCLAIM to consumer B with `min_idle 0`. Assert
  B.`pending_count == 0` and the id appears in the deleted-ids return array, not the claimed array.
  Pre-fix, B.`pending_count == 1` and the PEL/`XINFO CONSUMERS` counts are wrong.
- **No regression in existing coverage.** `redis-regression/tests/stream_cgroups_tcl.rs` and
  `stream_cgroups_regression.rs` exercise XCLAIM/XAUTOCLAIM end-to-end and must stay green through
  Phases 1-2 (behaviour-preserving for the non-deleted path).

## Risks / open questions

- **`TIME <unix-ms>` is lossy today and stays lossy.** Both the current code (`pending.rs:286-288`,
  `_time_ms` parsed then discarded) and `ClaimOpts::apply` map `TIME` to `Instant::now()` because
  `PendingEntry.delivery_time` is a monotonic `Instant`, which cannot represent an absolute wall-clock
  time. This proposal preserves the existing (imperfect) behaviour rather than changing it; making
  `TIME` faithful requires switching `delivery_time` to a wall-clock representation and is out of
  scope. Flagged so the degradation is documented in one place instead of buried in two.
- **`ClaimOpts::touch_now` sentinel.** Encoding "set delivery_time = now" as `time: Some(0)` is
  ugly. Cleaner alternatives: a dedicated `enum DeliveryTime { Keep, Now, IdleMs(u64) }` field, or
  have XAUTOCLAIM pass `ClaimOpts { idle: Some(0), .. }` (idle 0 ⇒ `now - 0ms == now`). The latter is
  exactly equivalent and avoids the sentinel; pick one in Phase 0.
- **Return value.** `claim_pending` currently returns `()`. If a future caller needs the resulting
  `delivery_count` (e.g. to build the response without the separate read pass), return the new count
  or `&PendingEntry`. Kept void here to minimise the seam.
- **Does the method belong on `ConsumerGroup` or `Stream`?** XAUTOCLAIM's deleted-entry decision
  needs to know whether the id still exists in the stream — that check (`stream.contains(id)`,
  `pending.rs:428-437`) stays in the command/`Stream` because `ConsumerGroup` does not hold the
  entries. The group method only handles "given the existence decision, mutate the PEL correctly."
  Keeping the existence check out of `ConsumerGroup` preserves its single responsibility.

## Correctness flags

1. **Shallow duplication of the PEL-count invariant — CONFIRMED.** The claim/reassign logic is
   duplicated across XCLAIM (`pending.rs:267-300`) and XAUTOCLAIM (`pending.rs:459-481`), with the
   old-consumer decrement (`:268-274` ≡ `:460-466`) and new-consumer increment (`:295-299` ≡
   `:477-481`) byte-for-byte identical, and both reach into `ConsumerGroup`'s `pub pending`/`pub
   consumers` (`stream.rs:359,361`) from the command layer. The sum invariant is maintained by two
   distant call sites instead of one method. This is the structural flag the proposal exists to fix.

2. **XAUTOCLAIM corrupts `pending_count` for stream-deleted entries — CONFIRMED (real runtime bug).**
   `to_claim` (built from `group.pending.range`, `pending.rs:404-414`) includes PEL entries whose
   underlying stream message was deleted. The mutation loop (`:459-481`) processes **all** of
   `to_claim`, including the deleted ones: it decrements the old consumer, reassigns
   `pe.consumer = new`, bumps `delivery_count`, and **increments the new consumer's `pending_count`**.
   Only afterwards does `:484-487` remove the deleted ids via bare `group.pending.remove(id)` — which,
   unlike `ack` (`stream.rs:432-444`) and `remove_all_pel_refs` (`stream.rs:~890`), does **not**
   decrement the owning consumer. Net per deleted entry: old consumer −1, new consumer +1, then
   `pending.len()` −1 with no matching count decrement ⇒ the new consumer's `pending_count` ends up
   one **higher** than the entries it actually owns, breaking
   `sum(pending_count) == pending.len()`. Correct Redis behaviour is to evict such entries (return
   them in the deleted-ids array) without claiming them. Fix: route them through
   `drop_missing_pending` (Phase 2), i.e. never call `claim_pending` for an id not in the stream.

3. **`XCLAIM TIME` argument is parsed then discarded — CONFIRMED (pre-existing fidelity gap, not a
   count bug).** `pending.rs:286-288` binds `_time_ms` and sets `delivery_time = Instant::now()`,
   ignoring the supplied unix timestamp. Root cause is the monotonic `Instant` clock (see risks). The
   proposal preserves this behaviour in one place (`ClaimOpts::apply`) and documents it, rather than
   silently carrying it in two.

4. **`or_default()` in the sketch would not compile — NOTED.** `Consumer` has no `Default` impl (its
   `new` requires a `name`, `stream.rs:315-324`); the unified method must use
   `consumers.entry(consumer.clone()).or_insert_with(|| Consumer::new(consumer.clone()))`. Today the
   commands avoid this by calling `get_or_create_consumer` (`stream.rs:379-383`) before the loop and
   then using `get_mut`; folding consumer creation into `claim_pending` removes that ordering
   dependency, but the call site may keep `get_or_create_consumer` for the "claim zero entries but the
   consumer should still exist" case (Redis creates the target consumer even when nothing is claimed).

5. **No other PEL-count mismatch found.** The old-consumer decrement, new-consumer increment, and
   `touch()` are present and correct in both copies for the non-deleted path; `add_pending`
   (`stream.rs:419-427`), `ack` (`stream.rs:432-444`), and `remove_all_pel_refs` (`stream.rs:~890`)
   are all count-correct. The only count defect is flag 2; everything else here is the duplication
   risk of flag 1.
