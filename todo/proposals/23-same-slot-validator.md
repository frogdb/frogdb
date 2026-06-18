# Proposal: Same-Slot (CROSSSLOT) Validator

Status: proposed
Date: 2026-06-17

## Problem

"Do all the keys in this request live together?" is one rule, but FrogDB answers it with a
hand-rolled key loop re-typed at every command site, and stamps the rejection with a CROSSSLOT
string literal copy-pasted next to each loop. The rule is not owned by a module; it is owned by
*convention* — every new multi-key command author is expected to remember to write the loop and to
spell the error byte-for-byte the same way. That is a shallow seam: the interface a caller must
reconstruct (compute each key's home, compare, and emit the exact wire string) is as large as the
one-line helper that should hide it.

Worse, the rule has **two** notions that the call sites silently pick between, and the choice is
implicit in which helper a given site happens to call:

| Notion | Helper | Definition | Used by |
|---|---|---|---|
| Internal-shard sameness | `shard_for_key(key, n)` | `CRC16_XMODEM(hashtag(key)) % 16384 % n` (`shard/helpers.rs:55-59`) | non-cluster multi-shard routing, scripts, transactions |
| Cluster hash-slot sameness | `slot_for_key(key)` | `CRC16_XMODEM(hashtag(key)) % 16384` (`shard/helpers.rs:62-65`) | cluster-mode validation (`guards.rs`) |

The two are not independent: because `shard_for_key(k, n) == slot_for_key(k) % n`, **same-slot
strictly implies same-shard** — the cluster check is the stronger of the two. A reader cannot tell
which guarantee a given site enforces without tracing whether it called `shard_for_key` or
`slot_for_key`; the type system says nothing. (The doc-comment claim that shards use `xxhash64` is
folklore — both notions derive from the *same* CRC16 hash; the shard is just the slot taken
`mod num_shards`.)

The literal itself has **three** independent source-of-truth homes and **eight** additional inline
copies:

- `frogdb-types/src/error.rs:139` — `CommandError::CrossSlot => "CROSSSLOT Keys in request don't hash to the same slot"`
- `frogdb-core/src/scripting/error.rs:26` — `#[error("CROSSSLOT Keys in request don't hash to the same slot")]`
- `frogdb-server/src/slot_migration/redirect.rs:31` — `redirect::crossslot()` (proposal 17)

Proposal 17 already created `redirect::crossslot()` to be the single owner of this wire string, and
already migrated one site (`guards.rs:232`) onto it — but it stopped there. The string is still
inlined at eight call sites, and 17 never touched the *logic* (the key loop) at all. This proposal
finishes what 17 started for the literal and does for the **check** what 17 did for MOVED/ASK: gives
it one owner.

**Deletion test.** If the same-slot rule were already a deep module, deleting that module would
delete: the eight inline `Response::error("CROSSSLOT …")` strings, six hand-rolled key-comparison
loops, the three dead same-shard helpers in `server/src/routing.rs` (zero callers, below), and the
fragmented `note_cluster_slot` / `add_transaction_shard` / WATCH-loop trio that tracks the
transaction target three different ways. Today none of those can be deleted, because the rule lives
in the callers, not in a module. That is the signal the seam is in the wrong place.

This proposal owns the **same-locality contract**: one `SlotValidator` primitive (over both shards
and slots), the CROSSSLOT literal routed through proposal-17's `redirect::crossslot()` everywhere,
and the transaction target accumulated *through* the validator instead of via three ad-hoc spots. It
also folds in a 🔴 correctness flag in the same family — `core/src/shard/blocking.rs` formats a MOVED
redirect inline, bypassing the proposal-17 redirect seam and breaking IPv6.

## Current state

### Routing — three CROSSSLOT exits, one inline shard loop (`connection/routing.rs:92-164`)

```rust
// Multi-key command: check if all keys are on the same shard
let first_shard = shard_for_key(keys[0], self.num_shards);
let all_same_shard = keys[1..]
    .iter()
    .all(|key| shard_for_key(key, self.num_shards) == first_shard);

if all_same_shard {
    return self.execute_on_shard(first_shard, Arc::clone(cmd)).await;
}

// Keys span multiple shards
if handler.requires_same_slot() {
    return Response::error("CROSSSLOT Keys in request don't hash to the same slot"); // :106
}
if !self.allow_cross_slot {
    return Response::error("CROSSSLOT Keys in request don't hash to the same slot"); // :111
}
// … scatter dispatch …
None => {
    // Command doesn't support scatter-gather
    Response::error("CROSSSLOT Keys in request don't hash to the same slot")          // :162
}
```

The loop (`:93-96`) computes the shard sameness; three separate exits (`:106`, `:111`, `:162`) each
re-spell the literal. (Note: the COPY two-phase path at `:201-202` computes `source_shard`/
`dest_shard` *to allow* cross-shard work — it is deliberately **not** a CROSSSLOT guard and must stay
outside the validator.)

### Cluster slot validation — the same loop over `slot_for_key` (`connection/guards.rs:225-234`)

```rust
let first_slot = slot_for_key(keys[0]);

// CROSSSLOT check - all keys must hash to same slot
for key in &keys[1..] {
    let slot = slot_for_key(key);
    if slot != first_slot {
        return Some(redirect::crossslot());   // :232 — already uses the proposal-17 owner
    }
}
```

This is the *only* site that uses the stronger CRC16-slot notion, and the *only* site already routed
through `redirect::crossslot()`. The loop, however, is still hand-rolled — identical in shape to
routing's, differing only in `slot_for_key` vs `shard_for_key`. It is reached from
`dispatch.rs:590-593` before any shard routing.

### Scripts — `EVAL`/`EVALSHA` and `FCALL` each re-roll it (`scripting/eval.rs`, `scripting/function.rs`)

`classify_script_shards` (`eval.rs:67-85`) sorts+dedups shards and emits `CrossSlotForbidden`, which
both EVAL (`eval.rs:61-63`) and EVALSHA (`eval.rs:213-215`) turn into the inline literal:

```rust
ScriptShards::CrossSlotForbidden => {
    Response::error("CROSSSLOT Keys in request don't hash to the same slot")  // eval.rs:62, :214
}
```

`FCALL` open-codes the loop *and* the literal directly (`function.rs:43-56`):

```rust
let first_shard = shard_for_key(&keys[0], self.num_shards);
for key in &keys[1..] {
    if shard_for_key(key, self.num_shards) != first_shard {
        return Response::error(
            "CROSSSLOT Keys in request don't hash to the same slot",          // function.rs:51
        );
    }
}
```

Both use the shard notion (the cluster CRC16-slot check already ran in `guards.rs` upstream).

### Transactions — the target is tracked in *three* fragmented spots

EXEC consumes a `TransactionTarget` and re-spells the literal when it is `Multi`
(`transaction.rs:156-166`):

```rust
TransactionTarget::Multi(_) => {
    record_transaction_metrics(/* "crossslot" */);
    return vec![Response::error(
        "CROSSSLOT Keys in request don't hash to the same slot",              // transaction.rs:164
    )];
}
```

The target is *built* during queuing by a loop that drives two different state mutators depending on
mode (`transaction.rs:479-493`):

```rust
let is_cluster = self.cluster.cluster_state.is_some();
for key in &keys {
    let shard = shard_for_key(key, self.num_shards);
    if is_cluster {
        let slot = slot_for_key(key);
        if self.state.note_cluster_slot(slot, shard) {   // spot 1 — cluster slot path
            continue;
        }
    }
    self.state.add_transaction_shard(shard);             // spot 2 — shard fold
}
```

`note_cluster_slot` (`state.rs:692-714`) remembers the first slot and rewrites `target` to `Multi`
on a slot mismatch; `add_transaction_shard` (`state.rs:673-686`) folds shards `None → Single →
Multi`. They are two near-identical `Multi`-promotion state machines, gated on the
`cluster_first_slot` field (`state.rs:48-51`). Then WATCH re-rolls *its own* third loop, ignoring
both (`transaction.rs:371-384`):

```rust
let mut target_shard: Option<usize> = None;
for key in args {
    let shard = shard_for_key(key, self.num_shards);
    match target_shard {
        None => target_shard = Some(shard),
        Some(s) if s != shard => {
            return Response::error(
                "CROSSSLOT Keys in request don't hash to the same slot",      // transaction.rs:379
            );
        }
        _ => {}
    }
}
```

So the transaction "are these keys co-located?" question is answered by three code paths
(`note_cluster_slot`, `add_transaction_shard`, the WATCH loop) that happen to agree today but have no
shared owner enforcing that they must.

### Dead prior art — three same-shard helpers nobody calls (`server/src/routing.rs:16-82`)

```rust
pub fn validate_same_shard(keys: &[&[u8]], num_shards: usize) -> Result<usize, ShardMismatchError>
pub fn require_same_shard(keys: &[Bytes], num_shards: usize) -> Result<(), CommandError>
pub fn require_same_shard_id(keys: &[Bytes], num_shards: usize) -> Result<usize, CommandError>
```

These are documented "for future use" and have **zero non-test callers** (verified: no
`routing::require_same_shard` / `validate_same_shard` references anywhere). A *fourth*, separate copy
of the same loop lives at `commands/src/utils.rs:639` (`require_same_shard → CommandError::CrossSlot`)
and *is* used by the in-shard sorted-set commands (`sorted_set/set_ops.rs`, `sorted_set/pop.rs`). The
existence of three dead helpers plus a live duplicate is the smell: "all keys on one shard" has no
single owner, so a re-implementation was free to appear (four times) and rot (three times).

### The CROSSSLOT literal inventory

| file:line | What it guards | Notion | Mode | Construction |
|---|---|---|---|---|
| `routing.rs:106` | `requires_same_slot` cmd (MSETNX) across shards | shard | standalone | inline literal |
| `routing.rs:111` | `!allow_cross_slot` across shards | shard | standalone | inline literal |
| `routing.rs:162` | scatter-unsupported cmd across shards | shard | standalone | inline literal |
| `guards.rs:232` | cluster slot mismatch | slot | cluster | `redirect::crossslot()` ✓ |
| `eval.rs:62` | EVAL cross-shard, no `allow_cross_slot` | shard | standalone | inline literal |
| `eval.rs:214` | EVALSHA cross-shard | shard | standalone | inline literal |
| `function.rs:51` | FCALL cross-shard | shard | standalone | inline literal |
| `transaction.rs:164` | EXEC target == `Multi` | shard/slot | both | inline literal |
| `transaction.rs:379` | WATCH cross-shard | shard | standalone | inline literal |

Source-of-truth definitions: `types/error.rs:139` (`CommandError::CrossSlot`),
`core/scripting/error.rs:26` (`ScriptError::CrossSlot`), `redirect.rs:31` (`crossslot()`).

## Proposed design

Introduce `SlotValidator`: one place that answers "do these keys share a home?" for both notions, and
emits the rejection through proposal-17's `redirect::crossslot()` — never a fresh literal.

### The primitive

```rust
/// Single owner of the same-locality rule. The CROSSSLOT wire string is
/// produced only by `redirect::crossslot()` (proposal 17); this type never
/// inlines it. Two methods, one rule applied to two homes:
///   same_slot  — CRC16 hash slot   (cluster mode; the strict notion)
///   same_shard — internal shard    (standalone multi-shard; slot % num_shards)
/// Because shard == slot % num_shards, same_slot ⇒ same_shard.
pub struct SlotValidator;

impl SlotValidator {
    /// All keys must map to one internal shard. Empty ⇒ Ok(None). Else CROSSSLOT.
    pub fn same_shard<K: AsRef<[u8]>>(
        keys: &[K],
        num_shards: usize,
    ) -> Result<Option<usize>, Response> {
        let mut it = keys.iter().map(|k| shard_for_key(k.as_ref(), num_shards));
        let Some(first) = it.next() else { return Ok(None) };
        if it.all(|s| s == first) { Ok(Some(first)) } else { Err(redirect::crossslot()) }
    }

    /// Cluster mode: all keys must map to one CRC16 hash slot. Strictly stronger
    /// than `same_shard`.
    pub fn same_slot<K: AsRef<[u8]>>(keys: &[K]) -> Result<Option<u16>, Response> {
        let mut it = keys.iter().map(|k| slot_for_key(k.as_ref()));
        let Some(first) = it.next() else { return Ok(None) };
        if it.all(|s| s == first) { Ok(Some(first)) } else { Err(redirect::crossslot()) }
    }
}
```

The `K: AsRef<[u8]>` bound is the one refinement over the sketch: routing passes `&[&[u8]]`
(`handler.keys`), while scripts and transactions pass `&[Bytes]`. One generic primitive serves both
without copying keys. Returning `Option<usize>`/`Option<u16>` hands the caller the resolved
shard/slot it would otherwise recompute (routing wants the shard to dispatch to; WATCH wants its
single shard; the empty case is `Ok(None)`, matching every site's "no keys ⇒ no check").

### The transaction accumulator

The three fragmented spots collapse into one accumulator that *uses* the primitive and owns the
`Multi`-promotion state machine once:

```rust
/// Folds keys into a transaction target during MULTI queuing. Owns BOTH the
/// cluster slot rule and the shard fold (None → Single → Multi) that today live
/// in `note_cluster_slot` + `add_transaction_shard` + the WATCH loop.
pub struct TxnSlotAccumulator {
    num_shards: usize,
    cluster: bool,
    first_slot: Option<u16>,        // cluster only
    target: TransactionTarget,
}

impl TxnSlotAccumulator {
    /// Queue-time: fold one command's keys. In cluster mode a slot mismatch
    /// promotes the target to `Multi`; in standalone mode a shard mismatch does.
    pub fn add_keys(&mut self, keys: &[Bytes]) { /* … */ }

    /// EXEC-time: `Multi` ⇒ Err(redirect::crossslot()); else the resolved target.
    pub fn resolve(self) -> Result<TransactionTarget, Response> { /* … */ }
}
```

WATCH no longer re-rolls a loop — it calls `SlotValidator::same_shard(args, num_shards)?` and gets
its single target shard directly. EXEC reads `accumulator.resolve()?` instead of matching
`TransactionTarget::Multi` and re-spelling the literal. `note_cluster_slot`, the WATCH loop, and the
standalone-vs-cluster branch in `queue_command` all disappear into the accumulator;
`add_transaction_shard` becomes its private fold.

### Where the literal lives

The CROSSSLOT string stays in proposal-17's `redirect.rs`, the same family as MOVED/ASK/CLUSTERDOWN —
all "the keys/slot you asked for are not here" redirects. `redirect::crossslot()` already exists; this
proposal simply makes *every* site call it. (See Correctness flags for collapsing the three remaining
literal *definitions* — `types`, `core/scripting`, `redirect` — onto one shared owner, which the
blocking.rs MOVED fix forces us to address anyway.)

### Why this is the right depth

- **Locality.** The cluster slot rule and the shard rule are each defined once. Changing the rule —
  e.g. if FrogDB ever lets a command opt into per-key routing, or changes how hash tags interact with
  shards — is one edit to `SlotValidator`, not a nine-site, two-notion hunt that silently diverges if
  you miss one. The "which notion does this site use?" ambiguity becomes a method name.
- **Leverage.** A ~30-line primitive plus a small accumulator delete eight inline literals, six
  hand-rolled loops, three dead helpers, and the three-way transaction-target fragmentation — and
  close the entire class of "a new multi-key command forgot the check / mistyped the string" bug.
- **Deletion test.** The migration is a net deletion at every call site and removes whole artifacts
  (the dead `server/src/routing.rs` trio, `note_cluster_slot`, the WATCH loop). If the new module
  could not delete those, its shape would be wrong.
- **Not a new adapter layer.** This deepens an existing seam: `redirect::crossslot()` is already the
  literal's owner; `SlotValidator` is the missing *logic* owner beside it. It is not a wrapper callers
  may bypass — the raw inline loops and literals are removed and a lint gate forbids their return.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Behavior-preserving **except** the
IPv6 MOVED fix in Phase 5, which is the point of that phase.

1. **Phase 0 — introduce `SlotValidator`.** Add `same_shard` / `same_slot` (generic over
   `AsRef<[u8]>`) beside `redirect.rs` (e.g. `slot_migration/validator.rs`). Unit-test the arithmetic
   (empty, single, hash-tag co-location, mismatch) and the `same_slot ⇒ same_shard` property. No call
   sites change. `just check frogdb-server`.
2. **Phase 1 — migrate the standalone shard sites.** Repoint `routing.rs:93-112,160-163`,
   `eval.rs:67-85` (`classify_script_shards`), and `function.rs:43-56` onto
   `SlotValidator::same_shard` + `redirect::crossslot()`. Pure substitution; behavior identical.
3. **Phase 2 — migrate the cluster slot site.** Replace the `guards.rs:225-234` loop with
   `SlotValidator::same_slot` (the literal there is already `redirect::crossslot()`).
4. **Phase 3 — transaction accumulator.** Introduce `TxnSlotAccumulator`; route `queue_command`
   (`transaction.rs:479-493`), WATCH (`transaction.rs:371-384`), and EXEC (`transaction.rs:156-166`)
   through it. Delete `note_cluster_slot` (`state.rs:692-714`) and the WATCH loop; fold
   `add_transaction_shard` into the accumulator. EXEC's `Multi` arm becomes `resolve()?`.
5. **Phase 4 — delete dead prior art.** Remove `validate_same_shard` / `require_same_shard` /
   `require_same_shard_id` from `server/src/routing.rs` (zero callers). Optionally re-point
   `commands/src/utils.rs:639` at a shared core-level loop (it returns `CommandError`, not `Response`,
   so it stays a thin in-crate wrapper around the same key-comparison — see open questions).
6. **Phase 5 — fix the blocking.rs MOVED (the 🔴 flag).** Route `core/src/shard/blocking.rs:112-117`
   through the shared address formatter so the redirect is bracketed for IPv6 (see Correctness flags
   for the crate-boundary fix).
7. **Gate.** Add a grep gate to `just lint`: outside `redirect.rs`, no `Response::error("CROSSSLOT`
   and no inline `"MOVED {`/`"ASK {` format strings — every redirect must come from the seam.

## Testing impact

- **Validator unit tests, no live cluster.** `same_shard` / `same_slot` are pure and unit-testable
  directly: empty ⇒ `Ok(None)`; single key ⇒ `Ok(Some(_))`; hash-tagged keys co-locate; a known
  cross-slot pair ⇒ `Err`. Pin the `same_slot ⇒ same_shard` property over random keys (the strict
  ordering is the whole reason there are two methods). These cases move out of integration-only
  coverage into fast unit tests.
- **Accumulator transitions.** Unit-test `None → Single → Multi` for both modes, and that
  `resolve()` returns CROSSSLOT iff `Multi`. Today this logic is only reachable through a full
  MULTI/EXEC integration flow.
- **Literal-parity regression.** A test that every migrated site's error bytes equal
  `redirect::crossslot()` — guards against a future re-inline drifting the string. The existing
  integration assertions (`integration_cluster.rs:7691` MSET CROSSSLOT, `integration_pubsub.rs:720`
  SSUBSCRIBE, `redis-regression` `cluster_sharded_pubsub_tcl.rs:78`) keep passing unchanged.
- **WATCH / EXEC parity.** Assert WATCH and queue-time agree on the target shard for the same key set
  (they share the accumulator now) — previously two independent loops.
- **IPv6 MOVED (fails today).** Drive `blocking.rs`'s migration-MOVED path with an IPv6
  `target_addr` and assert the reply is `MOVED <slot> [<v6>]:<port>`. Today it emits the unbracketed,
  unparseable `MOVED <slot> <v6>:<port>`; after Phase 5 it is bracketed. This is the regression test
  pinning the flag.

## Risks / open questions

- **Two notions, one or two methods.** Because `shard == slot % num_shards`, a single primitive
  parametrized by "the modulus, or none" *could* serve both. The proposal keeps two named methods
  (`same_shard` returning `usize`, `same_slot` returning `u16`) because the return types and the wire
  meaning differ, and the name documents which guarantee a site enforces — the very ambiguity this
  proposal removes. Collapsing them to one generic would re-introduce that ambiguity at the call site.
- **COPY is not a CROSSSLOT site.** `routing.rs:201-202` computes two shards *to perform* a
  cross-shard copy. The validator must not be applied there; the migration touches only the guard
  exits, not the two-phase COPY path.
- **Scripts use the shard notion, deliberately.** EVAL/EVALSHA/FCALL validate shard-sameness, not
  CRC16-slot sameness, because the cluster slot check already ran in `guards.rs` upstream
  (`dispatch.rs:590`). The migration must keep them on `same_shard`; switching them to `same_slot`
  would double-validate and reject standalone multi-shard scripts that `allow_cross_slot` permits.
- **Cross-crate command-layer duplicate.** `commands/src/utils.rs:639` is in a different crate and
  returns `CommandError::CrossSlot`, not a `Response`. Fully unifying it with `SlotValidator` would
  require the shared key-comparison loop to live in `frogdb-core`/`frogdb-types` (which both crates
  reach) with thin per-crate wrappers choosing the error type. In scope to *note*; the minimal version
  leaves it as a wrapper and only deletes the truly-dead `server/src/routing.rs` trio.
- **The literal still has three definitions.** `types/error.rs:139`, `core/scripting/error.rs:26`,
  and `redirect.rs:31` each spell the string. `redirect::crossslot()` is the *Response* owner, but
  `CommandError::CrossSlot` (rendered when an in-shard command rejects) and `ScriptError::CrossSlot`
  are independent. A `frogdb-types` raw-string owner that all three delegate to would make it truly
  one — and the blocking.rs fix below forces that question for MOVED regardless, so addressing both
  redirect strings together is natural.

## Correctness flags

1. **🔴 `blocking.rs` formats MOVED inline — bypasses the proposal-17 seam and breaks IPv6 —
   CONFIRMED.** `core/src/shard/blocking.rs:112-117` builds the migration redirect by hand:

   ```rust
   let _ = entry.response_tx.send(Response::error(format!(
       "MOVED {} {}:{}",
       slot,
       target_addr.ip(),
       target_addr.port()
   )));
   ```

   This is exactly the divergent rendering proposal 17 eliminated everywhere else: it joins
   `ip()` and `port()` with a bare colon, so an IPv6 target yields `MOVED 1234 2001:db8::1:6379` —
   unbracketed and **unparseable** (a client cannot tell where the address ends and the port begins).
   `redirect::moved()` (`redirect.rs:13-15`) renders the same address as `[2001:db8::1]:6379` via
   `fmt_addr`, the only unambiguous form. So this site both (a) re-implements a redirect literal that
   17 declared single-owned, and (b) carries a real IPv6 bug 17 already fixed at every other MOVED
   site. It is a genuine proposal-17 gap: 17's grep for MOVED constructors missed this one because it
   lives in `core`, not `server`.

   **Crate boundary.** `frogdb-core` cannot depend on `frogdb-server`'s `slot_migration::redirect`
   (the dependency runs the other way). Two fixes:
   - **(a) shared formatter in `frogdb-types`.** Put `fmt_addr` and the raw redirect strings
     (`moved`/`ask`/`clusterdown`/`crossslot`) in `frogdb-types` — a crate both `core` and `server`
     already depend on, and which *already* hosts the CROSSSLOT literal (`error.rs:139`). `redirect.rs`
     becomes a thin `Response`-wrapping re-export; `blocking.rs` calls the shared formatter. This also
     collapses the three literal definitions noted above onto one owner.
   - **(b) defer formatting to `server`.** Have `core` hand the `(slot, SocketAddr)` up to the server
     layer and format there. Rejected: `blocking.rs` builds the final `Response` *inside* the shard
     actor and pushes it straight down the waiter's `oneshot` — there is no server-layer seam on that
     path without restructuring the blocked-waiter response channel.

   **Recommend (a).** It is crate-boundary-safe, fixes the IPv6 bug, and is the natural home given
   `frogdb-types` already owns the CROSSSLOT string — making this proposal's "one literal" goal and
   proposal 17's "one MOVED owner" goal land in the same place. Mark this a correctness flag; fix it in
   Phase 5.

2. **Eight inline CROSSSLOT literals + six duplicated key loops — CONFIRMED (code-smell).** Listed in
   the inventory table above. Proposal 17 created `redirect::crossslot()` and migrated one site
   (`guards.rs:232`) but left the other eight inline and never touched the loops. The risk is drift: a
   future edit to the wire string (or the rule) must find all nine sites and both notions. Fix: route
   every site through `SlotValidator` + `redirect::crossslot()`; add the lint gate.

3. **Dead same-shard helper trio — CONFIRMED (code-smell, not a runtime bug).**
   `server/src/routing.rs:16-82` exposes `validate_same_shard`, `require_same_shard`, and
   `require_same_shard_id`, documented "for future use," with zero non-test callers (verified). A
   fourth, *live* copy of the loop sits in `commands/src/utils.rs:639`. Their existence is the symptom:
   "all keys on one shard" has no single owner, so re-implementations were free to appear and rot.
   Fix: delete the trio; the validator is the owner.

4. **Transaction target tracked three ways — CONFIRMED (fragmentation).** `note_cluster_slot`
   (`state.rs:692-714`), `add_transaction_shard` (`state.rs:673-686`), and the WATCH loop
   (`transaction.rs:371-384`) are three `Multi`-promotion paths with no shared owner forcing them to
   agree. They agree today; nothing prevents a future edit to one from diverging. Fix: the
   `TxnSlotAccumulator` becomes the single owner of transaction co-location.
