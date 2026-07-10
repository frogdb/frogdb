# 44 — Keyspace-event key accuracy: over-emission on source keys, under-emission via Suppressed

**Status:** proposed
**Severity:** Important (Redis-parity divergence, misleading notifications to subscribers)
**Found:** 2026-07-09 during proposal 42 (PFMERGE could not declare its Redis-parity `pfadd`
event); full sweep 2026-07-10.

## Problem

The keyspace-notification seam emits the spec's single event name on **every** key
`handler.keys(args)` returns (`core/src/shard/keyspace_notify.rs:65-87`, invoked from
`post_execution.rs:213-217`). `EventSpec` has no way to name a destination key, give different
keys different event names, or defer to runtime. Result: three defect classes across ~27
commands.

### Class A — dest-only commands notify on read-only source keys (over-emission)

Redis emits the STORE-family event on the destination only. FrogDB emits it on every key:

| Command | Spec site | KeySpec | Dest |
|---|---|---|---|
| ZRANGESTORE | `commands/src/sorted_set/store_remove.rs:20-30` | `FirstTwo` | `args[0]` |
| ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE | `commands/src/sorted_set/set_ops.rs:212,398,697` | `DestThenNumkeys` | `args[0]` |
| SINTERSTORE / SUNIONSTORE / SDIFFSTORE | `commands/src/set.rs:538,479,609` | `All` | `args[0]` |
| COPY | `commands/src/generic.rs:478-488` | `FirstTwo` | `args[1]` (source gets `copy_to` too) |

### Class B — two-sided commands need per-key (and sometimes runtime) event names

| Command | Spec site | Redis behavior | FrogDB behavior |
|---|---|---|---|
| RENAME / RENAMENX | `generic.rs:60-70,125-135` | `rename_from` on src, `rename_to` on dst | `rename_from` on **both** |
| SMOVE | `set.rs:910-920` | `smove_from` on src, `smove_to` on dst | single `smove` on both |
| RPOPLPUSH | `list.rs:768-777` | `rpop` on src, `lpush` on dst | `rpoplpush` on both |
| LMOVE / BLMOVE | `list.rs:830-839`, `blocking.rs` | direction-dependent `lpop`/`rpop` on src + `lpush`/`rpush` on dst (depends on LEFT/RIGHT args) | single `lmove` on both |

### Class C — candidate-key pops notify on keys never touched

`keys()` returns every candidate; only the first non-empty key is popped:

- ZMPOP (`sorted_set/pop.rs:153-166`, `NumkeysAt`) — all N candidates get `zmpop`.
- BLPOP / BRPOP / BZPOPMIN / BZPOPMAX (`blocking.rs`, `AllButLast`) — on the immediate
  (non-blocking) path all candidates get the pop event.

Which key fires is knowable only at runtime.

### Class D — commands suppressed because the seam couldn't express dest-only (under-emission)

Documented workaround pattern — `EventSpec::Suppressed` with a justification comment instead of
wrong events (PFMERGE's comment at `commands/src/hyperloglog.rs:155-183` names ZRANGESTORE as
the known-bad precedent):

- PFMERGE — Redis emits `pfadd` on dest. Suppressed.
- BITOP (`bitmap.rs:209-224`) — Redis emits `set` on dest. Suppressed.
- SORT … STORE (`sort.rs:439-453`, Dynamic keys) — Redis emits `sortstore` on dest. Suppressed.
- GEORADIUS / GEORADIUSBYMEMBER STORE (`geo.rs:413,538`), GEOSEARCHSTORE (`geo.rs:347-363`) —
  Redis emits `georadius`-family store events on dest. Suppressed.
- LMPOP (`list.rs:916-930`) — suppressed while its structural twin ZMPOP over-emits
  (inconsistent both ways; Redis emits `lpop`/`rpop` on the popped key).

Correct today (genuinely per-key writes, no change): DEL/UNLINK (`All`), MSET/MSETNX
(`Stride`).

### Caveat on Redis behavior claims

The Redis-side names above are from model knowledge, not verified against Redis source this
session. **Implementation must verify each event name/key against Redis's
`notifyKeyspaceEvent` call sites (or live Redis with `notify-keyspace-events KEA`) before
encoding it in a spec** — accuracy over assumed parity ([[feedback_observability_accuracy]]).
Redis also emits secondary events (e.g. `del` on the dest when a STORE result is empty and the
dest is deleted); capture these while verifying.

## Design

Two mechanisms, matching the two shapes of the problem. Both preserve the declarative-spec
philosophy (proposal 01): static facts stay in `CommandSpec`; only genuinely runtime-dependent
facts move to a `CommandContext` deposit (precedent: `hll_wal_delta` from proposal 42, and
`WalStrategy::PersistDestination(usize)` proving the "distinguished key index" concept —
`core/src/command.rs:205-207`).

### 1. Static: `EventSpec::EmitsAt` for fixed dest-only emission (Classes A + D-static)

```rust
pub enum EventSpec {
    /// Emits `class`/`name` for each extracted key (DEL, MSET). Unchanged.
    Emits { class: KeyspaceEventFlags, name: &'static str },
    /// Emits `class`/`name` on exactly one extracted key: `keys()[key_index]`.
    /// For STORE-family commands whose remaining keys are read-only sources.
    EmitsAt { class: KeyspaceEventFlags, name: &'static str, key_index: usize },
    /// Handler deposits events at runtime via `ctx.notify_event(...)`; the seam
    /// drains them. For commands whose emitted key/name depends on execution.
    Dynamic,
    Suppressed,
    NotApplicable,
}
```

- Seam change in `emit_keyspace_notifications_for_command`: `EmitsAt` indexes into
  `handler.keys(args)` (works for `FirstTwo`, `DestThenNumkeys`, `All`, `Skip`, and `Dynamic`
  key shapes alike — the index is into the *extracted key list*, not raw args). Out-of-range =
  debug_assert + no emission.
- `CommandSpec::validate()` gains: `EmitsAt.key_index` must be valid for the spec's minimum
  arity/key count (statically checkable for all non-Dynamic KeySpecs).
- Migrations: ZRANGESTORE/Z*STORE/S*STORE → `EmitsAt { key_index: 0 }`; COPY →
  `EmitsAt { key_index: 1 }`; PFMERGE → `EmitsAt { key_index: 0, name: "pfadd" }` (delete the
  workaround comment); BITOP → `EmitsAt` on dest with the Redis-verified name; GEOSEARCHSTORE →
  `EmitsAt { key_index: 0 }`; SORT-STORE and GEORADIUS-STORE (Dynamic keys, dest emission only
  when STORE arg present) → `EventSpec::Dynamic` (see below), since the *presence* of the dest
  is runtime-dependent.

### 2. Runtime: `EventSpec::Dynamic` + context deposit (Classes B + C + D-dynamic)

- `CommandContext` gains `keyspace_events: SmallVec<[(Bytes, &'static str, KeyspaceEventFlags); 2]>`
  and a `notify_event(key, name, class)` method (encapsulated, per [[feedback_encapsulation]]).
- Execution seam captures it alongside `hll_wal_delta` into the `WriteRecord`/summary; the
  notifications effect step emits deposited events for `EventSpec::Dynamic` commands instead of
  iterating `keys()`.
- No-op interplay: `write_was_noop = true` already skips the whole effect pipeline, so deposits
  from a no-op path are naturally discarded — same contract as the WAL delta.
- Migrations: RENAME/RENAMENX (`rename_from` src + `rename_to` dst), SMOVE (`smove_from` +
  `smove_to` — only when the move actually happened; SMOVE of a non-member is a no-op),
  LMOVE/BLMOVE/RPOPLPUSH (direction-resolved pop event on src + push event on dst), ZMPOP/LMPOP
  and BLPOP/BRPOP/BZPOPMIN/BZPOPMAX immediate path (event on the actually-popped key only),
  SORT-STORE/GEORADIUS-STORE (dest event only when STORE used and result written). Secondary
  `del` events (empty-result STORE deleting dest, moves emptying the source) get deposited in
  the same call sites — verify each against Redis first.
- Blocked-then-woken path for the blocking family: the wake path re-executes the command, so
  the deposit happens on the execution that actually pops; confirm during implementation that
  the wake path routes through the same effect pipeline.

### validate() hardening

Add the invariant this sweep proves is needed: a spec with a multi-key `KeySpec`
(`FirstTwo`/`All`/`AllButLast`/`Skip`/`NumkeysAt`/`DestThenNumkeys`) and plain `Emits` must be
one of an explicit allowlist (DEL, UNLINK, MSET, MSETNX — commands that genuinely write every
key). Anything else fails registry validation at startup/test time, so the next STORE-style
command cannot silently reintroduce the bug. (Registry-wide exhaustiveness tests from proposal
01 give this teeth.)

## Tests

1. Integration (pubsub): per affected command, subscribe `__keyevent@0__:<name>` +
   `__keyspace@0__:<key>` and assert exact key set and names — at minimum ZRANGESTORE,
   ZINTERSTORE (incl. empty-result `del` case if Redis-verified), SINTERSTORE, COPY, RENAME
   (`rename_from`+`rename_to`), SMOVE (incl. non-member no-op → no events), LMOVE both
   directions, RPOPLPUSH, ZMPOP/BLPOP immediate path (event only on popped key), PFMERGE
   (`pfadd` on dest only; no-op merge → nothing), BITOP, SORT STORE.
2. Registry test: the new validate() invariant sweeps all ~380 specs.
3. Unit: `EmitsAt` extraction per KeySpec shape; deposit drain order; deposits discarded on
   `write_was_noop`.
4. Redis-regression crate: port the relevant `notify.tcl` cases if not already present
   (`keyspace_tcl.rs` currently asserts data movement, not notifications).

## Sequencing

1. Mechanism: `EventSpec::EmitsAt` + `Dynamic` + ctx deposit + seam + validate() invariant.
2. Class A + PFMERGE/BITOP/GEOSEARCHSTORE migrations (static dest-only).
3. Class B (renames/moves, per-key names).
4. Class C + remaining Dynamic-key STOREs (runtime-dependent).

Each phase independently shippable; phase 1 alone unblocks PFMERGE parity deferred from
proposal 42.

## Open decisions

1. Whether Redis's secondary events (`del` on emptied STORE dest / emptied move source) are in
   scope now or a follow-up — decide after verifying actual Redis behavior.
2. `EmitsAt` vs folding a `dest_index` into `KeySpec` itself: rejected here to keep `KeySpec`
   purely about extraction (COMMAND GETKEYS parity) — events are a separate concern.
3. LMPOP-vs-ZMPOP inconsistency resolves to both using `Dynamic` (phase 4); confirm no other
   Suppressed command was masking a same-shape bug.
