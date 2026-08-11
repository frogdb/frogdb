# Proposal 92 — one `ExpiryDecision` table: NX/XX/GT/LT is written out five times, and three of the five put the delete branch on the wrong side of the conditions

Round 38 · lane: commands + types · candidate **CT4** · effort **M** ·
**no locked crate edited** (`frogdb-commands` is not one of the four locked areas; `grep -rn "FM-"
frogdb-server/crates/commands/src` returns **nothing**) · **no seam gate constrains the change**
(the new module reads **no** clock — `now` is a parameter — so `lint-clock-seam` is not merely
satisfied but structurally inapplicable; `lint-no-typed-unwrap` is unaffected)

**Verified at HEAD `25d3873642b31162dd21965cffdf685048ac518d`** (worktree `arch-round-38-99`,
branch `worktree-arch-round-38-99`). Every file:line below was re-derived at this SHA. Nothing is
inherited from the lane brief. **No code file in this proposal's set is dirty** — the only
modified path in the tree is `.scratch/arch-deepening/proposals/85-frogdb-macros-fate.md`, held by
a concurrent author.

**The headline is not the one the brief expected.** The brief flagged a suspected HEXPIRE-only
compat divergence. Verified against upstream source, the divergence is **real, larger, and spans
both command families**: the delete-on-past-deadline branch sits *before* the condition checks in
all five copies, where Redis puts it *after* all four of them. Five decision cells across
`EXPIRE`/`EXPIREAT`/`HEXPIRE`/`HPEXPIREAT` reply the wrong integer **and destroy user data the
condition was written to protect**. The refactor is worth doing on locality grounds alone; the
ordering defect is what makes it urgent, and it is exactly the class of bug a decision *table*
cannot express.

## Corrections to the lane brief

| Brief claim | Verified at `25d3873` |
|---|---|
| "`parse_expire_conditions` has twins at `expiry.rs:181` and `:205`" | **Line numbers correct, characterization stale.** The *flag-scanning loop* was already extracted into `parse_expire_condition_flags` (`expiry.rs:152-177`) and both twins call it. What is still duplicated is the **14-line mutual-exclusion block**, byte-for-byte identical at `:186-199` and `:212-225` (verified by exact comparison). The twins differ in exactly one token: `ArgParser::from_position(args, 2)` (`:184`) vs `ArgParser::new(args)` (`:210`). **No** case-sensitivity or error-string drift — both messages are identical strings. This part of the brief is a **small** finding, not the case for the proposal. |
| "the apply/decision block is repeated across EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT, plus a per-field copy in `execute_hexpire_common`" | **Verified, 5 copies**: `expiry.rs:287-321`, `:376-408`, `:453-492`, `:537-576`, `hash.rs:1062-1123`. |
| "`HEXPIRE k 0 NX` … may return 2 where Redis returns 0 … verify against OUR code order" | **Verified in code-order terms, and it is worse than stated.** In `execute_hexpire_common` **all four** conditions sit below the delete branch (`hash.rs:1062-1083` vs `:1085-1120`), so NX, XX *and* GT all diverge. And the same inversion exists in the **key** family for GT (`expiry.rs:297-300` before `:304-318`) — which the brief did not suspect at all. Full cell-by-cell table in §The ordering divergence. |
| effort **M** | **M confirmed**, but the split is not what the brief assumed: the mechanical refactor is S; the M comes entirely from the **behavior fix** and its regression tests (§Effort). |
| "Latent" | **Refuted. Live.** Two of the five divergent cells silently delete a key/field that upstream preserves. |

Two findings the brief did not name: **46 hand-written reply constructions** implement what is an
**8-cell** mapping (§Problem 4), and `execute_hexpire_common` carries **two dead degrees of
freedom** — an unreachable `None` arm and a `time_converter`/`is_past_or_zero` pair that encode the
same predicate twice (§Problem 5).

## Summary

Eight commands — `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `HEXPIRE`, `HPEXPIRE`,
`HEXPIREAT`, `HPEXPIREAT` — answer one question: *given the condition flags, the current deadline,
and the proposed deadline, do I set it, leave it alone, or delete the thing?* That question has
**three** answers and **eight** reply cells. It is currently answered by **five separately
maintained straight-line procedures** totalling ~200 lines and **46** hand-written
`Response::Integer(…)` constructions, spread across two files 700 lines apart.

The five copies are not identical, and the drift is diagnostic:

- **Comment drift.** Only `EXPIRE` carries the line explaining the tricky cell — `expiry.rs:305`,
  `// GT on key without TTL: return 0 (Redis behavior: GT requires existing TTL to compare)`. The
  three key-family siblings dropped it in the copy; the hash-family copy never had it.
- **Order drift.** The four key-family copies put NX/XX *above* the delete branch and GT/LT
  *below* it. The hash-family copy puts **all four** below. Neither matches upstream, and the two
  families do not match **each other** — so the same flag on the same past deadline means three
  different things depending on which of the five procedures you land in.

The proposal: extract one **pure decision module**, `commands/src/expiry_decision.rs`, whose
**interface** is a total function over the decision's actual inputs —

```rust
ExpiryDecision::evaluate(cond, current: Option<Instant>, new: NewDeadline, now: Instant)
    -> ExpiryDecision   // Apply(Instant) | Skip | Delete
```

— plus an 8-cell reply table `ExpiryFamily::{Key,HashField}::reply(outcome) -> i64`. The five
procedures become **adapters**: parse, look up the current deadline, call the table, act on the
verdict, map the reply. The condition **ordering becomes a property of one `match`** instead of a
property of five statement sequences, which is what makes the divergence unrepeatable rather than
merely fixed once.

The **leverage** claim is precisely located: not the ~200 deleted lines (a new module and its
tests give most of them back — §Deletion test), but the fact that **the decision table is
unit-testable with zero I/O and the reply mapping is exhaustive over a 2×4 product**, where today
every one of the 46 reply cells is reachable only by booting a `TestServer` and doing a RESP round
trip. `expiry.rs` contains **zero `#[cfg(test)]` modules** at HEAD.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/commands/src/expiry.rs` | 817 | **Primary.** Four decision blocks (`:287-321`, `:376-408`, `:453-492`, `:537-576`) collapse to adapter calls. The two condition parsers (`:181-201`, `:205-227`) fold to one. `ExpireConditions` (`:126-146`) moves to the new module and gains derives. **Zero `FM-` tags; zero `#[cfg(test)]` modules** in the whole file. Recent churn: 8 commits, most recently `2fb1051c` (clock-seam sweep) and `00dfb0ab` (expiry clock seam). |
| `frogdb-server/crates/commands/src/expiry_decision.rs` | **new, ~130 + ~180 test** | **Primary.** `ExpireConditions`, `NewDeadline`, `ExpiryDecision`, `ExpiryOutcome`, `ExpiryFamily`, `evaluate`, `reply`. No `use frogdb_core::clock` — the module reads no clock and touches no store. |
| `frogdb-server/crates/commands/src/hash.rs` | 2327 | **Primary.** `execute_hexpire_common:1062-1123` (the per-field decision) becomes one `evaluate` call; the local `enum FieldAction` (`:1045-1050`) is replaced by the shared verdict; the `time_converter: impl Fn(i64) -> Option<Instant>` parameter (`:994`) loses its `Option` and its dead `None` arm (`:1069-1075`). The four adapters at `:1237`, `:1283`, `:1329`, `:1375` each shed one closure. **Zero `FM-` tags.** Recent churn: 8 commits. |
| `frogdb-server/crates/commands/src/lib.rs` | 473 | **Primary.** One line: `pub(crate) mod expiry_decision;` beside `pub mod expiry;` (`:35`). No feature gate — both `expiry` and `hash` are unconditional modules, so the new module and its unit tests build under the default `core-profile`. |
| `frogdb-server/crates/redis-regression/tests/expire_tcl.rs` | 908 | **Primary (additive).** 58 tests today; **no test combines a past/negative operand with `GT`** — the gap that let the key-family divergence live. Adds the rows from §The ordering divergence. |
| `frogdb-server/crates/redis-regression/tests/hash_field_expire_tcl.rs` | 1718 | **Primary (additive).** 39 tests today; `HEXPIRE … 0 …` with a condition flag is absent. Adds the hash-family rows. |
| `frogdb-server/crates/commands/src/string.rs` | 1805 | **Read-only evidence — not edited.** `GETEX` (`:424-529`) proves the scope boundary: its option loop (`:451-465`, `:489-527`) handles `EX/PX/EXAT/PXAT/PERSIST` and **has no NX/XX/GT/LT at all** (§Scope boundary). |
| `frogdb-server/crates/core/src/command.rs` | — | **Read-only evidence.** `WalStrategy::actions_with_delta:658-661` — the `PersistFirstKey` → `WalAction::Persist` mapping behind the adjacent suspect in §Adjacent finding. |

## Problem

### 1. Two condition parsers, one of them a 14-line clone

`expiry.rs:181-201` and `:205-227`. The flag loop is already shared (`:152-177`); what is not is
the validation tail. Exact comparison of `:186-199` against `:212-225` reports **identical** — the
same two `if`s, the same two error strings, in the same order:

```rust
    // NX and (XX|GT|LT) are mutually exclusive
    if conditions.nx && (conditions.xx || conditions.gt || conditions.lt) {
        return Err(CommandError::InvalidArgument {
            message: "NX and XX, GT or LT options at the same time are not compatible".to_string(),
        });
    }
    // GT and LT are mutually exclusive
    if conditions.gt && conditions.lt { … }
```

The only real difference is the parser's start position — `from_position(args, 2)` vs `new(args)`.
That is an argument, not a function. One `parse_expire_conditions(args: &[Bytes])` taking the
already-sliced condition args, with the four key-family call sites passing `&args[2..]`, removes
the twin outright. **Cheap, uncontroversial, and it is the smallest part of this proposal.**

### 2. Five decision procedures

| Copy | Range | Shape |
|---|---|---|
| `EXPIRE` | `expiry.rs:287-321` | `contains` → `get_expiry` → NX → XX → **delete if `seconds <= 0`** → build deadline → GT → LT → `set_expiry` |
| `PEXPIRE` | `expiry.rs:376-408` | identical modulo `secs`→`ms` and `from_secs`→`from_millis` |
| `EXPIREAT` | `expiry.rs:453-492` | same, plus a second delete branch for `expires_at <= now` |
| `PEXPIREAT` | `expiry.rs:537-576` | identical to `EXPIREAT` modulo `unix_secs_to_instant`→`unix_ms_to_instant` |
| `HEXPIRE` family | `hash.rs:1062-1123` | **delete if past** → **delete if `expires_at <= now`** → NX → XX → GT → LT → `SetExpiry` |

**Proof of identity, EXPIRE vs PEXPIRE** (`:287-321` against `:376-408`) — the entire diff is
three lines, two of them the unit and one of them a **lost comment**:

```
-        if seconds <= 0 {                                       |  +        if ms <= 0 {
-        let expires_at = clock::now() + Duration::from_secs(…); |  +        … Duration::from_millis(…);
-        // GT on key without TTL: return 0 (Redis behavior: …)  |  (absent)
```

**Proof of identity, EXPIREAT vs PEXPIREAT** (`:453-492` against `:537-576`) — the entire diff is
**two** lines, the operand name and the converter. Thirty-eight of forty lines are byte-identical.

**Proof of drift, EXPIRE vs EXPIREAT** — same two files, same author-intent, different structure:
`EXPIREAT` inserts a *second* delete branch (`:471-475`) and drops the `GT` explanation. Both
copies still evaluate GT/LT *after* their delete branches.

The hash copy is the same procedure again, transposed into a per-field loop that accumulates a
local `enum FieldAction { NotFound, Delete, Skip, SetExpiry(Instant) }` (`hash.rs:1045-1050`).
**That enum is the decision type this proposal is asking for** — it already exists, it is already
correct, and it is already the right shape. It is simply declared *inside a function body* in the
one call site that happens to need to defer its mutations, so the other four cannot reach it.
Making it a crate-level type is not an invention; it is **relocation to the only place where all
five sites can see it**.

### 3. The ordering divergence (live)

Upstream Redis 8.0 evaluates **every condition first** and the past-deadline delete **last**. Both
families, verbatim:

`src/expire.c`, `expireGenericCommand` — `if (flag) { …NX… …XX… …GT… …LT… }` and only then:

```c
    if (checkAlreadyExpired(when)) {
        int deleted = dbGenericDelete(…);
        …
        addReply(c, shared.cone);
```

`src/t_hash.c:542-563`, `hashTypeSetExpiryListpack` — the conditions, then:

```c
    /* If expired, then delete the field and propagate the deletion. */
    if (unlikely(checkAlreadyExpired(expireAt))) {
        propagateHashFieldDeletion(…); hashTypeDelete(…);
        return HSETEX_DELETED;      /* == 2 */
    }
```

FrogDB inverts it. The divergent cells, derived by reading both sides — **not** by running a
server (see §Honesty about verification):

| Case | Upstream | FrogDB at `25d3873` | Where |
|---|---|---|---|
| `EXPIRE k -10 GT`, key **has** TTL | `0`, key kept (`when <= current_expire`) | **`1`, key deleted** | `expiry.rs:297-300` fires before `:306-312` |
| `EXPIRE k -10 GT`, key **has no** TTL | `0`, key kept (`current_expire == -1` ⇒ GT always fails) | **`1`, key deleted** | same |
| `HEXPIRE k 0 NX FIELDS 1 f`, field **has** TTL | `0`, field kept (`expireSetCond == HFE_NX` ⇒ not met) | **`2`, field deleted** | `hash.rs:1062-1067` fires before `:1085-1096` |
| `HEXPIRE k 0 XX FIELDS 1 f`, field **has no** TTL | `0`, field kept (`HFE_XX` on absent prev) | **`2`, field deleted** | same |
| `HEXPIRE k 0 GT FIELDS 1 f`, either TTL state | `0`, field kept | **`2`, field deleted** | same |

The `EXPIREAT`/`PEXPIREAT`/`HEXPIREAT`/`HPEXPIREAT` variants divide the same way with a past
absolute timestamp instead of a negative relative one (`expiry.rs:471-475`, `hash.rs:1078-1083`).

Two cells that look divergent and **are not**, checked and cleared so the fix does not overshoot:

- `EXPIRE k -10 LT` — upstream's `LT` passes when there is no TTL *and* when the past `when` is
  below a live TTL, so it reaches `checkAlreadyExpired` and deletes: `1`. FrogDB: `1`. **Agrees**,
  and both existing regression tests (`expire_tcl.rs:881`, `:891`) stay green.
- `EXPIRE k -10 NX` / `XX` — NX/XX already sit above the key-family delete branch, so those two
  cells were already right. **Only GT moves in the key family.**

Why the tests missed it: `expire_tcl.rs` has 58 tests including three dedicated `GT` cases
(`:707`, `:721`, `:735`) — all with **future** operands — and two negative-operand cases
(`:881`, `:891`) — both with **`LT`**. The product cell (`negative operand` × `GT`) is exactly the
one nobody wrote. `hash_field_expire_tcl.rs` has 39 tests and never combines a zero operand with a
condition flag.

**This is the argument for a table.** Five procedures × four flags × two TTL-states × two
deadline-classes is an 80-cell space explored today by reading straight-line code; a decision table
makes it a 16-row `match` an adversary can read in one screen and a test can enumerate.

### 4. Forty-six reply constructions for an eight-cell mapping

`Response::Integer(…)` occurrences inside the five procedures, counted exactly:

| Procedure | count |
|---|---|
| `EXPIRE` `:258-322` | 8 |
| `PEXPIRE` `:354-410` | 8 |
| `EXPIREAT` `:442-494` | 9 |
| `PEXPIREAT` `:526-578` | 9 |
| `execute_hexpire_common` `:988-1160` | 12 |
| **total** | **46** |

The mapping they implement has **eight** cells:

| outcome | `ExpiryFamily::Key` | `ExpiryFamily::HashField` |
|---|---|---|
| `Missing` (no such key / no such field) | `0` | `-2` |
| `Skipped` (condition not met) | `0` | `0` |
| `Applied` | `1` | `1` |
| `Deleted` (deadline already passed) | `1` | `2` |

Thirty of the 46 are the literal `0`, written out at 30 sites that must all agree. Six are
`if deleted { 1 } else { 0 }` (`:299`, `:388`, `:466`, `:474`, `:550`, `:558`) and four are
`if result { 1 } else { 0 }` (`:321`, `:409`, `:493`, `:577`). **Both ternaries are
already-decided branches**: the key's
presence was established by `ctx.store.contains(key)` above (`:283`, `:372`, `:449`, `:533`) on a
single-threaded shard, so the `else` arm is unreachable — upstream states the same fact as
`serverAssertWithInfo(c, key, deleted)`. Folding them into the table replaces ten defensive
expressions with two total ones.

### 5. Two dead degrees of freedom inside the shared hash helper

`execute_hexpire_common` takes **both** `time_converter: impl Fn(i64) -> Option<Instant>` (`:994`)
**and** `is_past_or_zero: impl Fn(i64) -> bool` (`:995`). Across all four call sites the second
is exactly the predicate under which the first returns `None`:

| Command | `time_converter` `None` when | `is_past_or_zero` |
|---|---|---|
| `HEXPIRE` `:1243-1249` | `secs <= 0` | `secs <= 0` |
| `HPEXPIRE` `:1289-1295` | `ms <= 0` | `ms <= 0` |
| `HEXPIREAT` `:1335-1341` | `ts < 0` | `ts < 0` |
| `HPEXPIREAT` `:1381-1387` | `ts < 0` | `ts < 0` |

Because `is_past_or_zero` is checked first and `continue`s (`hash.rs:1062-1067`), the `None` arm
at `:1069-1075` — which pushes `Skip`/`0` — is **unreachable for all four commands**. It is also
*wrong* if it were reached: a past deadline is `Delete`/`2`, not `Skip`/`0`. Dead code that
disagrees with its own live neighbour is the definition of a latent trap.

A second one: `unix_secs_to_instant` (`expiry.rs:37-47`) and `unix_ms_to_instant` (`:50-60`)
return `Option<Instant>` and **both arms of both functions return `Some`** — they are total. The
two `.ok_or(CommandError::NotInteger)?` guards at `:469` and `:553`, and the `Option` in the hash
converter signature (`hash.rs:994`), are ceremony around a value that is never `None`.

## Proposed change

### The module

New file `frogdb-server/crates/commands/src/expiry_decision.rs`, `pub(crate)`, ~130 lines. It
imports **`std::time::Instant` and nothing else** — no store, no clock, no `Response`, no `Bytes`.
The wire grammar deliberately does **not** move: `parse_expire_conditions` stays in `expiry.rs`
where the `ArgParser` and the error strings already live. Parsing is grammar; this module is
decision, and keeping the seam there is what makes it dependency-free and trivially testable.

```rust
/// NX/XX/GT/LT as parsed from the wire. Filled by `expiry.rs`'s one parser;
/// `Default` replaces today's hand-written `ExpireConditions::none()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ExpireConditions { pub nx: bool, pub xx: bool, pub gt: bool, pub lt: bool }

/// The proposed deadline. `Elapsed` is a *class*, not an instant: relative
/// operands `<= 0` have no representable `Instant`, and upstream treats them
/// identically to an absolute stamp already behind `now`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NewDeadline { Elapsed, At(Instant) }

/// What the caller must do. Exhaustive; there is no fourth answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpiryDecision { Apply(Instant), Skip, Delete }

impl ExpiryDecision {
    /// Upstream order, stated once: **all four conditions, then the
    /// already-elapsed delete.** (`expire.c` expireGenericCommand;
    /// `t_hash.c` hashTypeSetExpiryListpack.)
    pub(crate) fn evaluate(
        cond: ExpireConditions,
        current: Option<Instant>,   // None = no TTL = "infinite", per upstream's -1
        new: NewDeadline,
        now: Instant,
    ) -> Self { … }
}
```

`evaluate`'s body is one flat sequence over a 2 (TTL state) × 4 (flag) product plus the terminal
elapsed check — ~20 lines, no `?`, no allocation, no I/O:

- `nx && current.is_some()` → `Skip`
- `xx && current.is_none()` → `Skip`
- `gt` → `Skip` when `current.is_none()`, or when `new` is not strictly after `current`
- `lt` → `Skip` when `current.is_some_and(|c| new` is not strictly before `c)`
- then: `new` elapsed (either `Elapsed`, or `At(t)` with `t <= now`) → `Delete`
- else → `Apply(t)`

`NewDeadline::Elapsed` orders strictly below every `current` the callers can present, because each
caller has already established the key/field is live (`ctx.store.contains`, `hash.contains` +
lazy purge), so a `current` at or before `now` cannot reach here — the same precondition
[`FM-PERSISTENCE-044`](../../hardening/specs/persistence-failure-modes.md) states from the store
side. One `debug_assert!` and one unit test pin it; no prose workaround is required.

### The reply table

```rust
pub(crate) enum ExpiryOutcome { Missing, Skipped, Applied, Deleted }
pub(crate) enum ExpiryFamily { Key, HashField }

impl ExpiryFamily {
    pub(crate) fn reply(self, outcome: ExpiryOutcome) -> i64 {
        match (self, outcome) {
            (ExpiryFamily::Key,       ExpiryOutcome::Missing) => 0,
            (ExpiryFamily::HashField, ExpiryOutcome::Missing) => -2,
            (_,                       ExpiryOutcome::Skipped) => 0,
            (_,                       ExpiryOutcome::Applied) => 1,
            (ExpiryFamily::Key,       ExpiryOutcome::Deleted) => 1,
            (ExpiryFamily::HashField, ExpiryOutcome::Deleted) => 2,
        }
    }
}
```

**This is where the leverage claim lives, so it is worth being exact about what it buys and what
it does not.** It does *not* make the two families share a reply — they demonstrably differ in two
of four cells, and the enum says so. What it buys is that the difference is **one 8-row match
instead of 46 scattered literals**, that adding a third family (`HGETEX`/`HSETEX` already reply on
a third scale) is a variant plus two rows rather than a new procedure, and that a golden test can
assert all eight cells in eight lines. The reply codes stay exactly as they are today —
**this table is a pure re-expression; the fix in §The ordering divergence is what changes bytes on
the wire, and only in the five named cells.**

### The five adapters

Each call site keeps everything the table is deliberately not given — arg parsing, overflow
guards, the store lookups, the mutation, the WAL/notification framing — and loses the decision:

```rust
// EXPIRE, after the existing overflow guards and `contains` check
let cond = parse_expire_conditions(&args[2..])?;
let new  = if seconds <= 0 { NewDeadline::Elapsed }
           else { NewDeadline::At(clock::now() + Duration::from_secs(seconds as u64)) };
let outcome = match ExpiryDecision::evaluate(cond, ctx.store.get_expiry(key), new, clock::now()) {
    ExpiryDecision::Skip      => ExpiryOutcome::Skipped,
    ExpiryDecision::Delete    => { ctx.store.delete(key); ExpiryOutcome::Deleted }
    ExpiryDecision::Apply(at) => { ctx.store.set_expiry(key, at); ExpiryOutcome::Applied }
};
Ok(Response::Integer(ExpiryFamily::Key.reply(outcome)))
```

`execute_hexpire_common` keeps its two-pass structure (gather under a shared handle, mutate under
`get_hash_mut`) unchanged — the local `FieldAction` becomes the shared `ExpiryDecision`, its
`NotFound` arm becomes `ExpiryOutcome::Missing`, and the `time_converter` parameter narrows from
`impl Fn(i64) -> Option<Instant>` to `impl Fn(i64) -> NewDeadline`, absorbing `is_past_or_zero` and
deleting the unreachable arm (§Problem 5).

## Deletion test

Applied honestly. **Deleted:** ~200 lines of decision procedure across five sites, 14 lines of
cloned validation, one unreachable `None` arm and its parameter, two `.ok_or(…NotInteger)?`
guards over total functions, and 46 reply constructions → 5. **Added:** ~130 lines of module
and ~180 lines of unit tests. **Net production lines: roughly −70. Net repo lines: roughly +110**,
and this proposal does not pretend that is a win by itself.

What is actually deleted is **the obligation to restate the condition order five times**, and the
measure is the derivative: adding `HGETEX`'s condition handling, or a sixth expiry command,
currently costs a sixth copy of a procedure that three of five existing copies got wrong; after
this it costs an `ExpiryFamily` variant and two table rows, and **cannot be added without a
compiler-enforced decision about all four outcomes**. A refactor scored on deleted lines would
fail here. The thing being removed is a **decision that is currently spelled five times and
spelled differently three of those times**.

## Testability improvement

1. **From zero to exhaustive.** `expiry.rs` has **no `#[cfg(test)]` module at all** at HEAD;
   every one of its 46 reply cells is reachable only via `TestServer::start_standalone()` + a RESP
   round trip in `redis-regression`. `evaluate` is a pure function of four `Copy` arguments, so the
   **full** condition space — 4 flags × 2 TTL states × 3 deadline classes (elapsed / at-or-before
   `now` / future) = 24 cells, plus the 5 legal flag combinations the parser admits — is a table
   test that runs in microseconds with no server, no store, no clock.
2. **The precedent already exists in the same crate.** `hash.rs:2091-2130`
   (`mod expiry_grammar_pin_tests`) builds a `CommandContext` over a leaked `HashMapStore` to pin
   expiry *error strings* at the unit level. The decision table needs strictly less than that — no
   context at all.
3. **Determinism without the clock seam.** `now` is a parameter, so tests pass a fixed `Instant`
   and never touch `clock::now()`. The module is the rare one where `lint-clock-seam` has nothing
   to check because there is nothing to check.
4. **The five divergent cells get named regression tests** in the existing upstream-derived files
   (`expire_tcl.rs`, `hash_field_expire_tcl.rs`), each asserting **both** the integer reply and
   the survival of the key/field — the reply alone would have caught only three of the five, and
   the data loss is the part that matters:

   ```rust
   // expire_tcl.rs — the cell the 58 existing tests miss
   client.command(&["SET", "foo", "bar", "EX", "100"]).await;
   assert_integer_eq(&client.command(&["EXPIRE", "foo", "-10", "GT"]).await, 0);
   assert_integer_eq(&client.command(&["EXISTS", "foo"]).await, 1);   // NOT deleted

   client.command(&["SET", "bar", "v"]).await;                        // no TTL
   assert_integer_eq(&client.command(&["EXPIRE", "bar", "-10", "GT"]).await, 0);
   assert_integer_eq(&client.command(&["TTL", "bar"]).await, -1);

   // hash_field_expire_tcl.rs — helpers as used at :944-947 today
   client.command(&["HSET", "h", "f", "v"]).await;
   client.command(&["HEXPIRE", "h", "100", "FIELDS", "1", "f"]).await;
   let arr = unwrap_array(client.command(&["HEXPIRE", "h", "0", "NX", "FIELDS", "1", "f"]).await);
   assert_integer_eq(&arr[0], 0);                                    // not 2
   assert_integer_eq(&client.command(&["HEXISTS", "h", "f"]).await, 1); // NOT deleted
   ```
5. **A tightening the tests cannot express today**: `hash_field_expire_tcl.rs:1325-1356`
   documents that FrogDB's mutual-exclusion error text differs from upstream's
   `"Multiple condition flags specified"`. With one parser the text is stated once, so aligning it
   later is a one-line change instead of a two-site one. (Aligning it is **not** proposed here —
   it is a wire-visible string and belongs to whoever owns compat messages.)

## Scope boundary

**In scope — exactly eight commands**, all of which route through one of the five procedures:
`EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` (`expiry.rs`) and `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`,
`HPEXPIREAT` (`hash.rs`, via `execute_hexpire_common`).

**Out of scope, and the reason is structural, not preference** — these commands set expiry but
have **no NX/XX/GT/LT axis at all**, upstream or here, so there is no decision for the table to
own:

| Command | Verified | Why it is out |
|---|---|---|
| `GETEX` | `string.rs:424-529` | Option loop at `:451-465` / `:489-527` accepts only `EX PX EXAT PXAT PERSIST`; anything else is `SyntaxError`. No condition flags. |
| `SET … EX/PX/EXAT/PXAT` | `string.rs` | `SET`'s `NX`/`XX` are **key-existence** conditions on the *value* write, a different predicate with a different reply (`nil` vs `OK`). Folding it in would merge two unrelated meanings of the same two letters. |
| `PERSIST` | `expiry.rs:687-717` | Unconditional clear; `Arity::Fixed(1)` (`:693`) — no option slot exists. |
| `HPERSIST`, `HGETEX`, `HSETEX` | `hash.rs:1540`, `:1832`, `:1940` | Their expiry options parse through `parse_field_expiry_option` (`hash.rs:1638-1685`): `EX/PX/EXAT/PXAT/PERSIST/KEEPTTL`. No NX/XX/GT/LT. |

**Not in scope even though it is one keyword away:** `parse_hexpire_args` (`hash.rs:896-946`)
scans for the `FIELDS` keyword and treats everything before it as condition args, so
`HEXPIRE k 60 XX GT FIELDS …` is accepted here while upstream (`t_hash.c:3747-3760`) reads **at
most one** condition token at `argv[3]` and otherwise fails with `"Mandatory argument FIELDS is
missing or not at the right position"`. FrogDB accepts a **superset** of upstream's grammar for
the hash family. That is a *grammar* divergence in the parser, not a *decision* divergence in the
table; it is noted here so a reviewer does not think it was missed, and it is deliberately left
alone.

## Adjacent finding — flagged, not claimed, not in scope

While tracing the `Delete` branch's effects, the WAL strategy on the delete-capable expiry
commands looks wrong. The static chain, so a reviewer can check it in three minutes:

1. All four key-family specs declare `wal: WalStrategy::PersistFirstKey` — `expiry.rs:243`,
   `:339`, `:427`, `:511` — and all four can `ctx.store.delete(key)`. (`PERSIST` at `:697` uses
   the same strategy and correctly never deletes.) The four hash-family specs do the same
   (`hash.rs:1222` and siblings) and delete the key when the hash empties (`hash.rs:1160-1163`).
2. `WalStrategy::PersistFirstKey` maps to `WalAction::Persist(key)`
   (`core/src/command.rs:658-661`).
3. `WalAction::Persist` → `WalTarget::write_set` (`core/src/shard/persistence.rs:108`), whose
   production impl is **a no-op when the key is absent**: `if let Some(wal) = … && let Some(value)
   = self.store.get_hot(key)` (`:143-153`). The crate's own unit test
   `persist_always_writes_set` (`:539-553`) asserts a `Write::Set` against the *test* target for an
   absent key — the test double does not model the `get_hot` miss.
4. The commands that *do* delete their own first key use `WalStrategy::PersistOrDeleteFirstKey`
   (`list.rs:204`, `set.rs:94`, `blocking.rs:36`, `hash.rs:199` for `HDEL`, …) which writes a
   tombstone via the `contains` probe (`persistence.rs:116-122`).

Read together, that says `EXPIRE k -1` on a **key with no prior TTL** deletes it in memory and
records nothing durable, so a WAL replay restores it — permanently, since it has no deadline for
lazy expiry to catch. The mitigating case (a key that *had* a TTL) is already covered by
`FM-PERSISTENCE-036`.

**This is a static reading, it is not verified by execution, and it is not part of proposal 92.**
It is a persistence-durability claim, which means it is spec-first work: an `FM-PERSISTENCE-…` row
and a forcing test, owned by whoever holds the persistence area — not a one-word spec edit smuggled
in behind a commands refactor. Filed here because this proposal is the reason anyone looked.

## Risks / scope boundaries vs siblings

**vs proposal 93 (hash-field-expiry dual bookkeeping) — the boundary is decision vs storage.**
`execute_hexpire_common` maintains **two** records of the same fact: the per-hash entry
(`hash.set_field_expiry` / `hash.remove`, `hash.rs:1133-1143`) and the store-level index
(`ctx.store.set_field_expiry` / `remove_field_expiry`, `:1148-1158`), written in two separate loops
with a `drop`(`:1042`)/`get_hash_mut`(`:1128`) re-borrow between them, plus the empty-hash key
delete at `:1160-1163`. **Every one of those lines is 93's, and 92 does not touch them.** 92 changes only *which verdict*
each field carries into the mutation loops; the loops themselves, their order, their borrow
structure and their atomicity are byte-identical before and after. Concretely: 92 replaces the
local `enum FieldAction` (`:1045-1050`) with the shared type and rewrites the `match &actions[i]`
arms' *patterns*, not their *bodies*. If 93 lands first, 92 rebases onto whatever verdict-consuming
shape 93 leaves behind; if 92 lands first, 93 inherits a named, tested verdict type to restructure
around. **Neither ordering blocks the other**, and the shared file (`hash.rs`) is the only contact
point.

**vs proposal 90 (`CommandSpec::DEFAULT`, solo-last sweep of `frogdb-commands`) — real conflict,
ordering required.** 90 rewrites the field lists of all 296 spec statics, including the eight this
proposal's commands own (`expiry.rs:237-253`, `:333-350`, `:421-438`, `:505-522`; `hash.rs:1217`
and its three siblings). 92 does **not** edit any spec static — it edits `execute` bodies, the
condition parsers, and the module list in `lib.rs:35`. The overlap is therefore *file-level, not
line-level*, in `expiry.rs`, `hash.rs` and `lib.rs`. Since 90 is declared solo-last for the crate,
**92 lands first and 90 sweeps over it**; 90 gains one more file to sweep (`expiry_decision.rs`
declares no `CommandSpec`, so in fact it gains none). The one line both want is `lib.rs`'s module
list — a one-line append, trivially mergeable.

**vs proposal 83 (lazy-expiry authority).** 83 owns *when a deadline is noticed* (the read path,
the sweeper, the store's purge-on-access contract). 92 owns *what a deadline-setting command
decides*. They meet at exactly one assumption — that a key reaching the decision point is live, so
`current` is never already elapsed — which 92 states as a `debug_assert!` plus a unit test rather
than as a silent dependency. If 83 changes that contract, 92's assertion is where it fires.

**vs proposal 89 (chunk codec).** No contact: different crates, different files.

**Behavioral risk of the fix itself.** The five corrected cells strictly *remove* a mutation
(delete → no-op) and change one reply integer each. Nothing new enters the write-effect pipeline:
`WRITE_EFFECT_ORDER` (`core/src/shard/post_execution.rs:282-292`) runs the same effects on strictly
fewer writes, replicas receive the same verbatim command and reach the same verdict from the same
table, and no keyspace notification is added. The regression risk is the inverse case — a client
relying on `EXPIRE k -1 GT` deleting. That is a client relying on a documented-elsewhere upstream
behavior being wrong; the pre-production policy in `CLAUDE.md` settles it.

**Feature-flag risk: none.** `expiry` (`lib.rs:35`) and `hash` (`:39`) are unconditional modules —
no `cmd-full`/`json`/`stream` gating — so the new module compiles and its tests run under the
default `core-profile` and no iteration loop has to alternate features.

## Honesty about verification

The upstream side of §The ordering divergence was verified by **reading Redis 8.0 source**
(`src/expire.c` `expireGenericCommand`; `src/t_hash.c:519-570` `hashTypeSetExpiryListpack` and
`:3730-3800` `hexpireGenericCommand`) fetched at authoring time. The FrogDB side was verified by
reading the code at `25d3873`. **No server was started and no real-server differential was run** —
per the lane instruction. Every claim in this document is therefore a *code-order* claim, which is
strong enough to justify the failing test but **not** a substitute for it: the implementation must
begin by writing the five regression rows in §Testability and watching them fail, and if any of
them passes, the corresponding row in the divergence table is wrong and comes out of this document
rather than being explained away.

## Effort

**M overall.** The split:

| Part | Size | Notes |
|---|---|---|
| Module + reply table | **S** | ~130 lines, no dependencies, no I/O |
| Unit tests for the table | **S** | ~180 lines, mechanical enumeration |
| Four key-family adapters | **S** | Each `execute` loses ~25 lines, gains ~10; no signature changes |
| `execute_hexpire_common` adapter | **S–M** | Also narrows two parameters to one and deletes the unreachable arm; the two mutation loops are untouched |
| Parser fold (two → one) | **S** | Four call sites pass `&args[2..]` |
| **The behavior fix + 5 regression rows** | **M** | The only part that is not mechanical: five wire-visible cells change, each needs a reply assertion *and* a survival assertion, and the write-effect reasoning above must be re-checked against a real run |

Single-crate iteration throughout (`just check frogdb-commands`, `just test frogdb-commands`), with
`just test redis-regression` for the added rows. No mutation-gate obligation — `frogdb-commands` is
not a locked crate and no `FM-` tag exists anywhere in `crates/commands/src`.

## Independently-landable hotfixes

**H1 — the ordering fix, without the refactor. S. Recommended to land first, separately.**
Move the delete branch below the condition checks at all five sites: in `expiry.rs`, relocate
`:297-300` below `:313-318` (and `:386-389`, `:464-467`+`:471-475`, `:548-551`+`:555-559`
correspondingly); in `hash.rs`, move `:1062-1067` and `:1078-1083` below `:1120`. Roughly 30 moved
lines plus the five regression rows. **It is a pure reordering with no new abstraction**, it fixes
data loss, and it is reviewable in one screen — so it should not wait on the M-sized refactor. The
refactor then lands as a pure no-op restructuring with the corrected order already pinned by
tests, which is the ideal shape for review.

**H2 — parser fold. S, zero behavior change.** Collapse `parse_expire_conditions` /
`parse_expire_conditions_from_slice` (`expiry.rs:181-227`) into one function over an
already-sliced `&[Bytes]`; four call sites pass `&args[2..]`. 14 duplicated lines removed. Lands
independently of everything above.

**H3 — delete the unreachable arm. S, zero behavior change.** Remove `hash.rs:1069-1075` and
narrow `time_converter` to a non-`Option` return, or (smaller still) leave the signature and just
delete the arm with the `is_past_or_zero` precondition cited. Independent of H1/H2.

**Not a hotfix: the WAL strategy in §Adjacent finding.** It is spec-first persistence work and
must not ride along.

## References

- `frogdb-server/crates/commands/src/expiry.rs:126-227` (conditions + twin parsers), `:287-321`,
  `:376-408`, `:453-492`, `:537-576` (the four key-family decisions)
- `frogdb-server/crates/commands/src/hash.rs:988-1160` (`execute_hexpire_common`), `:1237`,
  `:1283`, `:1329`, `:1375` (the four adapters)
- `frogdb-server/crates/commands/src/string.rs:424-529` (`GETEX` — scope boundary)
- `frogdb-server/crates/core/src/command.rs:645-670`,
  `frogdb-server/crates/core/src/shard/persistence.rs:106-160` (§Adjacent finding chain)
- `frogdb-server/crates/redis-regression/tests/expire_tcl.rs` (58 tests),
  `.../hash_field_expire_tcl.rs` (39 tests)
- Redis 8.0 `src/expire.c` `expireGenericCommand`; `src/t_hash.c:519-570`, `:3730-3800`
- Sibling proposals: `90-commandspec-default.md` (crate-conflict, solo-last),
  `83-lazy-expiry-authority.md` (read-path boundary), 93 (hash-field storage bookkeeping, pending)
