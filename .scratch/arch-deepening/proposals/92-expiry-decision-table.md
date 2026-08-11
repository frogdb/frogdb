# Proposal 92 — one `ExpiryDecision` table: NX/XX/GT/LT is written out five times, and three of the five put the delete branch on the wrong side of the conditions

Round 38 · lane: commands + types · candidate **CT4** · effort **M** ·
**no locked crate edited** (`frogdb-commands` is not one of the four locked areas; `grep -rn "FM-"
frogdb-server/crates/commands/src` returns **nothing**) · **no seam gate constrains the change**
(the new module reads **no** clock — `now` is a parameter — so `lint-clock-seam` is not merely
satisfied but structurally inapplicable; `lint-no-typed-unwrap` is unaffected)

**Verified at HEAD `25d3873642b31162dd21965cffdf685048ac518d`, re-verified for revision 2 at
`dd840ca3bb3a70319c424d62885e753e51abfdf5`** (worktree `arch-round-38-99`, branch
`worktree-arch-round-38-99`). `git diff --stat 25d3873 dd840ca3 -- frogdb-server/crates/{commands,
core,types,redis-regression,server}` is **empty** — every code line cited below is byte-identical
at both SHAs; the intervening commits are proposal markdown only. Nothing is inherited from the
lane brief.

**Revision 2 — what the adversarial review changed.** The core defect claim survived intact
(the ordering inversion is real at all five sites; three of five cells diverge; the refactor is
worth doing), and the verified inventory — 46/30 reply counts, the twin-parser diff, the five-site
census, the sibling boundaries vs 90/93/83, the condition-axis scope, the write-rows-first
discipline — is unchanged. Three design claims were **refuted and are retracted here**:

1. **The `if result { 1 } else { 0 }` else arm is *not* unreachable** (§Problem 4). `set_expiry`
   returns `false` *and deletes* on a past-deadline-unswept key. Revision 1's adapter sketch would
   have replied `1` there — a behavior regression **and** a lie. The reply table now carries that
   answer (§The reply table).
2. **The `debug_assert!` that `current` is never already elapsed is false and is deleted**
   (§The module). Every accessor on the path is raw by design, and `FM-PERSISTENCE-044` does not
   cover the `EXPIRE` family. The assert would fire on `SET k v PX 5` / wait / `EXPIRE k 100 GT`
   in every debug build.
3. **H1 is not "pure reordering with no new abstraction"** (§Independently-landable hotfixes).
   Moving the delete below GT/LT forces the elapsed-vs-`At` distinction inline at all five sites,
   or the reordered code panics on negative operands. H1 is **S–M**, and it must flip a regression
   test that currently *blesses* the divergence.

Revision 2 also **adds a sixth divergent cell** — hash-field **resurrection**, which is data
corruption rather than a wrong reply code (§The ordering divergence) — narrows H3, widens the WAL
adjacent finding, and corrects several line ranges.

**The headline is not the one the brief expected.** The brief flagged a suspected HEXPIRE-only
compat divergence. Verified against upstream source, the divergence is **real, larger, and spans
both command families**: the delete-on-past-deadline branch sits *before* the condition checks in
all five copies, where Redis puts it *after* all four of them. Five decision cells across
`EXPIRE`/`EXPIREAT`/`HEXPIRE`/`HPEXPIREAT` reply the wrong integer **and destroy user data the
condition was written to protect** — and a sixth, found in revision 2, goes the other way and
**resurrects an already-dead hash field with a fresh TTL**. The refactor is worth doing on
locality grounds alone; the ordering defect is what makes it urgent, and it is exactly the class of
bug a decision *table* cannot express.

## Corrections to the lane brief

| Brief claim | Verified at `25d3873` |
|---|---|
| "`parse_expire_conditions` has twins at `expiry.rs:181` and `:205`" | **Line numbers correct, characterization stale.** The *flag-scanning loop* was already extracted into `parse_expire_condition_flags` (`expiry.rs:152-177`) and both twins call it. What is still duplicated is the **14-line mutual-exclusion block**, byte-for-byte identical at `:186-199` and `:212-225` (verified by exact comparison). The twins differ in exactly one token: `ArgParser::from_position(args, 2)` (`:184`) vs `ArgParser::new(args)` (`:210`). **No** case-sensitivity or error-string drift — both messages are identical strings. This part of the brief is a **small** finding, not the case for the proposal. |
| "the apply/decision block is repeated across EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT, plus a per-field copy in `execute_hexpire_common`" | **Verified, 5 copies**: `expiry.rs:287-321`, `:376-409`, `:453-493`, `:537-577`, `hash.rs:1062-1123`. (Revision 1 wrote the last three as `:376-408`/`:453-492`/`:537-576`, stopping one line short of each block's terminal `Ok(Response::Integer(if result …))` at `:409`/`:493`/`:577`.) |
| "`HEXPIRE k 0 NX` … may return 2 where Redis returns 0 … verify against OUR code order" | **Verified in code-order terms, and it is worse than stated.** In `execute_hexpire_common` **all four** conditions sit below the delete branch (`hash.rs:1062-1083` vs `:1085-1120`), so NX, XX *and* GT all diverge. And the same inversion exists in the **key** family for GT (`expiry.rs:297-300` before `:304-318`) — which the brief did not suspect at all. Full cell-by-cell table in §The ordering divergence. |
| effort **M** | **M confirmed**, but the split is not what the brief assumed: the mechanical refactor is S; the M comes entirely from the **behavior fix** and its regression tests (§Effort). |
| "Latent" | **Refuted. Live.** Five of the six divergent cells silently delete a key/field that upstream preserves, and the sixth **resurrects an already-dead hash field with a fresh TTL** (§The ordering divergence). |

Two findings the brief did not name: **46 hand-written reply constructions** implement what is a
**10-cell** mapping (§Problem 4), and `execute_hexpire_common` carries **two dead degrees of
freedom** — an unreachable `None` arm and a `time_converter`/`is_past_or_zero` pair that encode the
same predicate twice (§Problem 5).

## Summary

Eight commands — `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `HEXPIRE`, `HPEXPIRE`,
`HEXPIREAT`, `HPEXPIREAT` — answer one question: *given the condition flags, the current deadline,
and the proposed deadline, do I set it, leave it alone, or delete the thing?* That question has
**three** answers, **five** outcomes (the third answer splits on whether the store accepted the
write) and **ten** reply cells. It is currently answered by **five separately
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

— plus a 10-cell reply table `ExpiryFamily::{Key,HashField}::reply(outcome) -> i64`. The five
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
| `frogdb-server/crates/commands/src/expiry.rs` | 817 | **Primary.** Four decision blocks (`:287-321`, `:376-409`, `:453-493`, `:537-577`) collapse to adapter calls. The two condition parsers (`:181-201`, `:205-227`) fold to one. `ExpireConditions` (`:126-146`) moves to the new module and gains derives. **Zero `FM-` tags; zero `#[cfg(test)]` modules** in the whole file. Recent churn: 8 commits, most recently `2fb1051c` (clock-seam sweep) and `00dfb0ab` (expiry clock seam). |
| `frogdb-server/crates/commands/src/expiry_decision.rs` | **new, ~130 + ~180 test** | **Primary.** `ExpireConditions`, `NewDeadline`, `ExpiryDecision`, `ExpiryOutcome`, `ExpiryFamily`, `evaluate`, `reply`. No `use frogdb_core::clock` — the module reads no clock and touches no store. |
| `frogdb-server/crates/commands/src/hash.rs` | 2327 | **Primary.** `execute_hexpire_common:1062-1123` (the per-field decision) becomes one `evaluate` call; the local `enum FieldAction` (`:1045-1050`) is replaced by the shared verdict; the `time_converter: impl Fn(i64) -> Option<Instant>` parameter (`:994`) absorbs `is_past_or_zero` (`:995`) and loses its wrong `None` arm (`:1069-1075`); the `Option` itself **stays** until the `EXPIREAT` overflow guard exists (§Problem 5). The four adapters at `:1237`, `:1283`, `:1329`, `:1375` each shed one closure. **Zero `FM-` tags.** Recent churn: 8 commits. |
| `frogdb-server/crates/commands/src/lib.rs` | 473 | **Primary.** One line: `pub(crate) mod expiry_decision;` beside `pub mod expiry;` (`:35`). No feature gate — both `expiry` and `hash` are unconditional modules, so the new module and its unit tests build under the default `core-profile`. |
| `frogdb-server/crates/redis-regression/tests/expire_tcl.rs` | 908 | **Primary (additive).** 58 tests today; **no test combines a past/negative operand with `GT`** — the gap that let the key-family divergence live. Adds the rows from §The ordering divergence. |
| `frogdb-server/crates/redis-regression/tests/hash_field_expire_tcl.rs` | 1718 | **Primary — additive *and* one edit.** 39 tests today; `HEXPIRE … 0 …` with a condition flag is absent, so the hash-family rows are added. **Not purely additive**: `tcl_hpexpireat_field_not_exists_or_past` (`:305-338`) currently **asserts the divergence and blesses it in a comment** — `assert_integer_eq(&arr[3], 2)` at `:337` under `// FrogDB deletes fields with past time regardless of NX condition` (`:336`). The fix flips that `2` → `0` and deletes the two comment lines. |
| `frogdb-server/crates/core/src/store/hashmap.rs` | — | **Read-only evidence — not edited.** `set_expiry:1220-1234` (the `check_and_delete_expired` guard that makes the `if result` ternary live), `delete:952-954` (raw `uninstall` — what makes the `if deleted` ternary dead), `contains:957-959`, `get_expiry:1236-1238`, `set_field_expiry:1364-1370` (unconditional). |
| `frogdb-server/crates/types/src/types/hash.rs` | — | **Read-only evidence — not edited.** `contains:341-346` and `get_field_expiry:537-539` are raw lookups; `set_field_expiry:518-521` inserts unconditionally — together the resurrection path in §The ordering divergence. |
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
| `PEXPIRE` | `expiry.rs:376-409` | identical modulo `secs`→`ms` and `from_secs`→`from_millis` |
| `EXPIREAT` | `expiry.rs:453-493` | same, plus a second delete branch for `expires_at <= now` |
| `PEXPIREAT` | `expiry.rs:537-577` | identical to `EXPIREAT` modulo `unix_secs_to_instant`→`unix_ms_to_instant` |
| `HEXPIRE` family | `hash.rs:1062-1123` | **delete if past** → **delete if `expires_at <= now`** → NX → XX → GT → LT → `SetExpiry` |

**Proof of identity, EXPIRE vs PEXPIRE** (`:287-321` against `:376-409`) — the entire diff is
three lines, two of them the unit and one of them a **lost comment**:

```
-        if seconds <= 0 {                                       |  +        if ms <= 0 {
-        let expires_at = clock::now() + Duration::from_secs(…); |  +        … Duration::from_millis(…);
-        // GT on key without TTL: return 0 (Redis behavior: …)  |  (absent)
```

**Proof of identity, EXPIREAT vs PEXPIREAT** (`:453-493` against `:537-577`) — the entire diff is
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

**A sixth cell, and it is the worst one: hash-field resurrection.** The five rows above are wrong
*replies* attached to a delete upstream would not perform. This one is a **write that brings dead
data back**, and it is reached through the *condition* side rather than the delete side:

```
HSET  h f v
HPEXPIRE h 5 FIELDS 1 f          # field deadline 5 ms out
… wait 10 ms, before the field sweeper runs …
HEXPIRE h 100 GT FIELDS 1 f      # → FrogDB: 1, field alive again with a fresh 100 s TTL
```

Every step is verified in code. `execute_hexpire_common` **never calls
`purge_expired_hash_fields`** — grep across `hash.rs:980-1170` returns nothing, in direct contrast
to its read-side sibling `execute_httl_common`, which purges as its first act (`hash.rs:1178`).
So `hash.contains(f)` (`types/hash.rs:341-346`, a raw listpack/map probe) answers **true** for the
logically-dead field, and `ctx.store.get_field_expiry(key, f)` (`hashmap.rs:1380-1382`, a raw index
read) hands back a **past `Instant`**. `GT` then compares `now + 100 s` against that past instant,
**passes**, and the field lands in `FieldAction::SetExpiry`. The mutation loop writes it
unconditionally on both sides of the dual bookkeeping — `hash.set_field_expiry`
(`hash.rs:1139` → `types/hash.rs:518-521`, a bare `insert`) and `ctx.store.set_field_expiry`
(`hash.rs:1154` → `hashmap.rs:1364-1370`, a bare index `set`). Neither has an is-it-still-alive
guard. Upstream lazily expires the field on access, so the same sequence there answers `-2` and
leaves nothing behind.

| Case | Upstream | FrogDB at `25d3873` | Where |
|---|---|---|---|
| `HEXPIRE h 100 GT FIELDS 1 f`, field's **existing** deadline already passed, sweeper has not run | `-2`, field stays gone (*medium confidence* — see below) | **`1`, field resurrected with a fresh 100 s TTL** | no purge in `hash.rs:988-1126`; `hash.rs:1139`/`:1154` write unconditionally |

**Confidence is deliberately asymmetric here.** The FrogDB side is high confidence — every line is
cited above. The **upstream** side is **medium**: `t_hash.c`'s lazy-expiry-on-access path was not
re-read at revision-2 authoring time, so "upstream replies `-2`" is inferred from its documented
lazy-expiry contract rather than from a line cite. The implementation must therefore write this row
as an **exploratory** test first and let the observed FrogDB value stand on its own: even if
upstream turns out to answer something other than `-2`, *resurrecting a field whose deadline has
passed* is wrong against FrogDB's own expiry contract, and that is the assertion that matters.

**The key family escapes this by accident, not by design.** `EXPIRE k 100 GT` on a past-deadline-
unswept key runs the identical logic — raw `contains` (`hashmap.rs:957-959`), raw `get_expiry`
(`:1236-1238`), `GT` passes against the stale past deadline — and is saved only because
`HashMapStore::set_expiry` opens with `check_and_delete_expired` (`hashmap.rs:1220-1224`), which
deletes the key and returns `false`, so the reply is `0` and nothing is resurrected. The hash-field
setters have no such guard. Any future change that drops the guard turns the key family into the
same bug; a decision module that is *told* whether `current` is already elapsed is what makes that
structural rather than incidental (§The module).

Two cells that look divergent and **are not**, checked and cleared so the fix does not overshoot:

- `EXPIRE k -10 LT` — upstream's `LT` passes when there is no TTL *and* when the past `when` is
  below a live TTL, so it reaches `checkAlreadyExpired` and deletes: `1`. FrogDB: `1`. **Agrees**,
  and both existing regression tests (`expire_tcl.rs:881`, `:891`) stay green.
- `EXPIRE k -10 NX` / `XX` — NX/XX already sit above the key-family delete branch, so those two
  cells were already right. **Only GT moves in the key family.**

Why the tests missed it — **and, in one case, why they did not miss it at all.** `expire_tcl.rs`
has 58 tests including three dedicated `GT` cases (`tcl_expire_with_gt_option_on_key_with_lower_ttl`
`:702`, `…_higher_ttl` `:716`, `…_without_ttl` `:730`) — all with **future** operands — and two
negative-operand cases (`:876-883`, `:886-893`) — both with **`LT`**, both asserting `1` + `TTL -2`,
both of which stay green under the fix. The product cell (`negative operand` × `GT`) is exactly the
one nobody wrote.

`hash_field_expire_tcl.rs` has 39 tests and never combines a **zero** operand with a condition
flag — but it **does** combine a past *absolute* stamp with `NX`, and it **pins the divergent
answer**:

```rust
// hash_field_expire_tcl.rs:317-337 — tcl_hpexpireat_field_not_exists_or_past
let past_ms = format!("{}", (now_secs() - 1) * 1000);
… HPEXPIREAT myhash <past_ms> NX FIELDS 4 f1 f2 f3 f4 …
assert_integer_eq(&arr[3], 2); // f4 deleted despite having TTL (past time overrides NX)
```

with the line above it reading `// FrogDB deletes fields with past time regardless of NX condition`
(`:336`). **So revision 1's claim that "no existing test pins the divergent behavior" is retracted**:
one does, and it blesses it in a comment. Under the corrected order `f4` — which has a live TTL from
`HEXPIRE myhash 1000 NX FIELDS 1 f4` at `:313-315` — fails `NX` and must answer `0` with the field
untouched. `arr[0]`/`arr[1]` (`f1`, `f2`, no TTL ⇒ `NX` satisfied ⇒ past ⇒ delete) stay `2`, and
`arr[2]` (`f3`, absent) stays `-2`. Exactly one assertion changes, and the two comment lines come
out with it.

**This is the argument for a table.** Five procedures × four flags × **three** `current`-states
(none / future / already-elapsed) × two deadline-classes is a 120-cell space explored today by
reading straight-line code; a decision table makes it a 16-row `match` an adversary can read in one
screen and a test can enumerate. Revision 1 wrote "two TTL-states" and got 80 — the third state is
the one that produced the resurrection cell, which is precisely the point.

### 4. Forty-six reply constructions for a ten-cell mapping

`Response::Integer(…)` occurrences inside the five procedures, counted exactly:

| Procedure | count |
|---|---|
| `EXPIRE` `:258-322` | 8 |
| `PEXPIRE` `:354-410` | 8 |
| `EXPIREAT` `:442-494` | 9 |
| `PEXPIREAT` `:526-578` | 9 |
| `execute_hexpire_common` `:988-1160` | 12 |
| **total** | **46** |

The mapping they implement has **ten** cells — revision 1 said eight and was wrong by one row
(see the ternary analysis below):

| outcome | `ExpiryFamily::Key` | `ExpiryFamily::HashField` |
|---|---|---|
| `Missing` (no such key / no such field) | `0` | `-2` |
| `Skipped` (condition not met) | `0` | `0` |
| `Applied` | `1` | `1` |
| `VanishedUnderApply` (the store swept the key while applying) | `0` | *unreachable — see below* |
| `Deleted` (deadline already passed) | `1` | `2` |

Thirty of the 46 are the literal `0`, written out at 30 sites that must all agree. Six are
`if deleted { 1 } else { 0 }` (`:299`, `:388`, `:466`, `:474`, `:550`, `:558`) and four are
`if result { 1 } else { 0 }` (`:321`, `:409`, `:493`, `:577`). **The two families of ternary are
not the same thing, and revision 1 collapsed them — the correction is load-bearing for the reply
table.**

- **The six `if deleted` ternaries are genuinely dead.** `HashMapStore::delete` is a raw
  `uninstall(key).is_some()` (`hashmap.rs:952-954`) with no expiry guard, and `contains` is a raw
  `data.contains_key` (`:957-959`), established a few lines above at `:283`, `:372`, `:449`,
  `:533` on a single-threaded shard. Nothing between the probe and the delete can remove the key,
  so `deleted` is always `true`. Upstream states the same fact as
  `serverAssertWithInfo(c, key, deleted)`. **This half of revision 1's claim stands.**
- **The four `if result` ternaries are live, and the `else` arm is the one that matters.**
  `HashMapStore::set_expiry` opens with `if self.check_and_delete_expired(key) { return false; }`
  (`hashmap.rs:1220-1224`). A key that is present but **past its deadline and not yet swept** —
  reachable because `contains`/`get_expiry` are both raw — is therefore **deleted by `set_expiry`,
  which then answers `false`**, and the command replies `0`. Revision 1's adapter sketch mapped
  every non-`Skip`/non-`Delete` verdict to `ExpiryOutcome::Applied` ⇒ `1`, which would reply **`1`
  for a key that was just destroyed and had no expiry set** — a wire-visible regression *and* a
  false statement about the store's state. The table must carry the store's boolean answer, hence
  the fifth outcome above.

  `VanishedUnderApply` maps to `0` for the key family, byte-identical to today. It is unreachable
  for the hash family because the two field-expiry setters (`types/hash.rs:518-521`,
  `hashmap.rs:1364-1370`) return `()` and write unconditionally — which is precisely the
  resurrection bug in §The ordering divergence, and precisely why the outcome deserves a name
  rather than a silent `bool`. If 83 later gives the field setters the same guard, the hash column
  gains a real value here instead of a fifth open-coded branch.

Folding them into the table replaces ten open-coded expressions with one exhaustive `match` — and,
unlike revision 1's version, it does so **without changing a single reply integer**.

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

**How thoroughly dead, exactly** — this matters for H1, because reordering makes the arm live
again. `execute_hexpire_common` rejects negative operands outright at `hash.rs:1001-1005`
(`"invalid expire time, must be >= 0"`), *above* the loop. So:

- For `HEXPIRE`/`HPEXPIRE` the only operand that can still reach `is_past_or_zero` and satisfy it
  is **literally `0`** — the `< 0` half of `secs <= 0` is already unreachable.
- For `HEXPIREAT`/`HPEXPIREAT` the predicate is `ts < 0`, which the guard has *already* excluded:
  their `is_past_or_zero` closures (`hash.rs:1341`, `:1387`) are **doubly dead**. Every past
  absolute stamp in the hash family is therefore caught by the *other* delete branch,
  `expires_at <= clock::now()` at `:1079-1083` — which also sits above all four conditions, so the
  divergence survives the death of the first branch.

A second, **narrower** finding — revision 1 overstated this one and it is corrected here.
`unix_secs_to_instant` (`expiry.rs:37-47`) and `unix_ms_to_instant` (`:50-60`) do return `Some` on
**both** arms, so the two `.ok_or(CommandError::NotInteger)?` guards at `:469` and `:553` are dead
*as written*. But "never returns `None`" is **not** the same as "total": both bodies add a
`Duration` to a `SystemTime` and then to an `Instant` (`:38`+`:42`, `:51`+`:55`), and
`Add<Duration>` **panics** on overflow rather than returning `None`. `EXPIREAT`/`PEXPIREAT` have
**no big-operand guard at all** — contrast `EXPIRE`'s two-stage guard at `expiry.rs:263-278` and
`PEXPIRE`'s at `:363-367`; `EXPIREAT`'s `execute` goes `parse_i64` → conditions → `contains`
(`:442-453`) with nothing in between. By static reading, `EXPIREAT k 9223372036854775807` reaches
`:42` and adds ~9.2×10¹⁸ seconds to a monotonic base, which cannot be represented — a **shard
panic**, not a `NotInteger` error.

Two honesty notes on that claim. (a) It is **unexecuted** — a static reading of `std`'s `Add`
impls, not an observed crash. (b) The *millisecond* variant is materially safer and is already
exercised green: `expire_tcl.rs:555-567` (`tcl_pexpireat_with_big_integer_works`) sends
`PEXPIREAT foo 9223372036854770000` and asserts `1`, because `i64::MAX` **milliseconds** is only
~9.2×10¹⁵ seconds. The seconds-unit `EXPIREAT` is the exposed one; `expire_tcl.rs:486-530` covers
big operands for `EXPIRE` and `PEXPIRE` only. **Consequence for this proposal: the `Option` may not
simply be deleted from these two converters as a cleanup** (§H3) — the guard it stands in for has
to exist first, and that is filed as its own issue by the orchestrator, not fixed here.

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

/// What the caller must do. Exhaustive; there is no fourth *verdict*.
/// (The reply side has five *outcomes*, because `Apply` splits on whether the
/// store accepted the write — see `ExpiryOutcome`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpiryDecision { Apply(Instant), Skip, Delete }

impl ExpiryDecision {
    /// Upstream order, stated once: **all four conditions, then the
    /// already-elapsed delete.** (`expire.c` expireGenericCommand;
    /// `t_hash.c` hashTypeSetExpiryListpack.)
    pub(crate) fn evaluate(
        cond: ExpireConditions,
        // None = no TTL = "infinite", per upstream's -1.
        // Some(t) with `t <= now` is REACHABLE: every accessor on the callers'
        // path is raw, so a past-deadline-unswept key/field arrives here. See
        // "the already-elapsed `current`" below — this is not a precondition.
        current: Option<Instant>,
        new: NewDeadline,
        now: Instant,
    ) -> Self { … }
}
```

`evaluate`'s body is one flat sequence over a 3 (`current` state: none / future / already-elapsed)
× 4 (flag) product plus the terminal elapsed check — ~20 lines, no `?`, no allocation, no I/O:

- `nx && current.is_some()` → `Skip`
- `xx && current.is_none()` → `Skip`
- `gt` → `Skip` when `current.is_none()`, or when `new` is not strictly after `current`
- `lt` → `Skip` when `current.is_some_and(|c| new` is not strictly before `c)`
- then: `new` elapsed (either `Elapsed`, or `At(t)` with `t <= now`) → `Delete`
- else → `Apply(t)`

**The already-elapsed `current` — revision 1's precondition was false and is retracted.**
Revision 1 asserted that `current` can never be at or before `now`, backed it with a
`debug_assert!`, and cited `FM-PERSISTENCE-044` as the store-side statement of the same fact.
All three are wrong, and the assert would **fire in every debug build** on
`SET k v PX 5` / wait 10 ms / `EXPIRE k 100 GT`:

| Accessor on the decision path | Behavior | Cite |
|---|---|---|
| `Store::contains` | **raw** by design — its doc explicitly contrasts `exists_unexpired`, "a key past its TTL reads as absent even if active/lazy expiry has not yet removed it", which `contains` does *not* do | `core/src/store/mod.rs:410-411` declaring it, `:413-423` documenting the contrast; impl `hashmap.rs:957-959` is a bare `data.contains_key` |
| `Store::get_expiry` | **raw** — `self.data.get(key).and_then(…)`, no deadline comparison | `hashmap.rs:1236-1238` |
| `Hash::contains` | **raw** listpack/map probe | `types/src/types/hash.rs:341-346` |
| `Store::get_field_expiry` | **raw** index read | `hashmap.rs:1380-1382` |
| lazy purge on the write path | **`execute_hexpire_common` never calls `purge_expired_hash_fields`** — contrast `execute_httl_common`, which purges as its first statement | `hash.rs:988-1126` (absent) vs `hash.rs:1178` |

And `FM-PERSISTENCE-044`'s Observable row
([`persistence-failure-modes.md:626-636`](../../hardening/specs/persistence-failure-modes.md))
enumerates `PERSIST`, `RENAME`, `RENAMENX`/`EXISTS`, `TYPE`, `EXPIRETIME`/`PEXPIRETIME` — the
**`EXPIRE` family is not in it**. That row is about read-through and `PERSIST`; it makes no
statement about what `EXPIRE … GT` observes. The citation is withdrawn.

So `evaluate` must **handle a past `current` explicitly**, and the honest reading is that this is a
*policy* question, not an implementation detail:

- **Option A (behavior-preserving, the default for this proposal).** `current` is compared as-is;
  a past `current` simply loses `GT` comparisons the way any small instant does. This reproduces
  today's key-family behavior exactly (the store's `set_expiry` guard converts the resulting
  `Apply` into `VanishedUnderApply`/`0`) and today's hash-family behavior exactly — **including the
  resurrection bug**. 92 then does not fix that bug; it *names* it, at one site, with a test that
  says so.
- **Option B (fix it here).** The adapters switch to `exists_unexpired` / call
  `purge_expired_hash_fields` before gathering, so an elapsed `current` never reaches `evaluate`
  and the resurrection cell disappears. This is a **lazy-expiry-authority** decision: it changes
  which commands purge on access, which is exactly the contract
  [proposal 83](83-lazy-expiry-authority.md) owns.

**This proposal does not decide between them — 83 does.** What 92 guarantees is that whichever
answer 83 picks is expressed **once**, in one function signature, instead of being an emergent
property of five raw-accessor call sequences. Until 83 rules, 92 implements Option A and pins the
resurrection cell with an exploratory test (§Testability), so the behavior is *recorded* rather
than *assumed*.

### The reply table

```rust
pub(crate) enum ExpiryOutcome {
    Missing,
    Skipped,
    Applied,
    /// The verdict was `Apply`, but the store answered `false`: the key was
    /// present-but-past-deadline and `set_expiry`'s `check_and_delete_expired`
    /// guard (`hashmap.rs:1220-1224`) removed it instead. Nothing was set and
    /// the key is gone — the reply must say so.
    VanishedUnderApply,
    Deleted,
}
pub(crate) enum ExpiryFamily { Key, HashField }

impl ExpiryFamily {
    pub(crate) fn reply(self, outcome: ExpiryOutcome) -> i64 {
        match (self, outcome) {
            (ExpiryFamily::Key,       ExpiryOutcome::Missing)            => 0,
            (ExpiryFamily::HashField, ExpiryOutcome::Missing)            => -2,
            (_,                       ExpiryOutcome::Skipped)            => 0,
            (_,                       ExpiryOutcome::Applied)            => 1,
            (ExpiryFamily::Key,       ExpiryOutcome::VanishedUnderApply) => 0,
            // Unreachable today: the field setters return `()` and never
            // refuse. Stated rather than `unreachable!()` so that if 83 gives
            // them the store's guard, this is a value to choose, not a panic.
            (ExpiryFamily::HashField, ExpiryOutcome::VanishedUnderApply) => -2,
            (ExpiryFamily::Key,       ExpiryOutcome::Deleted)            => 1,
            (ExpiryFamily::HashField, ExpiryOutcome::Deleted)            => 2,
        }
    }
}
```

The `VanishedUnderApply` row is **not** an invention of this proposal — it is what
`if result { 1 } else { 0 }` (`expiry.rs:321`, `:409`, `:493`, `:577`) already computes today
(§Problem 4). Revision 1 dropped it on a false unreachability claim; keeping it is what makes the
table a re-expression rather than a rewrite.

**This is where the leverage claim lives, so it is worth being exact about what it buys and what
it does not.** It does *not* make the two families share a reply — they demonstrably differ in
**three of five** cells, and the enum says so. What it buys is that the difference is **one 10-row
match instead of 46 scattered literals**, that adding a third family (`HGETEX`/`HSETEX` already
reply on a third scale) is a variant plus five rows rather than a new procedure, and that a golden
test can assert all ten cells in ten lines. The reply codes stay exactly as they are today —
**this table is a pure re-expression; the fix in §The ordering divergence is what changes bytes on
the wire, and only in the named cells.**

### The five adapters

Each call site keeps everything the table is deliberately not given — arg parsing, overflow
guards, the store lookups, the mutation, the WAL/notification framing — and loses the decision:

```rust
// EXPIRE, after the existing overflow guards and `contains` check
let cond = parse_expire_conditions(&args[2..])?;

// ONE clock sample, used for both the deadline and the elapsed test. Two
// `clock::now()` calls would let the deadline be built against one instant and
// judged against a later one — the exact drift `now_pair` (expiry.rs:18-34)
// exists to eliminate. Revision 1's sketch sampled twice; that was a bug.
let now = clock::now();

// `Elapsed` is a class, not an instant: `seconds` may be negative, and
// `Duration::from_secs(seconds as u64)` on a negative value is ~1.8e19 seconds,
// which panics the `Add`. The branch must stay ABOVE the arithmetic.
let new = if seconds <= 0 { NewDeadline::Elapsed }
          else { NewDeadline::At(now + Duration::from_secs(seconds as u64)) };

let outcome = match ExpiryDecision::evaluate(cond, ctx.store.get_expiry(key), new, now) {
    ExpiryDecision::Skip      => ExpiryOutcome::Skipped,
    ExpiryDecision::Delete    => { ctx.store.delete(key); ExpiryOutcome::Deleted }
    // `set_expiry` returns false — and deletes — when the key is present but
    // past its (unswept) deadline. Preserve that answer; do not assume `true`.
    ExpiryDecision::Apply(at) => if ctx.store.set_expiry(key, at) {
        ExpiryOutcome::Applied
    } else {
        ExpiryOutcome::VanishedUnderApply
    },
};
Ok(Response::Integer(ExpiryFamily::Key.reply(outcome)))
```

`execute_hexpire_common` keeps its two-pass structure (gather under a shared handle, mutate under
`get_hash_mut`) unchanged — the local `FieldAction` becomes the shared `ExpiryDecision` and its
`NotFound` arm becomes `ExpiryOutcome::Missing`. The two time parameters collapse into one:
`time_converter: impl Fn(i64) -> Option<Instant>` **+** `is_past_or_zero: impl Fn(i64) -> bool`
become `deadline_of: impl Fn(i64) -> Option<NewDeadline>`, where the relative closures return
`Some(NewDeadline::Elapsed)` for a zero operand and the absolute ones return
`Some(NewDeadline::At(t))`. **The `Option` stays** — it is the surviving escape hatch for
`unix_secs_to_instant`'s (currently unreachable) `None`, which the AT variants must keep answering
as an error rather than as a silent `Skip`, and which cannot be dropped before the overflow guard
of §Problem 5 exists. What goes is the *duplicated predicate* and the wrong `Skip`/`0` arm at
`hash.rs:1069-1075`: a `None` now becomes `CommandError::NotInteger`, matching the key family's
`:469`/`:553` treatment instead of contradicting it.

The mutation loop is unchanged, so the hash family has no `VanishedUnderApply` path — the field
setters cannot refuse (§The reply table).

## Deletion test

Applied honestly. **Deleted:** ~200 lines of decision procedure across five sites, 14 lines of
cloned validation, one wrong-and-unreachable `None` arm (`hash.rs:1069-1075`), one of the two
redundant time parameters (`is_past_or_zero`), and 46 reply constructions → 5.
**Not deleted, contra revision 1:** the two
`.ok_or(CommandError::NotInteger)?` guards at `expiry.rs:469`/`:553`. They are dead as written, but
they sit on functions that panic rather than return `None` on overflow, and `EXPIREAT` has no
operand guard at all (§Problem 5) — removing the `Option` there is a *change of failure mode*, not
a cleanup, and it waits on the separately-filed overflow issue. **Added:** ~130 lines of module and
~180 lines of unit tests. **Net production lines: roughly −65. Net repo lines: roughly +115**, and
this proposal does not pretend that is a win by itself.

What is actually deleted is **the obligation to restate the condition order five times**, and the
measure is the derivative: adding `HGETEX`'s condition handling, or a sixth expiry command,
currently costs a sixth copy of a procedure that three of five existing copies got wrong; after
this it costs an `ExpiryFamily` variant and five table rows, and **cannot be added without a
compiler-enforced decision about all five outcomes**. A refactor scored on deleted lines would
fail here. The thing being removed is a **decision that is currently spelled five times and
spelled differently three of those times**.

## Testability improvement

1. **From zero to exhaustive.** `expiry.rs` has **no `#[cfg(test)]` module at all** at HEAD;
   every one of its 46 reply cells is reachable only via `TestServer::start_standalone()` + a RESP
   round trip in `redis-regression`. `evaluate` is a pure function of four `Copy` arguments, so the
   **full** condition space — 4 flags × **3** `current` states (none / future / **already
   elapsed**, the third being the one revision 1 wrongly assumed impossible) × 3 deadline classes
   (elapsed / at-or-before `now` / future) = 36 cells, plus the 5 legal flag combinations the
   parser admits — is a table test that runs in microseconds with no server, no store, no clock.
   The elapsed-`current` column is the one that cannot be written at all today without a sleep and
   a race against the sweeper.
2. **The precedent already exists in the same crate.** `hash.rs:2091-2130`
   (`mod expiry_grammar_pin_tests`) builds a `CommandContext` over a leaked `HashMapStore` to pin
   expiry *error strings* at the unit level. The decision table needs strictly less than that — no
   context at all.
3. **Determinism without the clock seam.** `now` is a parameter, so tests pass a fixed `Instant`
   and never touch `clock::now()`. The module is the rare one where `lint-clock-seam` has nothing
   to check because there is nothing to check.
4. **The divergent cells get named regression tests** in the existing upstream-derived files
   (`expire_tcl.rs`, `hash_field_expire_tcl.rs`), each asserting **both** the integer reply and
   the survival of the key/field — the reply alone would have caught only three of the five delete
   cells, and the data loss is the part that matters:

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

   **One of these rows is an edit, not an addition** —
   `hash_field_expire_tcl.rs:305-338` (`tcl_hpexpireat_field_not_exists_or_past`) asserts
   `arr[3] == 2` today under a comment that blesses the divergence. The fix flips it to `0`,
   deletes `:336-337`'s two comment lines, and adds an `HTTL`/`HEXISTS` survival assertion for
   `f4`. Any implementation that leaves that test green has not fixed anything.

   **A sixth row is exploratory, not a pin** — the resurrection cell:

   ```rust
   // hash_field_expire_tcl.rs — data resurrection, not just a wrong reply code
   client.command(&["HSET", "h", "f", "v"]).await;
   client.command(&["HPEXPIRE", "h", "5", "FIELDS", "1", "f"]).await;
   tokio::time::sleep(Duration::from_millis(50)).await;   // deadline passed, sweeper may not have run
   let arr = unwrap_array(client.command(&["HEXPIRE", "h", "100", "GT", "FIELDS", "1", "f"]).await);
   // TODAY: 1, and HGET h f returns "v" with a fresh 100 s TTL.
   // REQUIRED: the field stays dead — no reply may be paired with a live field.
   assert_integer_eq(&client.command(&["HEXISTS", "h", "f"]).await, 0);
   ```

   This one is **timing-sensitive by construction** (it races the field sweeper) and the exact
   integer depends on the Option A / Option B ruling in §The module, so it is written as an
   exploratory test whose *survival* assertion is the load-bearing half. If it turns out the
   sweeper always wins in the harness, the row is downgraded to a unit test on `evaluate` with an
   elapsed `current` and the regression row is dropped — recorded either way, never assumed.
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
| `GETEX` | `string.rs:424-529` | Option loop at `:451-465` / `:489-527` accepts only `EX PX EXAT PXAT PERSIST`; anything else is `SyntaxError`. No condition flags — **but see the elapsed-delete note below: it is out of scope, not correct.** |
| `SET … EX/PX/EXAT/PXAT` | `string.rs` | `SET`'s `NX`/`XX` are **key-existence** conditions on the *value* write, a different predicate with a different reply (`nil` vs `OK`). Folding it in would merge two unrelated meanings of the same two letters. |
| `PERSIST` | `expiry.rs:687-717` | Unconditional clear; `Arity::Fixed(1)` (`:693`) — no option slot exists. |
| `HPERSIST`, `HGETEX`, `HSETEX` | `hash.rs:1540`, `:1832`, `:1940` | Their expiry options parse through `parse_field_expiry_option` (`hash.rs:1638-1685`): `EX/PX/EXAT/PXAT/PERSIST/KEEPTTL`. No NX/XX/GT/LT. |

**The scope statement has to be sharper than "no condition flags", because there are two axes.**
This module owns the **condition axis** (NX/XX/GT/LT) and *only* that axis. It does **not** own the
**elapsed-delete axis** — "what happens when the requested deadline is already in the past" — which
exists on commands that have no conditions at all, and which FrogDB gets wrong there too:

> `GETEX k EXAT <past>` / `PXAT <past>` **silently drops the option** (`string.rs:504-520`): the
> `target.duration_since(now)` returns `Err` for a past stamp, and because the `set_expiry` call
> sits *inside* `if let Ok(duration)`, the branch simply falls through — the key keeps whatever TTL
> it had and the client gets a value back. Upstream's `getexCommand` treats a past `EXAT`/`PXAT` as
> an immediate delete. **Verified on the FrogDB side; the upstream side is from the documented
> `GETEX` semantics, not a line cite.**

Two coherent responses, and this proposal takes the second: (a) widen the module to own the elapsed
class for every expiry-setting command, `GETEX` included — larger, and it drags `string.rs` into a
commands+types refactor; or (b) **state the boundary explicitly and leave `GETEX` alone**, which is
what §Files involved already does by marking `string.rs` read-only. The `GETEX` behavior is recorded
here so the next person does not read "out of scope" as "checked and correct" — it is **out of
scope and independently wrong**, and it belongs with the elapsed-delete family of issues the
orchestrator files, not with the condition table.

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

Read together, that says a delete-branch expiry command removes the key in memory and **records
nothing durable**, so a WAL replay restores it.

**The exposure is wider than revision 1 said.** Revision 1 scoped the damage to "a key with no
prior TTL", on the reasoning that a key that *had* a TTL is caught on recovery by
`FM-PERSISTENCE-036`. That is only true when the durable deadline has itself passed. The
already-past `EXPIREAT`/`PEXPIREAT` branches (`expiry.rs:472-475`, `:556-559`) fire on keys whose
**last durable record carries a still-future deadline** — `SET k v EX 3600` then
`EXPIREAT k <yesterday>` deletes in memory while the WAL still says "expires in an hour". Recovery
has nothing to filter on, so those keys come back too, and they come back *with a live TTL*, which
makes the resurrection quieter than the no-TTL case rather than louder. The same holds for
`EXPIRE k -10` / `PEXPIRE k -10` (`:297-300`, `:386-389`) against a key with a live durable TTL.
So the exposed set is: **every key whose durable record has no deadline or a still-future one, at
the moment an expiry command deletes it in memory.**

**There is in-tree precedent for exactly this class of bug**, which is why it should not be waved
off as theoretical: `SMOVE` declared `WalStrategy::PersistFirstKey` while mutating two keys, lost
the destination on restart, and is now pinned by `test_smove_destination_survives_restart`
(`server/tests/integration_persistence.rs:487-533`, doc comment at `:487-493`) whose text names
the same `PersistFirstKey`-does-not-cover-my-deletes shape, right down to "the destination add
never reached the WAL and was lost on restart".

**And the unit test currently pins the opposite of production.** `persist_always_writes_set`
(`core/src/shard/persistence.rs:539-553`) asserts `Write::Set` for **both** a present and an
absent key against `TestTarget` — but `TestTarget` never consults its `present` set on the
`write_set` path, whereas the production `ShardWorker::write_set` (`:143-153`) drops the write
entirely on a `get_hot` miss. The test therefore certifies a behavior the real target does not
have, which is why the gap survived.

**Fix sketch, for whoever picks it up** (not implemented here): the four key-family specs and the
four hash-family specs move to `WalStrategy::PersistOrDeleteFirstKey`, whose `contains` probe
(`persistence.rs:116-122`) writes a tombstone when the key is gone; a restart regression in
`integration_persistence.rs` modeled on the `SMOVE` test asserts the key stays gone across a
restart for both the no-TTL and still-future-TTL cases; and `TestTarget::write_set` is tightened to
model the `get_hot` miss so the unit test stops disagreeing with production.

**This is a static reading, it is not verified by execution, and it is not part of proposal 92.**
It is a persistence-durability claim, which means it is spec-first work: an `FM-PERSISTENCE-…` row
and a forcing test, owned by whoever holds the persistence area — not a one-word spec edit smuggled
in behind a commands refactor. **It must not ride along with 92 under any circumstance** — 92's
whole safety argument is that it edits no locked crate, and `frogdb-persistence`/`frogdb-recovery`
are locked at gate 0.85. Filed here because this proposal is the reason anyone looked.

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

**vs proposal 83 (lazy-expiry authority) — a real dependency, not a clean seam.** 83 owns *when a
deadline is noticed* (the read path, the sweeper, the store's purge-on-access contract). 92 owns
*what a deadline-setting command decides*. Revision 1 claimed they meet at an assumption — "a key
reaching the decision point is live, so `current` is never already elapsed" — pinned by a
`debug_assert!`. **That assumption is false** (§The module: every accessor on the path is raw, and
`execute_hexpire_common` performs no purge), so the assert is deleted and the relationship is the
other way round: **92 exposes a question that only 83 can answer.**

Concretely, 92 turns an unwritten assumption into a typed parameter — `current: Option<Instant>`,
which *may* be in the past — and offers two rulings (Option A: compare as-is, preserving today's
behavior including the resurrection cell; Option B: purge/`exists_unexpired` first, eliminating
it). 92 ships Option A and a test that records the behavior. **83 decides which is right**, and
after 92 that decision is a one-line change at one call site per family instead of an audit of
five raw accessor sequences. Ordering: either may land first; if 83 lands first, 92's adapters are
written to whatever purge contract 83 leaves.

**vs proposal 89 (chunk codec).** No contact: different crates, different files.

**Behavioral risk of the fix itself.** The five corrected delete cells strictly *remove* a mutation
(delete → no-op) and change one reply integer each; the sixth (resurrection) removes a *write*. Nothing new enters the write-effect pipeline:
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
begin by writing the regression rows in §Testability and watching them fail, and if any of
them passes, the corresponding row in the divergence table is wrong and comes out of this document
rather than being explained away.

**Three claims are weaker than the rest and are labeled as such, so nobody inherits them as
settled:**

| Claim | Strength | What would settle it |
|---|---|---|
| The resurrection cell's **upstream** answer (`-2`) | **Medium.** `t_hash.c`'s lazy-expiry-on-access path was **not** re-read at revision-2 authoring time; the value is inferred from upstream's documented lazy-field-expiry contract. The FrogDB half is high confidence and fully cited. | Reading `hashTypeGetValue`/`hashTypeIsFieldsWithExpire` in `t_hash.c`, or one differential run |
| `EXPIREAT k 9223372036854775807` panics the shard (§Problem 5) | **Static reading, unexecuted.** Derived from `std`'s panicking `Add<Duration>` impls plus the absence of any operand guard in `EXPIREAT::execute` (`expiry.rs:442-469`). The `PEXPIREAT` counterpart is *not* exposed and is green today (`expire_tcl.rs:555-567`). | One `EXPIREAT` with `i64::MAX` against a running server — which is why it is filed as its own issue rather than asserted here |
| The WAL gap in §Adjacent finding | **Static reading, unexecuted**, and it crosses into a locked crate | An `FM-PERSISTENCE-…` row plus a restart regression, owned by the persistence area |

## Effort

**M overall.** The split:

| Part | Size | Notes |
|---|---|---|
| Module + reply table | **S** | ~130 lines, no dependencies, no I/O |
| Unit tests for the table | **S** | ~180 lines, mechanical enumeration |
| Four key-family adapters | **S** | Each `execute` loses ~25 lines, gains ~10; no signature changes |
| `execute_hexpire_common` adapter | **S–M** | Also narrows two parameters to one and deletes the unreachable arm; the two mutation loops are untouched |
| Parser fold (two → one) | **S** | Four call sites pass `&args[2..]` |
| **The behavior fix + 6 regression rows** | **M** | The only part that is not mechanical: six wire-visible cells change, each needs a reply assertion *and* a survival assertion, **one existing test flips** (`hash_field_expire_tcl.rs:337`), the elapsed-vs-`At` distinction has to be introduced at all five sites before the delete can move (§H1), and the write-effect reasoning above must be re-checked against a real run |

Single-crate iteration throughout (`just check frogdb-commands`, `just test frogdb-commands`), with
`just test redis-regression` for the added rows. No mutation-gate obligation — `frogdb-commands` is
not a locked crate and no `FM-` tag exists anywhere in `crates/commands/src`.

## Independently-landable hotfixes

**H1 — the ordering fix, without the refactor. S–M. Still recommended to land first, separately.**
Move the delete branch below the condition checks at all five sites: in `expiry.rs`, relocate
`:297-300` below `:313-318` (and `:386-389`, `:464-467`+`:471-475`, `:548-551`+`:555-559`
correspondingly); in `hash.rs`, move `:1062-1067` and `:1078-1083` below `:1120`.

**Revision 1 called this "a pure reordering with no new abstraction … reviewable in one screen".
Both halves are retracted.** Moving the delete below GT/LT means the *conditions must run first*,
and the conditions need `expires_at` — which is exactly the value that does not exist for a past
operand:

- **Key family, the panic.** `expiry.rs:302` is
  `let expires_at = clock::now() + Duration::from_secs(seconds as u64);` and it sits **below** the
  `seconds <= 0` delete precisely because `seconds as u64` on a negative operand wraps to
  ~1.8×10¹⁹, and `Instant + Duration::from_secs(1.8e19)` **panics**. Hoisting it above the delete
  branch turns `EXPIRE k -10 GT` from a wrong answer into a shard crash. The fix therefore has to
  introduce the elapsed-vs-`At` distinction inline at each site — the same `NewDeadline` split the
  module formalizes — which *is* a new abstraction, however small.
- **Hash family, the dead arm wakes up.** Moving `hash.rs:1063-1067` below `:1120` routes past
  operands into `time_converter`, which returns `None` for exactly those operands
  (`:1243-1249`, `:1289-1295`, `:1335-1341`, `:1381-1387`) — so the arm at `:1069-1075` that H3
  proposes to delete becomes **live**, and it pushes `Skip`/`0` where the answer is `Delete`/`2`.
  H1 and H3 therefore interact: whichever lands second must account for the other.
- **A test currently blesses the bug and must be flipped.**
  `hash_field_expire_tcl.rs:305-338` asserts `assert_integer_eq(&arr[3], 2)` (`:337`) under
  `// FrogDB deletes fields with past time regardless of NX condition` (`:336`). H1 changes that
  `2` to `0` and deletes the comment lines. **Revision 1's "no existing test pins the divergent
  behavior" is retracted.**

Realistic size: ~30 moved lines, plus one small deadline-classification helper per family, plus the
regression rows, plus one flipped assertion. **The conclusion is unchanged** — land the behavior
fix before the refactor, so the refactor lands as a pure no-op restructuring with the corrected
order already pinned by tests, which is the ideal shape for review. Only the "one screen" framing
goes.

**H2 — parser fold. S, zero behavior change.** Collapse `parse_expire_conditions` /
`parse_expire_conditions_from_slice` (`expiry.rs:181-227`) into one function over an
already-sliced `&[Bytes]`; four call sites pass `&args[2..]`. 14 duplicated lines removed. Lands
independently of everything above.

**H3 — delete the unreachable arm. S, zero behavior change — but narrower than revision 1 said.**
Remove **only** the dead `None` arm at `hash.rs:1069-1075` — either by replacing it with the
`is_past_or_zero` precondition cited in a comment, or (better) by making `None` produce
`CommandError::NotInteger` the way `expiry.rs:469`/`:553` do, so the two families stop
contradicting each other. Optionally fold `is_past_or_zero` (`hash.rs:995`) into `time_converter`
at the same time, since all four closures are local to the file.

**Do NOT remove the `Option` from `unix_secs_to_instant`/`unix_ms_to_instant` (`expiry.rs:37-47`,
`:50-60`) as part of H3.** Revision 1 called them "total"; they are not — they never *return*
`None`, but they **panic** rather than fail on overflow, and `EXPIREAT`/`PEXPIREAT` have no
big-operand guard (§Problem 5). Deleting the `Option` there converts a dead-but-honest error path
into a silent commitment to "this can never overflow", which is the opposite of what the code
needs. The missing overflow guard is filed as a **separate issue** (orchestrator-owned) — H3 waits
on nothing, it just stops at the hash-side arm.

Sequencing note: H3 must land **before** H1, or H1 wakes the arm up (§H1). If H1 lands first, the
arm stops being dead and H3 becomes a behavior fix rather than a cleanup.

**Not a hotfix: the WAL strategy in §Adjacent finding.** It is spec-first persistence work in a
locked area and must not ride along.

## References

- `frogdb-server/crates/commands/src/expiry.rs:126-227` (conditions + twin parsers), `:287-321`,
  `:376-409`, `:453-493`, `:537-577` (the four key-family decisions); `:18-34` (the one-sample
  clock doctrine the adapters must follow), `:37-60` (the two converters), `:263-278` / `:363-367`
  (the overflow guards `EXPIREAT`/`PEXPIREAT` lack)
- `frogdb-server/crates/commands/src/hash.rs:988-1160` (`execute_hexpire_common`), `:1001-1005`
  (negative-operand rejection, which makes `is_past_or_zero` mostly dead), `:1069-1075` (the dead
  `None` arm), `:1178` (`execute_httl_common`'s purge — the contrast that shows the write path has
  none), `:1237`, `:1283`, `:1329`, `:1375` (the four adapters)
- `frogdb-server/crates/core/src/store/hashmap.rs:952-954` (raw `delete`), `:957-959` (raw
  `contains`), `:1220-1234` (`set_expiry`'s `check_and_delete_expired` guard — the live `else`
  arm), `:1236-1238` (raw `get_expiry`), `:1364-1370`/`:1380-1382` (raw field-expiry set/get);
  `frogdb-server/crates/core/src/store/mod.rs:410-423` (`contains` vs `exists_unexpired`)
- `frogdb-server/crates/types/src/types/hash.rs:341-346` (raw `contains`), `:518-521`
  (unconditional `set_field_expiry`), `:537-539` (raw `get_field_expiry`)
- `frogdb-server/crates/commands/src/string.rs:424-529` (`GETEX` — scope boundary), `:504-520`
  (past `EXAT`/`PXAT` silently dropped — the elapsed-delete axis this module does not own)
- `frogdb-server/crates/core/src/command.rs:645-670`,
  `frogdb-server/crates/core/src/shard/persistence.rs:106-160` (§Adjacent finding chain),
  `:539-553` (`persist_always_writes_set` — the unit test that pins the opposite of production),
  `frogdb-server/crates/server/tests/integration_persistence.rs:487-533` (`SMOVE` precedent)
- `.scratch/hardening/specs/persistence-failure-modes.md:626-636` (`FM-PERSISTENCE-044` — cited by
  revision 1, **withdrawn**: its Observable list does not include the `EXPIRE` family)
- `frogdb-server/crates/redis-regression/tests/expire_tcl.rs` (58 tests; `:486-530` big-operand
  coverage for `EXPIRE`/`PEXPIRE` only, `:555-567` `PEXPIREAT` big operand green, `:702`/`:716`/
  `:730` the three future-operand `GT` cases, `:876-893` the two negative-operand `LT` cases),
  `.../hash_field_expire_tcl.rs` (39 tests; `:305-338` the test that currently blesses the
  divergence)
- Redis 8.0 `src/expire.c` `expireGenericCommand`; `src/t_hash.c:519-570`, `:3730-3800`
- Sibling proposals: `90-commandspec-default.md` (crate-conflict, solo-last),
  `83-lazy-expiry-authority.md` (read-path boundary), 93 (hash-field storage bookkeeping, pending)
