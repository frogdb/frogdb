# Proposal 49 — the function registry stops carrying execution state; `FunctionLibrary` answers questions instead of exposing fields

*Round 38. Candidates TX8 (phase 1) + TX9 (phase 2) of the txn+vll+scripting lane. One proposal
because both are the same defect at two scales: `FunctionRegistry` and `FunctionLibrary` are
**shallow modules** — their interfaces are nearly as large as their implementations, so callers
learn the implementation instead of the behaviour. Phase 1 shrinks the registry's interface by
deleting the part nothing uses; phase 2 shrinks the library's by replacing five public fields with
two projections.*

## Summary

`FunctionRegistry` carries a `running_function: Option<RunningFunctionInfo>` whose only writer,
`set_running_function`, has **zero callers anywhere in the tree** — not in production, not in tests
(verified: `grep -rn set_running_function --include='*.rs'` returns the definition and nothing
else). Its getter `get_running_function` has zero callers too, so `FUNCTION STATS` has replied
`running_script` → `Null` for every request the server has ever served, and always will. The state
the seam was meant to hold already exists one level down and is already wired: `ScriptExecutor`
tracks `running: AtomicBool` and `LuaVm` tracks `start_time: Option<Instant>`, and `FUNCTION KILL`
already reads exactly that per-shard state through a scatter — so the usual argument for wiring the
seam ("it enables FUNCTION KILL") is **false: KILL works today and does not go through the
registry**. This proposal recommends **deleting** the seam, because as typed it cannot be made
correct (one `Option` for N concurrently-executing shards, and a precomputed `duration_ms` that is
stale the instant it is written), and because the honest report belongs at the executor — a
separately-scoped follow-up. Phase 2 then makes `FunctionLibrary`'s five public fields private
behind `describe(with_code)` and `snapshot()`, collapsing the two independent walks of its
internals (`handle_function_list` and `dump_libraries`) onto one owner.

## Files involved (verified paths + current line counts)

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/scripting/src/registry.rs` | 413 | **Owner of phase 1.** `FunctionStats` (L11–19, `running_function` at L18); `RunningFunctionInfo` (L21–30); `FunctionRegistry.running_function` field (L46, initialised L61); `stats()` (L165–171); the dead `set_running_function` (L174–176) / `get_running_function` (L179–181) / `library_names` (L184–186); the test-only `function_count` (L189–191) / `library_count` (L193–196); the four `library.functions.keys()` walks (L83, L98, L104, L126) and `library.name.clone()` (L74) that phase 2 re-points. `matches_pattern` (L198–236) is **H3's**, not ours — see the drift note. |
| `frogdb-server/crates/scripting/src/library.rs` | 121 | **Owner of phase 2.** `FunctionLibrary` with five `pub` fields (L9–24: `name` L11, `code` L14, `functions` L17, `engine` L20, `description` L23); the two constructors (L28–52); the already-correct accessors nobody calls from outside the crate — `get_function` (L62–64), `function_names` (L67–69), `function_count` (L72–74); `add_function` (L56–59), which lowercases the map key while `RegisteredFunction.name` keeps its original case. |
| `frogdb-server/crates/scripting/src/persistence.rs` | 440 | `dump_libraries` (L53–84) — **walker #1**: `registry.list_libraries(None)` at L63, then `lib.name.as_bytes()` (L69) and `lib.code.as_bytes()` (L74). Also `save_to_file` (L175–…), which routes through it at L176. |
| `frogdb-server/crates/server/src/connection/scripting/function.rs` | 410 | `handle_function_list` (L108–200) — **walker #2**: `list_libraries(pattern)` at L152, then `lib.name` (L159), `lib.engine` (L162), `lib.functions.values()` (L167) with `func.name`/`func.description`/`func.flags` (L171/L174/L178), `lib.code` (L193). `handle_function_stats` (L214–254) — the dead `running_script` branch at L225–234 and the always-taken `Response::Null` at L236. `handle_function_dump` (L257–264). **Sibling 48 owns L14–73 and L84–99 of this same file** — see the boundaries table. |
| `frogdb-server/crates/scripting/src/loader.rs` | 654 | `validate_library` (L410–430) — walker #3: `library.functions.is_empty()` (L412), `library.functions.keys()` (L417). Both already have accessors (`function_count`, `function_names`). |
| `frogdb-server/crates/core/src/shard/functions.rs` | 118 | `handle_function_call` — walker #4: `registry_guard.get_library(&lib_name)` then `lib.code.clone()` (L44–48). The FCALL execution path, i.e. exactly where phase 1's "wire it" option would have had to write. |
| `frogdb-server/crates/server/src/function_store.rs` | 394 | Walker #5: `library.name.clone()` (L111). Also the two call sites that **discard** `load_library`'s `Ok(Vec<String>)` (L118 `Ok(_) => {}`, L235), and `snapshot_command_args` (L272–280), whose `dump_libraries` call at L274 is the FM-REPLICATION-055 mechanism. |
| `frogdb-server/crates/core/src/scripting/executor.rs` | 1215 | **The state that already exists.** `ScriptExecutor.running: AtomicBool` (L124), set true at L312 and false at L339 around every script/function execution; `kill_script` (L477–482); `is_running` (L483–485). |
| `frogdb-server/crates/core/src/scripting/lua_vm.rs` | 841 | `ExecutionState.start_time: Option<Instant>` (L27) — set at execution start (L152–169), cleared by `reset()` (L63); `LuaVm::is_running` (L502–507). This is the live, correct duration source the registry's precomputed `duration_ms` was pretending to be. |
| `frogdb-server/crates/core/src/shard/scripting.rs` | 261 | `handle_script_kill` (L250–260) — reads `executor.is_running()` and calls `kill_script()`. Proof that the kill path is wired without the registry. |
| `frogdb-server/crates/core/src/shard/dispatch_scripting.rs` | — | `ScriptingMsg::ScriptKill` arm (L73–76). The message the two KILL handlers scatter. |
| `frogdb-server/crates/scripting/src/lib.rs` | 36 | Exports `RunningFunctionInfo` (L34) — phase 1 removes it. |
| `frogdb-server/crates/core/src/lib.rs` | — | Re-exports `RunningFunctionInfo` again (L95). The dead type crosses two crate seams. |
| `frogdb-server/crates/server/tests/functions.rs` | — | `test_function_stats` (L406–431): asserts only `arr.len() >= 2`. |
| `frogdb-server/crates/redis-regression/tests/functions_tcl.rs` | — | Four `FUNCTION STATS` tests (L1027, L1051, L1076, L1101): every one asserts only `matches!(resp, Response::Array(_))`. `tcl_function_kill_when_not_running` (L215) asserts `NOTBUSY`. |
| `.scratch/hardening/specs/replication-failure-modes.md` | — | **The one locked spec that touches this surface** — FM-REPLICATION-054 (L1259), -055 (L1277), -056 (L1298), -057 (L1312). See "Locked-spec check" below. No row edits required. |

## Locked-spec check

Scripting is **not** a locked area — there is no `scripting-failure-modes.md`, no `frogdb-scripting`
mutation gate, and `grep -rn 'FM-' frogdb-server/crates/scripting/` returns nothing. But the
assumption that *no* locked spec covers this surface is **wrong**, and the discrepancy matters:
`replication-failure-modes.md` scope part six (L67–72) explicitly claims the function registry as
the control lane, and three rows constrain the exact code both phases touch:

* **FM-REPLICATION-054** (L1259) — Observable says "`FUNCTION LIST` shows the library"; NOT-observable
  says a read subcommand (`LIST`, `DUMP`, `STATS`, `HELP`, `KILL`) must never generate a replication
  frame. Phase 2 rewrites `handle_function_list`'s body; it must stay a pure read.
* **FM-REPLICATION-055** (L1277) — Invariant names the whole-registry `FUNCTION RESTORE <dump> FLUSH`
  broadcast, i.e. `snapshot_command_args` → `dump_libraries`. Phase 2 rewrites `dump_libraries`'s
  walk. **The dump bytes must be byte-identical**, or a mixed-version link breaks.
* **FM-REPLICATION-057** (L1312) — Observable says read subcommands "are still served". Phase 1
  changes what `FUNCTION STATS` *contains* only in the sense that it makes the always-taken branch
  the only branch; the reply shape is unchanged.

None of these rows names a `FunctionRegistry` or `FunctionLibrary` method, so **no spec row needs
editing**. And neither phase edits a locked *crate* — `frogdb-scripting`, `frogdb-core` and
`frogdb-server` are all outside the four locked pairs — so no `just mutants-gate` step is owed. The
obligation is narrower and concrete: the four `integration_replication_functions.rs` forcing tests
(`a_function_loaded_on_the_primary_reaches_an_attached_replica`,
`function_delete_removes_one_library_on_the_replica_and_keeps_the_rest`,
`only_the_four_state_changing_subcommands_replicate`,
`a_replica_that_full_syncs_receives_the_primarys_existing_libraries`,
`a_client_can_not_load_a_function_on_a_replica`) must pass unchanged, and phase 2 owes a
byte-equality test on `dump_libraries` output across the refactor.

## Problem (concrete verified evidence)

### 1. `set_running_function` fails the deletion test — and so does its getter

```
$ grep -rn "set_running_function" --include='*.rs' .
frogdb-server/crates/scripting/src/registry.rs:174:    pub fn set_running_function(&mut self, info: Option<RunningFunctionInfo>) {

$ grep -rn "get_running_function" --include='*.rs' .
frogdb-server/crates/scripting/src/registry.rs:179:    pub fn get_running_function(&self) -> Option<&RunningFunctionInfo> {
```

Two definitions, zero call sites. The lane note said "zero *non-test* callers"; the verified truth
is stronger — **zero callers, including tests**. Apply the deletion test to the seam: delete
`set_running_function`, and no complexity reappears at any caller, because there is no caller. It
was never hiding anything.

The field it writes is read in exactly one place — `stats()` at `registry.rs:169` — and that value
reaches exactly one consumer, `handle_function_stats` at `function.rs:219`. Since nothing ever
writes it, `function.rs:225`'s `if let Some(ref running) = stats.running_function` is a branch whose
`Some` arm (L226–234) is **unreachable in every build the project has ever produced**, and L236's
`Response::Null` is the unconditional answer:

```rust
// function.rs:224-237  (verified)
result.push(Response::bulk(Bytes::from_static(b"running_script")));
if let Some(ref running) = stats.running_function {   // never true
    ...                                               // L226-234 dead
} else {
    result.push(Response::Null);                      // always
}
```

This is a **live under-report**: a client watching a long `FCALL` on FrogDB is told, with a
positive-looking reply, that nothing is running. It is also the whole of `RunningFunctionInfo`'s
reason to exist — a type exported twice (`scripting/lib.rs:34`, `core/lib.rs:95`) for a field
nobody sets.

### 2. The state the seam wanted already exists, one level down, and `FUNCTION KILL` already uses it

The premise "wire it in the FCALL path so FUNCTION KILL becomes possible" does not survive reading
the kill path. `FUNCTION KILL` is implemented and working, and it never touches the registry:

* `function.rs:395-408` scatters `ScriptingMsg::ScriptKill` to every shard via
  `scatter_gather().find_first(...)` — the same mechanism `SCRIPT KILL` uses (`script.rs:110`).
* `dispatch_scripting.rs:73` routes it to `handle_script_kill`.
* `core/shard/scripting.rs:250-260`:
  ```rust
  if !executor.is_running() {
      return Err("NOTBUSY No scripts in execution right now.".to_string());
  }
  executor.kill_script()
  ```
* `executor.rs:124` `running: AtomicBool`, stored `true` at L312 immediately before
  `vm.prepare_execution` and `false` at L339 immediately after `vm.cleanup_execution` — a scoped
  pair on the real execution path, for both EVAL and FCALL.
* `lua_vm.rs:27` `start_time: Option<Instant>`, set at L152–169 and cleared by `reset()` at L63 —
  a live clock, not a snapshot.

So the registry seam would be a **second copy of state that already exists**, kept at the wrong
scale (process-wide) by the wrong writer (the connection layer) for the benefit of one report.

### 3. As typed, the seam cannot be made correct

Three independent shape errors, each of which forces a redesign before "wire it" could even be
attempted:

1. **Cardinality.** `Option<RunningFunctionInfo>` is single-valued, but FCALL routes to
   `shard_senders[target_shard]` (`function.rs:61`) and each `ShardWorker` owns its own
   `ScriptExecutor`. N shards can be executing N functions simultaneously — the code's own comment
   at `function.rs:389` says "only one can be running a script at a time **per shard**". A second
   concurrent FCALL would overwrite the first's entry, and the first's completion would clear the
   second's.
2. **Staleness.** `RunningFunctionInfo.duration_ms: u64` (`registry.rs:29`) is a plain number fixed
   at write time. Written at FCALL entry it is `0` forever; it can only be correct if the reader
   computes `now - start`, which requires storing an `Instant` — which is what `lua_vm.rs:27`
   already does.
3. **Leak safety.** `set_running_function(Some)` / `set_running_function(None)` around a body with
   `?`-returns and error paths leaks a phantom running function into process-lifetime state on any
   early exit. Making that safe needs a guard type — reinventing the scoped `store(true)/store(false)`
   pair `executor.rs:312/339` already has.

Any correct version is therefore "N entries, live start instants, scope-guarded" — which is
`ScriptExecutor` plus `LuaVm`, reached by a fan-out. It is not this field.

### 4. Three more dead entries and a duplicated question on the registry's interface

The same sweep found the registry's interface carrying more than the two dead methods above
(verified with `grep -rn '\.<name>('`):

| Method | `registry.rs` | Production callers | Test callers |
| --- | --- | --- | --- |
| `set_running_function` | L174–176 | 0 | **0** |
| `get_running_function` | L179–181 | 0 | **0** |
| `library_names` | L184–186 | 0 | **0** |
| `function_count` | L189–191 | 0 | 1 (own test, L357) |
| `library_count` | L193–196 | 0 | 1 (own test, L356) |
| `stats()` | L165–171 | 1 (`function.rs:219`) | 1 |
| `get_library` | L140–142 | 1 (`shard/functions.rs:44`) | 3 |
| `list_libraries` | L153–162 | 2 (`persistence.rs:63`, `function.rs:152`) | 3 |

`function_count()`/`library_count()` also duplicate `FunctionStats.function_count`/`library_count`
— two ways to ask one question, one of which no production code uses. And `load_library` returns
`Result<Vec<String>, _>` whose `Ok` payload every production caller discards (`function_store.rs:118`
`Ok(_) => {}`, `function_store.rs:235`, `server/init.rs:426`); only `registry.rs:272` reads it. That
return value is additionally *wrong* for any caller who did want it: it is built from
`library.functions.keys()` (L104), which `add_function` lowercased (`library.rs:57`), so it would
report `myfunc` for a function registered as `myFunc`.

### 5. `FunctionLibrary` is anemic: six sites reach through it, two rebuild whole projections

All five fields are `pub` (`library.rs:11–23`), so the interface *is* the implementation and every
caller is coupled to the `HashMap`. Verified reach-through sites:

| Site | Fields read |
| --- | --- |
| `persistence.rs:69,74` (`dump_libraries`) | `name`, `code` |
| `function.rs:159,162,167,193` (`handle_function_list`) | `name`, `engine`, `functions` (+ each `func.name`/`description`/`flags`), `code` |
| `shard/functions.rs:45` (`handle_function_call`) | `code` |
| `registry.rs:74,83,98,104,126` | `name`, `functions.keys()` ×4 |
| `loader.rs:412,417` (`validate_library`) | `functions.is_empty()`, `functions.keys()` |
| `function_store.rs:111` (`FunctionStore::load`) | `name` |

Two of these — `dump_libraries` and `handle_function_list` — are not single-field reads but
**independent full projections of the same object**, written in two crates, that must agree about
which libraries exist and what each one contains. The cost is already visible: `dump_libraries`
carries `(name, code)` only, so `FUNCTION RESTORE` re-executes the source to recover flags — a
sensible choice that is nowhere stated at the type, because there is no type.

The reach-through also hides a real name-case rule. `add_function` (`library.rs:56-59`) keys the map
by `name.to_ascii_lowercase()` while `RegisteredFunction.name` (`function.rs:61`) keeps the original
case. `handle_function_list` correctly reads `func.name` from the *value* (L171); `registry.rs:83-84`
reads a *key* and calls `.to_ascii_lowercase()` on it again — a no-op that is direct evidence the
distinction is not legible from outside. Accessors that already encode it (`function_names`,
`get_function`) exist at `library.rs:62-69` and have **no callers outside `library.rs`'s own tests**.

## Proposed change

### Phase 1 (TX8) — delete the seam. **Recommended.**

| | Option A: WIRE into the FCALL path (M) | **Option B: DELETE the seam (S)** |
| --- | --- | --- |
| Enables `FUNCTION KILL`? | **No.** KILL is already wired through `executor.is_running()` (§2). | No change — KILL keeps working. |
| Fixes the `running_script` under-report? | Only after fixing three shape errors first (§3): `Option` → per-shard collection, `duration_ms` → `Instant`, plus a leak guard. | No. Makes the gap *legible* instead of disguised as a live branch. |
| Cost per FCALL | A **write** lock on the process-wide `Arc<RwLock<FunctionRegistry>>` at entry *and* exit — serialising every shard's FCALLs against each other and against every `FUNCTION LIST`/`STATS`/`DUMP` reader, for a report. | Zero. |
| Correctness risk | Phantom entries on early-return paths; concurrent overwrite across shards. | None — deletes unreachable code. |
| Interface effect | Registry keeps execution-lifecycle state it does not own. | Registry becomes what it says it is: a library store. |

**Recommend Option B**, for the reason the table makes plain: Option A's stated benefit is already
delivered elsewhere, and its actual deliverable (a truthful report) is *not* achievable at this
seam without redesigning the seam. Concretely:

1. `registry.rs` — delete `running_function` (L46, L61), `set_running_function` (L174–176),
   `get_running_function` (L179–181), `RunningFunctionInfo` (L21–30), and
   `FunctionStats.running_function` (L18). Update the L169 initialiser and the L411 test assertion.
2. `registry.rs` — delete `library_names` (L184–186, zero callers). Delete `function_count`
   (L189–191) and `library_count` (L193–196), whose only callers are the registry's own tests at
   L356–357; re-point those two assertions at `stats()`, which answers the same question and *is*
   used in production.
3. `registry.rs` — change `load_library`'s return to `Result<(), FunctionError>` (its `Ok` payload
   has no production reader and is lowercase-mangled, §4). Three call sites already ignore it.
4. `scripting/lib.rs:34` and `core/lib.rs:95` — drop `RunningFunctionInfo` from both export lists.
5. `function.rs:223-237` — replace the dead branch with an unconditional `running_script` → `Null`,
   plus a short comment naming *why* it is static and where the real state lives
   (`ScriptExecutor::is_running`), so the next reader does not re-add the field:

   ```rust
   // `running_script` is unconditionally Null: the running-script state lives per
   // shard in `ScriptExecutor` (`running` + `LuaVm::start_time`), which is what
   // FUNCTION KILL reads. Reporting it here needs a fan-out, not a process-wide
   // mirror — see the follow-up issue.
   result.push(Response::bulk(Bytes::from_static(b"running_script")));
   result.push(Response::Null);
   ```

**Deliberately not in this proposal — the follow-up that makes the report truthful.** Add a
`ScriptingMsg::ScriptStatus` reply carrying `Option<(name, command, elapsed_ms)>` derived from
`executor.is_running()` + `LuaVm::start_time.elapsed()`, scattered by `handle_function_stats` the
same way `handle_function_kill` already scatters `ScriptKill` (`function.rs:395-408`). Sized **M**.
It is filed separately, not folded in, because (a) it edits `ScriptingMsg` and
`dispatch_scripting.rs`, which sibling 48 is also editing, and (b) it needs a **documented
deviation**: Redis is single-threaded so `running_script` is one script, whereas a sharded FrogDB
can have several — the reply must either pick the longest-running or grow, and that is a compat
decision, not a refactor. Deleting the field first is what makes that decision reachable: today the
wrong answer is already half-built and invites completion.

### Phase 2 (TX9) — `FunctionLibrary` answers questions; fields go private

Make all five fields private and add two plain-data projections, both owned by `library.rs`:

```rust
// frogdb-scripting: plain data, no Response, no wire types (house style: plain-data
// signatures across a seam, per ADR-0002's closing rule).
pub struct FunctionDescription {
    pub name: String,            // original case, from RegisteredFunction.name
    pub description: Option<String>,
    pub flags: Vec<String>,      // FunctionFlags::to_strings()
}

pub struct LibraryDescription {
    pub name: String,
    pub engine: String,          // as stored; the caller uppercases for the wire
    pub functions: Vec<FunctionDescription>,
    pub code: Option<String>,    // Some iff with_code
}

pub struct LibrarySnapshot {     // the dump/persist projection
    pub name: String,
    pub code: String,
}

impl FunctionLibrary {
    pub fn describe(&self, with_code: bool) -> LibraryDescription { ... }
    pub fn snapshot(&self) -> LibrarySnapshot { ... }
    pub fn name(&self) -> &str { &self.name }
    pub fn code(&self) -> &str { &self.code }
    // get_function / function_names / function_count already exist (L62-74)
}
```

Then re-point every walker:

* `persistence.rs:63-77` → `for lib in registry.list_libraries(None) { let s = lib.snapshot(); ... }`.
  **Byte-for-byte identical output** (same field order, same `u32` LE lengths) — required by
  FM-REPLICATION-055.
* `function.rs:152-197` → `for lib in libraries { let d = lib.describe(with_code); ... }`, and the
  loop becomes pure RESP encoding of a plain struct. The `to_ascii_uppercase()` on `engine` stays at
  the server, where the wire format is decided.
* `shard/functions.rs:45` → `lib.code().to_string()`.
* `registry.rs:74` → `library.name().to_string()`; L83/L98/L104/L126 → `library.function_names()`
  (which returns the lowercased index keys — exactly what the index wants, now stated by the method
  rather than inferred from the map).
* `loader.rs:412` → `library.function_count() == 0`; L417 → `library.function_names()`.
* `function_store.rs:111` → `library.name().to_string()`.

Construction keeps its two existing entry points (`new`, `with_metadata`) plus `add_function`, so
`parser.rs:114-127` (`ParsedLibrary::into_library`) is unaffected.

## Before / After

**Before** — a caller of `FunctionLibrary` must know: it is a struct with five public fields; that
`functions` is a `HashMap`; that its *keys* are lowercased but its values' `.name` is not; which of
the two to use for a client-visible reply; and that `FunctionFlags` needs `.to_strings()`. Six sites
know all of that, and two of them independently assemble a full projection from it.

**After** — a caller must know two methods. `describe(with_code)` answers "what does a client see?"
and `snapshot()` answers "what goes on the wire/disk?". The case rule, the flag rendering and the
map are implementation. Adding a field to `FunctionLibrary` becomes a one-file change instead of a
six-site sweep.

For the registry: **before**, twelve public methods of which three are dead, two are test-only, and
one duplicates `stats()`; **after**, seven, every one with a production caller.

## Testability improvement

1. **An unkillable-by-construction region disappears.** `function.rs:226-234` cannot be covered by
   any test, because no code path can make `stats.running_function` `Some`. Under mutation testing
   that whole region yields survivors that no test can ever kill — the outcome the house rule says
   must be *documented at the code with why it is unobservable*. Deleting is strictly better than
   documenting: there is nothing to be unobservable about.
2. **The `LIST` projection becomes unit-testable in its owning crate.** Today, asserting "WITHCODE
   includes `library_code` and plain LIST does not", or "flags render as `no-writes`", requires a
   live server and RESP parsing (`server/tests/functions.rs`, `functions_tcl.rs`). After phase 2 it
   is `assert!(lib.describe(false).code.is_none())` inside `frogdb-scripting`. This matters
   materially: `cargo mutants -p <crate>` runs only that package's own tests, so behaviour forced
   solely from server integration tests contributes nothing to the owning crate's score — the
   projection currently has **no** in-crate test at all.
3. **One round-trip test covers two wire paths.** `snapshot()` is the single source for the
   `FUNCTION DUMP` reply (`function.rs:262`), the `functions.fdb` file (`persistence.rs:176`) and
   the full-resync frame (`function_store.rs:274`, FM-REPLICATION-055). A test that
   `dump → restore → dump` is byte-identical proves all three, and is the exact regression guard the
   byte-stability risk needs.
4. **The name-case rule gets one enforceable home.** `describe()` returns display names,
   `function_names()` returns index keys. A test can pin `describe()` preserving `myFunc` while
   `function_names()` yields `myfunc` — a distinction currently unstated and, at `registry.rs:83-84`,
   demonstrably misunderstood.
5. **Existing coverage is not disturbed.** Every `FUNCTION STATS` test in the tree
   (`functions.rs:406`, `functions_tcl.rs:1027/1051/1076/1101`) asserts only that the reply is an
   `Array`; none inspects `running_script`. Phase 1 is invisible to all five, which is both good
   news (no churn) and the reason the dead branch survived this long.

## Risks / scope boundaries vs siblings

| Sibling | Owns | Overlap with 49 | Resolution |
| --- | --- | --- | --- |
| **47** (eval.rs) | `connection/scripting/eval.rs` — EVAL/EVALSHA orchestration, shard-reply error mapping | None. 49 touches no file 47 touches. | Independent; land in any order. |
| **48** (function.rs dispatch/classification) | `connection/scripting/function.rs` **L14–73** (`handle_fcall` routing, `SlotValidator::same_shard`, keyless→shard-0) and the subcommand match at **L84–99** | **Same file, disjoint line ranges.** 49 owns L148–200 (`handle_function_list` body) and L214–254 (`handle_function_stats` body) — the *reply construction*, not the dispatch. | **48 lands first** (it closes a live capability gap); 49 rebases. Line ranges do not collide, so a textual conflict is unlikely, but the file is shared — do not run both in the same worktree. **Explicit ask of 48:** when wiring the FCALL path, do **not** reach for `set_running_function` — 49 deletes it, and §3 explains why re-adding it would be wrong. If 48 wants running-script state, it belongs in the follow-up's `ScriptStatus` fan-out. |
| **45** (VLL) | `frogdb-vll`, `core/shard/vll.rs` | None. | Independent. |
| **H3 hotfix** (landing separately) | `registry.rs` `matches_pattern` (L198–236), its test (L386–396), and the `list_libraries` call site (L157) — replaced by `frogdb_types::glob_match` (`types/src/glob.rs:23`) | **Same file.** 49's phase-1 deletions at L174–196 are immediately above H3's region, so line numbers in this document will have drifted by implementation time. | **Not re-proposed here.** Land H3 first; re-verify `registry.rs` line numbers before starting. Phase 1 does not change `list_libraries`'s signature or its filter, and phase 2 changes only what the *callers* of `list_libraries` do with the returned `&FunctionLibrary`s — so neither phase constrains H3's choice of glob. |
| **Follow-up issue** (truthful `running_script`) | `ScriptingMsg`, `dispatch_scripting.rs`, `core/shard/scripting.rs`, `handle_function_stats` | Would touch `ScriptingMsg`, which 48 also touches. | Deferred deliberately (see phase 1). File after both 48 and 49 land. |

**Other risks.**

* **Dump byte-stability (highest).** `dump_libraries` feeds a replication frame
  (FM-REPLICATION-055) and an on-disk file. `snapshot()` must reproduce field order and `u32` LE
  framing exactly. Mitigation: the round-trip test in Testability §3, plus a golden-bytes assertion
  against a fixed library set, written **before** the refactor.
* **Deleting a `pub` item from a library crate.** `RunningFunctionInfo`, `library_names`,
  `function_count`, `library_count` are all `pub` and two are re-exported through `frogdb-core`.
  Verified zero external consumers in this workspace (`frogctl`, `frogdb-operator`, `testing/fuzz`
  included). FrogDB is pre-production and breaking changes are explicitly acceptable, so no
  deprecation cycle.
* **`FUNCTION STATS` reply shape must not change.** Phase 1 must keep the field order
  `running_script`, `<null>`, `engines`, `[LUA, [...]]` — the four regression tests only check
  `Array`, so nothing else guards it. Add one assertion pinning the RESP shape as part of phase 1;
  that is a coverage *gain*, not overhead.
* **Making fields private is a wide mechanical edit.** Six sites, all in-workspace, all
  compiler-caught: removing `pub` turns every missed one into a build error. No silent breakage
  is possible.
* **Description field.** `FunctionLibrary.description` (L23) is written only as `None`
  (`parser.rs:117`) and read nowhere. `describe()` should carry it anyway (Redis's `FUNCTION LIST`
  has a library-level description slot the reply currently omits) — but wiring that slot into the
  reply is a **behaviour change and out of scope**; note it and leave the wire untouched.

## Effort estimate

* **Phase 1 (TX8, delete): S.** One struct field, one struct, two methods, three more dead methods,
  two export lists, one return-type narrowing with three already-ignoring call sites, one branch
  collapsed to a constant, three test assertions updated. No behaviour change on the wire. The only
  new test is the `FUNCTION STATS` shape pin. Independently landable ahead of phase 2, and ahead of
  or behind 47/48.
* **Phase 2 (TX9, encapsulate): M.** Three new plain-data structs plus two methods and two
  accessors in `library.rs`; five fields de-`pub`ed; six walker sites re-pointed across three
  crates; `handle_function_list` reduced to RESP encoding; `dump_libraries` reduced to a
  `snapshot()` loop. New tests: `describe(with_code)` both ways, flag rendering, name-case
  distinction, and the dump byte-stability round-trip. The long pole is proving the dump bytes
  unchanged, not the diff.
* **Ordering.** Phase 1 before phase 2 (phase 1 shrinks the registry surface phase 2 then re-points
  through), and both after H3.

### Independently-landable hotfix

**None that fixes a client-visible defect.** Stated plainly because it would be easy to mislabel
phase 1 as one: deleting the dead seam does **not** make `FUNCTION STATS` truthful — the reply is
`Null` before and after. What it fixes is the code's *claim* to report, which is a maintainability
defect, not a client-visible one. The client-visible fix is the deferred `ScriptStatus` fan-out
(M), which is not a hotfix and must not be rushed ahead of the Redis-deviation decision it needs.

Phase 1 is nonetheless the natural independently-landable slice: it is self-contained, touches no
locked crate, changes no wire bytes, and leaves phase 2 as "and now the fields go private".
