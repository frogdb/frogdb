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
`running_script` → `Null` for every request the server has ever served, and always will. That is a
**client-visible under-report today** (issue 85/F15, severity 2), and phase 1 deliberately does not
fix it — see §1 and the hotfix section.

The state the seam was meant to hold already exists one level down: `ScriptExecutor` tracks
`running: AtomicBool` and `LuaVm` tracks `start_time: Option<Instant>`, and **`FUNCTION KILL` is
wired through `executor.is_running()` and does not go through the registry at all**. Whether that
kill is *deliverable* to a shard that is mid-`FCALL` is a separate, already-filed defect (issue
85/F8, severity 4) that this proposal neither causes nor fixes — see §2. Either way the registry
seam is not on the KILL path, so "wiring it enables FUNCTION KILL" is not an argument for keeping
it.

This proposal recommends **deleting** the seam, because as typed it cannot be made correct (one
`Option` for N concurrently-executing shards, and a precomputed `duration_ms` that is stale the
instant it is written), and because the honest report belongs at the executor — a separately-scoped
follow-up whose shape §"deliberately not in this proposal" now specifies (a per-shard shared status
cell, *not* a message fan-out, for the reason F8 gives). Phase 2 then makes `FunctionLibrary`'s five
public fields private behind `describe(with_code)` and `snapshot()`, collapsing the two independent
walks of its internals (`handle_function_list` and `dump_libraries`) onto one owner.

**This proposal is the ruling issue 34 asked for.** `34-dead-code-deletion-sweep.md` lists
`set_running_function` as item **09/F15** with a *contested* checkbox (L86–87: "`set_running_function`
is *called* from the FCALL entry path … rather than deleted"), and its "What to fix" preamble states
that for a contested item "the owning proposal's alternative must be accepted or explicitly rejected
in the commit message". Issue **85/F15** rules the opposite way ("either wire the field or remove it
from the reply"). §§2–3 below **reject the wire-it alternative** and elect deletion; the implementing
commit must say so in those terms, and both issues' checkboxes should be marked resolved against
this document.

## Files involved (verified paths + current line counts)

Two distinct files in this tree are named `function.rs`. They are disambiguated everywhere below as
**`connection/scripting/function.rs`** (the server-side `FUNCTION` subcommand handlers) and
**`scripting/src/function.rs`** (the `FunctionFlags` / `RegisteredFunction` types). A bare
`function.rs` never appears.

All line numbers were re-verified against the current worktree **after** the H3 glob hotfix
(`0b034a4f`, branch `worktree-agent-a84216c599d8af135`, pending merge to `main`) — see the risks
table.

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/scripting/src/registry.rs` | 413 | **Owner of phase 1.** `FunctionStats` (L11–19, `running_function` at L18); `RunningFunctionInfo` (L21–30); `FunctionRegistry.running_function` field (L46, initialised L61); `stats()` (L165–171); the dead `set_running_function` (L174–176) / `get_running_function` (L179–181) / `library_names` (L184–186); the test-only `function_count` (L189–191) / `library_count` (L193–196); the four `library.functions.keys()` walks (L83, L98, L104, L126) and `library.name.clone()` (L74) that phase 2 re-points. `matches_pattern` (L198–236) is **deleted by H3 `0b034a4f`**, not by us — see the drift note. |
| `frogdb-server/crates/scripting/src/library.rs` | 121 | **Owner of phase 2.** `FunctionLibrary` with five `pub` fields (L9–24: `name` L11, `code` L14, `functions` L17, `engine` L20, `description` L23); the two constructors (L28–52); the accessors `get_function` (L62–64), `function_names` (L67–69), `function_count` (L72–74); `add_function` (L56–59), which lowercases the map key while `RegisteredFunction.name` keeps its original case. |
| `frogdb-server/crates/scripting/src/function.rs` | — | `FunctionFlags::to_strings` (L34–59) and `RegisteredFunction` (L59–68), whose `pub name: String` at **L61** is the original-case display name. Not edited by either phase; cited by phase 2's `FunctionDescription`. |
| `frogdb-server/crates/scripting/src/persistence.rs` | 440 | `dump_libraries` (L53–84) — **walker #1**: `registry.list_libraries(None)` at L63, then `lib.name.as_bytes()` (L69) and `lib.code.as_bytes()` (L74). Format constants `DUMP_VERSION` (L10) and `MAGIC` (L13) — the **real** on-disk/on-wire compat constraint (§ Locked-spec check). `save_to_file` (L175–…) routes through `dump_libraries` at L176. `test_dump_restore_roundtrip` (L288) already exists — phase 2 extends it. |
| `frogdb-server/crates/server/src/connection/scripting/function.rs` | 410 | `handle_function_list` (L108–200) — **walker #2**: `list_libraries(pattern)` at L152, then `lib.name` (L159), `lib.engine` (L162), `lib.functions.values()` (L167) with `func.name`/`func.description`/`func.flags` (L171/L174/L178), `lib.code` (L193). `handle_function_stats` (L214–254) — the dead `running_script` branch at L225–234 and the always-taken `Response::Null` at L236. `handle_function_dump` (L257–264). `handle_function_kill` (L387–409). **Sibling 48 owns L14–73 of this file and explicitly disclaims the rest** — see the boundaries table. |
| `frogdb-server/crates/scripting/src/loader.rs` | 654 | `validate_library` (L410–430) — walker #3: `library.functions.is_empty()` (L412), `library.functions.keys()` (L417). Both already have accessors (`function_count`, `function_names`). |
| `frogdb-server/crates/core/src/shard/functions.rs` | 118 | `handle_function_call` — walker #4: `registry_guard.get_library(&lib_name)` (L44) then `lib.code.clone()` (L45). The FCALL execution path, i.e. exactly where phase 1's "wire it" option would have had to write. |
| `frogdb-server/crates/server/src/function_store.rs` | 394 | Walker #5: `library.name.clone()` (L111). The two call sites that **discard** `load_library`'s `Ok(Vec<String>)` (L118 `Ok(_) => {}`, L235). `snapshot_command_args` (L272–280), whose `dump_libraries` call at L274 is the FM-REPLICATION-055 mechanism. **Hosts an `FM-` tag**: `/// FM-REPLICATION-054` at **L377** over `only_the_four_state_changing_subcommands_replicate` at **L379** — so this file being edited in phase 2 puts `just lint-failure-modes` in scope even though no row changes. |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | — | `dispatch_message(&mut self, …)` (L370) and its `ShardMessage::Scripting(m) => self.dispatch_scripting(m).await` arm (**L381**). The single-task serialisation that makes issue 85/F8 true — see §2. |
| `frogdb-server/crates/core/src/scripting/executor.rs` | 1215 | **The state that already exists.** `ScriptExecutor.running: AtomicBool` (L124), set true at L312 and false at L339 around every script/function execution; `kill_script` (L477–482); `is_running` (L483–485). |
| `frogdb-server/crates/core/src/scripting/lua_vm.rs` | 841 | `ExecutionState.start_time: Option<Instant>` (L27) — set at execution start (L152–169), cleared by `reset()` (L63); `LuaVm::is_running` (L502–507). This is the live, correct duration source the registry's precomputed `duration_ms` was pretending to be. |
| `frogdb-server/crates/core/src/shard/scripting.rs` | 261 | `handle_script_kill` (L250–260) — reads `executor.is_running()` and calls `kill_script()`. Proof that the kill path is wired without the registry. |
| `frogdb-server/crates/core/src/shard/types.rs` | — | `ShardScripting` (L529) owns `executor: Option<ScriptExecutor>` (L531), read via `executor()` (L552) and **replaced at runtime** by `set_executor` (L568, called from `worker.rs:498`). Constrains the deferred follow-up's shape — see below. |
| `frogdb-server/crates/core/src/shard/dispatch_scripting.rs` | — | `ScriptingMsg::ScriptKill` arm (L73–76). The message the two KILL handlers scatter. |
| `frogdb-server/crates/scripting/src/lib.rs` | 36 | Exports `RunningFunctionInfo` (L34) — phase 1 removes it. |
| `frogdb-server/crates/core/src/lib.rs` | — | Re-exports `RunningFunctionInfo` again (L95). The dead type crosses two crate seams. |
| `frogdb-server/crates/server/tests/functions.rs` | — | `test_function_stats` (L406–431): asserts only `arr.len() >= 2`. `test_function_kill_not_busy` (L625): NOTBUSY only. `registry.load_library(lib, false).unwrap()` (L672). |
| `frogdb-server/crates/redis-regression/tests/functions_tcl.rs` | — | Four `FUNCTION STATS` tests (L1027, L1051, L1076, L1101): every one asserts only `matches!(resp, Response::Array(_))`. `tcl_function_kill_when_not_running` (L215) asserts `NOTBUSY`. |
| `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs` | — | Header L36 + exclusions L37–43: the seven upstream `Timedout … SCRIPT KILL` tests are excluded as "redis-specific". Issue 85/F16 rules that header **wrong**. Consequence for us: no test in the tree kills a *running* script. |
| `.scratch/hardening/specs/replication-failure-modes.md` | — | **The one locked spec that touches this surface** — FM-REPLICATION-054 (L1259), -055 (L1277), -056 (L1298), -057 (L1312). See "Locked-spec check" below. No row edits required. |
| `.scratch/testing-improvements-round2/issues/open/34-dead-code-deletion-sweep.md` | — | Item **09/F15** (L49–51, checkbox L86–87) — *contested*: wants `set_running_function` wired. This proposal is the ruling that rejects it. |
| `.scratch/testing-improvements-round2/issues/open/85-scripting-residual-test-gaps.md` | — | **F8** (KILL undeliverable to a busy shard, sev 4), **F15** (`running_script` permanently null, sev 2, rules *opposite* to 34), **F16** (the wrong `scripting_tcl.rs` exclusion header). |

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
  broadcast under `PROPAGATION_ORDER`, i.e. `snapshot_command_args` → `dump_libraries`, and the
  Observable is a **converged set** ("holds exactly the primary's set"). Phase 2 rewrites
  `dump_libraries`'s walk.

  **Correction to an earlier draft of this document.** The row pins the *mechanism* and the
  *converged set*, **not the bytes**. Its single forcing test
  (`a_replica_that_full_syncs_receives_the_primarys_existing_libraries`) runs a primary and a replica
  built from the same source tree, so an encoding change applied to both sides is invisible to it —
  the test **cannot detect** an encoding change. The correct statement of the constraint is therefore
  narrower and lives elsewhere: `snapshot()` **must not change the encoding**, because
  (a) `dump_libraries` also writes `functions.fdb` on disk (`persistence.rs:53–84`, framed
  `MAGIC` + `DUMP_VERSION` at `persistence.rs:10/13`, read back by `restore_libraries` which rejects
  a version mismatch at `persistence.rs:106`), so a changed encoding without a `DUMP_VERSION` bump
  makes an existing node's functions unreadable after restart; and (b) rolling upgrades put two
  different builds on one link. Neither hazard is guarded by FM-055's test; both are guarded by the
  round-trip + golden test in Testability §3.
* **FM-REPLICATION-057** (L1312) — Observable says read subcommands "are still served". Phase 1
  changes what `FUNCTION STATS` *contains* only in the sense that it makes the always-taken branch
  the only branch; the reply shape is unchanged.

None of these rows names a `FunctionRegistry` or `FunctionLibrary` method, so **no spec row needs
editing**. And neither phase edits a locked *crate* — `frogdb-scripting`, `frogdb-core` and
`frogdb-server` are all outside the four locked pairs — so no `just mutants-gate` step is owed. That
claim survives phase 1's `load_library` return-type narrowing **only because every locked-crate call
site discards the payload positionally**: `frogdb-recovery/src/tests.rs:1238` is
`registry.load_library(…).expect("library loads into a fresh registry");` as a statement, which
compiles unchanged against `Result<(), FunctionError>`. If any locked-crate site ever *bound* the
payload, phase 1 would become a locked-crate edit and the mutation gate would apply. Verified today:
none does.

The concrete obligations are:

* The **five** forcing tests for the four rows must pass unchanged. Four are in
  `frogdb-server/crates/server/tests/integration_replication_functions.rs`:
  `a_function_loaded_on_the_primary_reaches_an_attached_replica` (L78, tag L76),
  `a_replica_that_full_syncs_receives_the_primarys_existing_libraries` (L108, tag L106),
  `a_promoted_replica_keeps_the_libraries_it_replicated` (L133, tag L131),
  `function_delete_removes_one_library_on_the_replica_and_keeps_the_rest` (L158, tag L156),
  `a_client_can_not_load_a_function_on_a_replica` (L190, tag L188). The fifth,
  **`only_the_four_state_changing_subcommands_replicate`, is *not* in that file** — it is a unit test
  inside `frogdb-server/crates/server/src/function_store.rs` at **L379**, tagged at **L377**. An
  earlier draft of this document attributed it to the integration file; that was wrong.
* Because `function_store.rs` is a phase-2-edited file **that hosts an `FM-` tag**,
  `just lint-failure-modes` is in scope for phase 2 even though no spec row changes: the lint
  enforces spec↔test agreement in both directions, so moving or renaming that test — or reflowing the
  lines around it — must keep the tag adjacent to its `#[test]`.
* Phase 2 owes an encoding-stability test on `dump_libraries` output across the refactor, in the
  shape Testability §3 specifies (extend the existing `test_dump_restore_roundtrip`, plus a
  *single-library* golden).

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
reaches exactly one consumer, `handle_function_stats` at `connection/scripting/function.rs:219`.
Since nothing ever writes it, `connection/scripting/function.rs:225`'s
`if let Some(ref running) = stats.running_function` is a branch whose `Some` arm (L226–234) is
**unreachable in every build the project has ever produced**, and L236's `Response::Null` is the
unconditional answer:

```rust
// connection/scripting/function.rs:224-237  (verified)
result.push(Response::bulk(Bytes::from_static(b"running_script")));
if let Some(ref running) = stats.running_function {   // never true
    ...                                               // L226-234 dead
} else {
    result.push(Response::Null);                      // always
}
```

**This is a client-visible under-report, today.** A client watching a long `FCALL` on FrogDB is told,
with a positive-looking reply, that nothing is running. Issue 85/F15 scores it severity 2 for exactly
that reason. **Phase 1 does not fix it** — it makes the `Null` honest in the source instead of
disguised as a live branch, and hands the fix to the follow-up. Anyone reading the hotfix section at
the end of this document should read "not a client-visible fix" as a statement about *phase 1's
effect*, not a denial that the defect is client-visible. It is.

The dead field is also the whole of `RunningFunctionInfo`'s reason to exist — a type exported twice
(`scripting/lib.rs:34`, `core/lib.rs:95`) for a field nobody sets.

### 2. `FUNCTION KILL` does not go through the registry — and its own deliverability is a separately filed defect

The premise "wire it in the FCALL path so FUNCTION KILL becomes possible" fails for a reason that
does not depend on whether KILL works: **KILL is wired through `executor.is_running()` and never
touches the registry.**

* `connection/scripting/function.rs:387-409` (`handle_function_kill`) scatters
  `ScriptingMsg::ScriptKill` to every shard via `scatter_gather().find_first(...)` — the same
  mechanism `SCRIPT KILL` uses (`script.rs:110`).
* `dispatch_scripting.rs:73` routes it to `handle_script_kill`.
* `core/shard/scripting.rs:250-260`:
  ```rust
  match self.scripting.executor() {
      Some(executor) => {
          if !executor.is_running() {
              return Err("NOTBUSY No scripts in execution right now.".to_string());
          }
          executor.kill_script().map_err(|e| e.to_string())
      }
      None => Err("ERR scripting not available".to_string()),
  }
  ```
* `executor.rs:124` `running: AtomicBool`, stored `true` at L312 immediately before
  `vm.prepare_execution` and `false` at L339 immediately after `vm.cleanup_execution` — a scoped
  pair on the real execution path, for both EVAL and FCALL.
* `lua_vm.rs:27` `start_time: Option<Instant>`, set at L152–169 and cleared by `reset()` at L63 —
  a live clock, not a snapshot.

So the registry seam would be a **second copy of state that already exists**, kept at the wrong
scale (process-wide) by the wrong writer (the connection layer) for the benefit of one report.

**Correction to an earlier draft: "KILL works today" is false, and this proposal does not need it.**
An earlier version of this document asserted that `FUNCTION KILL` is "implemented and working". The
wiring is real; the *delivery* is not. `event_loop.rs:381` dispatches
`ShardMessage::Scripting(m) => self.dispatch_scripting(m).await` on the **same `&mut self` task**
that then runs `dispatch_scripting` → `handle_function_call` → `execute_function`, which is
synchronous CPU-bound Lua. While a shard is executing an `FCALL`, no further `ShardMessage` is
dequeued on that shard, so `ScriptingMsg::ScriptKill` sits in the channel and
`handle_script_kill` never runs — the `kill_flag` is never set. The kill is structurally
undeliverable precisely when it is needed.

That is **not a finding of this proposal**; it is pre-filed as
`.scratch/testing-improvements-round2/issues/open/85-scripting-residual-test-gaps.md` **F8**
(severity 4, priority 15), with the same mechanism cited.

No test in the tree contradicts this, because **no test kills a running script**:

* `server/tests/functions.rs:625` (`test_function_kill_not_busy`) — NOTBUSY on an idle server.
* `redis-regression/tests/functions_tcl.rs:215` (`tcl_function_kill_when_not_running`) — NOTBUSY.
* `connection/scripting/script.rs:325` (`function_kill_returns_when_a_shard_stalls`) — a *stalled*
  shard fixture that pins the per-shard await bound; the stalled shard is not running a script.
* `redis-regression/tests/scripting_tcl.rs:36-43` — the seven upstream
  `Timedout … can be killed by SCRIPT KILL` tests are excluded under a "FrogDB has a different
  script-execution model / redis-specific" header. Issue 85/**F16** rules that header **wrong**:
  FrogDB implements SCRIPT KILL, so those are FrogDB's own untested paths, not Redis internals.

**The deletion recommendation survives unchanged.** It never rested on KILL working; it rests on
(a) KILL not reading the registry, and (b) §3's three shape errors. If anything, F8 strengthens it:
the *message-passing* answer to "report/kill what a busy shard is doing" is the one thing that
provably cannot work, which is why the follow-up below is respecified as a shared cell rather than a
fan-out.

### 3. As typed, the seam cannot be made correct

Three independent shape errors, each of which forces a redesign before "wire it" could even be
attempted:

1. **Cardinality.** `Option<RunningFunctionInfo>` is single-valued, but FCALL routes to
   `shard_senders[target_shard]` (`connection/scripting/function.rs:61`) and each `ShardWorker` owns
   its own `ScriptExecutor` (`shard/types.rs:531`). N shards can be executing N functions
   simultaneously — the code's own comment at `connection/scripting/function.rs:388-389` says "only
   one can be running a script at a time **per shard**". A second concurrent FCALL would overwrite
   the first's entry, and the first's completion would clear the second's.
2. **Staleness.** `RunningFunctionInfo.duration_ms: u64` (`registry.rs:29`) is a plain number fixed
   at write time. Written at FCALL entry it is `0` forever; it can only be correct if the reader
   computes `now - start`, which requires storing an `Instant` — which is what `lua_vm.rs:27`
   already does.
3. **Leak safety.** `set_running_function(Some)` / `set_running_function(None)` around a body with
   `?`-returns and error paths leaks a phantom running function into process-lifetime state on any
   early exit. Making that safe needs a guard type — reinventing the scoped `store(true)/store(false)`
   pair `executor.rs:312/339` already has.

Any correct version is therefore "N entries, live start instants, scope-guarded" — which is
`ScriptExecutor` plus `LuaVm`, reached by a **shared per-shard cell** (not a message fan-out; see the
follow-up's amended shape and issue 85/F8). It is not this field.

### 4. Three more dead entries and a duplicated question on the registry's interface

`FunctionRegistry` has **thirteen** public methods (`grep -n 'pub fn' registry.rs`, excluding the
free function `new_shared_registry` at L243). The sweep found the interface carrying more than the
two dead methods above (verified with `grep -rn '\.<name>('`). The full table — an earlier draft
listed only eight rows and stated the total as twelve:

| Method | `registry.rs` | Production callers | Test callers |
| --- | --- | --- | --- |
| `set_running_function` | L174–176 | 0 | **0** |
| `get_running_function` | L179–181 | 0 | **0** |
| `library_names` | L184–186 | 0 | **0** |
| `function_count` | L189–191 | 0 | 1 (own test, L357) |
| `library_count` | L193–196 | 0 | 1 (own test, L356) |
| `stats()` | L165–171 | 1 (`connection/scripting/function.rs:219`) | 1 |
| `get_library` | L140–142 | 1 (`shard/functions.rs:44`) | 3 |
| `get_function` | L145–150 | 1 (`shard/functions.rs:36`, the FCALL lookup) | 4 |
| `list_libraries` | L153–162 | 2 (`persistence.rs:63`, `connection/scripting/function.rs:152`) | 3 |
| `load_library` | L69–115 | 3 (`function_store.rs:118`/`:235`, `server/init.rs:426`) | many |
| `delete_library` | L117–131 | 1 (`function_store.rs`, FUNCTION DELETE) | several |
| `flush` | L134–137 | 1 (`function_store.rs`, FUNCTION FLUSH/RESTORE) | several |
| `new` | L57–63 | many | many |

Phase 1 deletes five of the thirteen (`set_running_function`, `get_running_function`,
`library_names`, `function_count`, `library_count`), leaving **eight**.

`function_count()`/`library_count()` also duplicate `FunctionStats.function_count`/`library_count`
— two ways to ask one question, one of which no production code uses.

`load_library` returns `Result<Vec<String>, _>` whose `Ok` payload **no** caller in the tree binds
except one registry unit test. Complete site list (`grep -rn 'load_library' --include='*.rs'`,
excluding the unrelated free function `loader::load_library`):

| Site | Form | Effect of narrowing to `Result<(), _>` |
| --- | --- | --- |
| `function_store.rs:118` | `match registry.load_library(library, replace) { Ok(_) => {} … }` | none |
| `function_store.rs:235` | `if let Err(e) = registry.load_library(library, replace)` | none |
| `server/src/server/init.rs:426` | `if let Err(e) = registry.load_library(library, false)` | none |
| `registry.rs:272` | `let names = result.unwrap(); assert_eq!(names.len(), 2);` | **needs editing** — the only payload-binding site in the tree |
| `scripting/src/persistence.rs:281-282` | `.unwrap();` as a statement | none |
| `server/tests/functions.rs:672` | `.unwrap();` as a statement | none |
| `frogdb-recovery/src/tests.rs:1238` | `.expect("library loads into a fresh registry");` as a statement | none — and this is the **locked**-crate site, see Locked-spec check |

Note the path: it is `frogdb-server/crates/server/src/server/init.rs:426`, not `server/init.rs` as an
earlier draft wrote it.

That return value is additionally *wrong* for any caller who did want it: it is built from
`library.functions.keys()` (L104), which `add_function` lowercased (`library.rs:57`), so it would
report `myfunc` for a function registered as `myFunc`.

### 5. `FunctionLibrary` is anemic: six sites reach through it, two rebuild whole projections

All five fields are `pub` (`library.rs:11–23`), so the interface *is* the implementation and every
caller is coupled to the `HashMap`. Verified reach-through sites:

| Site | Fields read |
| --- | --- |
| `persistence.rs:69,74` (`dump_libraries`) | `name`, `code` |
| `connection/scripting/function.rs:159,162,167,193` (`handle_function_list`) | `name`, `engine`, `functions` (+ each `func.name`/`description`/`flags`), `code` |
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
by `name.to_ascii_lowercase()` while `RegisteredFunction.name` (**`scripting/src/function.rs:61`** —
the type file, not the connection handler) keeps the original case. `handle_function_list` correctly
reads `func.name` from the *value* (`connection/scripting/function.rs:171`); `registry.rs:83-84`
reads a *key* and calls `.to_ascii_lowercase()` on it again — a no-op that is direct evidence the
distinction is not legible from outside.

The accessors that already encode the rule (`library.rs:62-74`) have uneven use, and an earlier draft
of this document got one of them wrong:

| `FunctionLibrary` accessor | Callers |
| --- | --- |
| `get_function` (L62–64) | **A production caller exists: `registry.rs:148`**, inside `FunctionRegistry::get_function`, which is the FCALL lookup's second hop. Plus tests at `library.rs:98-99` and `parser.rs:232`. The earlier claim of "no callers outside `library.rs`'s own tests" was false. |
| `function_names` (L67–69) | Genuinely zero callers outside its own test (`library.rs:116`). |
| `function_count` (L72–74) | No production callers, but **test callers in three files**: `loader.rs:453`, `loader.rs:484`, `parser.rs:231`, plus `library.rs:97`. |

The consequence for phase 2 is small but real: `get_function` and `function_count` are already load-
bearing, so phase 2 is *extending* a partially-used accessor set rather than resurrecting a dead one,
and de-`pub`ing the fields cannot silently orphan them.

## Proposed change

### Phase 1 (TX8) — delete the seam. **Recommended.**

| | Option A: WIRE into the FCALL path (M) | **Option B: DELETE the seam (S)** |
| --- | --- | --- |
| Enables `FUNCTION KILL`? | **No.** KILL is wired through `executor.is_running()`, not the registry (§2). | No change — the KILL path is untouched. |
| Helps issue 85/F8 (KILL undeliverable to a busy shard)? | **No** — F8 is a message-delivery defect in the shard event loop; a registry field is on neither side of it. | No — and it does not worsen it. |
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
   has no production reader and is lowercase-mangled, §4). All three production call sites and all
   but one test site already discard it positionally; the **one site that must be edited** is
   `registry.rs:269-274` (`test_load_library`), whose `let names = result.unwrap();
   assert_eq!(names.len(), 2);` should become an assertion against `get_function` or `stats()`.
4. `scripting/lib.rs:34` and `core/lib.rs:95` — drop `RunningFunctionInfo` from both export lists.
5. `connection/scripting/function.rs:223-237` — replace the dead branch with an unconditional
   `running_script` → `Null`, plus a short comment naming *why* it is static and where the real
   state lives (`ScriptExecutor::is_running`), so the next reader does not re-add the field:

   ```rust
   // `running_script` is unconditionally Null: the running-script state lives per
   // shard in `ScriptExecutor` (`running` + `LuaVm::start_time`), which is what
   // FUNCTION KILL reads. Reporting it here needs a per-shard shared cell the
   // connection task can read without the shard's cooperation — not a process-wide
   // mirror and not a shard message (see issue 85/F8) — see the follow-up issue.
   result.push(Response::bulk(Bytes::from_static(b"running_script")));
   result.push(Response::Null);
   ```

**Deliberately not in this proposal — the follow-up that makes the report truthful.**

*Amended after review: the message fan-out originally proposed here does not work.* The first draft
proposed a `ScriptingMsg::ScriptStatus` reply scattered by `handle_function_stats` the same way
`handle_function_kill` scatters `ScriptKill`. That is defeated by the same mechanism as issue 85/F8:
`ScriptStatus` is a `ShardMessage`, so on the one shard whose answer matters — the one executing the
`FCALL` — it queues behind the very script it is asking about, and is answered only once that script
finishes, at which point the answer is "nothing is running". A shard message cannot report on a busy
shard.

The correct shape is a **per-shard shared status cell**, read directly by the connection task:

* One `Arc<ScriptStatusCell>` per shard (an `AtomicBool` plus the current function/library name and
  an `Instant`, or a small `ArcSwap`/`Mutex<Option<…>>` — the cell must be lock-free or
  uncontended-cheap on the write side, since it is written on every script entry/exit).
* Written by the executor at exactly the points that already exist: `executor.rs:312`
  (`running.store(true)`) and `executor.rs:339` (`running.store(false)`), so there is no new
  lifetime to leak-guard.
* Read by `handle_function_stats` on the **connection** task, which is not the blocked task — that
  is the whole point, and it is what makes the report work while a shard is wedged.
* **Ownership constraint (verified):** the cell must be owned by `ShardScripting`
  (`shard/types.rs:529`), **not** by `ScriptExecutor`. `ShardScripting::set_executor`
  (`types.rs:568`, called from `worker.rs:498` on a live `CONFIG SET` of the scripting config)
  replaces the whole executor at runtime; a cell owned by the executor would be swapped out from
  under every clone the connection layer holds, silently freezing the report.

Sized **M**. Filed separately, not folded in, because (a) it adds a construction-time seam through
`ShardScripting` and the connection `deps`, and (b) it needs a **documented deviation**: Redis is
single-threaded so `running_script` is one script, whereas a sharded FrogDB can have several — the
reply must either pick the longest-running or grow, and that is a compat decision, not a refactor.
It also pairs naturally with issue 85/F8's fixture (a busy shard), and with F8's own fix, which is
about kill *delivery* and needs the same "read/act on a shard without its cooperation" seam.
Deleting the field first is what makes that decision reachable: today the wrong answer is already
half-built and invites completion.

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
  **The encoding must not change** (same field order, same `u32` LE lengths, same `MAGIC` and
  `DUMP_VERSION`) — required by the on-disk `functions.fdb` contract and by rolling upgrades. Note
  this is *not* what FM-REPLICATION-055's forcing test checks; see the Locked-spec check.
* `connection/scripting/function.rs:152-197` → `for lib in libraries { let d = lib.describe(with_code); ... }`, and the
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

For the registry: **before**, thirteen public methods of which three are dead, two are test-only, and
one duplicates `stats()`; **after**, eight, every one with a production caller.

## Testability improvement

1. **Unreachable dead code disappears.** `connection/scripting/function.rs:226-234` cannot be covered
   by any test, because no code path can make `stats.running_function` `Some`.

   *Amended after review — the mutation-testing framing in the first draft was wrong on two counts.*
   First, `frogdb-server` is not one of the four locked pairs, so no `mutants-gate` applies to it at
   all. Second, and more concretely, this region generates **zero mutants** under the pinned
   `cargo-mutants 27.1.0` (`.mise.toml:38`): cargo-mutants mutates whole function bodies and binary /
   unary operators, and does **not** emit per-`if let`-arm deletions, while L226–234 contains no
   operator to mutate. The only mutant that would span it is the whole-body replacement of
   `handle_function_stats`, which returns `Response` — a type with **no `Default` impl anywhere in
   `frogdb-protocol`** (verified) — so that mutant is *unviable*, not a survivor. The correct
   statement is simply: **this is unreachable dead code**, and the house rule for unreachable code is
   deletion, not documentation. There is nothing to be unobservable about.
2. **The `LIST` projection becomes unit-testable in its owning crate.** Today, asserting "WITHCODE
   includes `library_code` and plain LIST does not", or "flags render as `no-writes`", requires a
   live server and RESP parsing (`server/tests/functions.rs`, `functions_tcl.rs`). After phase 2 it
   is `assert!(lib.describe(false).code.is_none())` inside `frogdb-scripting`. This matters
   materially: `cargo mutants -p <crate>` runs only that package's own tests, so behaviour forced
   solely from server integration tests contributes nothing to the owning crate's score — the
   projection currently has **no** in-crate test at all.
3. **One round-trip test covers three wire paths — by extending a test that already exists.**
   `snapshot()` is the single source for the `FUNCTION DUMP` reply
   (`connection/scripting/function.rs:262`), the `functions.fdb` file (`persistence.rs:176`) and the
   full-resync frame (`function_store.rs:274`, FM-REPLICATION-055).

   *Amended after review, on two points.*

   **(a) The round-trip test is not new.** `test_dump_restore_roundtrip` already exists at
   `persistence.rs:288`, built on the two-library `create_test_registry` fixture at
   `persistence.rs:~270-290`. Phase 2 **extends** it with a `dump == redump` assertion; it must not be
   presented as new coverage.

   **(b) A multi-library golden-bytes assertion is unwritable as first specified.**
   `FunctionRegistry.libraries` is a `HashMap<String, FunctionLibrary>` (`registry.rs:40`), so
   `list_libraries(None)` returns `self.libraries.values().collect()` in **unspecified iteration
   order** (`registry.rs:153-162`). `dump_libraries` emits libraries in exactly that order, so with
   more than one library the byte string is nondeterministic across runs and a fixed golden would be
   flaky. (The existing `test_dump_restore_roundtrip` already works around this — it locates its two
   libraries with `.find()` rather than by position.) Pick one of:

   * a **single-library** golden (deterministic; pins `MAGIC`, `DUMP_VERSION`, the two `u32` LE
     length prefixes and the checksum — everything the encoding contract actually needs) — *preferred*;
   * sort by library name before emitting, which is a **behaviour change to the dump** and therefore
     needs its own justification and a `DUMP_VERSION` conversation — not a free move;
   * assert a **multiset** of `(name, code)` pairs recovered by `restore_libraries`, which pins
     content but not framing.

   The recommendation is the single-library golden **plus** the extended round-trip: together they
   pin framing deterministically and content for N libraries.
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
| **46** (vll-acquire-error-unify) | `connection/scripting/eval.rs` **L268–281** (`continuation_error_to_response`), `vll/src/types.rs`, `scatter/executor.rs:149-157` | **None.** Different file in the same directory; 49 touches no `eval.rs` line. | Independent; land in any order. |
| **47** (eval.rs orchestration) | `connection/scripting/eval.rs` — EVAL/EVALSHA orchestration, shard-reply error mapping | **None.** 47's own boundaries table records "None" against 49 as well — the two agree. | Independent; land in any order. |
| **48** (fcall cross-shard) | `connection/scripting/function.rs` **L14–73 only** (`handle_fcall` routing, `SlotValidator::same_shard`, keyless→shard-0), plus `keyless_script_shard`, the `server/tests/functions.rs` multi-shard module and the VLL spec scope paragraph | **Same file, disjoint line ranges.** *Corrected after review:* an earlier draft of this document said 48 also owns the subcommand match at L84–99. It does not — 48's files table states of `connection/scripting/function.rs`: "`handle_fcall` (L14–73) … **Nothing else in the file (the `FUNCTION` subcommands) is touched**", and its boundaries table attributes **L76–313** to 49. 49 in practice edits only `handle_function_list` (L108–200) and `handle_function_stats` (L214–254) — the *reply construction*. L76–107 (`handle_function`'s subcommand match) is edited by neither. | **48 lands first** (it closes a live capability gap); 49 rebases. Line ranges do not collide, so a textual conflict is unlikely (expect at most an import-block conflict at L1–10); the file is shared — do not run both in the same worktree. **Explicit ask of 48:** when wiring the FCALL path, do **not** reach for `set_running_function` — 49 deletes it, and §3 explains why re-adding it would be wrong. If 48 wants running-script state, it belongs in the follow-up's per-shard status cell. |
| **45** (VLL) | `frogdb-vll`, `core/shard/vll.rs` | None. | Independent. |
| **H3 glob hotfix** — **already landed** | `registry.rs` `matches_pattern` and `match_pattern_recursive`, their test, and the `list_libraries` call site — replaced by `frogdb_types::glob_match`; also `commands/src/utils.rs` `simple_glob_match` and `redis-regression/tests/scan_regression.rs` | **Same file.** *Corrected after review:* there is no "H3 hotfix" document under `.scratch/arch-deepening/`. H3 is the glob hotfix and it **already exists as a standalone commit, sha `0b034a4f`** ("fix(commands,scripting): use canonical Redis glob in H/S/ZSCAN MATCH and FUNCTION LIST", 2026-08-10) on branch `worktree-agent-a84216c599d8af135`, **pending merge to `main`**. | **No dependency to schedule.** Every `registry.rs` line number in this document was re-verified against the current worktree; `matches_pattern` is still present at L198–236 here because `0b034a4f` has not merged yet, and phase 1's deletions at L174–196 sit immediately above it. When `0b034a4f` merges, `registry.rs` shrinks by ~34 lines below L196 and **phase 1's own targets (L11–196) do not move**. Phase 2 changes only what *callers* of `list_libraries` do with the returned `&FunctionLibrary`s, so it is indifferent to which glob is inside. Merge order is free; re-verify anyway before starting. |
| **Follow-up issue** (truthful `running_script`) | `shard/types.rs` (`ShardScripting`), `executor.rs:312/339`, connection `deps`, `handle_function_stats` | **No longer touches `ScriptingMsg`** — the amended shape is a shared cell, not a message (see phase 1). So the previously-noted collision with 48 over `ScriptingMsg` is gone. | Deferred deliberately (see phase 1). File after 49 lands; coordinate with issue 85/F8, which needs the same seam. |

**Other risks.**

* **Dump encoding stability (highest).** `dump_libraries` feeds a replication frame
  (FM-REPLICATION-055) and the on-disk `functions.fdb`. `snapshot()` must reproduce field order,
  `MAGIC`, `DUMP_VERSION` and `u32` LE framing exactly. The binding constraint is the **on-disk file
  and rolling upgrades**, not the FM-055 forcing test — that test builds both nodes from one tree and
  cannot see an encoding change (Locked-spec check). Mitigation: extend the existing
  `test_dump_restore_roundtrip` (`persistence.rs:288`) with `dump == redump`, plus a
  **single-library** golden-bytes assertion, both written **before** the refactor. A multi-library
  golden would be flaky — `libraries` is a `HashMap` (`registry.rs:40`) and the dump inherits its
  iteration order (Testability §3).
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

* **Phase 1 (TX8, delete): S.** One struct field, one struct, five dead/test-only methods (of the
  registry's thirteen, leaving eight), two export lists, one return-type narrowing whose only
  payload-binding site is `registry.rs:272`, one branch collapsed to a constant, three test
  assertions updated. No behaviour change on the wire. The only new test is the `FUNCTION STATS`
  shape pin. Independently landable ahead of phase 2, and ahead of or behind 46/47/48.
* **Phase 2 (TX9, encapsulate): M.** Three new plain-data structs plus two methods and two
  accessors in `library.rs`; five fields de-`pub`ed; six walker sites re-pointed across three
  crates; `handle_function_list` reduced to RESP encoding; `dump_libraries` reduced to a
  `snapshot()` loop. New tests: `describe(with_code)` both ways, flag rendering, name-case
  distinction, and the dump byte-stability round-trip. The long pole is proving the dump bytes
  unchanged, not the diff.
* **Ordering.** Phase 1 before phase 2 (phase 1 shrinks the registry surface phase 2 then re-points
  through). **No H3 dependency** — H3 is `0b034a4f`, already committed and pending merge, and it does
  not overlap either phase's targets (siblings table).

### Independently-landable hotfix

**None. Phase 1 is not a hotfix, and this proposal ships no fix for the client-visible defect.**

Stated carefully, because two things in this document could be read as contradicting each other and
do not:

* The always-`Null` `running_script` **is a client-visible under-report today** — §1, and issue
  85/F15 at severity 2. That is not in dispute.
* **Phase 1 does not fix it.** The reply is `Null` before and after. What phase 1 fixes is the code's
  *claim* to report — a maintainability defect — and it makes the client-visible gap legible instead
  of disguised as a live branch that a future reader would "finish".

The client-visible fix is the deferred per-shard status cell (**M**, respecified above). It is not a
hotfix: it needs a construction-time seam through `ShardScripting`, and a Redis-deviation decision
about how a sharded server reports N concurrently-running scripts. It must not be rushed ahead of
either.

Phase 1 is nonetheless the natural independently-landable slice: it is self-contained, touches no
locked crate, changes no wire bytes, and leaves phase 2 as "and now the fields go private".
