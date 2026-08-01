# scripting — residual test gaps (10 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/09 — residual findings after promotion to issues 19–76
Score: 10 findings, priority range 11–16
Area: `frogdb-server/crates/scripting/src/` (3309 LOC, **no `tests/` dir**) and `frogdb-server/crates/core/src/scripting/` (4079 LOC), with the adjacent `core/src/shard/{scripting,functions,dispatch_scripting,post_execution}.rs` and `server/src/connection/scripting/{eval,script,function}.rs` seams read but not owned

## Context

This is Lua `EVAL`/`EVALSHA` and the `FUNCTION`/`FCALL` subsystem: the sandbox, the loader, the
per-shard long-lived `LuaVm`, the `classify` gate and the effect-based replication path. Crate
`scripting` sits at **1634/1970 lines (82.9%)** and **2739/3387 regions (80.9%)** — the lowest
line coverage of any in-scope core crate, with region coverage 4pp below line coverage, i.e. the
covered lines are being entered on one branch only. Three files in this area are among the
workspace's 60 worst by absolute uncovered lines (`core/src/scripting/executor.rs` 287 uncovered,
`core/src/scripting/gate.rs` 286, `scripting/src/loader.rs` 200). Per-function depth classes are
**not recoverable** for this area — `depth.json` carries only per-crate rollups plus truncated
top-N tables and this export's `FNDA`/`DA` records are all zero — so the findings cite source
evidence rather than depth classes. The proposal's verdict on the shape of that coverage: the
subsystem *"is architecturally sound in its big decisions … and the tests pin those decisions
well. The risk lives entirely in the edges that no test touches"* — with two of the biggest gaps
actively *masked* by mislabelled parity exclusions.

## Promoted elsewhere

- F1 → issue 19, `.scratch/testing-improvements-round2/issues/` (theme T1 — hand-maintained parallel command tables drift from `CommandSpec`; the scripting write-flags list `is_write_command` lets `_RO`/`no-writes` scripts write)
- F2 → issue 67, `.scratch/testing-improvements-round2/issues/` (the `_protected` Lua table gains one strong entry per EVAL/FCALL → unbounded per-shard Lua heap)
- F3 → issue 37, `.scratch/testing-improvements-round2/issues/` (Lua sandbox escape via `_G`'s raw keys `__frogdb_backing` / `__frogdb_protected`)
- F4 → issue 60, `.scratch/testing-improvements-round2/issues/` (script timeout commits and replicates partial effects) **and** issue 24, `.scratch/testing-improvements-round2/issues/` (theme T6 — errors do not roll back their effects)
- F5 → issue 48, `.scratch/testing-improvements-round2/issues/` (FUNCTION libraries never replicated, absent from RDB/full-sync)
- F12 → issue 70, `.scratch/testing-improvements-round2/issues/` (unbounded allocations — FUNCTION LOAD capture VM with `memory_limit_bytes: 0`)

## Residual findings

### F6 — Cross-shard scripted writes propagate immediately and unframed, so replicas *do* observe intermediate script state

- **Severity** 4 — the module contract is explicit that they must not
  (`core/src/shard/post_execution.rs:630-634`: "multiple writes propagate as an atomic
  MULTI/EXEC-framed batch … so replicas never observe intermediate script state"). For a
  cross-shard script that guarantee is false, and the *ordering* is inverted: remote writes are
  committed and propagated mid-script, local writes only after the script returns.
- **Likelihood** 4 — default multi-shard standalone; nothing prevents a script from touching
  keys on two shards outside cluster mode (slot cohesion is only enforced for shebang scripts in
  cluster mode — see F13).
- **Effort** 4 — needs a multi-shard server with a replica and a way to observe intermediate
  replica state, or WAL/replication-stream capture at the `shard_driver` level.
- **Priority** 16
- **Evidence**: `core/src/scripting/gate.rs:484-487` routes a non-local key to
  `run_remote_inner`; the target shard runs `execute_script_sub_command`
  (`core/src/shard/scripting.rs:181-232`) which calls
  `self.run_script_write_effects(vec![record], conn_id).await` at `:225` — one record, so
  `EffectScope::Command` (`post_execution.rs:645-649`), propagated *during* the script. The
  local writes accumulate in `ctx.effects.script_writes` and only run at
  `shard/scripting.rs:130`. A script that errors after a successful remote write leaves that
  write propagated with no framing and no companion. No test exercises a cross-shard script
  with a replica attached.
- **Proposed test**: multi-shard primary + replica; script writes `{a}` (shard X) then `{b}`
  (shard Y) then errors. Assert both shards' replicas converge with the primary, and pin the
  framing that is actually emitted (per-shard `EffectScope::Command` is a legitimate design
  answer for a sharded engine — but then the doc comment must say so and the test must say so).
- **Boundary**: 3 → 5. See OPTIONS.
- **OPTIONS**:
  - *(a)* `shard_driver` + `fake-wal`: drive a cross-shard script and assert the WAL/propagation
    record sequence per shard (effort 3). Deterministic, no networking; asserts the mechanism
    not the observable.
  - *(b)* Server integration, multi-shard, one replica, read the replica mid-script via a second
    connection (effort 4). Observable but racy — needs a blocking point inside the script.
  - *(c)* Turmoil/deterministic-sim ordering test (effort 5). Overkill for a documented-contract
    mismatch.
  - **Recommendation**: (a), and use the result to correct the `post_execution.rs` doc comment;
    escalate to (b) only if the answer is "this really should be atomic".

### F7 — `FUNCTION RESTORE` is non-atomic: a mid-list failure destroys the previous libraries and leaves memory and disk disagreeing

- **Severity** 4 — `FUNCTION RESTORE … FLUSH` wipes the registry first and then loads
  library-by-library; a failure on library *k* leaves libraries 1..k-1 loaded, k..n missing, and
  everything that was there before permanently gone. Redis builds the replacement context and
  swaps atomically.
- **Likelihood** 3 — restoring a dump taken from a different build, or one containing a library
  whose name now conflicts, is exactly the disaster-recovery scenario RESTORE exists for.
- **Effort** 2 — the failure is reachable through the registry API with a hand-built payload.
- **Priority** 16
- **Evidence**: `server/src/connection/scripting/function.rs::handle_function_restore` —
  `registry.flush()` runs before the loop, and both the `load_library` parse failure and the
  `registry.load_library` conflict failure `return Response::error(...)` from inside the loop.
  `self.persist_functions()` is only reached on success, so after a failed restore the in-memory
  registry (partial) and `functions.fdb` (pre-restore) disagree until the next successful
  mutation — a restart silently "undoes" a partial restore. `server/tests/functions.rs:356`
  (`test_function_dump_restore`) covers only the happy path.
- **Proposed test**: load libraries A and B; `FUNCTION DUMP`; construct a payload whose second
  entry is invalid (or conflicts); `FUNCTION RESTORE <payload> FLUSH` → assert the command
  errors **and** `FUNCTION LIST` still returns exactly A and B. Same for `REPLACE` and the
  default `APPEND`.
- **Boundary**: 4 (server integration) — RESTORE is a connection-level command with no
  lower-level entry point; the harness already has `TestServer` + FUNCTION helpers.

### F8 — SCRIPT KILL / FUNCTION KILL cannot reach a shard that is running a script, and the timeout error is swallowable by user `pcall`

- **Severity** 4 — a runaway script wedges one shard with no operator remedy short of a process
  restart, and the operator-facing symptom is a scatter-gather timeout rather than anything
  diagnostic.
- **Likelihood** 3 — an accidental infinite loop is the classic scripting incident; the
  `pcall`-swallowing variant needs a script that wraps work in `pcall`, which is idiomatic.
- **Effort** 3 — server integration with a background script and a second connection.
- **Priority** 15
- **Evidence**: two independent mechanisms, both untested.
  (1) *Delivery*: `SCRIPT KILL` scatters `ScriptingMsg::ScriptKill`
  (`server/src/connection/scripting/script.rs:106-120`, and `function.rs:496-507` for
  `FUNCTION KILL`) onto each shard's message channel; the shard worker's event loop is the same
  single task that is *inside* `run_script` (`core/src/shard/scripting.rs:88-152`) for the
  duration of the script, so `handle_script_kill` (`shard/scripting.rs:235-245`) cannot run and
  `kill_flag` is never set. The kill is structurally undeliverable exactly when it is needed.
  (2) *Escapability*: the hook raises an ordinary `mlua::Error::RuntimeError`
  (`scripting/src/sandbox.rs:213`, `:225`), so
  `while true do pcall(function() while true do end end) end` catches both the timeout and the
  kill error and loops forever. The `lua_vm.rs` tests cover `request_kill`/`Unkillable` as a
  state machine only (`lua_vm.rs:727-733`); `server/tests/functions.rs:625` covers only the
  NOTBUSY branch; and every upstream test for this is excluded (see F16).
- **Proposed test**: start `EVAL "while true do end" 0` on connection A; on connection B assert
  (a) other commands on the same shard get a `BUSY` reply rather than hanging, (b) `SCRIPT KILL`
  returns OK within a bounded time and connection A receives the killed error, (c) the same with
  the script body wrapped in `pcall` still terminates, and (d) a script that has already written
  returns `UNKILLABLE`. Every assertion needs a hard timeout so the test fails rather than hangs.
- **Boundary**: 4 (server integration) — the whole point is a *second connection* interrupting a
  busy shard; nothing below the connection layer can express it.

### F9 — Lua error/status strings are passed to the RESP encoder unsanitised, so `{err=…}` can inject frames into the client stream

- **Severity** 4 — a CRLF in an error or status reply produces additional wire frames on that
  connection: the client's request/response alignment is broken and a forged reply can be
  attributed to the *next* command. Redis normalises newlines in error replies for this reason.
- **Likelihood** 2 — needs a script that returns attacker- or app-controlled text in an error
  table; multi-line error text from application code is a plausible accident.
- **Effort** 2 — crate-level or a single server round trip.
- **Priority** 14
- **Evidence**: `core/src/scripting/executor.rs::lua_to_response` returns
  `Response::Error(Bytes::from(e))` / `Response::Simple(Bytes::from(o))` straight from the Lua
  table's `err`/`ok` fields with no filtering; `redis.error_reply`/`status_reply`
  (`core/src/scripting/lua_vm.rs:566-604`) only special-case the empty string. The encoder
  (`protocol/src/response.rs:227-230`) wraps the bytes in
  `Resp2BytesFrame::SimpleString`/`Error` unchanged. Note the deliberate contrast: *Lua runtime*
  errors are first-line-stripped precisely because of this hazard
  (`lua_vm.rs:262-264`: "error messages are stripped to the first line because RESP simple errors
  must not contain newlines"), but the `{err=…}` *return-value* path bypasses that.
- **Proposed test**: `EVAL "return {err='ERR a\r\n+INJECTED'}" 0` followed by `PING` on the same
  connection — assert the second reply is `PONG` and that the first reply's error text contains
  no CR/LF. Same for `{ok=…}` and `redis.error_reply`. A second cheap case: a non-UTF-8 `err`
  value currently falls through the `t.get::<String>("err")` conversion and is silently
  reinterpreted as a generic table (→ nil) — pin whatever behaviour is chosen.
- **Boundary**: 4 (server integration) — the bug is *on the wire*; asserting on a `Response`
  value would miss it entirely. This is the rare case where the socket is the point.
- **Cross-area** (carried from the proposal's cross-area notes): F9's CRLF injection is only
  *reachable* through scripting but is *fixable* in `protocol/src/response.rs` (sanitise
  `WireResponse::Simple`/`Error` at encode time). The protocol area's equivalent finding is
  filed as issue 38, `.scratch/testing-improvements-round2/issues/`; if that lands
  encoder-level sanitisation, F9's test should assert at the protocol level and scripting keeps
  only a smoke test. Recommend the two be resolved together.

### F10 — Lua→RESP3 conversion is inferred rather than declared, so the same script returns different types to RESP2 and RESP3 clients

- **Severity** 3 — a wrong answer on a data path the user notices: a client that upgrades to
  RESP3 (as most modern clients now do by default) silently changes the type of every script
  result that is a float or a non-array table.
- **Likelihood** 4 — `HELLO 3` is the default in current client libraries.
- **Effort** 3 — needs a RESP3 connection, which the harness supports.
- **Priority** 14
- **Evidence**: `core/src/scripting/executor.rs::lua_to_response` — `Value::Number(n)` becomes
  `Response::Double(n)` under RESP3 but `Response::Integer(n as i64)` under RESP2, so
  `return 100.5` is `:100` to one client and a double to another (Redis truncates to `100` for
  both, and `scripting_tcl.rs::tcl_eval_lua_integer_to_redis_protocol` pins only the RESP2
  half). Under RESP3 a table with no `[1]` is walked with `t.pairs()` and emitted as a
  `Response::Map` in Lua's arbitrary hash order — Redis instead requires the script to *declare*
  a map with `{map={…}}`, and likewise `{double=}`, `{big_number=}`, `{set=}`. None of those
  field names is handled. `redis.setresp` is absent from the `redis` table entirely
  (the registered names are `call`, `pcall`, `log`, `LOG_*`, `REDIS_VERSION[_NUM]`, `sha1hex`,
  `error_reply`, `status_reply`, `replicate_commands`, `set_repl`, `REPL_*`), so a script
  containing the idiomatic `redis.setresp(3)` fails on the readonly-table `__index`. The
  `scripting_tcl.rs` header excludes all `resp3`-tagged tests.
- **Proposed test**: a conversion table driven over both protocol versions asserting each of:
  `return 3.7`, `return {1,2,3}`, `return {a=1}`, `return {ok='X'}`, `return {err='X'}`,
  `return {map={…}}`, `return {double=3.5}`, `return redis.call('CONFIG','GET',…)` — with the
  RESP2 and RESP3 expectations written side by side so a divergence is visible in the source.
  Include `redis.setresp(3)` returning either a working switch or a clean error, not a
  nonexistent-global error.
- **Boundary**: 4 (server integration) — protocol version is a connection property; a unit test
  on `lua_to_response` can cover the mapping but not the `HELLO 3` plumbing. Do both: the table
  at level 2 for breadth, three smoke cases at level 4 for the plumbing.

### F11 — `redis.call` silently drops `nil` arguments, turning a buggy call into a different valid command

- **Severity** 3 — `redis.call('SET', KEYS[1], v, 'EX', ttl)` with `v = nil` executes
  `SET key EX ttl`, i.e. it sets the key to the literal string `"EX"` with no TTL, and returns
  OK. Redis raises "Lua redis lib command arguments must be strings or integers". A wrong value
  is written and replicated with no error anywhere.
- **Likelihood** 3 — `nil` from a missing `ARGV[n]` or a failed lookup is the single most common
  Lua scripting mistake.
- **Effort** 1 — pure unit test on `lua_args_to_command`, plus one behavioural case.
- **Priority** 14
- **Evidence**: `core/src/scripting/bindings.rs::lua_args_to_command` — the `Value::Nil` arm is
  `continue` with the comment "Skip nil values (common in Lua when passing optional args)".
  Every other non-string/number/boolean type errors. `bindings.rs` has 5 inline tests total and
  none covers this arm.
- **Proposed test**: assert `lua_args_to_command` on `["SET", "k", nil, "EX", "60"]` errors
  rather than yielding `["SET","k","EX","60"]`; behavioural companion:
  `EVAL "redis.call('SET', KEYS[1], ARGV[1])" 1 k` with no `ARGV[1]` returns an error and leaves
  `k` unset. If the skip is deliberate, pin it explicitly with a comment referencing the Redis
  divergence — but it must not be silent.
- **Boundary**: 1 (pure unit) — argument marshalling is a pure function; the behavioural
  companion belongs at level 3 (`shard_driver`) so it asserts the resulting keyspace.

### F13 — FCALL never enforces slot cohesion, in any mode

- **Severity** 3 — a cluster `FCALL` may touch keys in slots this node does not own; the write
  lands on whatever shard the key hashes to locally with no `CROSSSLOT` rejection, so a later
  slot migration silently splits data the function treated as co-located. Redis enforces slot
  cohesion for FUNCTION exactly as for EVAL.
- **Likelihood** 3 — cluster mode plus FUNCTION plus a multi-key function; slot migration is the
  event that surfaces it.
- **Effort** 2 — the executor's inline cluster tests already do this for EVAL.
- **Priority** 13
- **Evidence**: `core/src/scripting/executor.rs::execute_function` passes
  `false, CrossSlotTracker::new(None)` into `execute_in_scope` (`executor.rs:526-531`) — the
  enforcement flag is hard-coded off and the tracker has no seed, so
  `gate.rs:239-241`'s `check_all` never runs for FCALL. This is *additional to*, not covered by,
  round-1 issue 26, whose declared residue is only "a plain (non-shebang) `EVAL` in cluster mode
  is not slot-enforced". The round-1 tests at `executor.rs` pin the EVAL matrix; there is no
  FCALL equivalent.
- **Proposed test**: mirror the round-1 EVAL matrix for FCALL: with `is_cluster_mode = true`, a
  function declaring `{tag}a` that calls `redis.call('get', '{other}b')` is rejected with "do not
  hash to the same slot"; the same with `allow-cross-slot-keys` in the shebang succeeds; a
  same-slot undeclared key succeeds. Also pin the non-shebang cluster EVAL residue in the same
  file so both halves of the divergence are recorded in one place.
- **Boundary**: 2 (crate API) — `executor.rs`'s existing cluster tests run real Lua through
  `eval`/`execute_function` with a `HashMapStore`; that is exactly the right level and already
  established.

### F14 — Library top-level code re-executes on every FCALL, with `redis.call` live — unlike at LOAD

- **Severity** 3 — a library whose top level has side effects performs them on every call;
  worse, `redis.call` is *bound* during that re-execution (it is a hard error during LOAD), so
  library top-level code can read and write the keyspace outside any registered function and
  outside the caller's declared keys. A library that loads cleanly can also fail at FCALL time.
- **Likelihood** 3 — any library with non-trivial top-level initialisation; the cost is paid on
  the hot path.
- **Effort** 2 — crate-level through `execute_function`.
- **Priority** 13
- **Evidence**: `core/src/scripting/executor.rs:526-538` — inside `execute_in_scope` (where
  `redis.call`/`pcall` are already bound to the live invoker), `execute_function` calls
  `setup_function_registration()`, then `vm.execute(lcc)` (the whole library body, shebang
  stripped), then `lock_runtime_environment()`, then the function invocation. During
  `FUNCTION LOAD` the same body runs on the capture VM where `redis.call` is a stub that errors
  (`scripting/src/loader.rs::setup_register_function_binding`). No test asserts what a
  side-effecting library top level does at FCALL time.
- **Proposed test**: a library whose top level does `redis.call('INCR', 'loadcount')`; assert
  `FUNCTION LOAD` errors (as it does today) and then, for a library whose top level increments a
  Lua-local counter, assert the counter's value after three FCALLs — pinning whether the intended
  contract is "once" or "per call". Second: assert that a top-level `redis.call` at FCALL time
  is rejected the same way it is at LOAD, so the two lifecycles agree.
- **Boundary**: 2 (crate API) — `execute_function` with a `HashMapStore` shows both the
  re-execution and the keyspace effect.

### F15 — `FUNCTION STATS` never reports a running function: the field is dead

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`).
`MASTER.md` §5 names `set_running_function` (zero callers, "which is why FUNCTION STATS
`running_script` is always null") among the dead code to delete, but cites no finding numbers, so
it claims nothing on its own — the sweep may delete the setter, in which case this finding
becomes "either wire the field or remove it from the reply", and the test below is what forces
that choice.

- **Severity** 2 — wrong observability on the one command an operator reaches for when a
  function is stuck; combined with F8 it means a wedged shard is invisible *and* unkillable.
- **Likelihood** 4 — any operator who runs `FUNCTION STATS` during an incident.
- **Effort** 2 — the registry API is directly testable; the end-to-end half needs a busy shard.
- **Priority** 12
- **Evidence**: `FunctionRegistry::set_running_function`
  (`scripting/src/registry.rs:174-176`) has **no callers anywhere in the workspace** — the only
  reference to the field outside `registry.rs` is the reader at
  `server/src/connection/scripting/function.rs:317`. `registry.rs`'s `test_stats` asserts
  `stats.running_function.is_none()` on a fresh registry, which is the only state that can ever
  occur. `server/tests/functions.rs:408` (`test_function_stats`) exercises the idle case only.
- **Proposed test**: while a long-running FCALL is in flight on connection A, assert
  `FUNCTION STATS` on connection B reports the running function's name, library and a non-zero
  duration. Pairs naturally with F8's test — same fixture, same busy script.
- **Boundary**: 4 (server integration) — the field is only meaningful concurrently with an
  in-flight call. Fold into F8's fixture rather than building a second one.

### F16 — Two parity-exclusion headers are wrong, and they exclude precisely the families that cover this area's worst gaps

- **Severity** 2 — no runtime consequence, but the headers are the record of what is
  *deliberately* not tested, and both are misleading in a way that has already prevented these
  gaps from being noticed.
- **Likelihood** 3 — the next person to audit this area reads the header and moves on.
- **Effort** 1 — reclassify and, where the behaviour exists, port the test.
- **Priority** 11
- **Evidence**: `redis-regression/tests/scripting_tcl.rs:36-44` excludes seven tests
  (`Timedout read-only scripts can be killed by SCRIPT KILL`, `… even when use pcall`,
  `Timedout scripts that modified data can't be killed by SCRIPT KILL`, …) under the heading
  "Script timeout / SCRIPT KILL (FrogDB has a different script-execution model)" and tags each
  `redis-specific — Redis-internal feature`. FrogDB *implements* SCRIPT KILL
  (`shard/scripting.rs:235`, `lua_vm.rs:472`, the `Unkillable` variant, the BUSY message in
  `sandbox.rs:222`) — these are not Redis-internal, they are FrogDB's own untested paths, and
  the `… can't be killed by SCRIPT KILL` case is the exact rule F4 shows the timeout path
  violates. Separately, `scripting_tcl.rs:66` excludes `cmsgpack` tests because "FrogDB may not
  ship cmsgpack", but it does (`sandbox.rs::setup_cmsgpack_library`, registered as a protected
  global; `msgpack_to_lua` is `single-test`). Round 1 already corrected a third wrong header in
  this same file (issue 26), so the file has form.
- **Proposed test**: not a test — a header correction plus porting the four SCRIPT KILL tests
  and the cmsgpack tests that FrogDB can actually satisfy, with the remainder retagged with the
  real reason.
- **Boundary**: 4 (the regression suite's own level) — these are parity tests; they belong where
  the rest of the ported suite lives.
- **Note**: the `… can't be killed by SCRIPT KILL` rule this header hides is the one F4 violates;
  F4 is promoted to issue 60, `.scratch/testing-improvements-round2/issues/`, so schedule the
  header correction alongside it.

### Deprioritised in the proposal (unnumbered — not counted as findings)

Carried verbatim so nothing in `proposals/09-scripting.md` is lost. The proposal gave these no
`F<n>` numbers, so they are outside the 16-finding arithmetic and carry no acceptance criterion.
Two of them — `wrote_in_readonly` and `extract_keys_from_command` — the proposal folded into F1,
which is promoted to issue 19, `.scratch/testing-improvements-round2/issues/`.

- **`math.random` is unseeded and Lua 5.4-random per VM.** Redis replaces it with a
  deterministic PRNG because Redis replicates the *script*; FrogDB replicates *effects*
  (`ScriptWriteRecord` → `run_script_write_effects`), so non-determinism inside a script cannot
  diverge a replica. Same reasoning retires `TIME`/`RANDOMKEY`/`SRANDMEMBER`/`SPOP` determinism
  as a replication concern. Worth one cheap parity note, not a finding. (The *cross-invocation*
  concern — a script calling `math.randomseed` affecting later scripts on the shard — is
  already covered by F3's isolation test.)
- **Corrupt `functions.fdb` at startup.** Already tested (`server/src/recovery/tests.rs:154-171`,
  "corrupt functions.fdb is not fatal").
- **FUNCTION persistence across restart.** Already tested three ways
  (`server/tests/functions.rs:652/706/751`).
- **`save_to_file` does not fsync the containing directory** (`scripting/src/persistence.rs`,
  temp file + `sync_all` + rename). Real, but the durability-boundary work is round-1 issue 12's
  territory and the blast radius here is one file of function source; flag it to the persistence
  agent rather than duplicating the fsync harness.
- **Two `persistence.rs` tests use fixed filenames under `std::env::temp_dir()`** — a
  parallel-execution collision waiting to happen, but a hygiene fix (use `tempfile`), not a
  coverage gap.
- **`wrote_in_readonly` is unreachable** (`executor.rs:338`, `:541`): `classify` rejects and
  returns before `mark_write`, so `has_writes` can never be true when `read_only` is. It is a
  dead guard that *looks* like a backstop for F1 and is not one. Folded into F1's evidence
  rather than filed separately.
- **Deep recursion / stack exhaustion and huge return values.** `unpack` is capped at 8000
  (`sandbox.rs:341`) and cjson has its own nesting limits; a dedicated fuzz corpus for Lua
  inputs would be the right tool but is new infrastructure (effort 5) for a bounded-severity
  outcome. Better folded into the existing `testing/fuzz` corpus work (round-1 issue 40) than
  built here.
- **`extract_keys_from_command`'s hand-written key-position table** (`bindings.rs`, ~150 lines)
  duplicating the registry's `keys()`. Same class of defect as F1, but it only affects
  slot-cohesion accounting and shard routing (a wrong key position routes to the wrong shard,
  which `run_local`/`run_remote` still executes correctly against the owning store). Fixing it
  is the same cross-check test as F1 — recommend extending F1's test to cover key positions
  rather than filing a second finding.
- **`parse_flags_value` map-style silently ignores unknown keys** while array style errors
  (`scripting/src/loader.rs`). A typo'd `no-write` in map style yields a *writable* function
  that the author believes is read-only. Low likelihood (map style is the rarer form) and it is
  a one-line unit test — bundle it into whatever loader work lands, not a standalone finding.

## Acceptance criteria

- [ ] F6: a test drives a cross-shard script that writes `{a}` on shard X, then `{b}` on shard Y, then errors, and asserts the per-shard WAL/propagation record sequence (framing included) and that both shards' replicas converge with the primary; and the `post_execution.rs:630-634` doc comment is corrected to match whatever framing the test pins.
- [ ] F7: a test asserts that `FUNCTION RESTORE <payload> FLUSH` with an invalid or conflicting second entry errors **and** leaves `FUNCTION LIST` returning exactly the pre-restore libraries A and B — repeated for `REPLACE` and the default `APPEND`.
- [ ] F8: a test asserts, against a shard running `EVAL "while true do end" 0` on connection A, that (a) other commands on that shard get `BUSY` rather than hanging, (b) `SCRIPT KILL` from connection B returns OK within a bounded time and connection A receives the killed error, (c) the same holds when the script body is wrapped in `pcall`, and (d) a script that has already written returns `UNKILLABLE` — every assertion under a hard timeout so the test fails rather than hangs.
- [ ] F9: a test asserts that after `EVAL "return {err='ERR a\r\n+INJECTED'}" 0` the next reply on the same connection is `PONG` and the error text contains no CR/LF — likewise for `{ok=…}` and `redis.error_reply` — and that a non-UTF-8 `err` value produces the pinned behaviour rather than silently becoming nil.
- [ ] F10: a conversion table asserts the RESP2 and RESP3 result of each of `return 3.7`, `return {1,2,3}`, `return {a=1}`, `return {ok='X'}`, `return {err='X'}`, `return {map={…}}`, `return {double=3.5}` and `return redis.call('CONFIG','GET',…)` side by side, and that `redis.setresp(3)` yields either a working switch or a clean error (not a nonexistent-global error).
- [ ] F11: a unit test asserts `lua_args_to_command(["SET","k",nil,"EX","60"])` errors rather than yielding `["SET","k","EX","60"]`, plus a behavioural test that `EVAL "redis.call('SET', KEYS[1], ARGV[1])" 1 k` with no `ARGV[1]` errors and leaves `k` unset.
- [ ] F13: a test asserts, with `is_cluster_mode = true`, that an FCALL declaring `{tag}a` and calling `redis.call('get','{other}b')` is rejected with "do not hash to the same slot", that `allow-cross-slot-keys` in the shebang makes it succeed, and that a same-slot undeclared key succeeds; and the non-shebang cluster EVAL residue is pinned in the same file.
- [ ] F14: a test asserts a library whose top level calls `redis.call('INCR','loadcount')` fails `FUNCTION LOAD`, asserts the value of a Lua-local top-level counter after three FCALLs (pinning "once" or "per call"), and asserts a top-level `redis.call` at FCALL time is rejected the same way it is at LOAD.
- [ ] F15: a test asserts that while a long-running FCALL is in flight on connection A, `FUNCTION STATS` on connection B reports the running function's name, library and a non-zero duration — or, if the sweep deletes `set_running_function`, that the field is absent from the reply rather than always null.
- [ ] F16: the `scripting_tcl.rs:36-44` header no longer tags the SCRIPT KILL family `redis-specific — Redis-internal feature`, the four SCRIPT KILL tests FrogDB can satisfy are ported, the `scripting_tcl.rs:66` cmsgpack exclusion is removed and those tests ported, and every remaining exclusion in both blocks carries its real reason.

## Depends on

- issue 05, `.scratch/testing-improvements-round2/issues/` (I5 — the "shard busy running a script" fixture; F8 and F15 both need it and nothing in the suite today starts a long-running EVAL and then talks to the same shard on a second connection. The fixture must guarantee teardown **even if the script cannot be killed**, which is precisely F8's undeliverable-kill path. The proposal's third requester, F4, is promoted to issue 60, `.scratch/testing-improvements-round2/issues/`)
