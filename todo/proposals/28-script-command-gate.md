# Proposal: Script Command Gate

Status: proposed
Date: 2026-06-17

## Problem

Every `redis.call()` / `redis.pcall()` a Lua script issues is, logically, one decision: *given this
command and its keys, is it allowed, and where does it run?* That decision is a single contract —
classify the command (forbidden? write-in-RO? cross-slot?), then route it (local shard, remote
shard, or rejected). But the contract is not owned by any one module. It is **split across two
files** that each re-derive the command's keys with a *different* function over a *different* key
set, and the two halves can — and in cluster mode do — disagree about the same command.

The two halves:

| Concern | Lives in | Key extraction | Hash granularity | When it runs |
|---|---|---|---|---|
| Validation + **cross-slot** rejection | `executor.rs:295-318` (the `redis.call`/`redis.pcall` closures) | `extract_keys_from_command` over **all** keys | `slot_for_key` → CRC16 slot `0..16384` | only when `enforce_cross_slot()` (cluster mode + shebang) |
| **Cross-shard** routing + dispatch | `lua_vm.rs:108-152` (`VmContextAccessor::execute_command`) | `extract_keys_from_command` over the **first** key only | `shard_for_key` → CRC16 slot `% num_shards` | whenever `num_shards > 1` |

The closures decide "is this command *legal*"; `execute_command` decides "which shard *runs* it".
Neither is the authority; each re-extracts keys (`bindings.rs:251`) and hashes them with a different
function (`shard/helpers.rs:55,62`) over a different key set (all keys vs. first key only). A seam
whose contract is "two call sites independently agree on key extraction and hashing" is a shallow
seam: the interface a caller must reason about is as large as the two implementations it is supposed
to hide.

**The deletion test.** If the call/route contract were a deep module, deleting it would delete: the
duplicated `extract_keys_from_command` call in `executor.rs`, the second one in `lua_vm.rs`, the
near-identical validation chain copied between the `call` and `pcall` closures, and the
silent-fallback comment in `execute_command`. Today none of those can be deleted, because the
contract lives *in the callers*, not in a module. That is the signal the seam is in the wrong place.

This proposal owns the **per-sub-command call/route contract**: one `ScriptCommandGate` that
extracts keys once, makes one cross-slot/cross-shard decision, and dispatches — used by *both*
validation and routing, so the two can never diverge. It also converts the single most dangerous
behavior in this path — a silent local write to the **wrong shard** — into an explicit error.

## Current state

### The routing half — `execute_command` (`lua_vm.rs:108-152`)

When `num_shards > 1`, `execute_command` does its *own* key extraction and shard selection, dispatches
a synchronous `ScriptSubCommand`, and blocks the Lua thread on the reply:

```rust
// Route to the correct shard based on the command's first key.
if exec_ctx.num_shards > 1 {
    let keys = extract_keys_from_command(&cmd_name, parts);
    if let Some(first_key) = keys.first() {
        let target_shard = shard_for_key(first_key, exec_ctx.num_shards);
        if target_shard != exec_ctx.shard_id {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            exec_ctx.shard_senders[target_shard]
                .try_send(ShardMessage::ScriptSubCommand {
                    command: parts.to_vec(),
                    conn_id: exec_ctx.conn_id,
                    protocol_version: exec_ctx.protocol_version,
                    response_tx: tx,
                })
                .map_err(|e| format!("ERR cross-shard dispatch failed: {e}"))?;
            // Use block_in_place to release the tokio worker thread
            // while we synchronously wait for the target shard's response.
            // This requires a multi-thread tokio runtime; on current_thread
            // (used by some tests), fall back to local execution.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tokio::task::block_in_place(|| rx.recv())
            }));
            match result {
                Ok(Ok(resp)) => return Ok(resp),
                Ok(Err(e)) => return Err(format!("ERR cross-shard response failed: {e}")),
                Err(_) => {
                    // block_in_place unavailable (current_thread runtime).
                    // Fall through to local execution — data lands on the
                    // wrong shard but avoids deadlock.
                }
            }
        }
    }
}
```

The `Err(_)` arm (lines ~139-144) is the trap. `block_in_place` panics if the current Tokio runtime
is **not** multi-threaded; `catch_unwind` swallows that panic and the code **falls through** to the
local-execution path below, where the command is run against *this* shard's store:

```rust
// SAFETY: These pointers are valid during script execution ...
let store = unsafe { &mut *exec_ctx.store_ptr };
// ... handler.execute(&mut ctx, args) against the LOCAL shard
```

So a command whose key belongs to shard *N* is executed against shard *self*. For a read that
returns the wrong (or empty) value; for a write — `SET`, `LPUSH`, `INCR` — it **persists data on the
wrong shard**, corrupting the keyspace, with no error returned to the script. The comment states the
trade-off plainly: *"data lands on the wrong shard but avoids deadlock."* Avoiding a deadlock by
silently corrupting data is not a trade-off; it is a correctness bug wearing a comment.

It is also **invisible to the test suite**. Production builds the runtime with
`new_multi_thread` (`server/src/main.rs:208`), so `block_in_place` works and the remote path is taken.
But `#[tokio::test]` defaults to the **current-thread** runtime, where `block_in_place` always panics
— so every multi-shard script test silently exercises the wrong-shard fallback and *passes*, while
production takes the real cross-shard path. The one environment that would catch the bug is the one
that masks it.

### The validation half — the `call` / `pcall` closures (`executor.rs:295-318`)

`setup_redis_bindings` builds two closures. They run an identical validation chain, then call the
*same* `execute_command`; the only difference is how each surfaces the outcome.

`redis.call` (raises on error):

```rust
let cn = String::from_utf8_lossy(&parts[0]).to_uppercase();
if let Some(err) = is_forbidden_in_script(&cn) { return Err(mlua::Error::RuntimeError(err.to_string())); }
if let Some(err) = is_forbidden_subcommand(&parts) { return Err(mlua::Error::RuntimeError(err.to_string())); }
if ro && is_write_command(&cn) { return Err(mlua::Error::RuntimeError("ERR Write commands are not allowed from read-only scripts".into())); }
if ecx { for k in &extract_keys_from_command(&cn, &parts) { let ks = slot_for_key(k); let mut t = stc.lock().unwrap(); match *t { None => *t = Some(ks), Some(s) if ks != s => return Err(mlua::Error::RuntimeError("ERR Script attempted to access keys that do not hash to the same slot".into())), _ => {} } } }
if is_write_command(&cn) { ac.mark_write(); }
match ac.execute_command(&parts) { Ok(r) => { if let Response::Error(ref e) = r { return Err(mlua::Error::RuntimeError(String::from_utf8_lossy(e).to_string())); } response_to_lua(lc, r) } Err(e) => Err(mlua::Error::RuntimeError(e)) }
```

`redis.pcall` (returns the error as a Lua table):

```rust
let cn = String::from_utf8_lossy(&parts[0]).to_uppercase();
if let Some(err) = is_forbidden_in_script(&cn) { let t = lc.create_table()?; t.set("err", err)?; return Ok(Value::Table(t)); }
if let Some(err) = is_forbidden_subcommand(&parts) { let t = lc.create_table()?; t.set("err", err)?; return Ok(Value::Table(t)); }
if ro && is_write_command(&cn) { let t = lc.create_table()?; t.set("err", "ERR Write commands are not allowed from read-only scripts")?; return Ok(Value::Table(t)); }
if ecx { for k in &extract_keys_from_command(&cn, &parts) { let ks = slot_for_key(k); let mut t = stp.lock().unwrap(); match *t { None => *t = Some(ks), Some(s) if ks != s => { let tbl = lc.create_table()?; tbl.set("err", "ERR Script attempted to access keys that do not hash to the same slot")?; return Ok(Value::Table(tbl)); } _ => {} } } }
if is_write_command(&cn) { ap.mark_write(); }
match ap.execute_command(&parts) { Ok(r) => response_to_lua(lc, r), Err(e) => { let t = lc.create_table()?; t.set("err", e)?; Ok(Value::Table(t)) } }
```

Read the two side by side: `is_forbidden_in_script`, `is_forbidden_subcommand`, the read-only write
guard, the cross-slot accumulation, and `mark_write` are **the same five checks in the same order**.
The *only* genuine difference between `call` and `pcall` is error propagation — `pcall` packages the
error into a `{err = ...}` Lua table and returns `Ok`, while `call` raises an `mlua` runtime error.
The validation logic is identical and is copied verbatim. Any future check (a new forbidden command,
an ACL gate) must be added twice, in lockstep, or `call` and `pcall` drift.

### The two key extractions disagree by construction

Both halves call the same `extract_keys_from_command` (`bindings.rs:251`), but they feed the result
to different hash functions over different key sets:

```rust
// shard/helpers.rs
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let slot = crc16::State::<crc16::XMODEM>::calculate(hash_key) as usize % REDIS_CLUSTER_SLOTS;
    slot % num_shards            // <- maps to a shard index 0..num_shards
}
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16::State::<crc16::XMODEM>::calculate(hash_key) % REDIS_CLUSTER_SLOTS as u16   // <- raw slot 0..16384
}
```

- The **validation** closure (`executor.rs:302,313`) accumulates `slot_for_key` over **all** keys and
  rejects if any two differ (cross-*slot*) — but only when `ecx` (cluster + shebang) is set.
- The **routing** code (`lua_vm.rs:118`) takes `shard_for_key` of the **first** key only (cross-*shard*),
  and runs whenever `num_shards > 1`, regardless of `ecx`.

These are different questions answered with different inputs. Two keys can share a shard but live in
different slots (`slot % num_shards` collides), or sit in the same slot logic yet be routed by only
the first key. The result: a command the validation half would reject as CROSSSLOT can be silently
*routed* by the routing half (when `ecx` is off), and a multi-key command the routing half sends to
the first key's shard may carry keys the target shard does not own — with no second check. There is
no shared notion of "the keys of this command" or "where this command belongs."

### A third, unused home

`scripting/router.rs` already defines a `ScriptRouter` trait with `SingleShardRouter` (returns
`ScriptError::CrossSlot` on span) and `CrossShardRouter` (returns `ScriptRoute::CrossShard`). It is
the *natural* home for exactly this decision — but it is **not used** by the eval path: grep shows no
`.route(` call site outside `router.rs` itself; it is only re-exported (`scripting/mod.rs:23`,
`lib.rs:115-116`). So the codebase has a routing abstraction *and* two hand-rolled re-derivations
that bypass it. The authority exists; nothing routes through it.

## Proposed design

Introduce `ScriptCommandGate`: one deep module that owns the per-sub-command contract. It extracts
keys **once**, makes **one** classification (forbidden / RO-write / cross-slot / local / remote
shard), and dispatches. Validation and routing read the *same* classification, so they cannot
disagree. The `call` and `pcall` closures shrink to "invoke the gate, then surface the `Result` in
my dialect."

### The seam

```rust
/// Single owner of the redis.call / redis.pcall contract: classify a
/// sub-command (one key extraction, one cross-slot/cross-shard decision) then
/// dispatch it. Validation and routing read the SAME plan, so the cross-slot
/// check and the routing decision can never disagree.
pub struct ScriptCommandGate<'a> {
    exec_ctx: &'a CommandExecutionContext,
    accessor: &'a VmContextAccessor,
    read_only: bool,
    cross_slot: &'a CrossSlotTracker, // shared slot accumulator (was the inline Mutex<Option<u16>>)
}

enum Plan {
    Local,           // keys (if any) belong to this shard; run against the local store
    Remote(usize),   // keys belong to shard N; dispatch a ScriptSubCommand
}

impl ScriptCommandGate<'_> {
    /// THE single decision. One key extraction; all validation; one routing
    /// choice; cross-slot accumulation. Returns an error string on rejection.
    fn classify(&self, parts: &[Bytes]) -> Result<Plan, String> {
        if parts.is_empty() {
            return Err("ERR wrong number of arguments for redis command".into());
        }
        let name = String::from_utf8_lossy(&parts[0]).to_uppercase();
        if let Some(err) = is_forbidden_in_script(&name) { return Err(err.into()); }
        if let Some(err) = is_forbidden_subcommand(parts) { return Err(err.into()); }
        let is_write = is_write_command(&name);
        if self.read_only && is_write {
            return Err("ERR Write commands are not allowed from read-only scripts".into());
        }

        // ONE extraction, shared by the cross-slot check AND the routing choice.
        let keys = extract_keys_from_command(&name, parts);
        if self.exec_ctx.enforce_cross_slot {
            self.cross_slot.check_all(&keys)?; // CROSSSLOT if keys span slots
        }
        if is_write {
            self.accessor.mark_write();
        }
        Ok(match keys.first() {
            Some(k) if self.exec_ctx.num_shards > 1 => {
                let shard = shard_for_key(k, self.exec_ctx.num_shards);
                if shard == self.exec_ctx.shard_id { Plan::Local } else { Plan::Remote(shard) }
            }
            _ => Plan::Local,
        })
    }

    /// Classify then dispatch. The fallback is an EXPLICIT error, never a
    /// silent local write to the wrong shard.
    fn dispatch(&self, parts: &[Bytes]) -> Result<Response, String> {
        let plan = self.classify(parts)?;   // ONE extraction + cross-slot check
        match plan {
            Plan::Local => self.run_local(parts),
            Plan::Remote(shard) => self.run_remote(shard, parts)
                .map_err(|_| "ERR cross-shard script call requires a multi-thread runtime".into()),
        }
    }
}
```

`run_remote` does the `ScriptSubCommand` dispatch and the `block_in_place` wait — but the
`catch_unwind` `Err(_)` arm now returns `Err`, so `dispatch` surfaces the explicit error above
**instead of falling through to `run_local`**. The wrong-shard write path is gone: a cross-shard call
on a runtime that cannot block either completes remotely or fails loudly. `run_local` is the existing
local-execution body unchanged.

### Before / after: the `call` and `pcall` closures

Before — each closure re-runs the five-check chain and calls `execute_command`, which *also* re-extracts
keys and re-decides routing (`executor.rs:295-318` + `lua_vm.rs:108-152`).

After — both closures are a thin dialect over `gate.dispatch`; validation and routing live once:

```rust
// redis.call — raises on error
let call_fn = lua.create_function(move |lc, args| {
    let parts = lua_args_to_command(args).map_err(rt_err)?;
    match gate.dispatch(&parts) {
        Ok(Response::Error(e)) => Err(rt_err(String::from_utf8_lossy(&e))),
        Ok(r) => response_to_lua(lc, r),
        Err(e) => Err(rt_err(e)),
    }
});

// redis.pcall — returns the error as a Lua table
let pcall_fn = lua.create_function(move |lc, args| {
    let parts = match lua_args_to_command(args) { Ok(p) => p, Err(e) => return err_table(lc, e) };
    match gate.dispatch(&parts) {
        Ok(r) => response_to_lua(lc, r),
        Err(e) => err_table(lc, e),
    }
});
```

The difference between `call` and `pcall` is now *exactly* what it is semantically — error
propagation, nothing else. `call` raises the `Result::Err`; `pcall` wraps it in `{err = ...}`. The
validation chain, the cross-slot accumulation, the routing, and the wrong-shard guard are written
once in the gate and shared.

### Why this is the right depth

- **Locality.** "Is this command legal and where does it run" becomes one function (`classify`) with
  one key extraction and one hash decision. The cross-slot check and the routing choice consume the
  *same* `keys` vector, so they cannot disagree about a command's keys. Adding a new validation rule
  (a forbidden command, an ACL gate) is a one-line edit in `classify`, not a paired edit in two
  near-identical closures that silently drift if you miss one.
- **Correctness.** The silent wrong-shard fallback (`lua_vm.rs:139-144`) is replaced by an explicit
  error in `dispatch`. The two-extraction divergence is eliminated because there is only one
  extraction. Both correctness flags below are closed by construction, not by a spot fix.
- **Deletion test.** The migration *deletes*: the second `extract_keys_from_command` call in
  `lua_vm.rs`, the duplicated validation chain in the `pcall` closure, the inline `Mutex<Option<u16>>`
  cross-slot state threaded as `stc`/`stp`, and the silent-fallback comment. If the gate could not
  delete those, its shape would be wrong.
- **Not a new adapter layer.** The gate *is* the routing authority `router.rs` already gestures at
  (`SingleShardRouter`/`CrossShardRouter`); the migration can fold the unused `ScriptRouter` decision
  into `classify` rather than adding a parallel abstraction. `execute_command` stops being a public
  bypass — its routing logic moves behind the gate and the closures call the gate, not the raw
  accessor.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Behavior-preserving **except** the
fallback change in Phase 3, which is the point — and which requires the test-runtime change in the
same phase so the suite reflects production.

1. **Phase 0 — introduce `ScriptCommandGate` + `classify`, no call sites changed.** Add the gate,
   `Plan`, and `CrossSlotTracker` (the slot accumulator extracted from the inline `Mutex<Option<u16>>`).
   Implement `classify` by *calling into* the existing validation helpers and `shard_for_key`, so its
   decision is provably identical to today's. Unit-test `classify` directly (no live shards): forbidden
   command, RO-write, cross-slot span, local vs. remote routing. `just check frogdb-core`.
2. **Phase 1 — route validation through the gate.** Rewrite the `call`/`pcall` closures
   (`executor.rs:295-318`) to call `gate.classify` for the five checks, still delegating execution to
   the existing `execute_command`. The closures now differ only in error surfacing. Cross-slot and
   forbidden-command behavior is identical; delete the duplicated chain from the `pcall` closure. Pure
   refactor.
3. **Phase 2 — move routing into the gate; `execute_command` becomes `run_local`.** Lift the
   shard-selection + `ScriptSubCommand` dispatch out of `execute_command` (`lua_vm.rs:108-152`) into
   `gate.run_remote`; reduce `execute_command` to the local-execution body (`run_local`). `classify`
   now owns the single routing decision; the closures call `gate.dispatch`. The second
   `extract_keys_from_command` call disappears. Still behavior-preserving on multi-thread runtimes.
4. **Phase 3 — make the fallback an explicit error (the fix) + fix the tests.** Change the
   `catch_unwind` `Err(_)` arm so `run_remote` returns `Err`, surfaced by `dispatch` as
   `"ERR cross-shard script call requires a multi-thread runtime"`. The silent local write is gone.
   In the same commit, switch the multi-shard script tests to
   `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` so they exercise the real remote path
   (matching `main.rs:208`) instead of the masked fallback. Add a test asserting the explicit error on
   a deliberately current-thread runtime.
5. **Gate.** Add a grep gate to `just lint`: outside the gate module, no `block_in_place` in the
   scripting crate and no second `extract_keys_from_command` in `lua_vm.rs` — key extraction and
   cross-shard blocking must go through the gate.

## Testing impact

- **`classify` is unit-testable without a live cluster.** Forbidden command, forbidden subcommand,
  read-only write rejection, cross-slot span (with `enforce_cross_slot` on and off), and local-vs-remote
  routing become fast unit tests over a synthetic `CommandExecutionContext` — no socket pair, no Lua
  VM. Today these are reachable only through full script execution.
- **The wrong-shard write is now assertable.** A test on a current-thread runtime can assert that a
  cross-shard `redis.call` returns the explicit
  `"ERR cross-shard script call requires a multi-thread runtime"` error rather than silently writing
  locally. This is the regression pin for Flag 1 — it *fails* today (the call appears to succeed and
  corrupts the local shard) and *passes* after Phase 3.
- **Multi-shard script tests move to a multi-thread runtime.** Every existing multi-shard eval test
  is re-annotated `flavor = "multi_thread"`, so it exercises the real `ScriptSubCommand` dispatch the
  way production does. Before this change those tests ran the masked local fallback; after it they
  prove the cross-shard path. This is the most important testing change: it removes the
  test-vs-production runtime mismatch that hid the bug.
- **call/pcall parity test.** Drive the same forbidden/cross-slot/cross-shard command through both
  `redis.call` and `redis.pcall` and assert they reject identically — `call` by raising, `pcall` by
  returning `{err = ...}` with the *same* message. Pins the "difference is only error propagation"
  contract so future checks added to the gate stay symmetric.
- **Cross-slot vs. cross-shard agreement.** A property test over random key sets asserts that the
  command the gate routes remote is exactly the command the gate would *not* reject as cross-slot
  (within one `classify` call), eliminating the historical divergence between `slot_for_key`
  (all keys) and `shard_for_key` (first key).

## Risks / open questions

- **Behavior change is intentional and user-visible.** A multi-shard script that today *appears* to
  succeed (while corrupting a shard) will, after Phase 3 on a non-multi-thread runtime, return an
  error. That is strictly more correct — a silent wrong-shard write is never the desired outcome — but
  any test or harness that depended on the accidental success must move to a multi-thread runtime.
  FrogDB is pre-production, so this is acceptable; it warrants a release note.
- **Should the fallback ever be local?** No. The recommendation is a **hard error always**: silently
  executing a cross-shard sub-command against the wrong shard violates correctness, and there is no
  context in which "write to the wrong shard" is preferable to "fail and ask for a multi-thread
  runtime." The only legitimate reason the old code fell back was to avoid a `block_in_place` panic on
  current-thread; the right fix is to require a multi-thread runtime for multi-shard scripts (which
  production already uses) and to error otherwise — not to corrupt data.
- **First-key routing for multi-key commands.** `classify` routes by the *first* key, like the code
  today. A multi-key command (`MSET k1 v1 k2 v2`) whose keys span shards is correctly *rejected* by
  the cross-slot check when `enforce_cross_slot` is on, but when it is off (non-cluster multi-shard)
  the command is routed to the first key's shard and the other keys are assumed co-resident. The gate
  makes this assumption *explicit and single-sited*; whether non-cluster multi-shard should reject
  cross-shard multi-key commands outright is a policy question the gate now makes easy to change (one
  branch in `classify`) — out of scope here.
- **Relationship to `ScriptRouter` (`router.rs`).** The unused `SingleShardRouter`/`CrossShardRouter`
  encode the same decision at script-entry granularity (all keys at once) that the gate makes at
  per-sub-command granularity. They should converge: either the gate consumes a `ScriptRouter`, or the
  trait is deleted in favor of the gate. This proposal folds the decision into `classify`; reconciling
  the entry-level router is a follow-up so the two cannot re-diverge.
- **`block_in_place` and runtime coupling stays.** The gate does not remove the dependency on a
  multi-thread runtime for cross-shard calls; it makes the dependency *explicit* (an error instead of a
  panic-and-corrupt). A fully async cross-shard call (awaiting the `ScriptSubCommand` reply without
  blocking) would remove the coupling but requires making the Lua bridge async — a larger change,
  noted but not undertaken here.

## Correctness flags

1. **🔴 Silent wrong-shard write on the cross-shard fallback — CONFIRMED.** In
   `execute_command` (`lua_vm.rs:108-152`), when a sub-command's first key belongs to a different
   shard, the code dispatches a `ScriptSubCommand` and waits via
   `block_in_place(|| rx.recv())` wrapped in `catch_unwind`. On a **current-thread** Tokio runtime
   `block_in_place` panics; `catch_unwind` catches it and the `Err(_)` arm (lines ~139-144) **falls
   through** to local execution — "*data lands on the wrong shard but avoids deadlock*". For a write
   (`SET`, `LPUSH`, `INCR`, …) this persists the value on the wrong shard with **no error returned**,
   silently corrupting the keyspace. It is invisible to the test suite because `#[tokio::test]`
   defaults to current-thread (always triggering the fallback) while production runs `new_multi_thread`
   (`server/src/main.rs:208`, always taking the remote path) — so tests pass and production corrupts.
   Fix: the gate's `dispatch` turns the fallback into an explicit error
   (`"ERR cross-shard script call requires a multi-thread runtime"`); never a local write. Multi-shard
   script tests move to `flavor = "multi_thread"`.

2. **Duplicated key extraction → cross-slot check and routing can disagree — CONFIRMED.** The
   cross-slot validation in the `call`/`pcall` closures (`executor.rs:302,313`) calls
   `extract_keys_from_command` over **all** keys and accumulates `slot_for_key` (raw slot `0..16384`),
   but only when `enforce_cross_slot()` is set (cluster + shebang). The routing in `execute_command`
   (`lua_vm.rs:116-118`) calls `extract_keys_from_command` again over the **first** key only and uses
   `shard_for_key` (`slot % num_shards`), whenever `num_shards > 1`. Two extractions, two hash
   functions, two key sets, two trigger conditions — for the same command. A command rejected as
   CROSSSLOT by one half can be routed by the other (when `ecx` is off), and a multi-key command routed
   by its first key may carry keys the target shard does not own with no second check. There is no
   shared authority for "the keys of this command." Fix: `classify` extracts once and feeds the same
   `keys` vector to both the cross-slot check and the routing decision, so they cannot diverge.
