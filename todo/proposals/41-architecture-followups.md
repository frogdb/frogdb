# Architecture Deepening — Follow-up Items (post-initiative)

Six discrete follow-ups after the connection-dispatch unification landed on `main` (`88c78e97`).
Each is independently reviewable/mergeable. Ordered easiest → involved.

---

## 1. Privatize `ConsumerGroup` stream fields (finish P-PEL)

**Why:** P-PEL encapsulated the PEL behind methods but left two scalar fields `pub` because
`core/shard/blocking.rs` read them and `core` was out of scope at the time.

**Files:** `frogdb-server/crates/types/src/types/stream.rs` (`ConsumerGroup`, lines ~464/476),
`frogdb-server/crates/core/src/shard/blocking.rs:822`.

**Before**
```rust
// stream.rs
pub struct ConsumerGroup {
    pub last_delivered_id: StreamId,   // line 464
    // ...
    pub entries_read: Option<u64>,     // line 476
}

// blocking.rs:822
let last_delivered = group.last_delivered_id;
```

**After**
```rust
// stream.rs
pub struct ConsumerGroup {
    last_delivered_id: StreamId,       // private
    entries_read: Option<u64>,         // private
}
impl ConsumerGroup {
    #[inline] pub fn last_delivered_id(&self) -> StreamId { self.last_delivered_id }
    #[inline] pub fn entries_read(&self) -> Option<u64> { self.entries_read }
}

// blocking.rs:822
let last_delivered = group.last_delivered_id();
```

**Note:** grep for every external read/write of both fields first — internal `stream.rs` mutation
stays field-level; only cross-module reads switch to accessors. If any external site *writes*
`last_delivered_id`, add a narrow setter rather than re-exposing the field.

**Test:** existing stream/XREADGROUP + blocking-XREADGROUP suites (behavior-preserving).
**Effort:** ~15 min.

---

## 2. Regression test: HOTKEYS / FT.CURSOR inside MULTI

**Why:** migrating them to the seam changed behavior from a silent no-op to *real execution* inside
`MULTI/EXEC` (the registry-union EXEC path now dispatches them). Covered by construction + the
CONFIG-deferral test, but nothing locks it in.

**Files:** `frogdb-server/crates/server/tests/integration_transactions.rs` (add test).

**After (new test, sketch)**
```rust
#[tokio::test]
async fn multi_exec_runs_connection_level_hotkeys_and_ftcursor() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.cmd(&["MULTI"]).await.assert_ok();
    c.cmd(&["HOTKEYS", "GET"]).await.assert_queued();        // +QUEUED
    // FT.CURSOR DEL on a non-existent cursor: exercises dispatch, deterministic reply
    c.cmd(&["FT.CURSOR", "DEL", "idx", "0"]).await.assert_queued();
    let replies = c.cmd(&["EXEC"]).await.as_array();          // 2 replies, real execution
    assert_eq!(replies.len(), 2);
    assert!(!replies[0].is_error(), "HOTKEYS GET executed, not a no-op");
}
```
Match the exact reply shapes to the live HOTKEYS/FT.CURSOR handlers (copy from their unit tests).

**Effort:** ~30 min.

---

## 3. Route `COMMAND GETKEYS` through the registry union → delete dual-registration stubs

**Why:** EVAL/EVALSHA/SCRIPT/FCALL/WATCH/DEBUG(OBJECT)/INFO each keep a shard `Command` stub
*solely* because `COMMAND GETKEYS`/`GETKEYSANDFLAGS` resolve keys through the `commands` map
(`registry.get`, Shard-only). If GETKEYS reads the union (`registry.get_entry` + `keys()`/
`dynamic_keys`), those ~7 stubs can be deleted — finishing the "single registry" goal.

**Files:** the `COMMAND` handler (`commands/src/*` — grep `GETKEYS`), `core/src/registry.rs`
(`CommandEntry::keys`), then the ~7 stub registrations in `server/register.rs` +
the stub structs in each `*_conn_command.rs`.

**Before** (COMMAND GETKEYS handler, conceptual)
```rust
let entry = registry.get(name)?;          // Shard-only map → misses Connection commands
let keys = entry.keys_with_flags(args);
```

**After**
```rust
let entry = registry.get_entry(name)?;    // the union — Shard OR Connection
let keys = entry.keys(args);              // already delegates to dynamic_keys for keyed conn cmds
```
Then delete each keyed connection command's shard stub, e.g.:
```rust
// register.rs — BEFORE (dual registration)
registry.register(EvalCommand);                 // stub, only for GETKEYS
registry.register_connection(&EVAL_CONN_COMMAND);
// AFTER
registry.register_connection(&EVAL_CONN_COMMAND); // stub gone; GETKEYS uses the union
```

**Caution:** verify `COMMAND GETKEYS` error semantics (no-key command → error) still match, and that
`COMMAND COUNT`/`COMMAND DOCS`/`COMMAND INFO` (which may also read `get`) are consistent. This is the
most involved item — touches the `commands` crate. Gate on the `introspection`/`getkeys`/
`movablekeys` regression suites.

**Test:** `introspection*`, `tcl_*command_getkeys*`, `tcl_command_is_marked_with_movablekeys`.
**Effort:** ~2-3 h (cross-crate, careful).

---

## 4. RESET — match Redis full-reset semantics (deauth + reply mode + DB)

**Why (Redis parity):** Redis `resetCommand` performs a *full* connection reset. FrogDB's
`reset()` currently omits three things Redis does:
1. **Deauthenticate / revert to default user** — Redis sets `c->user = DefaultUser` and
   `c->authenticated = default_user_is_nopass`. So: with `requirepass`/ACL, RESET drops the
   connection back to *unauthenticated*; with no auth configured, it stays as the default user.
2. **Reply mode → ON** (`CLIENT REPLY OFF/SKIP` cleared).
3. **`SELECT 0`** (DB index reset) — if/when multi-DB is supported.

FrogDB `reset()` today does: exit pub/sub, tracking→default, transaction→default, RESP2, name→None.

**Files:** `frogdb-server/crates/server/src/connection/state.rs:1195` (`reset()`), and the RESET
executor in `connection_state_conn_command.rs` (needs `acl_manager` to compute default-user auth —
already on `ConnCtx`).

**Before**
```rust
pub fn reset(&mut self) -> ResetEffects {
    let tracking_was_enabled = self.tracking.enabled;
    let was_in_pubsub = self.exit_pubsub();
    self.tracking = TrackingState::default();
    self.transaction = TransactionState::default();
    self.protocol_version = ProtocolVersion::Resp2;
    self.name = None;
    ResetEffects { was_in_pubsub, tracking_was_enabled }
}
```

**After**
```rust
/// `default_authed` = whether the default user is passwordless (no requirepass / nopass ACL) —
/// computed by the caller from the ACL manager, mirroring Redis `authenticated = nopass(DefaultUser)`.
pub fn reset(&mut self, default_authed: bool) -> ResetEffects {
    let tracking_was_enabled = self.tracking.enabled;
    let was_in_pubsub = self.exit_pubsub();
    self.tracking = TrackingState::default();
    self.transaction = TransactionState::default();
    self.protocol_version = ProtocolVersion::Resp2;
    self.name = None;
    self.reply_mode = ReplyMode::On;                     // (2) reply ON
    // self.select_db(0);                                // (3) when multi-DB lands
    self.auth = if default_authed {                      // (1) revert to default user / deauth
        AuthState::authenticated_default()
    } else {
        AuthState::NotAuthenticated
    };
    ResetEffects { was_in_pubsub, tracking_was_enabled }
}
```
The RESET executor passes `default_authed` from `ctx.acl_manager` (default user `nopass` &&
enabled). Reply is unchanged (`+RESET`).

**Redis reference:** `resetCommand` (t_string/server.c): unwatch, discard MULTI, unsubscribe all,
exit MONITOR, `selectDb(c,0)`, RESP2, disable tracking, revert to DefaultUser + re-evaluate
`authenticated`, clear name, reply ON, reply `+RESET`.

**Test:** extend `reset_*` unit tests (state.rs) + an integration test: with `requirepass`, `AUTH`,
then `RESET`, then a non-AUTH command → `NOAUTH`. Without requirepass, RESET keeps working.
**Effort:** ~1 h.

---

## 5. READONLY / READWRITE — error when cluster support is disabled (Redis parity)

**Why (Redis parity):** Redis `readonlyCommand`/`readwriteCommand` reply
`-ERR This instance has cluster support disabled` when `server.cluster_enabled == 0`. FrogDB
currently always replies `+OK` and sets a flag that's meaningless outside cluster mode.

**Files:** `frogdb-server/crates/server/src/connection/connection_state_conn_command.rs:180-215`
(READONLY/READWRITE executors). Needs a cluster-enabled signal on `ConnCtx` for the `authmut`
builder (an `is_cluster_mode()` already exists on `HotkeyClusterProvider`; either reuse a cluster
provider on `ConnCtx` or add a `cluster_enabled: bool` field wired in `conn_ctx_authmut`).

**Before**
```rust
fn execute<'a>(&'a self, ctx: &'a mut ConnCtx<'a>, _args: &'a [Bytes]) -> BoxFuture<'a, Response> {
    Box::pin(async move {
        if let Some(state) = ctx.conn_state.as_deref_mut() {
            state.set_readonly(true);
        }
        Response::ok()
    })
}
```

**After**
```rust
fn execute<'a>(&'a self, ctx: &'a mut ConnCtx<'a>, _args: &'a [Bytes]) -> BoxFuture<'a, Response> {
    Box::pin(async move {
        if !ctx.cluster_enabled {                        // (new) parity gate
            return Response::error("ERR This instance has cluster support disabled");
        }
        if let Some(state) = ctx.conn_state.as_deref_mut() {
            state.set_readonly(true);                     // READWRITE: set_readonly(false)
        }
        Response::ok()
    })
}
```

**Redis reference:** `readonlyCommand`/`readwriteCommand` (cluster.c): `if (server.cluster_enabled
== 0) { addReplyError(c,"This instance has cluster support disabled"); return; }`.

**Test:** `READONLY` in standalone → the error; in cluster mode → `+OK` + `is_readonly()` set
(cluster tests already cover the OK path).
**Effort:** ~45 min (mostly wiring `cluster_enabled` into `conn_ctx_authmut`).

---

## 6. Speed up (or de-flake) the slow multi-key PFCOUNT regression test

**Why:** `tcl_pfcount_multiple_keys_merge_returns_cardinality_of_union_{1,2}`
(`redis-regression/tests/hyperloglog_tcl.rs:217,253`) times out (~>45s) under full-suite parallel
load; passes in isolation (~24s, flagged SLOW). Pre-existing — unrelated to the initiative.

**Investigate first (profile which half dominates):**
- **Setup cost:** the test likely `PFADD`s a very large number of elements over the wire; under
  contention the round-trips dominate. → Fix by batching PFADD (fewer, larger commands) or reducing
  the element count while still exercising the merge.
- **Merge cost:** `commands/src/hyperloglog.rs` PFCOUNT loops `merged.merge(&hll)` per key
  (lines 109/153/161). Dense↔sparse HLL merge could be O(registers) per key. → If merge dominates,
  optimize the register-wise merge (SIMD/bulk-OR of the 16384-register max).

**Before → After (illustrative, once profiled):**
```rust
// hyperloglog.rs — per-key merge loop (current)
for key in keys { let hll = load(key)?; merged.merge(&hll); }
// If merge dominates: replace element-wise max with a bulk register max over the dense form.
```
If neither is cheaply fixable, raise this specific test's timeout in the harness and keep the SLOW
flag (it's a correctness test, not a perf gate).

**Effort:** ~1-2 h (profile-dependent). Lowest priority — pre-existing, not a regression.

---

## Suggested order
1 → 2 → 5 → 4 → 3 → 6. (1,2 trivial; 5,4 are the Redis-parity fixes; 3 is the cross-crate cleanup;
6 is profile-gated.) Each lands as its own commit/PR; 4 and 5 are behavior changes (Redis parity),
so call those out in their commit messages.
