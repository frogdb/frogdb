# Proposal: ACL Enforcement Seam

Status: proposed
Date: 2026-06-13

## Problem

ACL enforcement is a single security invariant — *deny the request, record it in the audit log,
return the Redis-shaped NOPERM error* — but it has no module that owns it. Instead, the invariant
is restated by hand at four call sites in the connection layer. Each site re-extracts keys, re-picks
the access type, re-formats the client-info string, re-decides whether to write an ACL log entry,
and re-builds the NOPERM error literal. The interface the acl crate exposes to these callers is
`bool` (`AuthenticatedUser::check_command` / `check_command_with_key`) — a *shallow* seam: it
answers "allowed?" and nothing else, so everything that must happen *on denial* leaks back out to
every caller, where it drifts.

This is the textbook shallow-interface failure. The acl crate already contains the *deep* half — a
`FullAclChecker` (`acl/src/checker.rs:135`) that turns a check into a `PermissionResult` carrying a
typed `AclError` whose `Display` is the exact Redis NOPERM string (`acl/src/error.rs:43-56`), plus a
log API (`AclLog::log_command_denied` / `log_key_denied` / `log_channel_denied`,
`acl/src/log.rs:228-258`). None of the four enforcement sites use any of it. They call the raw bool
methods and re-implement the response and the logging inline. The deep machinery sits one import
away, unused, while the callers reinvent a worse version of it.

**Deletion test.** Delete the ACL check from any one of the four sites and only that path breaks —
the other three keep working, so nothing fails to compile and no shared test necessarily fails. The
four checks are each independently load-bearing, and their coordination ("every execution path must
check *and* log before running") is implicit, enforced by nothing. That is the signature of a
missing seam: four independent failure modes where there should be one guarded chokepoint. A
security control whose completeness depends on every author remembering to copy four steps correctly
is a control that will, eventually, be incomplete — and two of the four sites are already
incomplete (see [Correctness flags](#correctness-flags)).

Verified evidence (paths relative to `frogdb-server/crates/server/src/connection/`):

| Site | Check call | Key extraction | Client-info | ACL log on deny | Error shaping |
|------|-----------|----------------|-------------|-----------------|---------------|
| `guards.rs:178` (`run_pre_checks`, command-level) | `user.check_command(cmd, sub)` | n/a (keyless) | `format!("{}:{}", ip, port)` inline | `log_command_denied` + `warn!` | inline `format!`, subcommand joined with **space** |
| `routing.rs:62` (`route_and_execute`, live key check) | `user.check_command_with_key(cmd, sub, key, access)` | `handler.keys()` + `key_access_type_for_flags` | `format!("{}:{}", ip, port)` inline | `log_key_denied` | static NOPERM-key literal |
| `transaction.rs:458` (`queue_command`, queue-time key check) | `user.check_command_with_key(cmd, sub, key, access)` | `entry.keys()` + `key_access_type_for_flags` | — none — | **none (missing)** | static NOPERM-key literal |
| `auth.rs:437`/`:462` (`handle_acl_dryrun`, simulation) | `user.check_command(cmd, sub)` + `user.check_key_access(key, ReadWrite)` | heuristic: first arg only, access always `ReadWrite` | n/a | none (DRYRUN is non-logging by design) | bulk string, **no NOPERM prefix**, subcommand joined with `|` |

A parallel split exists for channels: `guards.rs:104` (`validate_channel_access`) logs
`log_channel_denied`, but the in-transaction channel check at `transaction.rs:473`/`:483` does not
log at all.

**The security angle.** Divergent logging is not a cosmetic problem. `ACL LOG` is the audit trail an
operator uses to detect a compromised or misconfigured client. Two of the four sites
(`transaction.rs` key denials and channel denials) silently skip the log: a denied key access issued
inside `MULTI`/`EXEC`, or a denied `PUBLISH` channel inside a transaction, never appears in
`ACL LOG`. An attacker probing permissions from inside a transaction is invisible. Divergent error
shaping is the same class of risk in the other direction: `guards.rs:204` emits
`'config set'` where Redis (and this codebase's own `AclError::NoPermissionSubcommand`) emits
`'config|set'`, so clients and tooling that parse NOPERM messages disagree about what was denied.

## Current state

Real excerpts from each of the four sites.

### Site 1 — command-level check (`guards.rs:176-207`)

```rust
if let Some(user) = self.state.auth.user() {
    let subcommand = extract_subcommand(cmd_name, args);
    if cmd_name != "ACL" && !user.check_command(cmd_name, subcommand.as_deref()) {
        let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
        // Log with subcommand if present
        let log_cmd = if let Some(ref sub) = subcommand {
            format!("{}|{}", cmd_name.to_lowercase(), sub.to_lowercase())   // PIPE here
        } else {
            cmd_name.to_lowercase()
        };
        self.core.acl_manager.log().log_command_denied(
            &user.username,
            &client_info,
            &log_cmd,
        );
        warn!(/* ... */);
        // Return error with subcommand in message if present
        let err_cmd = if let Some(ref sub) = subcommand {
            format!("{} {}", cmd_name.to_lowercase(), sub.to_lowercase())   // SPACE here
        } else {
            cmd_name.to_lowercase()
        };
        return Some(Response::error(format!(
            "NOPERM this user has no permissions to run the '{}' command",
            err_cmd
        )));
    }
}
```

Note the internal inconsistency: the log uses `cmd|sub`, the error message uses `cmd sub`. Both are
hand-built; neither calls the acl crate's `AclError`.

### Site 2 — live key check (`routing.rs:56-76`)

```rust
if let Some(user) = self.state.auth.user()
    && !keys.is_empty()
{
    let access_type = key_access_type_for_flags(handler.flags());
    let subcommand = extract_subcommand(cmd_name, &cmd.args);
    for key in &keys {
        if !user.check_command_with_key(cmd_name, subcommand.as_deref(), key, access_type) {
            let client_info =
                format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
            let key_str = String::from_utf8_lossy(key);
            self.core.acl_manager.log().log_key_denied(
                &user.username,
                &client_info,
                &key_str,
            );
            return Response::error(
                "NOPERM this user has no permissions to access one of the keys used as arguments",
            );
        }
    }
}
```

`check_command_with_key` is `check_command && check_key_access` (`acl/src/user.rs:480-489`). Because
`run_pre_checks` (Site 1) already cleared the command, this re-runs the command check, then attributes
*any* `false` to the key — logging `log_key_denied` and returning the key message even if, in
isolation, the command itself were the denied half. The single `bool` cannot say which half failed.

### Site 3 — queue-time key + channel check (`transaction.rs:451-492`)

```rust
if let Some(user) = self.state.auth.user()
    && !keys.is_empty()
{
    let access_type = key_access_type_for_flags(entry.flags());
    let cmd_name = entry.name();
    let subcommand = extract_subcommand(cmd_name, &cmd.args);
    for key in &keys {
        if !user.check_command_with_key(cmd_name, subcommand.as_deref(), key, access_type) {
            return Response::error(
                "NOPERM this user has no permissions to access one of the keys used as arguments",
            );
        }
    }
}

// Check channel permissions for pub/sub commands inside transactions
if let Some(user) = self.state.auth.user() {
    let cmd_upper = cmd_name_str.as_ref();
    match cmd_upper {
        "PUBLISH" | "SPUBLISH" => {
            if let Some(channel) = cmd.args.first()
                && !user.check_channel_access(channel)
            {
                return Response::error(
                    "NOPERM this user has no permissions to access one of the channels used as arguments",
                );
            }
        }
        "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
            for channel in &cmd.args {
                if !user.check_channel_access(channel) {
                    return Response::error(
                        "NOPERM this user has no permissions to access one of the channels used as arguments",
                    );
                }
            }
        }
        _ => {}
    }
}
```

Same key check as Site 2, **minus the `log_key_denied` call and the `client_info`**. The channel
check is a hand-rolled re-implementation of `validate_channel_access` (`guards.rs:104`), also without
the `log_channel_denied` call. At `EXEC` time the queued commands are dispatched straight to the
shard via `ShardMessage::ExecTransaction` (`transaction.rs` `handle_exec`) and are **not** re-checked,
so this queue-time check is the *only* ACL key/channel check a transactional command ever gets — and
it is the one that does not log.

### Site 4 — DRYRUN simulation (`auth.rs:433-471`)

```rust
let subcommand = crate::connection::util::extract_subcommand(&command, cmd_args);
let cmd_allowed = user.check_command(&command, subcommand.as_deref());
if !cmd_allowed {
    let msg = if let Some(ref sub) = subcommand {
        format!(
            "This user has no permissions to run the '{}|{}' command",
            command.to_lowercase(),
            sub.to_lowercase()
        )
    } else {
        format!(
            "This user has no permissions to run the '{}' command",
            command.to_lowercase()
        )
    };
    return Response::bulk(Bytes::from(msg));
}

// Check key permissions for commands that take keys
// Simple heuristic: if there are args beyond the command (and subcommand),
// treat remaining args as potential keys
let key_start = if subcommand.is_some() { 1 } else { 0 };
if cmd_args.len() > key_start {
    let key = &cmd_args[key_start];
    if !user.check_key_access(key, frogdb_core::KeyAccessType::ReadWrite) {
        let msg = format!(
            "This user has no permissions to access the '{}' key",
            String::from_utf8_lossy(key)
        );
        return Response::bulk(Bytes::from(msg));
    }
}
```

DRYRUN intentionally returns a bulk string (no NOPERM prefix) and does not log — that part is
correct. But its key handling is a *third* re-implementation that does not match the enforcement it
is supposed to simulate: it inspects only one argument (`cmd_args[key_start]`) instead of
`handler.keys()`, and always asks for `ReadWrite` instead of `key_access_type_for_flags(flags)`. The
tool operators use to *audit* ACLs therefore disagrees with the real guard.

### The intended seam already exists as dead code

`handlers/mod.rs:47` declares exactly the trait this proposal wants —

```rust
pub trait AclChecker {
    fn check_command_permission(&self, cmd_name: &str) -> Result<(), Response>;
    fn check_key_permission(&self, key: &[u8], access: KeyAccess) -> Result<(), Response>;
    fn check_channel_permission(&self, channel: &[u8]) -> Result<(), Response>;
}
```

— but it has **no implementations and no callers** (verified: `check_command_permission` and
friends appear only in this definition), and it ships with its own duplicate `KeyAccess` enum
(`handlers/mod.rs:60`) shadowing `acl::KeyAccessType`. The seam was anticipated and then bypassed.
This is a deletion-test win waiting to happen.

## Proposed design

Introduce one deep `PermissionGuard` seam that owns *check + log + error shaping*. Every execution
path calls it exactly once; the raw `check_command` / `check_command_with_key` bool methods survive
only *inside* the acl crate's `FullAclChecker`, never in connection code.

### Where the seam lives

A thin method on `ConnectionHandler`, not a free function over `acl::User`. The denial path needs
three things the connection owns and the acl crate does not: the client address
(`self.state.addr`), the shared `AclLog` (`self.core.acl_manager.log()`), and the authenticated-user
fast path (`self.state.auth.user()`). The *pure* decision — "is this allowed, and if not, which
`AclError`?" — is delegated to the already-existing `FullAclChecker` (`acl/src/checker.rs:135`),
which is the only code that touches the raw bool methods. So:

- `acl::FullAclChecker` (deep, pure): check → `PermissionResult` / `AclError`. Already written.
- `ConnectionHandler::permission_guard()` (deep, effectful): resolves the user, runs the checker,
  formats client-info once, emits the correct `AclLog` entry, and shapes the `Response`. New, ~60
  lines, replaces ~120 lines spread across four files.

### Interface sketch

```rust
/// The single ACL enforcement chokepoint. Borrows everything a denial needs so each call site
/// is one line. Lives in connection/guards.rs.
pub(crate) struct PermissionGuard<'a> {
    user: Option<&'a AuthenticatedUser>,   // None => no-auth fast path, everything allowed
    checker: &'a FullAclChecker,           // pure check -> PermissionResult (acl crate)
    log: &'a AclLog,
    addr: SocketAddr,
    conn_id: u64,
}

impl<'a> PermissionGuard<'a> {
    /// Command + every key in one call. `Some(resp)` = denied (already logged + Redis-shaped),
    /// `None` = allowed. This is the whole interface a normal execution path sees.
    #[allow(clippy::result_large_err)]
    pub(crate) fn check(
        &self,
        cmd: &str,
        sub: Option<&str>,
        keys: &[&[u8]],
        access: KeyAccessType,
    ) -> Option<Response> {
        let user = self.user?;                        // no-auth fast path: allow
        // Command first; on deny we know it's the command, not a key.
        if let Some(err) = self.checker.check_command(user, cmd, sub).error() {
            return Some(self.deny(user, AclLogEntryType::Command, &log_object(cmd, sub), err));
        }
        for key in keys {
            if let Some(err) = self.checker.check_key_access(user, key, access).error() {
                let object = String::from_utf8_lossy(key);
                return Some(self.deny(user, AclLogEntryType::Key, &object, err));
            }
        }
        None
    }

    /// Channel sibling for PUBLISH/SUBSCRIBE-family commands.
    #[allow(clippy::result_large_err)]
    pub(crate) fn check_channels(&self, channels: &[Bytes]) -> Option<Response> { /* ... */ }

    /// Pure evaluation used by ACL DRYRUN: no log, returns the AclError so the caller can
    /// render it as a bulk string. Same check path as `check`, so DRYRUN cannot drift.
    pub(crate) fn evaluate(
        &self,
        cmd: &str,
        sub: Option<&str>,
        keys: &[&[u8]],
        access: KeyAccessType,
    ) -> Result<(), AclError> { /* ... */ }

    /// The one place client-info is formatted, the log is written, and the error is shaped.
    fn deny(&self, user: &AuthenticatedUser, kind: AclLogEntryType, object: &str, err: &AclError)
        -> Response
    {
        let client_info = format!("{}:{}", self.addr.ip(), self.addr.port());
        self.log.log(kind, log_reason(kind), &user.username, &client_info, object);
        warn!(conn_id = self.conn_id, username = %user.username, %object, "ACL denied");
        Response::error(err.to_string())   // AclError Display == exact Redis NOPERM string
    }
}
```

`err.to_string()` collapses the four hand-built error literals into one source of truth and fixes the
`config set` → `config|set` divergence for free, because `AclError::NoPermissionSubcommand`
(`acl/src/error.rs:47`) already uses the pipe.

### Before / after: Site 2 (routing.rs)

Before (21 lines, hand-rolled log + client-info + literal, redundant command re-check):

```rust
if let Some(user) = self.state.auth.user()
    && !keys.is_empty()
{
    let access_type = key_access_type_for_flags(handler.flags());
    let subcommand = extract_subcommand(cmd_name, &cmd.args);
    for key in &keys {
        if !user.check_command_with_key(cmd_name, subcommand.as_deref(), key, access_type) {
            let client_info =
                format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
            let key_str = String::from_utf8_lossy(key);
            self.core.acl_manager.log().log_key_denied(&user.username, &client_info, &key_str);
            return Response::error(
                "NOPERM this user has no permissions to access one of the keys used as arguments",
            );
        }
    }
}
```

After (4 lines, one call, no inline formatting, command-vs-key disambiguated inside the seam):

```rust
let sub = extract_subcommand(cmd_name, &cmd.args);
let access = key_access_type_for_flags(handler.flags());
if let Some(resp) = self.permission_guard().check(cmd_name, sub.as_deref(), &keys, access) {
    return resp;
}
```

Site 3 (transaction.rs) becomes the *same* call, which is the point: its missing log and its
hand-rolled channel branch both vanish, and it can no longer diverge from Site 2. Site 4 (DRYRUN)
calls `.evaluate(...)` with `handler.keys()` and `key_access_type_for_flags(flags)` and renders the
returned `AclError` as a bulk string — correct key set, correct access type, no separate code path.

### Why this is the right depth

- **Locality.** "Deny → log → shape" lives in `PermissionGuard::deny` and nowhere else. Changing the
  audit format, adding a denial metric, or correcting a NOPERM string is a one-method edit instead of
  a four-file sweep. The client-info string is formatted in exactly one place.
- **Leverage.** ~60 lines of guard replace ~120 lines across `guards.rs`, `routing.rs`,
  `transaction.rs`, and `auth.rs`, and *delete* the dead `AclChecker` trait + `KeyAccess` enum in
  `handlers/mod.rs`. Every future execution path gets check-and-log for free, in one call, and
  cannot forget to log because logging is not its responsibility.
- **Deletion test.** With the seam in place, removing the guard call from a path is a visible
  one-line deletion that a single shared denial test (below) catches on that path. Today the same
  mistake is invisible — and has already happened twice (`transaction.rs`). The migration is a net
  deletion that removes whole artifacts (the dead trait, the enum, four error literals, three
  client-info `format!`s), which is the test that the new interface is the right shape.
- **Not a new adapter.** This does not wrap the acl crate in a parallel abstraction commands may or
  may not use. It *connects* the deep half that already exists (`FullAclChecker` + `AclLog` +
  `AclError`) to the call sites that were bypassing it. The raw bool methods stay — but only behind
  `FullAclChecker`, where they belong.

## Migration plan

Behavior-preserving except for the two logging gaps and the one message-format divergence, which are
intentional fixes called out as they land. FrogDB is pre-production; no shims.

1. **Phase 0 — add the seam, no call sites change.** Add `PermissionGuard` + `ConnectionHandler::
   permission_guard()` in `guards.rs`, delegating to `FullAclChecker` and `AclLog`. Add the unit
   tests (below). `just check frogdb-server && just test frogdb-server`.
2. **Phase 1 — migrate Site 2 (routing.rs).** The reference path: it already logs and shapes
   correctly, so this is pure behavior-preserving substitution. Redis-compat suite is the net.
3. **Phase 2 — migrate Site 1 (guards.rs command check + channel check).** Behavior-preserving for
   the command-allowed case; the deny message changes from `'config set'` to `'config|set'`
   (Correctness flag 1). Update any test asserting the old string.
4. **Phase 3 — migrate Site 3 (transaction.rs).** Replacing the key branch *adds* the missing
   `log_key_denied`; replacing the channel branch *adds* the missing `log_channel_denied`
   (Correctness flags 2 and 3). Add regression tests asserting `ACL LOG` now records in-MULTI
   denials.
5. **Phase 4 — migrate Site 4 (auth.rs DRYRUN) onto `evaluate`** with `handler.keys()` +
   `key_access_type_for_flags`. Fixes the heuristic (Correctness flag 4): DRYRUN now agrees with the
   real guard on key set and access type.
6. **Phase 5 — deletion pass.** Remove the unused `AclChecker` trait and `KeyAccess` enum
   (`handlers/mod.rs:47-`), confirm no remaining `check_command_with_key` / `check_command` calls in
   `connection/` outside the guard, and add a grep gate to `just lint`:

   ```bash
   # ACL bool methods must only be reached through the guard / FullAclChecker.
   ! grep -rEn 'check_command(_with_key)?\(' frogdb-server/crates/server/src/connection \
       | grep -v 'permission_guard'
   ```

## Testing impact

- **One denial test exercises every path.** A table-driven test runs the *same* restricted user
  through command denial, key denial, and channel denial via Site 1, Site 2, and Site 3, asserting
  (a) the NOPERM response and (b) that `ACL LOG` gained an entry of the right `AclLogEntryType`.
  Today this requires per-path tests, and the two in-transaction paths have no log to assert on at
  all.
- **ACL-log emission becomes assertable at the seam.** `PermissionGuard::deny` is a single unit
  under test: feed it a denied `PermissionResult` and assert the `AclLog` entry's type, object,
  username, and client-info. Log emission is currently only observable end-to-end, which is why two
  sites silently lack it.
- **Message format pinned once.** NOPERM strings are asserted against `AclError::Display` in
  `acl/src/error.rs` (already tested at `error.rs:100`), so every path inherits the pipe-delimited
  subcommand form. No path can reintroduce `'config set'`.
- **DRYRUN parity test.** A property test: for a sample of commands, `evaluate(...)` from DRYRUN and
  `check(...)` from the live guard must agree (allow/deny) for the same user — impossible to state
  today because they use different key extraction and access types.
- **Existing suites unchanged** except the intentional fixes; Redis-compat and Jepsen ACL tests are
  the end-to-end net that no other reply changed.

## Risks / open questions

- **Per-command key extraction differences.** The guard takes `keys: &[&[u8]]` and an `access`; it
  does not itself know how to extract keys. Callers must keep passing `handler.keys()` /
  `entry.keys()` and `key_access_type_for_flags(flags)`. That is correct (key specs are a command-
  registry concern, not an ACL concern), but it means the guard cannot *enforce* that a caller passed
  the right keys — DRYRUN's bug was exactly a wrong key set. The grep gate plus the DRYRUN parity
  test are the mitigations; a tighter design would have the guard take the `&dyn Command` and call
  `.keys()` itself, considered if more callers appear.
- **Subcommand handling.** `extract_subcommand` only returns `Some` for `CONTAINER_COMMANDS`
  (`util.rs:221`), and `run_pre_checks` exempts `ACL`. The guard must preserve both: pass the
  extracted subcommand through, and keep the `cmd != "ACL"` exemption at the command-check call site
  (or model it as a guard option) so `ACL WHOAMI` stays reachable.
- **No-auth / default-user fast path.** All four sites guard on `if let Some(user) =
  self.state.auth.user()` and allow when `None`; `PermissionGuard { user: None, .. }` reproduces
  this. Confirm the semantics of a *restricted* default user (nopass + limited perms): if
  `auth.user()` returns `Some(default)` in that mode, the guard enforces it; if it returns `None`,
  it does not. This behavior must be identical before and after — pin it with a test.
- **Queue-time vs exec-time checks.** Transactional commands are checked only at queue time
  (`transaction.rs:458`); `handle_exec` dispatches straight to the shard with no recheck. The guard
  preserves this (it just makes the queue-time check log). Whether ACL should be re-evaluated at EXEC
  is a separate question — moot today because `auth.user()` is an authentication-time snapshot, so
  mid-transaction `ACL SETUSER` does not affect the live connection until re-auth. Worth a note, not
  a blocker.
- **`check_command_with_key` collapses command and key.** The guard deliberately splits them
  (command first, then keys) so a denial is attributed correctly and logged with the right
  `AclLogEntryType`. This is a behavior *improvement* (today a command denial reached via the key
  path would log as a key denial), but verify no test depends on the conflated behavior.
- **`FullAclChecker` vs `AllowAllChecker`.** The checker is currently constructed per call in the
  acl crate's own code; the guard needs a `&FullAclChecker`. Either store one on `CoreDeps`
  alongside `acl_manager`, or construct a cheap stack one from `FullAclChecker::from_manager`
  (`checker.rs:128`) inside `permission_guard()`. The latter avoids new shared state; confirm it is
  zero-cost enough for the hot path.

## Correctness flags

These were found while mapping the four sites. Each is a divergence where one path lacks a guarantee
another path has — the exact failure mode a single seam prevents.

1. **`guards.rs:204` — NOPERM subcommand message diverges from Redis.** Denied container subcommands
   yield `'config set'` (space) instead of `'config|set'` (pipe). The same function logs it with a
   pipe at `guards.rs:181`, and `AclError::NoPermissionSubcommand` (`acl/src/error.rs:47`) uses a
   pipe. Clients parsing NOPERM messages get the wrong command identity.
2. **`transaction.rs:458-462` — key denials inside MULTI are not audit-logged.** The queue-time key
   check returns NOPERM but never calls `log_key_denied`, unlike the live path (`routing.rs:66`).
   Since `EXEC` does not recheck, a denied key access in a transaction never appears in `ACL LOG`.
   Security-relevant: probing key permissions from inside `MULTI` is invisible to operators.
3. **`transaction.rs:473-488` — channel denials inside MULTI are not audit-logged.** The
   in-transaction `PUBLISH`/`SUBSCRIBE` channel check returns NOPERM but never calls
   `log_channel_denied`, unlike `validate_channel_access` (`guards.rs:113`). Same invisibility as
   flag 2, for channels.
4. **`auth.rs:458-468` — ACL DRYRUN key simulation disagrees with real enforcement.** DRYRUN checks
   only the first argument (`cmd_args[key_start]`), ignoring `handler.keys()`, and always requests
   `KeyAccessType::ReadWrite` instead of `key_access_type_for_flags(flags)`. Consequences: (a) a
   read-only command (e.g. `GET`) on a key the user holds with read-only access is reported *denied*
   though real enforcement *allows* it; (b) multi-key commands (`MSET`, `MGET`) and commands whose
   key is not the first arg are mis-checked. The ACL-audit tool is inaccurate.
