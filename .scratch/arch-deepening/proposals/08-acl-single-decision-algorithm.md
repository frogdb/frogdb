# Proposal 08 — Collapse the two ACL decision algorithms into one seam

## Summary (2-3 sentences)

The ACL permission-decision algorithm is written twice. `UserPermissions`
(`acl/src/user.rs`) is a field-by-field flattened copy of `PermissionSet` +
`CommandPermissions` (`acl/src/permissions.rs`) that re-implements `check_command`,
`check_key_access`, and `check_channel_access` a second time — one copy drives *online
enforcement* (per-connection snapshot), the other drives *offline rule editing and ACL
DRYRUN*. The two algorithms are byte-for-byte equivalent **today only because they are
hand-synchronised**; this proposal deletes `UserPermissions` and snapshots
`Arc<PermissionSet>` so there is one algorithm and one test suite. A secondary section
removes the one-adapter `AclChecker` trait, whose only load-bearing job is the `NOPERM`
error rendering.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/acl/src/user.rs` | 666 | Defines `UserPermissions` (struct L283-304) — the flattened copy — plus its four check methods (L339-430) and `AuthenticatedUser` (L433-495) |
| `frogdb-server/crates/acl/src/permissions.rs` | 630 | Defines `CommandPermissions::is_command_allowed` (L214-266) and `PermissionSet` (L374-457) — the *original* algorithm |
| `frogdb-server/crates/acl/src/manager.rs` | 623 | `authenticate` builds the snapshot via `UserPermissions::from_user` (L140); `get_user` returns a cloned `User` for DRYRUN (L244) |
| `frogdb-server/crates/acl/src/checker.rs` | 398 | `AclChecker` trait (L43-72), dead `AllowAllChecker` (L74-112), `FullAclChecker` (L114-182) with the `NOPERM` rendering (L143-155) |
| `frogdb-server/crates/server/src/connection/permission_guard.rs` | 389 | The single enforcement seam; holds a concrete `FullAclChecker` (L69, L86) and calls its three check methods |
| `frogdb-server/crates/server/src/connection/acl_conn_command.rs` | — | `acl_dryrun` (L230) calls `User::check_command` / `User::check_key_access` (L249, L278) — the *other* algorithm copy |

## Problem (concrete verified evidence)

**The same decision is implemented twice, in two types, in two files.**
`UserPermissions::check_command` (`user.rs:339-388`) and
`CommandPermissions::is_command_allowed` (`permissions.rs:214-266`) are the same
algorithm — subcommand-rules-first, explicit-deny precedence, a denied-category loop with
an allowed-command exception, `allow_all`, explicit-allow, then an allowed-category
loop — written out longhand in both places. The denied-category arm is the only cosmetic
difference:

```rust
// UserPermissions::check_command (user.rs:362-368)
for category in &categories {
    if self.denied_categories.contains(category)
        && !self.allowed_commands.contains(&cmd_lower)
    {
        return false;
    }
}
```

```rust
// CommandPermissions::is_command_allowed (permissions.rs:239-246)
for category in &categories {
    if self.denied_categories.contains(category) {
        if !self.allowed_commands.contains(&cmd_lower) {
            return false;
        }
    }
}
```

`A && B → return false` versus `if A { if B { return false } }` — logically identical.
`check_key_access` (`user.rs:390-403` vs `permissions.rs:401-414`) and
`check_channel_access` (`user.rs:405-418` vs `permissions.rs:416-429`) are *character*
copies. **I diffed all three pairs: there is no behavioural divergence today.** The two
copies are in sync — but only by hand.

**`UserPermissions` is a mechanical clone of the real type.** `from_user`
(`user.rs:306-321`) copies ten fields out of `PermissionSet` + `CommandPermissions` —
`allow_all`, `allowed_commands`, `denied_commands`, `allowed_categories`,
`denied_categories`, `key_patterns`, `all_keys`, `channel_patterns`, `all_channels`,
`subcommand_rules` — dropping only `rule_log` (the serialization-only field). It is
`PermissionSet` minus one field, re-typed.

**The two copies power the two halves of the same feature.** The `PermissionSet` copy
drives *offline* paths: `User::check_command` → `PermissionSet::check_command` →
`CommandPermissions::is_command_allowed`, reached by `ACL DRYRUN`
(`acl_conn_command.rs:249,278`, on a `User` cloned via `manager.get_user`). The
`UserPermissions` copy drives *online* enforcement: `authenticate` snapshots it
(`manager.rs:140`) into `AuthenticatedUser.permissions: Arc<UserPermissions>`
(`user.rs:439`), and the `PermissionGuard` calls it per command (`permission_guard.rs:102`).

**DRYRUN's entire contract is "predict what enforcement will do" — and it does so through a
different algorithm.** The DRYRUN handler even carries a comment promising it agrees with
live enforcement (`acl_conn_command.rs:267-273`). That promise is currently kept, but it
is kept by two independent code paths that no compiler links. Edit the category-precedence
rule in one `check_command` and forget the other, and `ACL DRYRUN` will confidently report
the opposite of what the server actually enforces — the single worst failure mode an ACL
preview tool can have — with no build error and no failing test unless someone wrote a
cross-checking one (none exists).

## Why it is shallow/fragmented (architecture vocabulary)

**Two Implementations behind one conceptual Interface.** "Is this command/key/channel
allowed for this user?" is a single question. FrogDB answers it with two Modules that share
no code: `PermissionSet`/`CommandPermissions` and `UserPermissions`. Neither is deep — each
is a thin pass over the same HashSets — and having two of them doubles the surface without
adding any capability. The second copy hides nothing the first does not.

**The Locality is inverted.** The knowledge "the ACL decision works like *this*" should live
in exactly one place. Instead it is smeared across `permissions.rs` (offline) and `user.rs`
(online), and the correctness of `ACL DRYRUN` — defined in a *third* file,
`acl_conn_command.rs` — depends on those two staying identical. Three files, one fact,
maintained by hand.

**The deletion test exposes the redundancy.** Delete `UserPermissions::check_command` and
online enforcement breaks; delete `CommandPermissions::is_command_allowed` and DRYRUN
breaks — yet the two bodies are the same algorithm. When two deletions break two features
but the deleted code is a copy, you have one Module wearing two hats. A single algorithm
that both callers reach would make each deletion break *everything at once* — which is the
correct, honest coupling for a single source of truth.

**No Adapter, just a parallel type.** `UserPermissions` is not an Adapter that reshapes
`PermissionSet` for a different consumer — the consumer (`FullAclChecker` →
`AuthenticatedUser`) needs the exact same three predicates with the exact same signatures.
It is a copy that exists only because `AuthenticatedUser` was given its own permissions type
instead of holding the one that already exists.

**The perf rationale does not hold.** One might defend the flattened copy as a
snapshot that avoids `Arc` indirection on the hot path. It does not:
`AuthenticatedUser.permissions` is *already* `Arc<UserPermissions>` (`user.rs:439`), so
every check already pays one `Arc` deref. And `from_user` (`manager.rs:140`) already
deep-clones all ten fields at auth time. Snapshotting `Arc<PermissionSet>` is the *same*
one-deref access pattern and the *same* one-clone-at-auth cost (or cheaper — see Risks), so
there is no performance argument for the second type.

## Proposed change (plain English)

Make `AuthenticatedUser` hold a snapshot of the real permission type, and delete the copy.

1. **`AuthenticatedUser.permissions` becomes `Arc<PermissionSet>`** instead of
   `Arc<UserPermissions>`. `authenticate` (`manager.rs:138-142`) snapshots
   `user.root_permissions` directly (`Arc::new(user.root_permissions.clone())`) instead of
   calling `UserPermissions::from_user`.
2. **Delete `UserPermissions` entirely** — the struct (`user.rs:283-304`), `from_user`,
   `allow_all`, and its four check methods (`user.rs:306-430`). `AuthenticatedUser`'s
   delegating methods (`user.rs:468-494`) now forward to `PermissionSet`, which already
   exposes `check_command`, `check_key_access`, and `check_channel_access`
   (`permissions.rs:401-434`). `AuthenticatedUser::default_user` uses
   `Arc::new(PermissionSet::allow_all())` (`permissions.rs:390`) in place of
   `UserPermissions::allow_all`.
3. **Fold the one genuinely-missing convenience into `PermissionSet`.**
   `check_command_with_key` exists on `UserPermissions` (`user.rs:422`) and `User`
   (`user.rs:118`) but not on `PermissionSet`. **However, a workspace grep finds zero
   production callers of `check_command_with_key`** — live enforcement checks command and
   keys separately (`permission_guard.rs` `check_command` + `check_keys_with_flags`). So the
   honest move is to **drop it**, not port it; add it to `PermissionSet` only if a caller
   ever materialises. One algorithm, one test suite (`permissions.rs` tests already cover
   the decision logic; the `user.rs` `UserPermissions` tests are redundant copies that get
   deleted with the type).

### Secondary: retire the one-adapter `AclChecker` trait

The `AclChecker` trait (`checker.rs:43-72`) is a Seam with a single production Adapter:

- **No `dyn AclChecker` exists** anywhere in the workspace (verified — zero matches). There
  are no generic-over-`AclChecker` callers.
- **The sole production consumer holds the concrete type.** `PermissionGuard.checker` is
  `FullAclChecker` (`permission_guard.rs:69`), constructed as `FullAclChecker::new(...)`
  (`permission_guard.rs:86`) — never behind the trait.
- **`AllowAllChecker` (`checker.rs:74-112`) has zero production callers** — only
  `checker.rs:233` (a test) and two re-exports. It is also semantically redundant:
  `AuthenticatedUser::default_user()` already carries allow-all permissions, so a
  no-auth instance grants everything through the *same* `FullAclChecker` path.
- **The trait's non-check methods are dead in production.** `is_auth_exempt`
  (`checker.rs:68`) is shadowed by the real one at `guards.rs:216`; the checker's
  `requires_auth()` (`checker.rs:179`) has no production caller (every `.requires_auth()`
  call targets `AclManager`, not the checker). `FullAclChecker::from_manager`
  (`checker.rs:128`) is unused.

Proposed: delete the `AclChecker` trait and `AllowAllChecker`; keep `FullAclChecker` as a
plain struct (or free functions), preserving its genuinely load-bearing job — turning a
`bool` decision into the correct `NOPERM` / `config|set` `AclError` reply
(`checker.rs:143-155`). That rendering is the module's real, visible responsibility; the
trait around it is scaffolding for polymorphism that never happened.

## Before / After

### Before — the online copy (`user.rs:339-388`, real code)

```rust
pub struct UserPermissions {           // 10 fields cloned from PermissionSet + CommandPermissions
    pub allow_all_commands: bool,
    pub allowed_commands: HashSet<String>,
    /* … 8 more … */
    pub subcommand_rules: Vec<SubcommandRule>,
}

impl UserPermissions {
    pub fn from_user(user: &User) -> Self { /* clone all 10 fields (user.rs:306-321) */ }

    pub fn check_command(&self, command: &str, subcommand: Option<&str>) -> bool {
        // …byte-for-byte the same body as CommandPermissions::is_command_allowed…
    }
    pub fn check_key_access(&self, key: &[u8], access: KeyAccessType) -> bool { /* copy */ }
    pub fn check_channel_access(&self, channel: &[u8]) -> bool { /* copy */ }
}
```

```rust
// manager.rs:138-142 — enforcement snapshot goes through the copy
Ok(AuthenticatedUser::new(
    username.to_string(),
    UserPermissions::from_user(user),   // deep-clones 10 fields into the parallel type
    rate_limit,
))
```

Meanwhile DRYRUN (`acl_conn_command.rs:249`) evaluates the *other* copy
(`User::check_command` → `CommandPermissions::is_command_allowed`) and trusts it to agree.

### After — one algorithm, snapshotted by `Arc` (illustrative sketch)

```rust
pub struct AuthenticatedUser {
    pub username: Arc<str>,
    pub permissions: Arc<PermissionSet>,      // the real type, not a copy
    pub rate_limit: Option<Arc<RateLimitState>>,
}

impl AuthenticatedUser {
    pub fn default_user() -> Self {
        Self { username: Arc::from("default"),
               permissions: Arc::new(PermissionSet::allow_all()),
               rate_limit: None }
    }
    pub fn check_command(&self, cmd: &str, sub: Option<&str>) -> bool {
        self.permissions.check_command(cmd, sub)   // same method DRYRUN calls
    }
    // check_key_access / check_channel_access forward identically
}
```

```rust
// manager.rs — snapshot the one type; no second algorithm
Ok(AuthenticatedUser::new(
    username.to_string(),
    Arc::new(user.root_permissions.clone()),   // or share an Arc<PermissionSet> — see Risks
    rate_limit,
))
```

`UserPermissions` and its four methods are **deleted**. Online enforcement and `ACL DRYRUN`
now call the *same* `PermissionSet::check_command`; they cannot disagree because there is
nothing left to disagree with.

### Editing the ACL decision rule, before vs after

| Task | Before | After |
| --- | --- | --- |
| Change category-precedence semantics | edit `CommandPermissions::is_command_allowed` **and** `UserPermissions::check_command` — miss one → DRYRUN silently lies about enforcement | edit one method; both paths move together |
| Add a permission predicate | add it to `PermissionSet` **and** re-copy into `UserPermissions` | add once to `PermissionSet` |
| Test the decision logic | two near-identical test modules (`permissions.rs` + `user.rs`) | one test module |
| Drift failure mode | **silent DRYRUN/enforcement mismatch at runtime** | impossible — single source |

## Testability improvement

**Today the two algorithms need parallel test suites, and nothing tests that they agree.**
`permissions.rs` tests `is_command_allowed` (L500-629); `user.rs` re-tests the same logic
through `UserPermissions` (`test_user_permissions`, `test_user_permissions_with_subcommand_rules`,
L594-635). These are duplicated coverage of one algorithm. There is **no** test asserting
DRYRUN and enforcement produce the same verdict for a given rule set — the property the
whole DRYRUN feature rests on — because there is no single function to point such a test at.

**After the change:**

1. **One decision test suite.** The `permissions.rs` tests become the canonical suite; the
   `UserPermissions` copies are deleted with the type. No coverage is lost — they tested the
   same branches.
2. **DRYRUN-vs-enforcement agreement is true by construction.** Both call
   `PermissionSet::check_command`; a divergence is no longer expressible, so it needs no
   test. (An integration test that once would have had to guard against drift is now
   unnecessary.)
3. **The checker tests simplify.** Deleting `AllowAllChecker` removes `test_allow_all_checker`
   (`checker.rs:231-248`); the remaining `FullAclChecker` tests still cover the load-bearing
   `NOPERM` rendering (`checker.rs:318-369`), which is the behaviour worth keeping.

## Risks / open questions

- **Clone cost at auth.** Replacing `UserPermissions::from_user` with
  `user.root_permissions.clone()` clones the same field set **plus** `rule_log` (the
  serialization log `UserPermissions` dropped). `rule_log` is small (one entry per applied
  ACL rule), cloned once per authentication, and shared read-only via `Arc` thereafter — a
  negligible cost on a non-hot path. If it ever matters, store users as
  `Arc<PermissionSet>` in the manager and snapshot by cloning the `Arc` (O(1)), which is
  *cheaper* than today's ten-field deep clone. Either way there is no per-command regression:
  access stays a single `Arc` deref, exactly as now.
- **`check_command_with_key` has no production callers** (verified) — the three definitions
  (`User`, `UserPermissions`, `AuthenticatedUser`, `user.rs:118/422/480`) are dead except for
  one internal delegation. Recommend deleting rather than porting to `PermissionSet`; add it
  back only when a real caller appears. This is a deletion, not a migration risk.
- **No behavioural divergence exists yet.** I diffed both `check_command`/`check_key_access`/
  `check_channel_access` pairs line-by-line: they are equivalent today. The proposal's value
  is *preventing future drift* and halving the surface, not fixing a current bug. Framed
  honestly, this is a locality/DRY hardening of a correctness-critical seam, not a bug fix.
- **`AuthenticatedUser` field type is public.** `permissions: Arc<UserPermissions>` →
  `Arc<PermissionSet>` is a breaking change to a `pub` field, but `AuthenticatedUser` is
  internal to the server (consumers: `manager.rs`, `permission_guard.rs`, `state.rs`,
  `checker.rs` tests — all in-tree, enumerated by grep). No external API. The two test
  construction sites that build `UserPermissions { … }` literals
  (`checker.rs:196/216/323`, `state.rs:1580`) migrate to `PermissionSet` builders.
- **`AclChecker` trait removal touches the `core` re-export.** `core/src/lib.rs:46-47`
  re-exports `AclChecker` and `AllowAllChecker`; those lines must drop the removed names.
  `FullAclChecker` stays. Low risk, compiler-guided.
- **Keep `FullAclChecker`'s `requires_auth` field?** It is stored but never read
  (`.requires_auth()` callers all target `AclManager`). Dropping the field is a clean
  follow-up but is orthogonal to the algorithm collapse; can be left for a separate cut to
  keep this change focused.

## Effort estimate

**S–M.** The primary change is mostly deletion: remove `UserPermissions` (~150 lines
including its tests), retype one `AuthenticatedUser` field, redirect three delegating
methods to `PermissionSet` (which already has the methods), and change one snapshot call in
`manager.rs`. The compiler drives the rest — every construction site of the deleted type
errors until migrated, and there are only a handful (all in-tree, enumerated above). The
secondary `AclChecker`/`AllowAllChecker` removal is independent, similarly deletion-heavy
(~40 lines plus a re-export line and one test), and can land as a second commit. No hot-path
behaviour changes and no new algorithm is written, which keeps it out of L; it is not pure S
only because it spans three crates' re-exports (`acl`, `core`, `server`) and touches a
correctness-critical enforcement seam that warrants a careful before/after diff of the
retained `PermissionSet` tests.
