# 58 — ACL SETUSER applies rules non-atomically and creates the user even when every rule fails

Status: needs-triage

> **MERGE BLOCKER for proposal 70**: proposal 70 (`.scratch/arch-deepening/proposals/70-*.md`)
> lists this defect as its second merge precondition — it must be FILED before 70 merges. This
> file satisfies that precondition. (The first precondition, the ACL-command-exempt bypass, is
> issue 17.)

## What to build

`AclManager::set_user` (`frogdb-server/crates/acl/src/manager.rs:167-192`) mutates live state
in place, rule by rule:

```rust
let user = users
    .entry(username.to_string())
    .or_insert_with(|| User::new(username));

for rule_str in rules {
    let rule = AclRule::parse(rule_str)?;
    rule.apply(user);
}
```

Two defects in one shape:

1. **Partial apply.** `AclRule::parse(rule_str)?` early-returns mid-loop with no rollback. `ACL
   SETUSER alice on >pw ~keys:* bogus-rule +@read` leaves alice enabled with a password and key
   pattern but *without* the `+@read` grant — or worse, `... +@all bogus-rule -@dangerous` leaves
   the user with `+@all` and no `-@dangerous`. A failing command silently widens or narrows a
   live user's permissions to an intermediate state the operator never specified.
2. **User created on fully-failing call.** The `entry().or_insert_with()` runs before any rule
   parses, so `ACL SETUSER newuser bogus` errors *and* leaves `newuser` behind (off, but it now
   exists, shows in `ACL LIST`/`ACL USERS`, and a later `ACL SETUSER newuser on ...` composes
   onto it instead of starting fresh).

Redis is explicitly atomic here: it validates every rule first and aborts the whole command with
no state change on any error. The intent gap is visible in the same file — the file-load path
(`manager.rs:280-321`) builds each `User` into a scratch `HashMap` and swaps it in only after
every line parses ("Replace users atomically", `:316-318`). `set_user` needs the same
build-aside-then-commit shape: clone-or-create a scratch `User`, parse **all** rules, apply to
the scratch copy, and only then insert into the map and update the rate-limit registry.

Security-adjacent (a failed admin command can leave a live user with unintended permissions) but
the fix is a small, mechanical atomicity change in one function; it does not touch enforcement
paths.

## Acceptance criteria

- [ ] `ACL SETUSER existing <valid> <invalid> <valid>` errors and leaves the user byte-identical
      to its pre-call state (no partial rule application)
- [ ] `ACL SETUSER newuser <invalid>` errors and `newuser` does not exist afterwards (absent
      from `ACL LIST`, `ACL USERS`, `ACL GETUSER`)
- [ ] Rate-limit registry only updated on the success path
- [ ] All-valid calls behave exactly as today
- [ ] Regression test `acl_setuser_is_atomic` covering both shapes above, failing on today's code
- [ ] `just test frogdb-acl` and the ACL regression suite green

## Blocked by

None - can start immediately

## Source

Round 38-99: proposal 70's second merge precondition, surfaced during review-phase issue filing
(the filer found it referenced in proposal 70 but never filed). Verified live on `origin/main`
at `manager.rs:167-192` vs the atomic load path at `:280-321`.

## Comments
