# 17 — The whole `ACL` command is exempt from ACL enforcement, so any authenticated user can self-grant `+@all`

Status: needs-triage

**PARKED per user (security)** — do not implement without user ruling.

## What to build

`PreDispatchView::check_permissions` gates every command through the unified ACL seam, except
that it short-circuits the entire `ACL` container first:
`frogdb-server/crates/server/src/connection/guards.rs:362-371` reads
`if cmd_name != "ACL" && let Some(guard) = self.permission_guard() { … guard.check_command(…) }`.
The stated justification in the comment at `:363` is "users need `ACL WHOAMI` to check their
identity", but the exemption is **unconditional and whole-command** — it covers `SETUSER`,
`DELUSER`, `GETUSER`, `LIST`, `USERS`, `LOAD`, and `SAVE` just as much as `WHOAMI`.

Nothing downstream re-imposes a check. `ACL_SPEC` declares `flags: CommandFlags::empty()`
(`acl_conn_command.rs:33-49`), so there is no whole-command `ADMIN` flag to catch it.
`acl_setuser` (`acl_conn_command.rs:172-187`) validates argument count and then calls
`ctx.acl_manager.set_user(…)` directly — it performs no permission check of its own. The only
other gate on the path is the admin-port split at `guards.rs:347-360`, and that is inert unless
the operator has configured it: `AdminConfig::default()` sets `enabled: false`
(`frogdb-server/crates/config/src/admin.rs:46`), and the `SPLIT_ADMIN_SURFACES` row for `ACL`
(`frogdb-server/crates/core/src/command_spec.rs:605`) only ever fires when `admin_enabled` is
true. So on a default-configured node with an ACL file, an authenticated user holding **zero**
grants (`-@all`) can run `ACL SETUSER <self> +@all ~* &*` and hold full privileges, or
`ACL DELUSER` every other user. Writing `-acl` or `-acl|setuser` into a user's rules is inert:
the rule parses, `ACL GETUSER` echoes it back, and enforcement never consults it.

This is **LIVE on main today** and is a straightforward privilege-escalation path, not a latent
one. Redis has no such exemption: `ACL` is an ordinary command subject to `ACLCheckAllPerm`, and
the only commands that bypass authorization are those carrying `CMD_NO_AUTH` (`AUTH`, `HELLO`,
`RESET`). Redis's self-service subcommands (`ACL WHOAMI`, `ACL CAT`, `ACL GENPASS`) are reachable
because they are granted, not because the container is skipped.

Fix direction: replace the whole-command exemption with a **subcommand allowlist** matching the
self-directed set already named by `SPLIT_ADMIN_SURFACES` — `WHOAMI`, `CAT`, `GENPASS`, `HELP`
(and arguably `GETUSER` restricted to the caller's own username, which is a separate ruling) —
and route everything else through `guard.check_command("ACL", Some(sub))` like any other
container command. The subcommand is already extracted three lines later at `guards.rs:367`; the
change is to move that extraction above the gate and test it, not to add new machinery. Note the
interaction with proposal 70 (`acl-registry-consult`), which reshapes how subcommand identity is
resolved for ACL grants: 70 neither creates nor worsens this defect, but 70's reviewer named this
as **merge precondition 1** — it should be filed and ruled before 70 merges.

## Acceptance criteria

- [ ] A user with no ACL grants (`-@all`) receives `NOPERM` for `ACL SETUSER`, `ACL DELUSER`,
      `ACL LIST`, `ACL USERS`, `ACL LOAD`, and `ACL SAVE`, on the ordinary port, with the admin
      port disabled
- [ ] `ACL WHOAMI` / `ACL CAT` / `ACL GENPASS` / `ACL HELP` still succeed for that same
      zero-grant user (identity self-service is preserved)
- [ ] A `-acl|setuser` rule written into a user's ruleset is actually enforced, not merely echoed
      back by `ACL GETUSER`
- [ ] Regression test `acl_setuser_denied_for_unprivileged_user` in the server ACL integration
      suite drives the full escalation attempt (`AUTH` as a `-@all` user → `ACL SETUSER <self>
      +@all ~*` → assert `NOPERM` → assert a subsequent `GET` is still denied), so the escalation
      cannot silently return
- [ ] `just test frogdb-server acl_setuser_denied_for_unprivileged_user` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 70
(`.scratch/arch-deepening/proposals/70-acl-registry-consult.md`), **merge precondition 1** —
raised by the author as an out-of-scope CRITICAL and confirmed by the reviewer as blocking
(review `159cb7a2`, item B5). The orchestrator dispatch attributed this cite to proposal 73;
`guards.rs:364` appears only in proposal 70's plan entry.

## Comments
