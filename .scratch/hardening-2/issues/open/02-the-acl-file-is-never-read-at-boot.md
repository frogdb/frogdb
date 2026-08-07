# The ACL file is never read at boot — every `ACL SETUSER` is lost on restart

Status: ready-for-agent
Type: bug (security / silent data loss)
Severity: likelihood 3/3 (every restart of any node with a configured `aclfile`), consequence 3/3
(the node comes back with an empty user set — either locked out or, worse, permissive) — score 9
Area: acl / server boot

## Problem

`AclManager::load()` (`frogdb-server/crates/acl/src/manager.rs:280`) is reachable from exactly one
place: the `ACL LOAD` command (`server/src/connection/acl_conn_command.rs:370`). The boot path
(`server/src/server/init.rs:240` → `acl/src/manager.rs:66-90`) only synthesizes the `default` user
from `requirepass`.

So a configured `aclfile` is written by `ACL SAVE` and never read back automatically. Every user,
rule, and password added with `ACL SETUSER` disappears on restart unless an operator remembers to
issue `ACL LOAD` by hand — and the operator cannot issue it if the credentials needed to connect
were themselves in the file.

Redis loads the aclfile during startup and **refuses to start** if it fails to parse, precisely
because a half-applied ACL configuration is a security posture change. FrogDB currently starts
clean with no users at all.

Compounding it: `ACL SAVE` itself is non-atomic and unsynced (`manager.rs:267-277` — bare
`File::create` + `write_all`, truncating in place), so a crash during save can leave a truncated
file that a boot-time load would then have to reject. Round-2 issue 75 covers the save half; this
issue covers the load half. Fix them together.

## Candidate fix

1. Call `AclManager::load()` in the boot sequence when `aclfile` is configured, before acceptors
   bind (`server/src/server/subsystems.rs:579-594` is where they bind today).
2. Fail the boot on a parse error rather than starting with an empty user set. Redis-parity, and
   the safe direction.
3. Route the save through the durable-write primitive (see campaign-2 issue 03 / round-2 issue 75).

Decide and record: what happens when `aclfile` is configured but absent — treat as empty (first
boot) or refuse? Redis treats a missing file as a startup error. Recommend matching it, with the
first-boot case handled by writing the file at `ACL SAVE` time only.

## Forcing test

A server test that configures an `aclfile`, `ACL SETUSER`s a non-default user, restarts the
server, and asserts the user still authenticates. Plus a negative test: a corrupt aclfile makes
the boot fail rather than silently proceed.

## Comments

Found by the campaign-2 durability-extraction survey, 2026-08-07, while mapping boot-time state
reconstruction outside `frogdb-recovery`.
