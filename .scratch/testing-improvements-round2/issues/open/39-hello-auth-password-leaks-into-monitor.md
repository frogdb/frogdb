# `HELLO … AUTH user pass` leaks the password into the MONITOR feed

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/03 F10 · MASTER.md §3
Score: severity 3 · likelihood 3 · effort 1 · priority 14
Area: frogdb-server / connection MONITOR

## Context

`MonitorEvent::new` redacts arguments only when the command name is exactly `AUTH`. Every other
credential-bearing command passes through verbatim, so the password appears in plaintext in a
stream any MONITOR-privileged client can read, and in any log that captures it. Modern clients
(redis-py, Lettuce) authenticate via `HELLO … AUTH` by default when a username is configured, and
running MONITOR is a standard debugging step, so the two meet routinely. MONITOR is already
privileged, which caps this below a true auth bypass.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `monitor.rs:26 MonitorEvent::new` redacts only `if cmd_name == "AUTH"`.
- The call site (`connection.rs:396`) passes the uppercased name, so case is handled, but `HELLO`,
  `CONFIG SET requirepass`, `CONFIG SET masterauth`, `ACL SETUSER u >pass` and `MIGRATE … AUTH` all
  pass through verbatim.
- **Why the existing test passes anyway**: the only redaction test is
  `monitor.rs:114 test_auth_args_redacted`, which feeds `"AUTH"` (`single-test`) — the one command
  the code does handle.

## What to fix

1. Replace the `cmd_name == "AUTH"` equality check with a redaction table keyed on
   `(cmd_name, arg position/keyword)` covering at minimum `AUTH`, `HELLO … AUTH`,
   `CONFIG SET requirepass`, `CONFIG SET masterauth`, `ACL SETUSER … >pass`, and
   `MIGRATE … AUTH` / `AUTH2`.
2. Keep the redaction a pure function of `(cmd_name, args)` so it can be table-tested without a
   connection.
3. Sweep for other consumers of the raw argument vector that reach a log or a feed (`SLOWLOG`,
   `CLIENT LIST` last-command, debug bundles) and route them through the same function.

## Acceptance criteria

- [ ] Unit table over `MonitorEvent::new` asserts no sensitive argument survives for `AUTH`,
      `HELLO 3 AUTH u p`, `CONFIG SET requirepass x`, `CONFIG SET masterauth x`,
      `ACL SETUSER u >p`, `MIGRATE … AUTH p` and `MIGRATE … AUTH2 u p`. Every row except plain
      `AUTH` **fails today**.
- [ ] One integration test runs MONITOR on one connection and `HELLO 3 AUTH` on another, then
      greps the captured feed for the password and asserts it is absent.
- [ ] The redaction table is the single source of truth — no second copy at the call site.

## Test boundary

**1** for the table (pure function of `(cmd_name, args)`), **4** for a single end-to-end
confirmation that the redaction sits on the path MONITOR actually uses. Level 4 is needed only for
that one case because the defect class includes "redaction exists but the feed bypasses it", which
a unit test cannot see.

## Depends on

Nothing.

## Re-triage 2026-08-06

**Verdict: still-valid**

Reproduces verbatim. `MonitorEvent::new` at `frogdb-server/crates/server/src/monitor.rs:27` still
gates redaction on `if cmd_name == "AUTH"` (`monitor.rs:28`) and copies every other command's args
through with `args.to_vec()` (`monitor.rs:33`). The call site moved:
`connection.rs:396` → `frogdb-server/crates/server/src/connection.rs:406-414`, still passing the
uppercased `cmd_name` and the raw `cmd.args`. The only redaction test is still
`test_auth_args_redacted` (`monitor.rs:114-120`), feeding plain `"AUTH"`. `format_event`
(`monitor.rs:74-90`) writes `event.args` out with `from_utf8_lossy` and no second filter, so there
is no downstream backstop. History on `monitor.rs` since filing is only `2fb1051c` (clock seam) —
no redaction change.
