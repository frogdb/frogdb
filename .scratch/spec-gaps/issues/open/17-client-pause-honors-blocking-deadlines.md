# 17: CLIENT PAUSE honors blocking deadlines — blocked state entered, timer runs

Status: ready-for-agent

## Origin

Distsys-review MAJ-14 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **adopt Redis semantics** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

`wait_if_paused` runs at dispatch (`frogdb-server/crates/server/src/connection/dispatch.rs:568`),
before execution — a blocking command issued during CLIENT PAUSE never reaches
`handle_blocking_wait`. Consequences:

- `CLIENT PAUSE 60000 WRITE` then `BLPOP k 1` waits ~61s in FrogDB; 1s in Redis
  (upstream test: "Blocking timeout following PAUSE should honor the timeout" — the
  client enters the blocked state and its deadline runs during pause).
- `INFO clients:blocked_clients` and `CLIENT LIST` report 0 blocked for the whole
  pause window — an operator pausing during failover cannot see how many clients are
  parked. Violates the observability-accuracy principle.
- The divergence lives only as a "FrogDB note" in
  `redis-regression/tests/list_tcl.rs:2910-2953`, whose assertion
  (`resp.is_none() || matches!(Bulk(None))`) passes whether or not a reply arrives —
  it forces nothing. No spec row exists.

## Ruled shape

Redis semantics: a blocking command issued under CLIENT PAUSE **enters the blocked
state** and its **deadline runs during the pause**. Pause gates execution, not
parking. `blocked_clients`/`CLIENT LIST` reflect the parked client immediately.

## What to build (spec-first; sequenced after spec-gaps issue 13)

1. TR row in `specs/blocking.md`: blocking command under CLIENT PAUSE registers its
   wait (blocked flag set, `blocked_clients` incremented) and times out on its own
   deadline regardless of the pause window; on pause expiry before deadline, normal
   satisfaction resumes.
2. FM row: NOT observable — a blocking command whose effective wait exceeds its
   requested timeout because of a concurrent CLIENT PAUSE.
3. Code: rides issue 13's run-loop restructure (blocked-client-as-state). The
   dispatch-time `wait_if_paused` must not swallow blocking commands: either blocking
   commands bypass the pause gate into their wait state, or the pause gate itself
   becomes a run-loop state that lets the blocking registration through. Design note:
   the write the blocking command would perform on satisfaction (e.g. BLPOP's pop)
   still respects WRITE-pause on the satisfaction path — parking is not a write.
4. Tighten `list_tcl.rs:2910-2953`: require the reply (timeout nil at ~1s), assert
   `blocked_clients` = 1 during the window; remove the "FrogDB note".
5. Deviations doc: remove/amend any entry recording the old behavior.

## Cross-references

- [Issue 13](13-blocking-wait-becomes-a-run-loop-state.md): hard prerequisite — the
  restructure makes pause-compatible parking natural (blocked = state on a readable
  connection, not an inline await behind the dispatch gate).
- [Issue 08](08-blocking-command-rows.md): blocking row family.

## Acceptance criteria

- [ ] TR + FM rows landed; `just lint-spec` green
- [ ] `BLPOP k 1` under `CLIENT PAUSE 60000 WRITE` replies nil at ~1s; forcing test
- [ ] `blocked_clients` counts the parked client during pause; forcing test
- [ ] Regression assertion tightened (forces the reply)
- [ ] Satisfaction path still respects WRITE-pause

## Blocked by

- [Issue 13](13-blocking-wait-becomes-a-run-loop-state.md) — run-loop restructure
  lands first.
