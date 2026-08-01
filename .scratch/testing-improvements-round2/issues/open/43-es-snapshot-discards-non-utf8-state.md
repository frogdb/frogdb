# `ES.SNAPSHOT` silently discards a non-UTF-8 snapshot state and replies OK

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/07 F1 · MASTER.md §3
Score: severity 5 · likelihood 3 · effort 1 · priority 20
Area: frogdb-commands / event sourcing

## Context

`ES.SNAPSHOT` stringifies the caller's state with `from_utf8(state).unwrap_or("")`, so any state
containing a non-UTF-8 byte is replaced by the empty string, written to the store, and answered
with `+OK`. A later `ES.REPLAY` then returns `(version=N, state="")`, so the consumer skips every
event `<= N` *and* has no state to rebuild from. The aggregate is destroyed in place and the loss
is unrecoverable. Binary-serialized aggregate state (protobuf/msgpack/bincode/CBOR) is the natural
choice for an event-sourcing snapshot, and nothing in the API says "text only".

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `commands/src/event_sourcing/snapshot.rs:46` —
  `let stored = format!("{}:{}", version, std::str::from_utf8(state).unwrap_or(""));` then
  `ctx.store.set(...)` and `Ok(Response::ok())`.
- Read side `commands/src/event_sourcing/replay.rs:52-80` splits on the first `':'`; its whole
  error family (lines 60-61, 64-67, 71-73, 76) is zero-exec.
- `event_sourcing/replay.rs` is 74.6% line coverage — the covered part is the happy path.

## What to fix

1. Stop round-tripping the state through `str`. Store `version` and the raw state bytes in a
   binary-safe encoding (length prefix, or a separator that cannot occur in the version field)
   so arbitrary bytes survive.
2. If binary state is to be rejected instead, reject it explicitly with an error and leave the
   stored snapshot **unchanged** — never `+OK` on a discard.
3. Pin the chosen semantics in the test; today the behaviour is neither.

## Acceptance criteria

- [ ] New test drives `ES.SNAPSHOT agg 5 <0xff 0xfe binary>` then `ES.REPLAY agg` and asserts
      either the returned state is byte-identical, or the SNAPSHOT is rejected with an error and
      the previously stored snapshot is unchanged. **Fails today** — today it replies `+OK` and
      returns an empty state.
- [ ] A companion assertion covers the destructive sequence: snapshot a valid state at version N,
      then snapshot binary state at version N+1, then `ES.REPLAY` — the earlier good state must not
      be silently replaced by `""`.
- [ ] At least one of `replay.rs`'s error branches (lines 60-61, 64-67, 71-73, 76) gains a test,
      since the whole family is zero-exec today.

## Test boundary

**3** — needs the real store and command dispatch, nothing from the socket. Not level 4: the
behaviour is command semantics plus a store write, and RESP adds no observation the driver cannot
make.

## Depends on

Decision D1 (home for command-semantics tests — inline units vs a `scenario_commands_*` family
under `shard_driver`) — issue 29, `.scratch/testing-improvements-round2/issues/`. Both command
agents recommend the `shard_driver` family, which is the boundary named above.
