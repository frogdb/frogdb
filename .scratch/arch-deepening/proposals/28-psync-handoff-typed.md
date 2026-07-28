# Proposal 28 — Make the PSYNC connection-takeover a typed dispatch outcome, not a stringly-typed sentinel `Response`

## Summary

PSYNC is the one command that does not produce a data reply — it asks the connection task to hand
its raw socket to the `PrimaryReplicationHandler`. Today that control-flow decision is smuggled
through the *data* path: `PsyncCommand::execute` returns a magic `Response::Array([Simple("PSYNC_HANDOFF"),
Bulk(id), Bulk(offset)])`, the connection layer string-matches the marker bytes and **re-parses the
`replication_id`/`offset` it had already parsed and re-serialized**, stashes them in an untyped
`Option<(String, i64)>`, and a post-loop block reconstructs the handoff. The "is this a takeover?"
decision has no type the compiler can track; the ordering invariant is split across four sites; the
`PSYNC_HANDOFF` marker is a bare byte-literal duplicated between two modules; and the handoff cannot
be unit-tested without standing up `handle_psync`. This proposal replaces the sentinel with a typed
dispatch outcome (`Dispatched::Handoff(PsyncHandoff)` / `FrameAction::Handoff`), so the takeover is
compiler-tracked end-to-end, parsed exactly once, and testable synchronously.

## Problem

`PsyncCommand::execute` has the signature every command has —
`fn execute(&self, ..) -> Result<Response, CommandError>` — but PSYNC's real output is not a
`Response`. Its real output is a *decision*: "stop treating this as a request/reply connection and
give the socket to the primary replication handler." Because the `Command` trait can only return a
`Response`, that decision is encoded as a sentinel array whose first element is the string
`PSYNC_HANDOFF`, and the connection machinery downstream has to *re-discover* the decision by pattern-
matching bytes it just emitted. The command layer serializes `(replication_id, offset)` into
`Response::Bulk`s; the connection layer parses them back out. One value, encoded and decoded across a
seam that exists only because the type system was bypassed.

Concretely the takeover is carried by **four** cooperating sites that must stay in agreement, none of
which the compiler links:

1. `PsyncCommand::execute` — parses `replication_id`/`offset`, then throws the parsed form away and
   re-emits the *raw* arg bytes inside a magic array (`commands/replication.rs`).
2. `Self::extract_psync_handoff` — string-matches `b"PSYNC_HANDOFF"` and **re-parses** the offset
   with `parse::<i64>()` and re-decodes the id from bytes (`connection/lifecycle.rs`).
3. `process_one_command` — stashes the re-parsed tuple into `pending_psync_handoff: Option<(String,
   i64)>` and returns `FrameAction::Break` (`connection.rs`).
4. the post-`run`-loop block — `take()`s the tuple and calls `handler.handle_psync(..)` on the raw
   stream (`connection.rs`).

Add a fifth: `DispatchStage::PsyncIntercept` (`connection/dispatch.rs`) is where PSYNC is actually
recognized and where the `primary_replication_handler`-presence guard first runs — but it hands the
decision *down* into a `Response` rather than *up* as an outcome, forcing sites 2–4 to reconstruct
what site 5 already knew.

This is a **shallow interface** leaking a control decision through a data channel. The `Command`
trait's `Response` return is too thin to carry PSYNC's real output (a socket takeover), so the caller
reaches back in and reconstructs it from a serialized string. **Locality** is poor: to understand one
takeover you read a command executor, a byte-matching extractor in a different module, a frame-action
dispatcher, and a post-loop block — and you must know that the string `"PSYNC_HANDOFF"` in file A must
byte-equal the literal in file B or the connection silently stops handing off. The invariant "PSYNC ⇒
takeover" has no single owner the compiler can hold to completeness; delete the marker match in one
place and nothing fails to compile, it just silently stops replicating.

## Evidence (verified file:line)

All paths under `frogdb-server/crates/`. Every claim below was opened and confirmed.

**Producer — magic sentinel array (`server/src/commands/replication.rs`):**
- `PsyncCommand::execute` (L380) parses `replication_id` via `str::from_utf8(&args[0])` (L385-388)
  and `offset` via `offset_str.parse::<i64>()` (L395-399), then **discards the parsed values** and
  returns the raw args re-wrapped (L411-415):
  ```rust
  Ok(Response::Array(vec![
      Response::Simple(Bytes::from_static(b"PSYNC_HANDOFF")),
      Response::Bulk(Some(args[0].clone())), // replication_id (raw bytes, re-echoed)
      Response::Bulk(Some(args[1].clone())), // offset          (raw bytes, re-echoed)
  ]))
  ```
- `CommandSpec` (L362-376) marks PSYNC `ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Replication)`
  — the strategy that per `core/src/command.rs:177-` means "requires connection takeover" — so the
  dispatcher already treats PSYNC specially before `execute` ever runs.

**Consumer — string match + re-parse (`server/src/connection/lifecycle.rs`):**
- `extract_psync_handoff(responses: &[Response]) -> Option<(String, i64)>` (L226-255):
  string-matches `marker.as_ref() == b"PSYNC_HANDOFF"` (L236), re-decodes the id via
  `String::from_utf8_lossy` (L240), and **re-parses the offset** with `.parse::<i64>()` (L246) — the
  same offset that was already parsed at `replication.rs:395` and validated there.

**Stash + break (`server/src/connection.rs`):**
- Field: `pending_psync_handoff: Option<(String, i64)>` (L173), doc-commented "Set when PSYNC command
  returns PSYNC_HANDOFF, processed after the loop."
- `process_one_command(..) -> FrameAction` (L289-291) calls `route_and_execute_with_transaction`
  (L418/L422), then `Self::extract_psync_handoff(&responses)` (L427); on `Some` it sets
  `self.pending_psync_handoff = Some(handoff_params)` (L435) and returns `FrameAction::Break` (L436).
- `FrameAction` enum (L194-201) has only `Continue` / `Break` / `SkipResponse` — **no typed handoff
  variant.** The `run` loop collapses it to a `bool`: `FrameAction::Break => should_break = true`
  (L672), so even the fact that this particular break is a handoff is erased at the loop boundary.

**Post-loop takeover (`server/src/connection.rs`):**
- L713 `if let Some((replication_id, offset)) = self.pending_psync_handoff.take()`; L725
  `self.framed.into_inner()` extracts the stream; L730 `.into_boxed()`; L731-733
  `handler.handle_psync(boxed_stream, self.state.addr, &replication_id, offset).await`; L765
  `return Ok(())` skipping normal cleanup.

**Handoff target (`replication/src/primary/mod.rs`):**
- `pub async fn handle_psync(self: &Arc<Self>, stream: BoxedStream, addr: SocketAddr,
  replication_id: &str, offset: i64) -> io::Result<()>` (L156-162) — already takes typed `&str` +
  `i64`, i.e. the string round-trip exists *only* to cross the command→connection seam and is undone
  immediately at the destination.

**Dispatch recognition + a redundant, divergent presence guard (`server/src/connection/dispatch.rs`):**
- `DispatchStage::PsyncIntercept` (L445-458): if `cmd_name == "PSYNC"`, it checks
  `self.cluster.primary_replication_handler.is_none()` and returns a **client error**
  `"ERR PSYNC not supported - server is not running as primary"` (L448-451); otherwise it calls
  `route_and_execute(cmd, cmd_name)` (L455) — which runs `execute` and produces the sentinel array —
  and returns it as `StageOutcome::ShortCircuit(vec![..])`.
- **Additional finding (not in the candidate):** the same presence check is duplicated at the handoff
  site with *different* behavior — `connection.rs:723` `if let Some(handler) = &..primary_replication_handler`
  else (L753-758) logs a silent `warn!("PSYNC handoff requested but no primary replication handler
  available")` and drops the connection. So "am I a primary?" is answered twice, in two vocabularies
  (client error vs. silent warn), separated by the whole read loop. A typed outcome lets the intercept
  own the gate once and makes the post-loop `else` provably dead.

**Return-type ripple is tiny.** `route_and_execute_with_transaction` returns `Vec<Response>`
(`dispatch.rs:280-292`) and has **exactly two real call sites**, both in `process_one_command`
(`connection.rs:418`, `:422`, identical but for a tracing span). Verified via grep: no other caller.

**No unit test pins the handoff seam.** `extract_psync_handoff` has zero test callers (grep: only its
definition and the one production call site). Every PSYNC test is a full socket integration test in
`server/tests/integration_replication.rs` — `test_psync_initial_request` (L93), `test_psync_with_replication_id`
(L805), `test_psync_invalid_args` (L867), the CONTINUE/FULLRESYNC cases (L1322, L1421) — each boots a
server and asserts "does not error" over the wire. There is no way today to assert "PSYNC yields a
handoff carrying *this* id and *this* offset" without a live socket and a running primary handler.

### Discrepancies with the exploration candidate

- **"string constant duplicated across crates" — corrected to *across modules within the `server`
  crate*.** Both the producer (`server/src/commands/replication.rs:412`) and the consumer
  (`server/src/connection/lifecycle.rs:236`) live in the **same** crate (`frogdb-server`'s `server`
  crate). The duplication is real and worth removing, but it is a two-module, one-crate duplication,
  not a cross-crate one. This slightly *lowers* the coupling severity (no crate-boundary contract is
  implicated) but does not change the premise.
- **Line offsets:** the return array is at `replication.rs:411-415` (candidate said 412-416); the
  other citations (`lifecycle.rs:226-255`, `connection.rs:427-436`, `:713-758`) are exact.
- **Missed finding:** the candidate did not note the duplicated/divergent `primary_replication_handler`
  presence check (`dispatch.rs:448` vs `connection.rs:723/753`); it strengthens the case and is folded
  into the design below.

Core premise verified and intact → **proceed.**

## Proposed design (Rust interface sketch — types/signatures only)

The decision "hand this socket to the primary replication handler" becomes a **first-class value**
produced once, at the single site that recognizes PSYNC, and carried by the existing outcome enums the
compiler already threads. No string, no re-parse, no marker constant.

### 1. A typed handoff value, parsed once

```rust
// server/src/commands/replication.rs (or a small `psync` submodule)

/// A validated request to hand a connection's raw socket to the
/// `PrimaryReplicationHandler`. The sole product of PSYNC dispatch.
pub(crate) struct PsyncHandoff {
    pub replication_id: String,
    pub offset: i64,
}

impl PsyncHandoff {
    /// The single PSYNC arg parse. Arg errors surface as `CommandError`
    /// (returned to the client exactly as today via the error `Response`).
    pub(crate) fn from_args(args: &[Bytes]) -> Result<Self, CommandError>;
}
```

This is the *only* place `replication_id`/`offset` are parsed. `handle_psync` already consumes
`&str` + `i64`, so the value flows target-shaped from parse to takeover with no round-trip.

### 2. A typed dispatch outcome, not a sentinel `Response`

The pre-dispatch stage machine gains a control outcome distinct from data:

```rust
// server/src/connection/dispatch.rs
pub(crate) enum StageOutcome {
    Continue,
    ShortCircuit(Vec<Response>),
    Handoff(PsyncHandoff),          // NEW — control, not data
}

/// What a fully-dispatched frame produced: either replies to write, or a
/// connection takeover to perform. Replaces the bare `Vec<Response>`.
pub(crate) enum Dispatched {
    Responses(Vec<Response>),
    Handoff(PsyncHandoff),
}

impl ConnectionHandler {
    pub(crate) async fn route_and_execute_with_transaction(
        &mut self,
        cmd: &Arc<ParsedCommand>,
        cmd_name: &str,
    ) -> Dispatched;   // was `-> Vec<Response>`
}
```

`DispatchStage::PsyncIntercept` now owns the whole PSYNC decision in one place: presence-gate →
`PsyncHandoff::from_args` → `StageOutcome::Handoff(..)` (or `ShortCircuit(vec![error])` on
not-primary / bad args). It no longer calls `route_and_execute`, so **`PsyncCommand::execute` and the
`PSYNC_HANDOFF` sentinel array are deleted**; the `CommandSpec`/registry entry stays for arity, flags,
and `COMMAND DOCS`, with `execute` reduced to a **non-panicking** internal-error guard that returns
`Err(CommandError::Internal(..))` — *not* `unreachable!()` — because `PsyncCommand` remains a
registered shard `Command`: a genuine panic here would take down the shard worker if a future
dispatch-order change ever violated the "PSYNC is always intercepted before `Execute`" invariant, so
we degrade gracefully with an internal-error `Response` instead.

### 3. A typed frame action; delete the string extractor

```rust
// server/src/connection.rs
enum FrameAction {
    Continue,
    Break,
    SkipResponse,
    Handoff(PsyncHandoff),   // NEW — carries the takeover out of `process_one_command`
}
```

`process_one_command` matches `Dispatched`: `Responses(v)` runs today's metrics/flush tail;
`Handoff(h)` returns `FrameAction::Handoff(h)`. The `run` loop, which today collapses everything to
`should_break: bool`, learns one arm that **stashes the handoff and sets `should_break`, then falls
through to the shared flush** — `FrameAction::Handoff(h) => { self.pending_psync_handoff = Some(h);
should_break = true; }` — rather than breaking directly out of the `select` loop. This matches the
existing structure at both the first-command match (`connection.rs:671-674`) and the drain match
(`:689-696`), where every arm sets `should_break` and falls through to the shared flush (`:700`)
before breaking. Preserving that pattern matters: in a pipelined batch where earlier frames buffered
replies, the PSYNC handoff must still flush those prior responses over the wire before the socket
takeover — a direct `break` would drop them. The field is retyped `Option<PsyncHandoff>`.
`Self::extract_psync_handoff` (the byte-matcher + re-parse) is **deleted outright**, and with it the
second `b"PSYNC_HANDOFF"` literal.

### 4. Post-loop takeover reads a typed value; dead guard removed

```rust
if let Some(PsyncHandoff { replication_id, offset }) = self.pending_psync_handoff.take() {
    // handler presence already guaranteed by PsyncIntercept — this becomes
    // a debug_assert / expect rather than a silent-warn `else` branch.
    ...
    handler.handle_psync(boxed_stream, self.state.addr, &replication_id, offset).await
}
```

Because `StageOutcome::Handoff` is only produced *after* the `primary_replication_handler.is_none()`
gate, the post-loop `else` that silently warns (`connection.rs:753-758`) is now unreachable and
collapses to an assertion — the presence check lives in exactly one place.

Net: PSYNC's output is a `PsyncHandoff`, produced once, carried by `StageOutcome::Handoff` →
`Dispatched::Handoff` → `FrameAction::Handoff`, each a variant the compiler forces every match to
consider. No `Response::Array` sentinel, no `PSYNC_HANDOFF` literal (0 remaining), no double parse,
no re-decode.

## Migration plan (ordered)

1. **Add `PsyncHandoff` + `from_args`** in `commands/replication.rs`, moving the parse logic out of
   `execute` verbatim (utf8 id, `parse::<i64>` offset, same `CommandError`s).
2. **Add `StageOutcome::Handoff(PsyncHandoff)`** and the `Dispatched` enum in `dispatch.rs`. Change
   `route_and_execute_with_transaction` to return `Dispatched`; the driver loop returns
   `Dispatched::Handoff` when a stage yields `StageOutcome::Handoff`, else `Dispatched::Responses`.
   The `unreachable!` tail is unchanged.
3. **Rewrite `DispatchStage::PsyncIntercept`** to: presence-gate (unchanged error), then
   `PsyncHandoff::from_args(&cmd.args)` → `StageOutcome::Handoff` on `Ok`, `ShortCircuit(vec![err])`
   on `Err`. Remove the `route_and_execute` call for PSYNC.
4. **Reduce `PsyncCommand::execute`** to a non-panicking internal-error guard
   (`Err(CommandError::Internal(..))`, kept only so the registry entry has an executor — see the
   panic caveat in design §2); delete the `Response::Array([PSYNC_HANDOFF, ..])` construction —
   **first `PSYNC_HANDOFF` literal gone.**
5. **Add `FrameAction::Handoff(PsyncHandoff)`**; retype `pending_psync_handoff: Option<PsyncHandoff>`.
   Update `process_one_command` to match `Dispatched` (both call sites at `connection.rs:418/422`).
6. **Delete `extract_psync_handoff`** (`lifecycle.rs:226-255`) — **second `PSYNC_HANDOFF` literal and
   the double-parse gone.**
7. **Update the `run` loop** (`connection.rs:671-674`) to handle `FrameAction::Handoff` by stashing
   the handoff and setting `should_break = true` (**not** breaking directly), so it falls through to
   the shared flush at `:700` and any buffered pipelined replies are written before the takeover; the
   drain loop (L689-696) likewise.
8. **Simplify the post-loop block** (`connection.rs:713-758`) to destructure `PsyncHandoff` and turn
   the no-handler `else` into an assertion.
9. `just check frogdb-server` — the compiler drives the rest; every `match` on the three enums errors
   until the `Handoff` arm is added. `just test frogdb-server` (or `tb-run` for the full replication
   suite).

The change is confined to the `server` crate; `replication`'s `handle_psync` and `BoxedStream` are
untouched (they already take the typed form).

## Test plan

- **New synchronous unit tests (the "test win"):**
  - `PsyncHandoff::from_args(&[b"repl-abc", b"12345"])` → `Ok(PsyncHandoff { replication_id:
    "repl-abc", offset: 12345 })`; assert id and offset directly, no socket, no `handle_psync`.
  - Arg-error parity: one-arg → `WrongArity`; non-utf8 id → `InvalidArgument`; non-numeric offset →
    `InvalidArgument` — mirroring the branches at `replication.rs:381-399` and the wire assertions in
    `test_psync_invalid_args`. **Caveat on the `WrongArity` branch:** it is *dead in the live dispatch
    path* — the `Arity` stage (`PRE_DISPATCH_ORDER` index 6) validates PSYNC's `Fixed(2)` arity before
    `PsyncIntercept` (index 9) is ever reached, so `from_args` never sees a wrong arg count in
    production. Keeping the arity check inside `from_args` and unit-testing it in isolation is fine as
    defensive validation, but this assertion tests `from_args` as a standalone function, **not**
    production wire behavior (the wire-level wrong-arity error is already covered by `Arity` and by
    `test_psync_invalid_args`). The offset/utf8 branches, by contrast, *are* the live path — they are
    what the intercept actually reaches after arity has passed.
  - Dispatch outcome: a PSYNC frame through the stage machine on a handler-present connection yields
    `Dispatched::Handoff(h)` with the parsed id/offset (no live primary needed); on a handler-absent
    connection yields the `"not running as primary"` error `Response` — pinning the single presence
    gate.
- **Regression guards:** existing integration tests in `server/tests/integration_replication.rs`
  (`test_psync_initial_request`, `test_psync_with_replication_id`, `test_psync_invalid_args`, the
  CONTINUE/FULLRESYNC cases at L1322/L1421) and `integration_cluster.rs` must stay green — they prove
  the end-to-end takeover still hands the socket over and produces FULLRESYNC/CONTINUE.
- **Exhaustiveness:** adding the `Handoff` variant to each enum makes the compiler enumerate every
  match — the "silently forgot to handle handoff" failure class the current string match permits
  becomes impossible.

## Risks & alternatives

- **`route_and_execute_with_transaction` return-type change.** It moves from `Vec<Response>` to
  `Dispatched`. Verified only two real callers (both in `process_one_command`), so the ripple is
  small and compiler-driven. This is the one public-ish signature change.
- **Alternative — keep `Vec<Response>`, stash typed inside the stage.** `PsyncIntercept` could set
  `self.pending_psync_handoff = Some(h)` as a side effect and `ShortCircuit(vec![])`, leaving the
  return type alone; `process_one_command` then checks `self.pending_psync_handoff.is_some()`. This
  still deletes the sentinel array, the marker string, and the double-parse, but reintroduces a
  side-effecting stash and a `bool`-ish check instead of a carried value — less compiler-forced.
  Offered as the lower-blast-radius fallback if the `Dispatched` return type is judged too invasive;
  the primary design is preferred because it makes the takeover a *value*, not a *side effect*.
- **`PsyncCommand` becomes a near-empty registry shell.** Its `execute` is dead once the intercept
  owns parsing. Keeping the `CommandSpec` preserves `COMMAND DOCS`/arity/flags; the residual executor
  documents the invariant "PSYNC is always intercepted." **It must return
  `Err(CommandError::Internal(..))`, not `unreachable!()`**: `PsyncCommand` is still registered as a
  shard `Command`, so a panic in `execute` would crash the shard worker if a future dispatch-order
  regression ever routed a PSYNC to `Execute`. A returned internal error degrades gracefully (the
  connection sees an error `Response`) while still flagging the broken invariant in logs/metrics.
  Acceptable; the alternative (dropping the registry entry) would lose command introspection.
- **MULTI/EXEC path — the one route that bypasses `PsyncIntercept`, and why it is safe.** The claim
  "PSYNC is always intercepted before `Execute`" is not universally true at the dispatch-stage level:
  `TransactionQueue` (`PRE_DISPATCH_ORDER` index 5) runs *before* `PsyncIntercept` (index 9), so a
  PSYNC issued inside a `MULTI` block is queued and never reaches the intercept. At `EXEC`,
  `execute_connection_level_in_transaction` (`transaction.rs:428-434`) deliberately short-circuits a
  queued PSYNC to `Response::ok()` **without** calling `PsyncCommand::execute` — `as_connection()`
  returns `None` because PSYNC is a shard `Command`, not a `CommandImpl::Connection`. So this path
  touches neither the intercept nor `execute`, and its `+OK` reply must be preserved. This proposal
  leaves that path untouched: it changes only the non-transactional intercept route and the residual
  `execute` guard, so the queued-PSYNC-inside-MULTI `+OK` behavior is unchanged, and `execute` remains
  genuinely unreachable in the live dispatch paths (intercept handles the direct case, the MULTI path
  short-circuits before `execute`). A regression test asserting `MULTI; PSYNC; EXEC` still yields `+OK`
  is worth adding to guard this.
- **turmoil.** The `#[cfg(feature = "turmoil")]` branch that cannot perform the handoff
  (`connection.rs:743-752`) is unchanged — it still receives a typed `PsyncHandoff` and logs the
  unsupported path. No behavioral change under simulation.
- **ADR alignment.** PSYNC/replication is the **data path**, which per `frogdb-server/docs/adr/0001-raft-cluster-metadata.md`
  never goes through Raft — this proposal touches only how the connection task *recognizes and routes*
  a takeover on that data path and does not involve the Raft Metadata Plane, Config Epoch, or the
  Replication Backlog semantics (`handle_psync`'s partial/full decision is untouched). Vocabulary per
  `frogdb-server/CONTEXT.md`: Primary/Replica, PSYNC/FULLRESYNC, Replication Backlog, CommandSpec.
- **Redis/Valkey/DragonflyDB parity.** In Redis (`replication.c`) `syncCommand`/`psyncCommand` do not
  return a RESP reply at all for the success path — they mutate the client into a replica
  (`CLIENT_SLAVE`/`replicaState`), write the `+FULLRESYNC`/`+CONTINUE` preamble directly to the
  socket, and the event loop stops treating the fd as a normal command client. That is exactly a
  *typed state transition on the connection*, not a data reply threaded back through the command
  dispatcher — the proposed `Dispatched::Handoff` is the Rust-typed analogue of Redis's client-flag
  transition, and closer to upstream intent than the current sentinel array. DragonflyDB similarly
  dedicates a replication flow object per replica connection rather than round-tripping the decision
  through a reply.

## Effort

**S–M.** One crate (`server`), ~6 files, all compiler-guided. The mechanical parts (new
`PsyncHandoff`, three enum variants, delete `extract_psync_handoff` + the sentinel array, retype one
field) are S; the M comes from the `route_and_execute_with_transaction` return-type change threading
`Dispatched` through `process_one_command`'s two call sites and the `run`/drain loops, plus porting
the arg-error parity into synchronous unit tests. No async, stream, or `handle_psync` changes; the
replication protocol on the wire is byte-for-byte unchanged.

## Related

None.

## Adversarial review

**Verdict: CONFIRMED.** The premise was verified end-to-end against the actual code; every cited
`file:line` matched (with the proposal's own line-offset and cross-module-not-cross-crate corrections
already applied). The five-site smuggling of the takeover decision through a `Response::Array`
sentinel is real, the typed-outcome design is feasible under the borrow checker, the crate boundary is
untouched, the wire protocol is byte-for-byte identical, and the "deletion test" passes (removes
`extract_psync_handoff`, both `PSYNC_HANDOFF` literals, the double-parse, and the untyped
`Option<(String, i64)>`, and makes the takeover unit-testable without a live primary). The reviewer
found **no critical or major issues** and recommended proceeding as written, incorporating four minor
precision/documentation notes. All four are resolved below.

1. **(minor) MULTI/EXEC path unmentioned — the one route that could falsify "PSYNC is always
   intercepted before `Execute`."** *Resolved.* Added a Risks bullet: `TransactionQueue`
   (`PRE_DISPATCH_ORDER` index 5) runs before `PsyncIntercept` (index 9), so a PSYNC inside `MULTI` is
   queued and bypasses the intercept; at `EXEC`, `execute_connection_level_in_transaction`
   (`transaction.rs:428-434`) short-circuits it to `Response::ok()` *without* calling
   `PsyncCommand::execute` (`as_connection()` returns `None` since PSYNC is a shard `Command`). The
   proposal now documents that this path touches neither the intercept nor `execute`, that its `+OK`
   behavior must be preserved (it is untouched by this change), and recommends a `MULTI; PSYNC; EXEC`
   regression test. This confirms rather than refutes the "execute is unreachable" claim.

2. **(minor) `WrongArity` unit-test branch is dead in the live dispatch path.** *Resolved.* The Test
   plan now flags that the `Arity` stage (index 6) validates PSYNC's `Fixed(2)` arity before
   `PsyncIntercept` (index 9), so `from_args` never sees a wrong arg count in production. The
   `WrongArity` assertion is explicitly framed as testing `from_args` as a standalone function (fine as
   defensive validation), **not** as exercising production wire behavior; the offset/utf8 branches are
   noted as the ones that actually are on the live path.

3. **(minor) "unreachable executor" wording ambiguous between `unreachable!()` panic and a returned
   internal error.** *Resolved.* Design §2, migration step 4, and the Risks bullet now specify the
   residual `execute` returns `Err(CommandError::Internal(..))` — explicitly **not** `unreachable!()` —
   because `PsyncCommand` stays a registered shard `Command`, so a panic would crash the shard worker
   if a future dispatch-order regression violated the invariant. The internal-error `Response` degrades
   gracefully while still flagging the broken invariant.

4. **(minor) Run-loop sketch's direct `break` diverges from the `should_break` + shared-flush
   pattern.** *Resolved.* Design §3 and migration step 7 now have the `FrameAction::Handoff` arm stash
   the handoff and set `should_break = true`, falling through to the shared flush at
   `connection.rs:700` (matching the existing first-command and drain matches), rather than breaking
   directly out of the `select` loop — so buffered pipelined replies are flushed over the wire before
   the socket takeover. Noted as an implementation caution; not a design flaw.

No claims were disputed; the design is unchanged in substance. Recommendation: **proceed as written**
with the four notes incorporated.
