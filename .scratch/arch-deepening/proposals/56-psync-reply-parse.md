# Proposal 56 — `PsyncReply`: the replica's half of the PSYNC grammar, parsed once

Round 38 · lane: replication · covers exploration-lane candidate **RC4** · effort **M**
(step 1) + **S** (step 2) · **LOCKED** area (`frogdb-replication`, mutation gate 0.85,
ADR 0004)

Verified at `ade5ab80` (lane HEAD `08c143d6` plus sibling proposal commits). Every line
citation below was read at that sha, not inherited from the candidate brief.

## Summary

`ReplicaConnection::psync` (`replica/connection.rs:224-341`, 118 lines, one production caller)
is the replica's entire understanding of what a primary said. It hand-rolls three different
grammars — the `+FULLRESYNC` / `+CONTINUE` / `-ERR` reply line, the `$<MARKER>` payload
prelude, and the count line — and interleaves them with four state mutations (`reset_to(0)`,
`set_state(Syncing)`, `shift_replication_id`, `set_state(Streaming)`) and three separate socket
reads. The **interface** is `io::Result<SyncType>`; the **implementation** is a wire parser, a
history-adoption policy, a state machine transition and a socket cursor, and a caller must know
all four to use it.

The proposal is to extract the grammar into a pure parse module — `replica/psync_reply.rs`,
sibling of `payload_reader.rs` — leaving `psync` as the I/O half that reads a line, asks the
parser what it means, and performs the effects. This is not a new idea in this crate: the
**primary** side of the same protocol already has exactly this shape
(`PartialSyncReplay::handle_partial_sync_request`, `primary/replay.rs:350`, documented as *"A
pure decision over `(state, req_offset, current)` … performs no I/O"*, returning a named
`ReplayDecision`). The producer of the reply is a pure function with a named outcome type; the
consumer is 118 lines of inline async. That asymmetry is the whole candidate.

**The work is two steps, and they must not be one commit.**

* **Step 1 — the move.** A byte-identical extraction: the pure parser reproduces today's
  accepted language *exactly*, `starts_with` and `>=` arity included, warts intact. No
  behaviour change, so no failure-mode row has to move first. This is the whole of the
  architecture win.
* **Step 2 — the grammar tightening.** Closing latent items 1 and 2 below (delimiter check,
  exact arity) **changes what the replica accepts**. In a locked crate that is a spec-first
  edit, not a refactor rider: it needs the failure-mode row the [spec gap](#spec-gap-to-file--the-home-for-step-2)
  section asks for, then a failing test, then the fix. It is small (two conditions) and lands
  on top of step 1 in an hour, but it is its own step with its own row.

An earlier draft of this proposal claimed "pure extraction, no behaviour change" *and* closed
both latent items in the same change. Those are contradictory: `+CONTINUEX abc` is a
`PartialSync` today (`connection.rs:315-332`) and would become an error, and
`+FULLRESYNC id 5 extra` is accepted today (`:241`) and would be rejected. No `Unkept` row
covers either input (`:1398-1435`), and no spec row constrains them — FM-REPLICATION-013's
*"exactly one of two first-line replies"* (spec `:327`) specifies what the **producer** emits,
not what the consumer must refuse. The two-step split is the fix.

**This proposal claims no live bug.** Two latent items and one verified test-coverage gap are
documented below, each with the reason it is unreachable or harmless today. The coverage gap is
written up as an independently-landable hotfix that needs no refactor.

**It is also, verbatim, an already-ruled campaign item.** `.scratch/replication-correctness/PRD.md:538-541`
rules D2 tier (ii): *"also split the PSYNC arm selection out of `ReplicaConnection::psync` into
a pure function beside `handle_partial_sync_request`, which is a genuine symmetry win (the
primary side already has it, the replica side does not)"* — authorized 2026-08-10, to land as a
stepping stone before the (iii) restructure. See [Boundaries](#risks--scope-boundaries-vs-sibling-proposals);
this proposal should be executed **as** that issue, not alongside it.

## Files involved

Package names are `frogdb-replication` / `frogdb-server`; the tree lays them out under
`frogdb-server/crates/`.

| path | lines | what 56 touches |
|---|---:|---|
| `frogdb-server/crates/replication/src/replica/connection.rs` | 1884 | **the change.** `psync` `:224-341`; `read_resp_line` `:34-52`; `psync_request_args` `:62-68`; `SyncType` `:103-107` (doc `:93-101`, derive `:102`); `set_state` `:159-163`; `read_ok_response` `:559-574` (same simple-line shape, see Problem). Tests: `:589-601`, `:608-659`, `:667-696` (`psync_against` helper), `:703-721`, `:727-757`, `:763-784`, `:1304-1363`, `:1381-1512` |
| `frogdb-server/crates/replication/src/fullsync.rs` | 1164 | the second and third realizations of the same grammar: `CHECKPOINT_MARKER` `:110`, `SNAPSHOT_MARKER` `:122`, `write_prelude` `:167-172`, `write_snapshot_prelude` `:179-184`, `write_marked_prelude` `:186-194`, `parse_file_count` `:230-244`, `read_prelude` `:251-269`. Tests `:823-864` (golden; tag `:823`, fn `:831`), `:866-887`, `:889-917`, `:964-979` |
| `frogdb-server/crates/replication/src/replica/mod.rs` | 560 | the only production caller: `conn.psync()` `:510`, the `SyncType` match `:511-519`; the error classifier this feeds, `:451-475`; the `sync_refusal` accessor INFO renders from, `:297-311` |
| `frogdb-server/crates/replication/src/primary/replay.rs` | 861 | **the model, unchanged.** `ReplayDecision` `:150`, `FullResyncReason` `:181`, `handle_partial_sync_request` `:350-370`, `can_replay` `:377-` |
| `frogdb-server/crates/replication/src/replica_session.rs` | 4574 | **not touched.** The producer: `+CONTINUE` at `:738`, `+FULLRESYNC` at `:790`. Proposal 53's file and D2 (iii)'s target |
| `frogdb-server/crates/replication/src/state.rs` | 958 | read-only: `shift_replication_id` `:339`, `adopt_replication_history` `:365`, `window_contains` `:386` |
| `frogdb-server/crates/replication/src/replica/payload_reader.rs` | 133 | the sibling module the new one is shaped after; untouched |
| `frogdb-server/crates/server/tests/integration_replication.rs` | — | `test_psync_is_refused_for_an_incompatible_replica_version` `:125` — one of FM-REPLICATION-064's fifteen forcing tests, and the only one **outside** the mutated crate |
| `.scratch/hardening/specs/replication-failure-modes.md` | 1571 | FM-REPLICATION-001 `:95-122`, -005 `:160-172`, -013 `:322-346`, -027 `:593-609`, -035 `:763-780`, -045 `:952-962`, -064 `:1494-1512` |

## Problem (verified evidence)

### One method, four responsibilities, in one interleaved chain

`psync` (`connection.rs:224-341`), in wire order:

| lines | what happens | kind |
|---|---|---|
| `:229-231` | read live offset + replication id, build `(repl_id, offset)` via `psync_request_args` | decision (already pure, already extracted) |
| `:232-236` | serialize and write the `PSYNC` command | I/O |
| `:237-238` | `read_resp_line` (byte-at-a-time, deliberately unbuffered) + `trim` | I/O |
| `:239-241` | `starts_with("+FULLRESYNC")`, `split_whitespace`, `parts.len() >= 3` | **grammar** |
| `:242-245` | `parts[1]` → id, `parts[2]` → `u64` or `"invalid offset in FULLRESYNC"` | **grammar** |
| `:246-265` | 20-line comment explaining why neither parsed half is adopted | the interface, written as prose |
| `:266-274` | `offsets.reset_to(0)`, error `"replication stream retired during FULLRESYNC"` | **state mutation** |
| `:275-276` | log, `set_state(Syncing)` | **state mutation** |
| `:277-284` | second socket read: marker line, `$` prefix check | I/O + **grammar** |
| `:288-298` | marker → `SyncType`, third socket read (count) via `CheckpointStreamCodec::parse_file_count` | **grammar** + I/O |
| `:299-308` | `"unsupported FULLRESYNC payload marker: {marker}"` | **grammar** |
| `:309-314` | `"malformed FULLRESYNC response"` | **grammar** |
| `:315-317` | `starts_with("+CONTINUE")`, `split_whitespace`, `parts.len() >= 2` | **grammar** |
| `:318-327` | `resumed_at = offsets.current()`, `state.write()`, conditional `shift_replication_id` | **state mutation** |
| `:330-332` | `set_state(Streaming)` | **state mutation** |
| `:333-334` | `strip_prefix('-')` → `"PSYNC error: {rest}"` | **grammar** |
| `:335-339` | `"unexpected PSYNC response: {line}"` | **grammar** |

**Everything a caller must know** to use `psync` correctly, none of which its signature states:

1. It **writes** to the socket before it reads.
2. It leaves the socket **at a different position per arm** — after the reply line on
   `PartialSync`, after the *prelude* on either full-sync arm — which is precisely why
   `receive_checkpoint` / `receive_snapshot` start at a file header rather than a marker.
3. It mutates `ReplicationState` (`:266`, `:324-327`), the shared offset atomic (`:266`), and
   `connection_state` + `link_up` together (`:276`, `:330`).
4. It has **nine distinguishable failure modes** collapsed into one `io::Error` — seven error
   strings spelled in its own body (`:244`, `:272`, `:282`, `:306`, `:312`, `:334`, `:338`) plus
   two propagated from callees (`read_resp_line`'s `"connection closed while reading RESP
   line"` / non-UTF-8 at `:40-51`, and `parse_file_count`'s two at `fullsync.rs:231-242`).
5. Those failures split into two classes — *before* the rewind at `:266` (the node's heads are
   untouched, the reconnect resumes from its live head) and *after* it (the heads are at 0, the
   reconnect asks `PSYNC ? -1`).

Point 5 is the sharpest evidence, in a narrower form than an earlier draft claimed. The
*post*-rewind half **is** written down in production: FM-REPLICATION-001's Invariant (spec
`:102`) states it — *"it rewinds the offset to 0 and leaves the replication id and failover
window alone"* — and `connection.rs:258-265` states it in prose at the branch. What is written
down **nowhere outside a test** is the *pre*-rewind class: that a failure reached before
`:266` leaves the live head intact, so the next `PSYNC` resumes from it rather than asking for
a full resync. Its only statement is the test's own doc (`connection.rs:1377-1380`) and the
`next_request` column of that test's table (`:1395`, rows at `:1398-1435`). The test knows
there are two error classes; the code says `io::Result<SyncType>`.

### The same grammar realized three times, and the three already disagree

| realization | site | accepts |
|---|---|---|
| production reader | `connection.rs:288-308` (inline) | `$FROGDB_CHECKPOINT` **and** `$FROGDB_SNAPSHOT` |
| codec reader | `fullsync.rs:251-269` `read_prelude` | `$FROGDB_CHECKPOINT` **only** (`:260-265`) |
| codec writers | `fullsync.rs:167-194` | both, via `write_marked_prelude` |

`read_prelude` has **no production caller** — `grep` finds it only at `fullsync.rs:709`, `:777`,
`:879` (round-trip tests) and `:968`, `:975` (its own negative test). So the codec that
FM-REPLICATION-035 describes as *"owns both directions of the grammar, so an encoder change
that is not matched by its inverse fails the round-trip"* (spec `:770`) has **two prelude
encoders and one decoder that rejects one of them**. Consequences, both verified:

* `write_snapshot_prelude` (`fullsync.rs:179`) is called from production
  (`replica_session.rs:1050`) and from one connection test fixture (`connection.rs:1767`), and
  is exercised by **no** round-trip and **no** golden test — `test_checkpoint_codec_golden_bytes`
  (`fullsync.rs:823-864`) pins `$FROGDB_CHECKPOINT\r\n` only, and all three round-trip tests
  call `write_prelude`. The only thing that reads a `$FROGDB_SNAPSHOT` prelude anywhere in the
  workspace is the hand-rolled branch at `connection.rs:289`.
* The gap is precisely scoped: **the snapshot encoder has no codec-owned inverse and no golden
  pinning its bytes.** It is *not* that FM-REPLICATION-035 claims coverage it does not deliver.
  That row's Invariant ends with an explicit disclaimer (spec `:770`): *"Markers themselves are
  FM-REPLICATION-001's; this row owns everything downstream of them."* Marker **rejection** is
  therefore FM-001's, whose Invariant (`:102`) already says *"`psync` rejects any marker it
  cannot install"* and whose Forced-by (`:104`) already names
  `psync_rejects_a_payload_that_carries_no_dataset`. Any new assertion about *refusing* a
  marker belongs on FM-001's Forced-by list; the round-trip and golden-bytes assertions this
  proposal adds belong on FM-035's, which owns the byte shape `$<MARKER>\r\n<count>\r\n` it
  already specifies (`:768`).

And the doc comment that justifies keeping the two apart is stale. `fullsync.rs:225-229` and
`:246-250` say the split exists because *"that reader must first distinguish `$FROGDB_CHECKPOINT`
from a plain `$<rdb_size>` RDB"* and *"so the RDB-vs-checkpoint decision is not entangled with
the envelope"*. There is no plain-RDB arm any more: `SyncType`'s own doc comment
(`connection.rs:93-101`) says *"There is deliberately no 'plain RDB' arm … the payload that
expressed it no longer exists on either side"* (issue 67). The marker set is closed at two, both
written by the same codec — so the stated reason for the second realization no longer holds.

### The asymmetry with the primary side

`primary/replay.rs` has, for the *producing* half of this exact exchange:

* `ReplayDecision` (`:150`) — a two-variant outcome type, so "neither arm" is unrepresentable;
* `FullResyncReason` (`:181`) — a five-way classifier for *why*;
* `can_replay` (`:377-`) — an ordered decision with no fallthrough;
* `handle_partial_sync_request` (`:350-370`) — *"The single entry point. A pure decision over
  `(state, req_offset, current)` plus the backlog's current contents; performs no I/O."*

FM-REPLICATION-013 is written **against those types** ("`ReplayDecision` is a two-variant enum,
so 'neither arm' is not representable", spec `:329`). The replica side has no outcome type at
all: `SyncType` (`connection.rs:103-107`) is a routing token telling the caller *what to read
next*, not a statement of what the primary said. There is no name in the codebase for "the
primary refused me", "the primary granted a continue under a new id", or "the primary said
something I do not understand" — only three `io::Error` strings.

### Why this is shallow, in the vocabulary

* **Interface vs implementation.** `psync`'s **interface is nearly as complex as its
  implementation**: the interface a caller must actually hold is the five-item list above, and
  the declared one is a single `Result`. That gap is the definition of shallow.
* **Deletion test.** Delete `SyncType` and inline the marker check into `connect_and_sync`:
  nothing else in the tree breaks. That is the tell — `SyncType` is a token, not a module. The
  unit that *would* fail the deletion test is the one that owns reply grammar + prelude grammar
  + the effect ordering between them, and that unit does not exist.
* **The interface is the test surface.** `psync` is the only place the replica interprets the
  primary's control plane, and there is no way to ask it a question without a socket: the
  grammar cannot be exercised, reused, or model-checked except through `tokio::io::duplex`.
  (`parse_reply_line` is a pure function, not a seam in the ADR-0004 sense — the point is the
  test surface, not dependency injection.) The D2 model needs exactly this decision and today
  would have to re-derive it.
* **Locality.** A change to the wire grammar today touches `psync`, `read_prelude`,
  `write_prelude`, `write_snapshot_prelude` and their tests, in two files, with the three
  readers/writers free to drift — which they already have.

### Two latent items, and one non-bug worth naming

1. **Prefix acceptance without a delimiter check (latent).** `:239` and `:315` test
   `starts_with`, so `+CONTINUEX abc` parses as a `+CONTINUE` carrying id `abc`, and
   `+FULLRESYNCX a 1` as a full resync. Unreachable from any peer this code can meet: the
   in-tree producer writes exactly `+CONTINUE {id}\r\n` (`replica_session.rs:738`) and
   `+FULLRESYNC {id} {offset}\r\n` (`:790`), and Redis writes the same two forms. It requires a
   peer that speaks neither protocol. **Latent** — but a one-line property in a pure parser.
2. **`parts.len() >= 3` / `>= 2` silently ignore trailing tokens (latent).** Same reachability
   argument: `+FULLRESYNC id 5 junk` is accepted today. Named because a mutation from `>=` to
   `==` is currently killable only through a scripted socket.

   Both items are **step 2**, not step 1. Tightening either changes the accepted input
   language, and no `Unkept` row (`:1398-1435`) and no spec row constrains those inputs today
   — FM-REPLICATION-013 `:327` is the *producer's* contract. In a crate locked at 0.85 that
   makes them spec-first work, homed on the new row described under
   [Spec gap to file](#spec-gap-to-file--the-home-for-step-2).
3. **A refusal is indistinguishable from a hiccup (not a bug — an interface gap).** A
   FM-REPLICATION-064 version refusal reaches the replica as `-ERR PSYNC refused - …` and is
   returned as an ordinary `io::Error` (`:334`). The reconnect loop (`replica/mod.rs:464-475`)
   therefore retries it forever with backoff to the 30 s cap, whereas an install refusal latches
   `sync_refusal` and *returns* (`:451-463`). This is **per spec** — FM-REPLICATION-064's
   Observable says only that the replica *"surfaces that same text in its own log"* (spec `:1499`),
   and says nothing about giving up; retrying is also correct, since the operator's fix is to
   upgrade a binary and the replica should reattach on its own. **No change proposed.**

   The sharpest form of the gap is on the **operator** surface, and it is worth writing down
   even though the ruling stands. `sync_refusal` is written in exactly one place —
   `connection.rs:543`, the `InstallError::Incompatible` arm — and it is what INFO's
   `master_sync_error` renders (`replica/mod.rs:297-311`, read through
   `core/src/shard/types.rs:132`). So an install-refused replica shows
   `master_link_status:down` *with the reason in `master_sync_error`*, while a permanently
   version-refused replica shows `master_link_status:down` with an **empty**
   `master_sync_error` and the reason only in a log line that has long since scrolled past —
   the exact "down vs given up" confusion `sync_refusal`'s own doc comment
   (`replica/mod.rs:306-308`) says the field exists to prevent. That difference is not a
   deliberate design decision anywhere; it falls out of the refusal being an untyped
   `io::Error`. A `PsyncReply::Refused` arm does not by itself change any of this, but it is
   what makes the choice *expressible* — after which "does a version refusal set
   `master_sync_error`?" becomes a spec question someone can answer, instead of an accident of
   `strip_prefix('-')`.

## Proposed change

### Step 1 — the byte-identical move

#### The module

A new `pub(crate)` module `frogdb-server/crates/replication/src/replica/psync_reply.rs`, added
to the `replica` mod block at `replica/mod.rs:3-8` beside `payload_reader`:

* `pub(crate) enum PsyncReply` with three arms carrying exactly what the wire said and nothing
  it implies:
  * `FullResync { granted_id: String, granted_offset: u64 }` — documented at the type as
    *granted, deliberately not adopted*, with the FM-REPLICATION-001 reference. The 20-line
    comment at `:246-265` moves here, where it is a statement about the type rather than an
    apology inside a branch.
  * `Continue { primary_id: Option<String> }` — `None` is the bare `+CONTINUE` a
    never-promoted primary sends; the "shift only when it actually changes" rule stays an
    *effect* in `psync`, because it reads live state.
  * `Refused(String)` — the `-` arm, with the `PSYNC error: {rest}` rendering FM-REPLICATION-064's
    Observable pins.
* `pub(crate) fn parse_reply_line(line: &str) -> io::Result<PsyncReply>` — total over any line,
  no I/O, no state, no clock. In step 1 it reproduces the **current** acceptance rules
  verbatim: `starts_with("+FULLRESYNC")`, `parts.len() >= 3`, `starts_with("+CONTINUE")`,
  `parts.len() >= 2`, `strip_prefix('-')`, and the four reply-line error strings
  (`:244`, `:312`, `:334`, `:338`) character-for-character. The warts move; they are not fixed
  here. A `// step 2 tightens this` comment marks each of the two, pointing at the issue.

#### The prelude half

The marker decision belongs in `CheckpointStreamCodec`, which already owns both writers, not in
a fourth place:

* add `pub enum PayloadKind { Checkpoint, Snapshot }` and
  `pub async fn read_marked_prelude<R>(r) -> io::Result<(PayloadKind, usize)>` in `fullsync.rs`,
  routing the count through the existing `parse_file_count` (FM-REPLICATION-035's *one* count
  parser and its `MAX_CHECKPOINT_FILE_COUNT` bound — do **not** duplicate the bound).
  **Naming:** the variants take the marker constants' own words —
  `Checkpoint` ↔ `CHECKPOINT_MARKER` (`fullsync.rs:110`), `Snapshot` ↔ `SNAPSHOT_MARKER`
  (`:122`) — and match `SyncType::{FullSyncCheckpoint, FullSyncSnapshot}` (`connection.rs:104-105`),
  which spec FM-REPLICATION-001 names in its Outcome variant cell (`:103`). An earlier draft
  said `Dataset`; that introduces a third word for the thing the wire, the constant, the
  `SyncType` arm and the spec row all call a snapshot, for no gain.
* keep `read_prelude` as a checkpoint-only shim delegating to it. **Not** for a lint reason:
  `scripts/failure-modes.py:468-502` keys exclusively on **test** names — it builds
  `tagged = {(tag.fm_id, tag.test)}` (`:472`) and compares against `mode.tests` basenames
  (`:483`, `:498`) — so renaming the *production* function `read_prelude` leaves
  `just lint-failure-modes` green. The reason to keep it is semantic: FM-REPLICATION-035's
  Forced-by names `test_read_prelude_rejects_bad_marker` (spec `:772`), and that name stays
  meaningful — and the Forced-by entry stays honest about what it forces — only while there is
  still a `read_prelude` with a checkpoint-only contract to reject a bad marker *for*.
* delete the two stale doc paragraphs (`fullsync.rs:225-229`, `:246-250`) that justify the
  split by the plain-RDB arm that no longer exists.

The one constraint FM-REPLICATION-005 imposes: `read_marked_prelude` must be reachable over the
**unbuffered** socket. `psync` reads the marker with `read_resp_line` today precisely so no
`BufReader` swallows the live tail; the extraction must keep the marker read byte-at-a-time (or
route it through `PayloadReader`, never a locally constructed `BufReader` — `connection.rs:110-113`
says so at the field). Simplest safe shape: `parse_marker(line: &str) -> io::Result<PayloadKind>`
as the pure half, called by `psync` on a `read_resp_line` result, with `read_marked_prelude`
built on it for the codec's own round-trip tests.

#### What `psync` becomes

```
write request  →  read_resp_line  →  parse_reply_line
   ├── Continue { primary_id }  → shift-if-changed, set_state(Streaming), SyncType::PartialSync
   ├── FullResync { .. }        → reset_to(0)  ← MUST stay before the prelude read
   │                              set_state(Syncing)
   │                              read_resp_line → parse_marker → parse_file_count
   │                              → SyncType::FullSync{Checkpoint,Snapshot}
   └── Refused(msg)             → Err
```

**The ordering constraint is the acceptance criterion.** `reset_to(0)` at `:266` happens
*before* the prelude is read, so a prelude failure still leaves the node asking `PSYNC ? -1`.
The pure parser must **not** carry the rewind, and the split must land exactly where the test
table at `:1398-1435` already draws it: rows 1-3 (`connection closed`, `unsupported … marker`,
`expected a checkpoint or dataset marker`) expect `("?", -1)`; rows 4-6 (`malformed FULLRESYNC
response`, `invalid offset in FULLRESYNC`, `PSYNC error`) expect the live head `(OLD_ID, 900)`.
That table is the regression guard for the extraction and must pass unedited.

`SyncType` stays as-is and keeps its caller contract (`replica/mod.rs:511-519`): it says *what to
read next*, `PsyncReply` says *what was said*. Merging them would re-entangle the two decisions
the split exists to separate.

### Step 2 — the grammar tightening, spec-first

Once step 1 has landed and the `Unkept` table has passed unedited against the extracted parser,
latent items 1 and 2 are two conditions in one pure function:

* `starts_with("+FULLRESYNC")` → the token before the first space equals `+FULLRESYNC`;
  likewise `+CONTINUE`.
* `parts.len() >= 3` → `== 3`; `parts.len() >= 2` → `== 2` (with the bare `+CONTINUE`,
  `parts.len() == 1`, still its own arm — it is a real wire form, `replica_session.rs:738`
  writes it whenever the primary was never promoted).

Order, per the locked-area rule: **row first** (the new row under
[Spec gap to file](#spec-gap-to-file--the-home-for-step-2), whose NOT-observable cell names
`+CONTINUEX`/`+FULLRESYNCX` and the trailing-token forms), **then a failing test** — four
`parse_reply_line` string cases, no socket — **then the fix**. The new rows added to the
`Unkept` table, if any, are additions; the six existing rows still pass unedited.

Rejected inputs move to the `"unexpected PSYNC response: {line}"` arm (`:338`), which is the
existing catch-all and needs no new error string. Both changes are strictly narrowing, and both
narrow onto inputs no in-tree or Redis producer emits, so there is no compatibility question —
only a spec-home question, which the row answers.

### Why this is depth, not a wrapper

The new module's interface is one line of text in, one three-arm value out — no socket, no
state, no ordering. Its implementation is the whole grammar and its error modes. The
information hidden is real: after the change, no caller anywhere needs to know that a
`+CONTINUE` may or may not carry an id, that a `+FULLRESYNC` has three whitespace-separated
fields, or that a refusal is a `-` prefix.

**Leverage**, counted honestly per function rather than as one number:

* `parse_reply_line` — the production `psync` (its one caller), the four `Unkept` rows that
  fail purely on grammar (`:1417-1434`), and the D2 model-check consumer, which needs exactly
  `fn(view, event) -> outcome` and today would have to re-derive this by hand.
* `parse_marker` / `read_marked_prelude` — the production `psync` and the codec's own
  round-trip and golden tests in `fullsync.rs`, which today can only round-trip the checkpoint
  marker.

**Locality**: a grammar change becomes one file instead of two files and four realizations.

## Testability improvement

1. **Four socket-driven cases become four string cases.** Every row of the `Unkept` table
   (`connection.rs:1398-1435`) currently costs a `tokio::io::duplex`, a `ReplicationState`, a
   `ReplicaOffset` and a **12-field struct literal spanning 14 lines** (`:1458-1471`) to assert
   a property of a *string*. Four of the six rows fail purely on grammar. After the split those
   are `assert!(matches!(parse_reply_line("+FULLRESYNC id"), Err(e) if …))`, and the
   socket-driven test shrinks to what it is actually for: the two-class effect ordering (heads
   rewound vs heads intact).
2. **`each_full_resync_marker_routes_to_its_own_payload_kind` (`:703-721`) gains the case it
   cannot express today.** It loops over both markers through a scripted socket; as a table over
   `parse_marker` it also gets the negative cases (`FROGDB_SNAPSHOTX`, empty marker, missing
   `$`) and, more importantly, the codec's `$FROGDB_SNAPSHOT` round-trip that exists nowhere.
3. **Mutation density, in the crate the gate measures.** `cargo mutants -p frogdb-replication`
   runs only this package's tests — ADR 0004's own consequences paragraph says the crate's
   measured 74.7% *"is a floor rather than a measurement"* for exactly this reason, and that
   *"raising the real score means moving forcing tests down into the crates, not tuning the
   gate"* (`adr/0004-replication-runtime-seams.md:64-70`). This extraction is that move applied
   one layer in: today the arm boundaries (`>= 3`, `>= 2`, `starts_with`, the `$` check, the
   `!=` at `:325`) are reachable only through scripted duplex sockets — slow, and each mutant
   needs a whole async fixture to die. As a pure function they are killed by one-line unit
   tests. The mutant *population* grows (a new function with many arms), so the ratio must be
   re-measured rather than assumed — see gate discipline below.
4. **It gives an untagged decision a name so a spec row can be written about it.** The three
   arm tests at `:703`, `:727`, `:763` carry **no `// FM-` tag**, and no row owns the
   replica-side `+CONTINUE` identity adoption: FM-REPLICATION-019 owns the *primary* minting and
   serving the window, FM-REPLICATION-022 owns `adopt_replication_history` on the demotion path.
   The replica deciding to call `shift_replication_id` because a `+CONTINUE` named a different
   id (`connection.rs:325-327`) is forced by nothing the lint can see. That is a spec gap, filed
   below — and it is the same row step 2 needs.

## Risks / scope boundaries vs sibling proposals

### LOCKED-area discipline

`frogdb-replication` is locked at gate **0.85** (ADR 0004).

* **Step 1 is a pure extraction with no behaviour change** — the one class of edit that does
  not begin with a failure-mode row. Every error string, and the *order* in which the two
  socket reads and the four state mutations occur, must be byte-for-byte and step-for-step
  identical. The `Unkept` table is the acceptance test and must pass **unedited**.
* **Step 2 is not**, and does not get to ride along on step 1's exemption. Row → failing test →
  fix, as [above](#step-2--the-grammar-tightening-spec-first).
* `just lint-failure-modes` must stay green. It is bidirectional and name-keyed
  (`scripts/failure-modes.py:468-502`): every `Forced by` name must resolve to a real test
  carrying the row's tag, and every `// FM-` tag must name a row listing it. Both directions key
  on **test** names only, so no production rename in either step can break it — but every new
  tagged test in either step needs its name added to the row's Forced-by cell in the same
  commit, or the test→spec direction errors.
* `just mutants-diff frogdb-replication` before pushing; if the score moves at all, run
  `just mutants frogdb-replication` + `just mutants-gate frogdb-replication 0.85`. Unlike
  proposal 55 (which deletes duplicated lines and shrinks the population), 56 *adds* a pure
  function with many arms — the denominator grows, so a diff run is not a safe proxy.

### Failure-mode rows whose Invariant / Observable / Forced-by touches the target lines

| row | how it touches `psync` / the prelude | obligation |
|---|---|---|
| **FM-REPLICATION-001** (`:95-122`) — *governing row* | Invariant (`:102`) names `psync` **twice**: *"`psync` rejects any marker it cannot install"* and *"on `+FULLRESYNC` `psync` adopts neither half of the granted pair — it rewinds the offset to 0 …"*. Outcome variant (`:103`) names `SyncType::{FullSyncCheckpoint, FullSyncSnapshot}`. Forced-by (`:104`) lists three in-crate socket tests over these lines: `psync_rejects_a_payload_that_carries_no_dataset` (`connection.rs:1304-1363`), `a_full_sync_that_never_delivers_a_dataset_leaves_the_old_history_alone` (`:1381-1512`), `a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone` (`:1519-`) | Behaviour identical in step 1; keep `psync` and `SyncType` named as they are. **This row, not -035, owns marker rejection** (-035's Invariant `:770` says so explicitly), so any new *refusal* assertion is added to this Forced-by list. **Flag for human review:** if the marker check moves bodily into the codec, *"`psync` rejects any marker it cannot install"* becomes a citation-level edit. Invariant prose is **never parsed** by the lint, so this would be silently stale — the recommended `parse_marker` shape (called *from* `psync`) keeps the sentence literally true and is preferred for that reason |
| **FM-REPLICATION-035** (`:763-780`) | Invariant (`:770`) names `parse_file_count` as *"the one count parser"* and `CheckpointStreamCodec` as owning *"both directions of the grammar"*, and ends *"Markers themselves are FM-REPLICATION-001's; this row owns everything downstream of them."* Observable (`:768`) pins the byte shape `$<MARKER>\r\n<count>\r\n`. Forced-by (`:772`) names `test_read_prelude_rejects_bad_marker` | Do not duplicate the count bound; keep a `read_prelude` for that Forced-by name to stay honest about. Adding `read_marked_prelude` + a `$FROGDB_SNAPSHOT` round-trip and golden **belongs here** (byte shape, both directions) and needs the two new test names added to this Forced-by cell — a linted edit, see the hotfix |
| **FM-REPLICATION-005** (`:160-172`) | Invariant: *"The payload paths never wrap the socket themselves: `PayloadReader` owns the `BufReader`"*; the reason `read_resp_line` (`connection.rs:29-33`) reads byte-at-a-time is this row | **Constraint, no edit.** No `BufReader` may appear in `psync`. The prelude read stays unbuffered |
| **FM-REPLICATION-064** (`:1494-1512`) | Observable (`:1499`) pins the replica-side rendering *"`PSYNC error: ERR PSYNC refused - …`"*, produced at `connection.rs:334`. Forced-by (`:1503`) names fifteen tests: eleven `version_compat.rs` unit tests, three `replica_session.rs` `handle_psync` tests (`psync_from_an_incompatible_major_is_refused_before_anything_is_registered` at `:3547` and two siblings) — **all in `frogdb-replication`** — plus `test_psync_is_refused_for_an_incompatible_replica_version` (`integration_replication.rs:125`), the one outside the mutated crate | `PsyncReply::Refused` must format identically. **Not a mutation win.** The `-` arm at `:334` is already mutation-covered inside the gated crate: `Unkept` row 6 (`connection.rs:1429-1434`) scripts `-ERR Can't SYNC while loading the dataset`, fingerprints `"PSYNC error"` (`:1432`, asserted `:1483`), and is tagged FM-REPLICATION-001 at `:1381`. The residual win is **spec attribution only**, and only for the *replica-side rendering* clause of `:1499`: today nothing tagged -064 asserts that rendering in-crate, so an in-crate `parse_reply_line` case tagged -064 (plus its name on the Forced-by cell — the linted direction) puts a forcing test where the clause actually lives |
| **FM-REPLICATION-045** (`:952-962`) | Invariant (`:959`) names `psync_request_args` by name (the divergence-rewind path relies on it sending `PSYNC ? -1`) | Keep the function and its name; it is already the pure half and is not moved |
| **FM-REPLICATION-027** (`:593-609`) | The `io::Error` `psync` returns is what the reconnect loop classifies (`replica/mod.rs:451-475`) | No change to the error *kind* of any arm |
| **FM-REPLICATION-013** (`:322-346`) | The **producer** contract our parser mirrors: *"Exactly one of two first-line replies, always … `+CONTINUE <replid>` or `+FULLRESYNC <replid> <offset>`"* (`:327`) | Not edited, and **not a home for step 2**: it constrains what the primary emits, not what the replica must refuse. The new module's doc comment should point at it — it is the only place the grammar is specified |

**Two stale prose citations found while verifying, both lint-invisible, neither owned by 56:**

* FM-REPLICATION-013's Invariant (`:329`) cites `state.rs:381` for `window_contains` (actual
  `state.rs:386`) and `primary/mod.rs:518` for `handle_psync` (actual `:591`).
* FM-REPLICATION-027's Invariant (`:600`) cites `replica/mod.rs:388` for the `link_up.store(false)`
  (actual `:523`) and `replica/mod.rs:286-341` for the retry loop (actual `:408-479`).

`failure-modes.py` parses only Forced-by lists ↔ `// FM-` tags (`:468-502`), so file:line drift
inside Invariant prose is invisible to every lint run. **Flagged for human review**, not fixed
here.

### Boundary vs proposal 53 (fullsync-emitter, RC1)

53 owns the **emit** side: `replica_session.rs:888-1104` and a new `fullsync/emitter.rs`. 56
touches neither file's emit paths. Three contact points, all small:

1. 53's ownership table declares `CheckpointStreamCodec` *"neither — shared, unchanged by
   both"*. 56 proposes to **extend** it (`read_marked_prelude` / `PayloadKind`). That is an
   additive edit inside `fullsync.rs`, not a change to anything 53 calls — but it contradicts
   53's stated assumption, so it must be announced. **Fallback if 53 objects:** put
   `parse_marker` in `replica/psync_reply.rs` and leave `fullsync.rs` untouched; 56 still lands,
   and the codec keeps its one-and-a-half decoders.
2. Both may add a `pub mod` line to `fullsync.rs:20-21` (53 adds `emitter`; the `pub use` block
   is `:23-24`). Trivial textual conflict; whoever lands second rebases.
3. **Golden overlap on `$FROGDB_SNAPSHOT` bytes.** 53's phase 1 adds
   `full_sync_snapshot_golden_bytes` (53 `:408-416`, listed in its phase table `:690`), driven
   through `handle_full`, pinning the whole emitted stream as one byte literal — including
   `$FROGDB_SNAPSHOT\r\n2\r\n` and the trailer's checksum hex. 56's hotfix adds a snapshot
   golden too. **No spec collision:** 53's goldens carry no `// FM-` tag of their own (53
   `:429` has them *layering on* `test_checkpoint_codec_golden_bytes`, FM-REPLICATION-035),
   while 56's are tagged -035 and named on its Forced-by cell. **The split is by level:** 53
   pins the bytes the primary **emits** at composition level (file order, marker selection,
   checksum coverage order, `rdb_size`); 56 pins the codec-owned **inverse** — that
   `read_marked_prelude` reads back what `write_snapshot_prelude` wrote, and that the prelude's
   own bytes are what -035's Observable says. If 53's phase 1 lands first (it is independently
   landable, 53 `:696`), 56's snapshot golden shrinks to the round-trip and the prelude bytes,
   and the composition-level literal is not duplicated.

**Order: 53 first.** It is larger, it deletes the two hand-rolled test senders
(`connection.rs:852`, `receiver.rs:73`), and 56 is additive against either state. Run the
crate's mutation gate **once at the end of the chain**, not per proposal.

### Boundary vs proposal 54 (replica-connection-wiring, RC2/RC10) — the hard edge

54 does two things inside 56's blast radius:

* changes `psync`'s own field reads at `connection.rs:229-230` to go through `offsets.state()`
  (54's Risks section: *"`psync` correlates the offset and the replication id in adjacent
  lines"*);
* replaces **every** `ReplicaConnection` struct literal — eight in `connection.rs` (54's Files
  table: `:623, 680, 802, 934, 1154, 1314, 1458, 1545`), including the four that belong to
  psync's tests, at `:623`, `:680`, `:1314`, `:1458`.

So 54 and 56 edit the same function *and* the same test fixtures. **Recommended order: 54
(RC2) → 56.** 54's own text already asks for RC2 ahead of D2 (ii) for exactly this reason
(*"extracting a pure arm selector is easier once `psync` reads one state handle instead of
two"*), and 56 then deletes or shrinks several of the literals 54 just rewrote rather than
forcing them to be rewritten twice. If 56 lands first, 54 rebases a two-line hunk onto a
shorter `psync` — recoverable, but strictly worse.

### Boundary vs proposal 55 (adopt-full-sync-landing, RC3)

Same file, different methods: 55 owns `receive_snapshot` (`:368-419`) and `receive_checkpoint`
(`:452-498`) and their landing tails; 56 owns `psync` (`:224-341`). **No shared hunk.** Both
flow through `set_state` (`:159-163`); 56 does not modify it. Both are `frogdb-replication`
changes needing the same gate — chain them. Order-independent, but 55's own recommendation
(55 → 54) plus 54's (54 → 56) yields a consistent chain: **53 → 55 → 54 → 56**.

### Relationship to `.scratch/replication-correctness/` — 56 *is* D2 tier (ii)

PRD `:533-554`, **ruled 2026-08-10**. D2 authorizes the full (iii) restructure of
`replica_session.rs`, with execution discipline: *"land (i) and (ii) first as stepping stones,
then (iii) as its own issue chain; every step spec-first against `replication-failure-modes.md`
(rows may move file:line citations but not meaning); the full mutation gate (0.85) re-runs after
(iii)"* (`:550-554`). Tier (ii) is, verbatim, this proposal (`:538-541`).

Consequences:

* **This should be filed and executed as the D2 (ii) issue**, not as an independent arch-round
  change competing with the campaign for the same function. The campaign has no issues yet
  (`replication-correctness/README.md`: *"Issues: none yet — decomposition pending after PRD
  ruling"*), so there is no issue-level dependency to name — 56 can be the first one. Step 1
  and step 2 are two issues in that chain, in that order.
* **What the arch lane adds beyond the PRD's one sentence**, and the reason to file it with this
  document attached: the third grammar realization and its divergence (`read_prelude` rejects
  `$FROGDB_SNAPSHOT`), the untested `write_snapshot_prelude`, the stale doc justification, the
  two-step split the "pure extraction" framing hides, and — most importantly — the
  **effect-ordering constraint** (rewind before prelude read; the two-class error table at
  `:1398-1435`) that the PRD does not mention and that a naive "extract the arm selection"
  would be free to break.
* **One deviation to flag, on a narrower argument than an earlier draft used.** The PRD says
  the pure function should live *"beside `handle_partial_sync_request`"* (`:539-540`) — i.e.
  `primary/replay.rs` — and glosses (ii) as deleting *"the asymmetry ADR 0004 implicitly
  created"* (`:544`). Recommend mirroring the *shape* and not the *location*. The argument is
  **not** an ADR clause: ADR 0004 is about the four seam implementations moving to
  `frogdb-replication-runtime`, and contains **no** primary/replica module-separation rule —
  the PRD's gloss reads the other way, treating the ADR as the asymmetry's source rather than
  its arbiter, and that should be acknowledged rather than papered over. The argument that does
  hold is the module boundary itself plus symmetry-of-shape: `primary/replay.rs` owns the
  backlog and the replay machinery, and a *replica*-side reply parser placed there would sit
  next to — and could reach — state it must never touch. Symmetry between the two sides is
  symmetry of **shape** (pure function, named outcome type, no I/O), not of address. Worth an
  explicit re-confirmation when the issue is filed.
* D2 (iii)'s gate discipline (full 0.85 re-run) covers 56 if 56 lands inside the chain;
  standalone, `mutants-diff` plus a full run if the ratio moves is the right bar.

**Unrelated collision noticed while reading the PRD:** the planned `ReplicationView`
(PRD `:92-123`, ruled by D7 to live in `frogdb-replication`) shares its name with the existing
`frogdb-debug` web-UI type (`crates/debug/src/web_ui/state.rs:77`, re-exported at
`crates/debug/src/lib.rs:33` and consumed by `server/src/debug_providers.rs:106`). Different
crates, so it compiles — but two `ReplicationView`s in one workspace is a reader trap worth
renaming before W1 writes the second one. Flagged for the campaign, out of scope here.

### Residual risk

**Low-to-moderate**, higher than 55's. The extraction is mechanical, but it is the *only*
interpreter of a control-plane protocol in a locked crate, and two of its steps are state
mutations whose order is load-bearing and pinned only by one test table. Review must check
four things specifically:

(a) the rewind (`:266`) still precedes the prelude read;
(b) no `BufReader` appeared anywhere in `psync` (FM-REPLICATION-005);
(c) **all seven error strings spelled in `psync`'s body** — `"invalid offset in FULLRESYNC"`
(`:244`), `"replication stream retired during FULLRESYNC"` (`:272`), `"expected a checkpoint or
dataset marker"` (`:282`), `"unsupported FULLRESYNC payload marker: {marker}"` (`:306`),
`"malformed FULLRESYNC response"` (`:312`), `"PSYNC error: {rest}"` (`:334`), `"unexpected PSYNC
response: {line}"` (`:338`) — are unchanged in text **and** in `io::ErrorKind`, as are the two
propagated from `read_resp_line` (`:40-51`) and `parse_file_count` (`fullsync.rs:231-242`);
(d) both `set_state` transitions (`Syncing` at `:276`, `Streaming` at `:330`) are unchanged in
position and order.

For step 1, add: the diff contains **no** change to any acceptance condition —
`starts_with`/`>=` survive verbatim, and any tightening in that diff is a review stop.

## Effort

**M (step 1) + S (step 2).**

Step 1: ~120 lines of new module (`psync_reply.rs`) with its unit tests, ~40 lines of codec
addition in `fullsync.rs`, `psync` shrinks from 118 lines to ~45, and 4 of the 6 `Unkept` rows
migrate from socket fixtures to string assertions. One file created, three edited, no public API
change (`SyncType` and `ReplicaConnection` keep their shapes).

Step 2: two conditions, four new string test cases, one new failure-mode row. The cost in both
steps is not the code — it is the LOCKED-area gate run and the review discipline above.

## Independently-landable hotfix — give the `$FROGDB_SNAPSHOT` prelude the inverse and the golden it has never had

Ships **without** the refactor and changes **no** production behaviour. It **does** carry one
spec edit — see below; an earlier draft claimed otherwise and was wrong.

**Gap (verified).** `CheckpointStreamCodec::write_snapshot_prelude` (`fullsync.rs:179-184`) is
called in production (`replica_session.rs:1050`) and has no codec-owned decoder
(`read_prelude:260-265` rejects the marker) and no round-trip or golden test — all three
round-trip tests (`:709`, `:777`, `:879`) and `test_checkpoint_codec_golden_bytes` (`:823-864`)
use `write_prelude` only. FM-REPLICATION-035's Observable (`:768`) already specifies the byte
shape `$<MARKER>\r\n<count>\r\n` for both markers, and its Invariant (`:770`) already says the
codec *"owns both directions of the grammar, so an encoder change that is not matched by its
inverse fails the round-trip"* — and for one of the two markers there is no inverse to fail.

**Fix.** Add `PayloadKind` + `read_marked_prelude` to `CheckpointStreamCodec` (delegating the
count to `parse_file_count`), keep `read_prelude` as the checkpoint-only shim, and add two tests
tagged `// FM-REPLICATION-035`: a `$FROGDB_SNAPSHOT` round-trip and a snapshot golden-bytes
assertion mirroring `:823-864`.

**Spec edit is mandatory, not optional.** `failure-modes.py`'s test→spec direction (`:491-502`)
errors on any `// FM-REPLICATION-035` tag whose test the row's Forced-by cell (`:772`) does not
name. So both new test names go on that cell **in the same commit**. That is one linted cell
edit — small, but it is a spec change and the hotfix must be reviewed as one.

**Three prelude readers, not two, until 56 lands.** After the hotfix the workspace has the
inline production reader (`connection.rs:288-289`), the `read_prelude` checkpoint-only shim, and
the new `read_marked_prelude`. That is one more realization than today, and it is the honest
cost of shipping the seam early: the count collapses to one when 56 step 1 routes `psync`
through `parse_marker`. Still net-positive standalone — the snapshot encoder gains an inverse
and a golden it has never had, which is coverage that exists whether or not the refactor is
approved — but it should not be sold as a simplification on its own.

~40 lines plus tests. It also lands the seam 56 needs, so the refactor arrives smaller.

## Spec gap to file — the home for step 2

No failure-mode row owns the **replica's half** of the PSYNC reply. Two distinct things live in
that hole, and one row should take both:

1. **What the replica accepts.** FM-REPLICATION-013 (`:322-346`) specifies the *producer*:
   *"Exactly one of two first-line replies, always"* (`:327`). Nothing specifies what a replica
   must do with a line that is neither — including the near-misses `+CONTINUEX abc`,
   `+FULLRESYNCX a 1`, and `+FULLRESYNC id 5 junk`, all of which today's `starts_with` / `>=`
   conditions (`:239`, `:241`, `:315`, `:317`) accept. That is the row step 2 needs before it
   can tighten them.
2. **What the replica does with a `+CONTINUE` identity.** FM-REPLICATION-019 owns the primary
   minting and serving its failover window; FM-REPLICATION-022 owns `adopt_replication_history`
   on the demotion path. The replica's own decision — *shift my history onto the id the grant
   named, freezing my current id at the offset the stream resumed from, and only when the id
   actually changed* (`connection.rs:318-329`) — is forced by three tests (`:703-721`,
   `:727-757`, `:763-784`) that carry **no `// FM-` tag** and are therefore invisible to
   `lint-failure-modes` in both directions.

Given FM-REPLICATION-019's own Observable (*"A sibling that presents `(inherited_replid,
offset)` … is answered `+CONTINUE` and resumes"*), the replica half of that handshake being
unspecced is a real gap in a locked area. File one row covering both: for (2) the three tests
already exist and are good, they just have nothing to bind to; for (1) the row must land
**before** step 2's tightening, and its NOT-observable cell is where the prefix and
trailing-token forms get named.
