# RESP payloads between 64 MB and 512 MB are accepted by the connection layer but cannot cross the replication link

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/14 F4 · MASTER.md §3 (availability / resource)
Score: severity 5 · likelihood 3 · effort 2 · priority 19
Area: frogdb-replication / frame codec; frogdb-server connection codec

## Context

Two unrelated ceilings govern the same byte stream: the connection layer accepts bulk values up
to 512 MB (Redis parity), while the replication frame codec rejects anything over 64 MB — and
only on decode. The encode side casts the length with an unchecked `as u32`. So the primary
commits the write and emits a frame the replica's decoder rejects; the stream drops, the replica
reconnects, the same frame is re-sent from the backlog, and the link never recovers. A
write-accepted / never-replicated wedge, on values that are explicitly inside the documented
limit FrogDB advertises.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`server/src/connection/codec.rs:36` — `const PROTO_MAX_BULK_LEN: i64 = 512 *
1024 * 1024;` enforced at `:176` and `:277`. `replication/src/frame.rs:186` — `pub const
MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;`, referenced **only** at `:315` (`decode`) and
`:434` (`Decoder::decode`), i.e. exclusively on the receiving side. `frame.rs:287` encodes
the length as `buf.put_u32(self.payload.len() as u32)` with no bound check and no
saturating guard.

Why nothing catches it: no test in `frame.rs:499-780` uses a payload anywhere near the limit;
the largest integration case, `test_large_value_replication`
(`server/tests/integration_replication.rs:937`), carries a single assertion and a modest value.

Proposal 14's cross-area note adds that `cluster/src/network.rs:77` defines its own independent
64 MB copy — three unrelated ceilings for the same byte stream. Whichever side is wrong, the
constants should be related explicitly rather than coincidentally.

## What to fix

1. Relate `PROTO_MAX_BULK_LEN`, `MAX_FRAME_SIZE` and `cluster/src/network.rs:77` explicitly —
   one source of truth, or a documented derivation between them.
2. Enforce the frame bound on **encode** (`frame.rs:287`): reject, or fragment, rather than
   truncating via `as u32`.
3. Decide which ceiling is authoritative for user data and make the connection layer refuse
   what the replication link cannot carry, so the write is never accepted in the first place.

## Options

Reproduced verbatim from proposals/14 F4:

- *Level 1 only*: catches the asymmetry, misses the reconnect-storm consequence. Fast.
- *Level 1 + one level-4 case*: **recommended** — the unit test is the regression guard, the
  integration test is the one-time proof the wedge exists.
- *Property test over payload size*: overkill; the interesting behaviour is a single
  threshold, not a distribution.

## Acceptance criteria

- [ ] A unit test asserts `ReplicationFrame::new(0, payload_of(MAX_FRAME_SIZE + 1))` is rejected
      at encode time (or that the codec fragments it). Fails today.
- [ ] `encode`→`decode` round-trips at exactly `MAX_FRAME_SIZE` and `MAX_FRAME_SIZE - 1`.
- [ ] An integration test (primary + replica) does `SET k <70 MB value>` and asserts the
      replica's `GET k` length matches and `master_link_status` is still `up` 2 s later.
- [ ] The three ceilings are asserted to agree (or their derivation is asserted) in one test.

## Test boundary

Level 1 for the encode/decode symmetry — pure codec arithmetic, where a socket would only make
it slow. Level 4 for the end-to-end wedge, which genuinely needs two processes and a real link;
level 1 alone cannot observe the reconnect storm that makes this severity 5.

## Depends on

nothing
