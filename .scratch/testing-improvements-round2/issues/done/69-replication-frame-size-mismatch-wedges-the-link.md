# RESP payloads between 64 MB and 512 MB are accepted by the connection layer but cannot cross the replication link

Status: done
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

- [x] A unit test asserts `ReplicationFrame::new(0, payload_of(MAX_FRAME_SIZE + 1))` is rejected
      at encode time (or that the codec fragments it). Fails today.
- [x] `encode`→`decode` round-trips at exactly `MAX_FRAME_SIZE` and `MAX_FRAME_SIZE - 1`.
- [x] An integration test (primary + replica) does `SET k <70 MB value>` and asserts the
      replica's `GET k` length matches and `master_link_status` is still `up` 2 s later.
- [x] The three ceilings are asserted to agree (or their derivation is asserted) in one test.

## Test boundary

Level 1 for the encode/decode symmetry — pure codec arithmetic, where a socket would only make
it slow. Level 4 for the end-to-end wedge, which genuinely needs two processes and a real link;
level 1 alone cannot observe the reconnect storm that makes this severity 5.

## Depends on

nothing

## Resolution — one derived ceiling, enforced on encode

Confirmed live, exactly as read: `frame.rs` checked `MAX_FRAME_SIZE` (64 MB) on **decode only**
and encoded the length with `buf.put_u32(self.payload.len() as u32)`. The connection layer
accepted 512 MB. Pre-fix red is recorded below.

Contract pinned by `FM-REPLICATION-011` in
`.scratch/hardening/specs/replication-failure-modes.md`: **the link carries every command the
connection layer accepted.**

### The derivation (what to fix, item 1)

New `frogdb-server/crates/protocol/src/limits.rs` is the single home for the wire ceilings:

- `PROTO_MAX_BULK_LEN = 512 MB` — Redis `proto-max-bulk-len`, the ceiling on *user data*. Moved
  here from `server/src/connection/codec.rs`, which now derives its `i64` view of it (a declared
  `$N` parses as `i64` and may be negative, so the local view stays).
- `PROTO_MAX_MULTIBULK_LEN` — moved alongside it, same reason.
- `MAX_INTERNAL_FRAME_LEN = 2 * PROTO_MAX_BULK_LEN` (1 GiB) — what every internal transport must
  carry: one accepted command is a maximal bulk plus name/key/framing, with one further maximal
  bulk of allowance.

`frogdb_replication::frame::MAX_FRAME_SIZE` and `frogdb_cluster::network::MAX_FRAME_SIZE` are now
*defined as* `MAX_INTERNAL_FRAME_LEN` rather than being independent copies. The cluster bus is in
scope because it carries user bytes too — `BusRpc::PubSubBroadcast` / `PubSubForward` ship what a
client published — so a bus ceiling below the connection layer's is the same accept-then-fail
shape. (`frogdb-cluster` gained a `frogdb-protocol` dependency for this.)

### The encode guard (item 2)

`ReplicationFrame::encode` returns `Result<Bytes, FrameEncodeError>` and the tokio `Encoder` uses
the same `ReplicationFrame::payload_fits` check, both **before** reserving or casting, so the
bound is enforced on the side that can still report it (`FrameEncodeError::PayloadTooLarge`,
converting to `io::ErrorKind::InvalidInput`). Reject, not fragment: reassembly state on both ends
of a path whose job is to be hard to desynchronise is the wrong trade, and Redis answers the same
situation (`PROTO_MAX_QUERYBUF_LEN`) by killing the connection.

Two production call sites in `replica_session.rs`: backlog replay in `start_streaming` propagates
with `?`; the live tail in `write_task` logs at `error` and drops the replica link.

### Divergence from item 3 — the connection layer is *not* tightened

Item 3 offered "make the connection layer refuse what the replication link cannot carry". Rejected,
loudly: that drops FrogDB below the `proto-max-bulk-len` it advertises, and Redis parity on what a
client may store is the stronger contract. The internal transports move up to meet the client
ceiling instead. Recorded as a non-guarantee in the FM row, together with the residual: a
*replicated encoding* above 1 GiB (a multi-value `MSET` of maximal bulks, a `SORT ... STORE`
result) now fails loudly on the wire instead of crossing it truncated.

### Divergence on the round-trip boundary

Acceptance criterion 2 asks for a round trip at exactly `MAX_FRAME_SIZE` and `MAX_FRAME_SIZE - 1`.
With the ceiling at 1 GiB that allocates several GiB in a test process for one branch. Instead the
boundary is asserted arithmetically (`payload_fits` at `0`, `MAX - 1`, `MAX`, `MAX + 1`) and a real
encode→decode round trip is done at **64 MiB + 1** — above the *old* ceiling, which is the
regression that matters.

### Tests

Level 1, `frogdb-server/crates/replication/src/frame.rs`, tagged `// FM-REPLICATION-011`:

- `the_frame_ceiling_is_derived_from_the_resp_bulk_ceiling` — the three ceilings agree by
  derivation; also pins `MAX_FRAME_SIZE <= u32::MAX` (past that the length field is lossy and the
  decode check is meaningless) and the `payload_fits` boundary.
- `encode_refuses_a_payload_larger_than_the_frame_ceiling` — both encoders refuse `MAX + 1` and
  leave the buffer untouched. **Failed pre-fix** (encoded happily).
- `a_payload_over_the_old_ceiling_round_trips_across_the_link` — 64 MiB + 1 through both
  `ReplicationFrame::decode` and the tokio codec. **Failed pre-fix.**

Level 4, `frogdb-server/crates/server/tests/integration_replication.rs`:

- `a_value_over_the_old_frame_ceiling_replicates_without_wedging_the_link` — `SET` of 70 MB on a
  primary, `WAIT 1` must ack, the replica's value must be whole, and 2 s later
  `master_link_status:up` and `connected_slaves:1`. **Failed pre-fix**: with `MAX_FRAME_SIZE`
  restored to 64 MB the `WAIT` ack assertion fails at once (the link is down, not slow). 3.3 s
  green, in-memory single case rather than the persistence matrix.

Also tightened `test_large_value_replication`, whose `if let Response::Bulk(Some(..))` silently
passed on a `Nil` — the "single assertion" the issue calls out.
