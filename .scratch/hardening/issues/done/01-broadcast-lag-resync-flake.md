# A replica full sync drops the live frames that arrive with the payload trailer

Status: done
Type: bug (data safety / replication)
Severity: likelihood 2/3 (needs the primary to be streaming while the payload is in flight — the
normal case under load), consequence 3/3 (silent, permanent replica divergence with
`master_link_status:up`, and `WAIT` structurally unsatisfiable) — score 6
Area: replication

## Symptom

`integration_replication::test_broadcast_lag_disconnect_and_resync::case_2_with_persistence`
fails intermittently under full-suite parallel load, first seen twice on 2026-07-31 (different
commits, unrelated diffs — command feature-gating and the concurrency-harness drain fix):

```
assertion `left == right` failed: seed write should be acked by the healthy replica
  left: 0
  right: 1
```

at `integration_replication.rs:7238`. It passed in isolation and on full-suite rerun both times,
so it was originally filed as a flaky test. Reproduced again on 2026-07-31 locally on macOS under
`just test frogdb-server` (1923/1924, the only failure), and twice more on 2026-08-02 during the
phase-2c full-sync work — same assertion, same line. Any host running the full suite in parallel
can hit it.

The failing assertion is a `WAIT` ack count against a *healthy* replica while a LagProxy throttles
the other. It is not a timeout: the replica never had the write to ack.

## Root cause

Not a test-timing problem — a real replica-side frame-loss bug on the full-sync path, root-caused
by reproducing it under load (54% failure rate with 24 CPU burners and 4 concurrent copies of the
test) and confirmed with a prototype fix (0/20 failures).

`frogdb-server/crates/replication/src/replica/connection.rs` wrapped the socket in a
*function-local* `BufReader` in both `receive_checkpoint` and `receive_snapshot`:

```rust
let mut reader = BufReader::new(&mut self.stream);
let (metadata, computed) = receive_checkpoint_files(&mut reader, &incoming, file_count).await?;
```

The primary starts streaming live WAL frames as soon as it has written the payload
(`start_streaming` — FM-REPLICATION-004), so by the time the `FullSyncMetadata` trailer lands the
socket usually holds frames right behind it. The read that completes the trailer is an 8 KiB
`BufReader` fill, so it pulls those frames into the reader's buffer. The reader is dropped at
function exit and its buffer goes with it — and the socket has no copy left.

`stream_replication` (`replica/streaming.rs`) then started from a fresh `BytesMut` on the raw
stream, so those bytes were never decoded, never applied, never ACKed:

* the replica's offset stays permanently short of the primary's stream position (81-121 bytes in
  the reproductions),
* `master_link_status:up` throughout — nothing reports a problem,
* `WAIT` can never be satisfied for anything at or above the lost range, which is exactly the
  assertion that failed,
* only a reconnect heals it, and it re-syncs from the replica's short offset.

The primary is innocent: FM-REPLICATION-004 holds on the wire — the frames were sent. Both payload
shapes had the bug; only checkpoint transfers are slow enough for the window to be wide under load.

## Resolution

Spec: **FM-REPLICATION-005** ("the receive→stream handoff loses no byte of the live tail") in
`specs/replication.md`.

Fix: one shared mechanism instead of a per-path patch, so a third payload shape cannot forget it.
New `frogdb-server/crates/replication/src/replica/payload_reader.rs` defines `PayloadReader`, a
`BufReader` guard that owns the socket borrow and, on `Drop`, hands whatever it read past the
payload to the connection's `pending_stream_bytes` — on every exit path, including the `?` returns
of a failed sync. Both `receive_checkpoint` and `receive_snapshot` obtain it through
`ReplicaConnection::payload_reader()`, the only sanctioned way to buffer this socket (the `stream`
field says so at its declaration). `stream_replication` seeds its decode buffer with
`take_pending_stream_bytes()` (take-once, so a residual cannot be replayed) and drains it *before*
its first socket read — the residual can already hold whole frames the primary is waiting to see
ACKed, and waiting for the next byte could wait forever.

Forcing tests (`frogdb-replication`, all deterministic, all over an in-memory duplex that delivers
trailer and frames in a single read):
`receive_checkpoint_streams_the_frames_that_trailed_the_payload`,
`receive_snapshot_streams_the_frames_that_trailed_the_payload`,
`a_payload_with_no_trailing_frames_leaves_the_stream_empty`,
`dropping_the_reader_hands_back_what_it_read_past_the_payload`,
`a_fully_consumed_reader_leaves_no_residual`. The first two fail before the fix with an empty frame
list (`left: []`).

End-to-end evidence, `test_broadcast_lag_disconnect_and_resync` (both rstest cases) under 24 CPU
burners, 4 concurrent copies per iteration:

| build | copies run | failed | rate |
|---|---|---|---|
| fix reverted (residual dropped, as before) | 24 | 13 | **54%** |
| fixed | 96 (24 iterations) | 0 | **0%** |

Every pre-fix failure is the filed assertion verbatim (`seed write should be acked by the healthy
replica`, `left: 0 / right: 1`), which is what ties the load flake to this root cause.
