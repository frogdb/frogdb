# Full-resync handoff drops the live frames the replica buffered past the metadata trailer

Status: ready-for-agent
Type: bug
Severity: likelihood 2/3, consequence 3/3 (score 6) — silent replica divergence, not a test bug
Area: replication

**Root-caused 2026-08-02: this is a product bug, not a flaky test.** The title and type were
`flaky-test` / "test_broadcast_lag_disconnect_and_resync flakes under full-suite load" until the
investigation below identified the failing assertion as a *true positive*. The test is doing its
job; do not "stabilise" it. Fix belongs with the sequenced replication product work.

## Symptom

`integration_replication::test_broadcast_lag_disconnect_and_resync::case_2_with_persistence`
fails intermittently under full-suite parallel load, twice on 2026-07-31 (different commits,
unrelated diffs — command feature-gating and the concurrency-harness drain fix):

```
assertion `left == right` failed: seed write should be acked by the healthy replica
  left: 0
  right: 1
```
at `integration_replication.rs:7238`. Passes in isolation and on full-suite rerun both times.

Reproduced again on 2026-07-31 during the issue-65 work, this time **locally on macOS** under
`just test frogdb-server` (1923/1924, this the only failure) — same assertion, same line. So it is
not testbox- or Linux-specific: any host running the full suite in parallel can hit it, which
raises its nuisance value and makes it cheap to iterate on locally.

Hit twice more on 2026-08-02 during the phase-2c full-sync work (`just test frogdb-server`
1913/1914, and once in a 185-test `just core-test-e2e replication` run), same assertion, same
message. Passed in isolation and on two consecutive reruns of the same filter, so still
load-dependent and still unrelated to the diff under test.

## Root cause

The replica silently **discards the first live WAL frames of every full resync** whenever those
frames arrive before it has finished reading the payload's metadata trailer.

Both full-sync receive paths wrap the connection in a *local* `BufReader`:

- `replica/connection.rs::receive_checkpoint` — `BufReader::new(&mut self.stream)`, driven by
  `fullsync::receive_checkpoint_files` (checkpoint path, `persistence.enabled = true`)
- `replica/connection.rs::receive_snapshot` — same shape (live-dataset path, persistence off)

`BufReader` fills in 8 KiB chunks, so the read that completes the `FullSyncMetadata` trailer also
pulls in whatever the primary has already written *after* it — the live WAL frames. The reader is
then dropped at the end of the function and its buffer goes with it. `stream_replication`
(`replica/streaming.rs`) starts from a fresh `BytesMut` on the raw `self.stream`, so those frames
are never decoded, never applied, and never acked.

The primary is innocent: `replica_session.rs::handle_full` captures `snapshot_offset` before the
cut, then `start_streaming` subscribes to the broadcast and replays `(snapshot_offset, current]`
from the backlog — FM-REPLICATION-004 holds on the wire. The bytes reach the replica's socket;
the replica throws them away.

Consequences on the failing run, all directly observed:

- The dropped frames are lost for the life of the connection. The replica's offset baseline is
  the snapshot offset, so it stays permanently behind by exactly the dropped byte count
  (81 B = 44 B `SET seed_key seed_value` + 37 B `REPLCONF GETACK *`), while advancing normally
  for every later frame.
- `WAIT 1 <timeout>` on the primary can therefore *never* be satisfied — the replica acks an
  offset that is structurally short of the target, so the test's 15 s condition-wait expires with
  `acked = 0`. This is a correct report of a replica that is not caught up.
- `INFO replication` reports `master_link_status:up` on the replica and
  `slave0:…,state=online` on the primary the whole time, i.e. the divergence is silent.
- The replica's keyspace is missing the write: `DBSIZE` 0 vs 1, `GET seed_key` nil, for the
  entire 15 s window.
- Self-healing only on reconnect: `psync_request_args` resumes from the live *applied* offset,
  which is behind, so the backlog replay closes the hole the next time the link drops. Nothing
  closes it while the link stays up, and a promotion in that window loses the writes.

### Evidence

Instrumented run (temporary `eprintln!` of `reader.buffer()` at the end of `receive_checkpoint`,
reverted). 8 runs under load, 6 failures:

```
6 x  leftover bytes in fullsync BufReader = 121 ::
     "FRPL…*3\r\n$3\r\nSET\r\n$8\r\nseed_key\r\n$10\r\nseed_value\r\n
      FRPL…*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
2 x  leftover bytes in fullsync BufReader = 0 :: ""
```

Every failure discarded exactly the two frames the assertion is waiting for; every pass discarded
nothing. Per-second `INFO`/`DBSIZE` sampling during a failure shows the primary's
`master_repl_offset` and the replica's offset advancing in lockstep 81 B apart (`81/0`,
`118/37`, `155/74`, …), which is the fingerprint of a lost prefix rather than a stalled link.

### Why persistence-only

The RocksDB checkpoint path is a multi-file transfer that takes long enough for the primary's
next writes to land in the same `BufReader` fill; the live-dataset path is a single small blob
that is normally drained before the seed write exists. Same latent bug in both — the in-memory
case is simply much harder to hit (0/12 failures under the same load, see below). Nothing about
the defect is persistence-specific.

## Repro

Deterministic-ish, ~50 % on a 10-core M-series macOS laptop; no testbox needed.

1. `cargo nextest list -p frogdb-server -E 'test(test_broadcast_lag)'` to build `main-*`.
2. Start 24 shell CPU burners (`( while :; do :; done ) &`) — 2.4x oversubscription.
3. Run 4 concurrent copies of
   `main-<hash> --exact
   integration_replication::test_broadcast_lag_disconnect_and_resync::case_2_with_persistence`
   per iteration.

Measured rates:

| variant | runs | failures | rate |
|---|---|---|---|
| `case_2_with_persistence`, unmodified | 24 | 13 | 54 % |
| `case_2_with_persistence`, instrumented | 8 + 8 | 6 + 6 | 75 % |
| `case_1_in_memory`, unmodified | 12 | 0 | 0 % |
| `test_writes_during_full_sync_are_not_lost` | 12 | 0 | 0 % |
| `case_2_with_persistence`, with the prototype fix below | 20 | 0 | 0 % |

In isolation the test passes in ~2.5 s.

## Fix direction (validated, not landed)

Carry the bytes the full-sync reader buffered past the trailer into the streaming loop instead of
dropping them. Prototyped as: capture `reader.buffer().to_vec()` at the end of
`receive_checkpoint` / `receive_snapshot` into a `pending_stream_bytes` field on
`ReplicaConnection`, and seed `stream_replication`'s `BytesMut` with it (decoding + acking the
frames it already contains) before the first socket read. 20/20 clean under the load that failed
13/24 without it.

A better-factored variant is to give the connection one long-lived buffered reader for the whole
session rather than per-phase `BufReader`s, so the handoff cannot lose bytes by construction —
`read_resp_line` already reads byte-by-byte precisely to dodge this hazard during the handshake,
which is the same problem solved locally in a third place.

Either way the fix needs its own regression coverage, because none of the existing tests can see
it:

- `replica_session::tests::full_sync_replays_writes_made_during_handoff` (FM-REPLICATION-004) is
  primary-side only — it proves the frames go *out*.
- `test_writes_during_full_sync_are_not_lost` tolerates the loss because the replica reconnects
  and heals from the backlog before the assertion.

Suggested guard: drive a replica-side receive with the payload trailer and the first live frames
delivered in a single write, and assert the frames are applied and acked. That belongs as a new
replica-side row under FM-REPLICATION-004 in `.scratch/hardening/specs/replication-failure-modes.md`
("the granted offset never runs ahead of the payload" currently covers only the sending half).

## Next steps

- Land the receive-side fix + regression test with the sequenced replication product work
  (phase 2c/3), then re-run the repro above to confirm 0/24.
- Extend FM-REPLICATION-004 (or add a sibling row) to cover the replica's receive→stream handoff.
- Leave the test's assertion exactly as it is: it is the only thing that caught this.
