# Solicited ACK awaits inside the frame-decode loop

Status: done
Type: AFK
Origin: adversarial review of wait-cluster-mode implementation (2026-07-28)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: replication

Resolution — the wait moved off the decode path entirely (the first of the two fix directions),
and lost its bound in the process. `drain_frames` no longer awaits anything on a GETACK: it
records the solicited offset in a `pending_ack: Option<u64>` the caller owns, and
`stream_replication` answers it from a third `select!` branch (`solicited_ack`), so the
socket-read branch and the ACK-tick branch stay pollable for as long as the answer is owed.
Two follow-on decisions: (a) a second GETACK arriving while one is owed *raises the target*
rather than queueing a second answer — ACKs are cumulative, so one ACK at the newer offset
answers both; (b) `AppliedOffset::wait_until_applied` dropped its `timeout` parameter and is now
unbounded and cancel-safe. The timeout only existed because the inline wait suppressed the
spontaneous ACK tick, so a wedged applier would otherwise have gone silent; off the decode path
the cadence keeps firing and carries exactly the truthful low offset the timeout would have made
the solicited ACK carry, so a bound would add a duplicate ACK and nothing else. Spec:
**FM-REPLICATION-006** (and the spec's scope note now covers the steady-state link, not only the
full-sync payload path).
Tests: `a_solicited_ack_does_not_stall_the_decode_loop` (red before the fix: the frame queued
behind the GETACK arrived a full cadence late), `a_second_getack_raises_the_target_to_the_newer_offset`
(red before: the answer to the first solicitation was a stale ACK at the pre-catch-up offset),
`a_solicited_ack_is_sent_as_soon_as_the_applier_catches_up`,
`the_ack_cadence_survives_a_solicitation_that_can_never_be_answered` (`replica/streaming.rs`, on a
new duplex `Link` harness that runs no frame consumer, so the applied head only moves when the test
moves it); `wait_until_applied_returns_as_soon_as_the_applier_catches_up`,
`wait_until_applied_parks_when_the_applier_can_no_longer_advance` (`replica/offset.rs`).

## Problem

`replica/streaming.rs` answers a solicited `REPLCONF GETACK` by awaiting
`wait_until_applied(..., self.ack_interval)` inline in the frame-decode `select!` loop.
While that wait is pending (applier stalled, up to `ack_interval` = 1s default), the loop
is not polling the socket and the spontaneous ACK tick does not fire: socket reads stop,
backpressure propagates to the primary, and the periodic ACK cadence hiccups.

Correctness-safe (the ACK is still eventually correct and carries the applied offset);
throughput/latency wart under `WAIT` + slow applier.

## Fix direction

Spawn the solicited-ACK wait onto a side task (ordered with other ACKs via the existing
ACK path), or cap the inline wait well below the ACK cadence so the decode loop never
stalls for a full interval.
