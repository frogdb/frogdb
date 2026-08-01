# Solicited ACK awaits inside the frame-decode loop

Status: needs-triage
Type: AFK
Origin: adversarial review of wait-cluster-mode implementation (2026-07-28)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: replication

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
