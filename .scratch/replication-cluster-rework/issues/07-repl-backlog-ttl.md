# 07 — the backlog has no TTL: a replica-less primary buffers writes forever

Status: ready-for-agent

## What to build

`PartialSyncReplay::new` arms the backlog floor whenever the recovered offset is non-zero, and
`begin_primary_stint` arms it at the promotion boundary. Once armed, every write is pushed into the
ring buffer for the rest of the process's life — whether or not any replica has ever connected, and
whether or not one ever will. A restarted standalone primary that once had a replica (so it
recovered a non-zero offset) pays ring-buffer memory and push cost on the write path forever, for
resume history nobody can use.

Redis bounds exactly this with **`repl-backlog-ttl`** (default 3600s): the backlog is freed after
that many seconds with zero connected replicas, and `master_repl_offset` keeps advancing without
it. Valkey inherits the same knob. Freeing it is safe because a resume request below the floor
already degrades to a full resync — the TTL only decides how long the cheap path stays available.

Proposed analogue:

- Config parameter `replication-backlog-ttl-secs` (0 = never free), plumbed like the other live
  mutable replication params (golden-config entry, `CONFIG SET`-able).
- Track "last moment the replica count fell to zero"; when the TTL elapses with the count still
  zero, `reset_backlog()` — which already exists (`ReplicationRingBuffer::reset`, used at both ends
  of a primary stint) and correctly disarms the floor, so a later `PSYNC` gets `+FULLRESYNC`
  instead of a `+CONTINUE` over history that was dropped.
- The offsets themselves must not move: freeing the backlog is not a stint change and must not
  touch the replid, `second_repl_offset`, or the live/applied heads.

## Acceptance criteria

- [ ] `replication-backlog-ttl-secs` exists, defaults to a Redis-comparable 3600, 0 disables
- [ ] With zero replicas for longer than the TTL, the ring buffer is emptied and the floor disarmed
- [ ] A replica connecting after the free gets `+FULLRESYNC`, never a `+CONTINUE` over a hole
- [ ] A replica reconnecting *before* the TTL still gets its `+CONTINUE`
- [ ] The timer resets whenever a replica is connected; a replica connected the whole time never
      triggers a free
- [ ] Freeing the backlog does not change the replication id or any offset

## Source

Adversarial review of `promotion-replid-psync.md` — the cost side of finding LOW-7 (2026-07-28).
LOW-7 itself (arming the floor at a recovered non-zero offset) was upheld as correct: the floor is
what stops a restarted node from claiming resume history its empty buffer does not hold. This issue
covers the price that decision charges when no replica ever arrives.
