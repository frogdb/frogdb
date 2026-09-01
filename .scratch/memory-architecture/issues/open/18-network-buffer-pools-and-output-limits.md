# 18: per-core network buffer pools and output-buffer limits

Status: ready-for-agent
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R11
Area: frogdb-server (connection/, frame_io.rs) + frogdb-net + frogdb-memory
Phase: 5 — network memory

## Why

Three related holes, one seam:

1. **Buffers never shrink.** Every connection's `Framed` read/write buffers
   (`connection/frame_io.rs`, `Framed::new(server, FrogDbResp2::default())` at ~230) start at
   the tokio-util default and grow to fit the largest frame ever seen, then keep that capacity
   for the connection's life. 10k idle connections that each once carried a 1MB MGET reply
   hold 10GB nobody can see or reclaim.
2. **No output limit for normal clients.** A pubsub hard limit exists
   (`connection.rs:617`, `core/src/pubsub.rs:40` — matching Redis
   `client-output-buffer-limit pubsub`), but a slow normal client draining a huge reply, or a
   replica-class connection, has no cap and no kill. Redis ships three classes
   (normal 0/0/0, replica 256mb/64mb/60, pubsub 32mb/8mb/60); FrogDB has one.
3. **RESP2 out-bytes invisible.** Write-buffer bytes buffered by `Framed::feed` are charged
   nowhere — `Subsystem::NetworkOutput` exists in the broker
   (`frogdb-server/crates/memory/src/budget.rs:28`, filed by [issue 05](../)) with a
   doc comment promising exactly this, and nothing opens it.

R11's ruling: per-core size-classed pools with lease/return, output charged to a
`NetworkOutput` budget, and one seam where all `client-output-buffer-limit` classes are
enforced.

## What to build

### 1. Per-core size-classed buffer pool

In frogdb-net (or a new module): pools of `BytesMut` in power-of-two classes (4KB–1MB;
larger = unpooled one-off). Per-core, no cross-core sharing — connections are pinned
([issue 02](../)), so the pool is single-threaded and lock-free by construction.
API: `lease(min_capacity) -> PooledBuf`, return-on-drop. **Shrink-when-idle**: returned
buffers above a per-class watermark are freed rather than pooled; a periodic sweep (piggyback
on an existing per-core tick, not a new timer) trims pools toward a low-water target so a
burst does not become a permanent high-water mark.

### 2. Wire the frame path to the pool

Read side: the codec's read buffer leases from the pool and re-leases smaller after bursts
(this is the piece [issue 19](../) builds on — its refcounted arg
slices keep a lease alive until the command completes; design the lease type to support
refcount handoff now, even though 19 lands later). Write side: `Framed::feed` buffering
replaced/backed by pooled buffers. Touch points: `connection/frame_io.rs`, `connection.rs`,
`connection/blocking.rs`, `migrate.rs` (all current `Framed` users).

### 3. `NetworkOutput` budget

Each core's broker opens `Subsystem::NetworkOutput` (`Disposition::Shed`). Every buffered
out-byte holds a `Charge`; the per-connection figure feeds `CLIENT LIST`/`CLIENT INFO`
(`obl`/`oll`/`omem` fields) and the broker breakdown feeds INFO/metrics. This closes the
RESP2 invisibility: charge at feed time in the frame path, not per-protocol-version.

### 4. One enforcement seam: `client-output-buffer-limit` classes

A single function on the write path decides, from (class, buffered bytes, soft-limit timer):
keep / disconnect. Classes: normal, replica, pubsub — with Redis's defaults and config surface
(`client-output-buffer-limit <class> <hard> <soft> <soft-seconds>`). The existing pubsub
enforcement in `connection.rs`/`pubsub.rs` migrates into this seam (delete the special case).
Replica connections: coordinate with the replication feed's existing hold-buffer accounting
(`Subsystem::ReplicationBacklog` covers the backlog ring; the *per-replica* output class is
this seam's job — do not double-charge the same bytes).

## Acceptance criteria

- [ ] Pooled, size-classed per-core buffers with lease/return and idle shrink; a test drives a
      burst then asserts capacity returns to the low-water target.
- [ ] A connection that once buffered a large reply does not retain that capacity while idle.
- [ ] `Subsystem::NetworkOutput` opened and charged; broker breakdown and
      `frogdb_memory_budget_*` metrics show out-bytes under load (RESP2 and RESP3 both).
- [ ] `CLIENT INFO` `omem` nonzero for a slow-drain client.
- [ ] All three output classes enforced at one seam; pubsub behavior unchanged
      (existing tests), replica and normal classes get new limit tests (slow reader hits hard
      limit → disconnect; soft limit + timer → disconnect after window).
- [ ] Budget-growth seam lint (issue 05's ratchet) count goes *down* — the frame-path entries
      leave the allowlist.
- [ ] `just test frogdb-server` green; no throughput regression on the existing bench
      (compare against [issue 04](../) baselines).

## Test boundary

Level 1 for the pool (single-threaded, trivially testable). Level 2/3 for limit enforcement —
slow-reader tests need a real socket pair with an unread server-side send; the turmoil
harness or the existing blocking-client test rig should cover it.

## Spec rows at R15

Yes — this issue creates rows [issue 22](../) must capture:
output-buffer class enforcement (per class: at hard limit, connection is disconnected, bytes
released, metric incremented) and the shed disposition of `NetworkOutput`. Draft them into
`specs/memory.md` (DRAFT) while building; 22 locks them.

## Out of scope

Zero-copy parse ([issue 19](../)), TLS buffer internals, kernel-level
tuning (SO_SNDBUF), replication backlog sizing (already budgeted), migrate-path streaming
rework beyond swapping its buffers to the pool.

## Depends on

[Issue 05](../) (broker + `NetworkOutput` subsystem — done), [issue 02](../)
(per-core pinning — done). No phase-3/4 dependencies.
