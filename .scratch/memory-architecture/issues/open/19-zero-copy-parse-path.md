# 19: zero-copy parse path

Status: ready-for-human
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R12
Area: frogdb-protocol (command.rs) + frogdb-server (connection/, dispatch)
Phase: 5 — network memory

## Why

RESP frame decoding is already zero-copy into the read buffer, and then
`ParsedCommand::try_from` throws that away: `frogdb-server/crates/protocol/src/command.rs:60`
and `:64` call `Bytes::copy_from_slice` on every argument — one memcpy plus one heap
allocation per arg, per command, on the hottest path in the server. A SET with a 1MB value
copies the megabyte before the shard ever sees it.

R12's ruling: args become refcounted slices of the leased read buffer
([issue 18](../)'s pool lease is designed for this
handoff). The command borrows the network buffer until it completes; the buffer returns to
the pool when the last slice drops.

**Fan-out constraints from the Linux validation** ([spike-report-linux.md](../../spike-report-linux.md)):
cross-core costs are real and measured — ~6 µs of CPU per distinct foreign core touched, and
cross-thread p99.9 at 512 clients measured 11× the colocated figure (park/unpark stall on the
foreign core's queue). The levers that follow: **one hop per distinct core** (batch all keys
for a foreign core into one message, never per-key hops), **cap fan-out width** (a command
touching all N cores serializes ~6N µs of CPU — wide MGETs should degrade to bounded-width
waves), and batching per foreign core is the lever that moved tails. Zero-copy interacts with
this: a borrowed slice handed to a foreign core pins the origin core's buffer for the hop's
duration, so the fan-out path must bound how long and how much it pins.

## What to build

### 1. Refcounted arg slices

`ParsedCommand` args become slices of the pooled read buffer — concretely: the pool lease
(issue 18) yields a refcount-capable buffer (`Bytes`-style shared handle over the leased
region); `try_from` slices it (`Bytes::slice_ref` shape) instead of copying. The buffer's
return-to-pool triggers when frames and all arg slices drop. Pipelined commands sharing one
read buffer each hold refs into it — a deliberately slow command in a pipeline pins the whole
buffer; acceptable, bounded by output-limit-style accounting if it shows up in practice.

### 2. Copy at the two escape points

Two places must own their bytes:

- **Cross-slot / cross-core hop**: args shipped to a foreign core are copied out at the hop
  boundary (into the hop message), so a foreign core never holds a ref into another core's
  pool and the origin buffer unpins at hop-send. This is also the correctness boundary for
  pool locality (per-core pools, no cross-core return).
- **Blocking commands**: a command that parks (BLPOP, XREAD BLOCK, WAIT) copies out the args
  it retains before parking — a parked command must not pin a network buffer for seconds.
  Copy in the park path (`connection/blocking.rs`, coordinator), not in parse.

Everything else — the ~99% case of a single-shard immediate command — runs copy-free from
socket to shard op.

### 3. Fan-out shaping (the Linux findings, made code)

While in the dispatch path, implement the two rules the spike measured:

- **One message per distinct foreign core** per command (group keys by core before
  dispatching; single wake per core). If dispatch already does this, assert it with a test
  and a counter metric (`hops per command` histogram).
- **Cap fan-out width**: config-capped concurrent foreign-core waves for wide multi-key
  commands (default cap ~4 outstanding cores; measure). Cite the ~6 µs/core CPU figure and
  the 11× p99.9 in the config's doc comment so the number has provenance.

### 4. Keyspace write boundary

Values written into the keyspace are copied (or re-encoded into value blocks, phase 4) at the
store boundary — storage never aliases network buffers. That boundary already exists (today's
copy in `try_from` just does it too early); the assertion to add: no `Bytes` rooted in a pool
lease is ever stored in a `Value`.

## Acceptance criteria

Landed (each with its forcing test, most tagged `FM-MEMORY-003` in `specs/memory.md`):
zero-copy `try_from` (`test_try_from_is_zero_copy`); escape-point copies at the keyspace
install seam, blocking park, scatter partition, foreign-core hop, and collection-internal
retention points (quicklist plain nodes, stream group/consumer/PEL names), all through the
single `frogdb_protocol::detach_bytes` chokepoint; hop batching K shards → K messages
(`a_scatter_over_k_shards_sends_exactly_k_lock_messages`,
`partition_batches_keys_per_shard_and_detaches_them`); pipelined-completion pool property
test (`a_pipelined_burst_of_slices_releases_the_lease_only_when_the_last_drops`; no unsafe
anywhere in the handoff, so no miri/loom needed).

Remaining — needs the Linux rig and a human decision, hence ready-for-human:

- [ ] **Fan-out width cap** (~4 concurrent foreign-core waves, config-capped, with the
      spike's ~6 µs/core and 11× p99.9 provenance in the doc comment). Touches the LOCKED
      vll/txn dispatch path (gate 0.90) and needs an atomicity ruling: bounded-width waves
      change when a wide command's locks are requested, which interacts with wound-retry
      fairness. Spec-first work.
- [ ] Bench vs [issue 04](../) baselines: large-value SET/GET throughput improves or
      holds; cross-thread p99.9 at 512 clients does not regress. Needs the issue-04 Linux
      rig; this session ran in local (macOS) mode.

## Test boundary

Level 1 for slice/refcount lifetime (property tests). Level 2 for dispatch batching. Bench
via the issue-04 rig on the Linux box for the tail-latency claims.

## Spec rows at R15

Landed as `FM-MEMORY-003` in `specs/memory.md` (DRAFT discipline: row and forcing tests in
the same commit). [Issue 22](../) inherits it.

## Out of scope

Output-path zero-copy (replies already build into pooled write buffers per issue 18; sendfile
style tricks out), io_uring, changing dispatch's queueing discipline beyond batching/width,
RESP protocol changes.

## Depends on

[Issue 18](../) — hard: the pool lease with
refcount handoff is the foundation. [Issue 02](../) pinning (done) — per-core pools
assume it.
