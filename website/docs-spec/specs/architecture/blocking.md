# Spec: architecture/blocking.md
Status: update
Audiences: A2, A3, A5

Goal: A reader walks away understanding how FrogDB implements blocking commands
(BLPOP, BRPOP, BLMOVE, BZPOPMIN/MAX, and stream blocking) in a shared-nothing
architecture and *why* the constraints exist: each shard owns a per-shard wait
queue, a blocking command first attempts the operation and only registers a
waiter if no data is available, wakes are FIFO per key and respect BLPOP key
order, blocking is rejected inside MULTI/EXEC and Lua (blocking there would
freeze the shard's queue/event loop), and cross-shard blocking is disallowed
(`-CROSSSLOT`) because coordinating a wait across independently owned shards
would defeat the shared-nothing model — the intended escape hatch is hash tags.
The reader also learns the honest deltas from Redis/Dragonfly, including the
slot-migration unblock behavior.

Not in scope: General concurrency/routing (concurrency.md); VLL locking used by
multi-key blocking wake evaluation (vll.md); the blocking sequence diagram
(request-flows.md §4). Link, don't restate.

Sources of truth (author must read and reconcile every claim):
- `frogdb-server/crates/core/src/shard/wait_queue.rs` — `ShardWaitQueue`,
  `WaitEntry` (fields: conn_id, keys, op, deadline, response channel), FIFO
  wake, and the real set of blocking ops (note it includes stream blocking such
  as `BlockingOp::XReadGroup`/XREAD, beyond the list/zset ops the page shows).
- `frogdb-server/crates/core/src/shard/blocking.rs` and
  `.../dispatch_blocking.rs` — registration/notification flow, wake-on-write.
- `frogdb-server/crates/core/src/types.rs` (or wherever `BlockingOp` is defined)
  — the authoritative `BlockingOp` variants.
- `frogdb-server/crates/core/src/shard/message.rs` — `BlockWait`,
  `UnregisterWait`, and the `BlockingNeeded` response used to hand off from the
  execute attempt to the wait registration.
- `frogdb-server/crates/config/src/` — idle-timeout / tcp-keepalive config names
  for the timeout table.
- Slot-migration unblock path (cluster) — confirm the `-MOVED`-on-migration
  behavior in the cluster/replication code before presenting it as implemented.

Existing content: `website/src/content/docs/architecture/blocking.md`. Good
rationale and edge-case coverage; the comparison and "fixed Redis bug" tables
make claims about other systems that need careful handling.

Structure (keep existing H2s):
- ## Industry Comparison — Redis/Valkey vs DragonflyDB vs FrogDB table. Verify
  **FrogDB's own** columns against source; treat the Redis/Dragonfly columns as
  external claims (see Drift guards).
- ## Architecture — per-connection blocking state, challenges, per-shard wait
  queue. Reconcile `BlockedConnection`/`WaitEntry`/`BlockingOp` shapes with
  source; expand `BlockingOp` to include stream-blocking ops if present.
- ## Registration and Notification Flow — the attempt-then-register sequence and
  wake-on-write; verify message names.
- ## Cross-Shard Blocking — not supported; `-CROSSSLOT`; hash-tag workaround.
- ## Connection State Integration — NORMAL↔BLOCKED; PING allowed, QUIT cancels.
- ## Command Interaction Matrix — MULTI/EXEC and Lua rejection with rationale.
- ## Edge Cases — key deleted/recreated, type change (`-WRONGTYPE`), FIFO on
  same key, idle-timeout exemption.
- ## Cluster Mode: Slot Migration Interaction — unblock-with-`-MOVED`.

Generated data: None.

Drift guards:
- **Separate FrogDB claims from external-system claims.** The comparison table
  and the "Redis (broken) vs FrogDB (fixed)" slot-migration table assert things
  about Redis, Valkey, and DragonflyDB. FrogDB's own behavior must be verified
  against source and stated confidently. Claims about *other* systems must be
  either (a) attributed and version-qualified (Redis/Valkey/Dragonfly behavior
  changes across versions) or (b) softened to describe FrogDB's behavior without
  asserting a competitor is "broken". Do not present the "fixes a known Redis
  bug" framing unless it can be attributed to a specific issue/version; prefer
  "FrogDB unblocks clients with `-MOVED` on slot migration" as a statement about
  FrogDB.
- **`BlockingOp` completeness.** The page lists only BLPop/BRPop/BLMove/
  BZPopMin/BZPopMax. Verify against the real enum and add stream-blocking ops
  (e.g. XREAD/XREADGROUP blocking) if they exist, or the page understates the
  feature.
- **Struct/message shapes.** `BlockedConnection`, `WaitEntry`, `ShardWaitQueue`,
  and the `BlockWait`/`UnregisterWait`/`BlockingNeeded` messages must match
  source; treat the current code blocks as illustrative and re-derive them.
- **Timeout table.** Idle-timeout exemption for blocked clients and tcp-keepalive
  behavior must match the connection/config code; verify config key names.
- **No unbacked numbers.** No latency figures; any timeout defaults must match
  the config crate.
- **S7 code-path check.** Any `crates/...` path named must exist; include this
  page in the S7 scan.
