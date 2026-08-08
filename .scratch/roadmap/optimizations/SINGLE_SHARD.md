# Single-Shard Mode Optimizations

FrogDB defaults to `num_shards=1`. The current architecture applies the full multi-shard machinery
(CRC16 routing, scatter-gather coordination, channel-based message passing, multi-threaded runtime)
even when there's only one shard. This overhead is measurable at high throughput — per-command CRC16
hashing, oneshot channel allocation, unnecessary shard verification loops, and work-stealing
scheduler overhead all add up.

## Tier 1: Runtime Routing Guards

Low-effort conditional checks gated on `num_shards == 1`.

- [ ] **Skip CRC16 in `shard_for_key()`** — Early-return `0` when `num_shards == 1`. Currently
  computes `extract_hash_tag()` + CRC16 XMODEM + double modulo on every key for every command.
  (`crates/core/src/shard/helpers.rs:55`)
- [ ] **Skip scatter-gather shard verification** — `route_and_execute()` iterates all keys computing
  `shard_for_key()` to check `all_same_shard` (`routing.rs:87-89`). With 1 shard, this is always
  true. Short-circuit at the routing level before the key iteration loop.
  (`crates/server/src/connection/routing.rs:85-93`)
- [ ] **Skip AtomicUsize round-robin** — `RoundRobinAssigner::assign()` uses `fetch_add(1, Relaxed)
  % num_shards` per connection. Always 0 when `num_shards == 1`.
  (`crates/server/src/acceptor.rs:38-39`)

## Tier 2: Single-Threaded Tokio Runtime

Replace `#[tokio::main]` with programmatic `tokio::runtime::Builder`. When `num_shards == 1`, use
`new_current_thread()` instead of `new_multi_thread()`.

- [ ] **Programmatic runtime builder** — Switch from `#[tokio::main]` to `Builder` in
  `crates/server/src/main.rs:63`. Select `new_current_thread()` when `num_shards == 1`,
  `new_multi_thread()` otherwise.
  - Eliminates: thread pool creation, work-stealing scheduler, cross-thread task migration, atomic
    contention on shared scheduler state.
  - Caveat: must verify all spawned tasks (replication, cluster, pub/sub) still function correctly
    on the current-thread runtime.

## Tier 3: Channel Hop Elimination (Architectural)

Every command currently pays the cost of a full channel round-trip even when source and destination
are the same task.

- [ ] **Eliminate oneshot channel per command** — Each `execute_on_shard` call allocates
  `oneshot::channel()`, sends via `mpsc`, and awaits the response
  (`crates/server/src/connection/routing.rs:286`). With 1 shard, the connection handler and shard
  worker could share an execution context.
  - `execute_command` takes `&mut ShardWorker` and has a real `.await` at WAL persistence
    (`crates/core/src/shard/execution.rs:125`), so `Rc<RefCell<>>` won't work across await points.
  - `handler.execute()` (line 73) is synchronous — only post-execution bookkeeping (version
    increment, WAL write, waiter satisfaction, replication broadcast) requires `&mut ShardWorker`.
  - Cleanest path: embed connection polling in the shard worker's event loop (DragonflyDB fiber
    model), but this is a significant refactor.
  - Note: VLL overhead is already effectively zero — lazily initialized and only triggered by
    cross-shard scatter-gather which never fires with `num_shards == 1`.
