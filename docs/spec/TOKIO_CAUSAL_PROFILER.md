# Tokio Causal Profiler — Design Document

A causal profiler for async Rust, adapting the [coz](https://github.com/plasma-umass/coz) algorithm to operate at tokio task/span granularity instead of OS thread/source-line granularity.

---

## 1. Overview

### What is Causal Profiling?

Traditional profilers show where time is *spent*, but not where improvement *matters*. A function consuming 30% of CPU might contribute nothing to end-to-end latency if it runs in parallel with a slower critical path.

Causal profiling answers: **"What if this code were X% faster — how much would end-to-end performance improve?"**

### How Coz Works

[Coz](https://github.com/plasma-umass/coz) implements causal profiling for native threads:

1. **Pick a target source line** via debug info
2. **Virtually speed it up** by pausing all *other* OS threads whenever a thread executes the target line
3. **Measure progress points** — end-to-end metrics like request completion or throughput counters
4. **Repeat** with random speedup percentages (0–100%) to build a causal profile

The key invariant: **delaying other threads by X% has the same relative effect as speeding up the target by X%.** This works because wall-clock time is genuinely wasted — delayed threads sit idle, shifting the relative timing between target and non-target code.

### Why Coz Doesn't Work with Tokio

Coz operates at the OS thread level. Tokio uses M:N scheduling — many async tasks multiplexed onto a few worker threads. This creates a fundamental mismatch:

- When coz pauses "other threads," it pauses *all* tasks on those threads, not just unrelated work
- Virtually speeding up a GET handler also pauses threads running unrelated SET handlers
- The causal relationships coz infers are confounded by task interleaving on shared worker threads
- Progress points can't cleanly map to individual request types

Coz can still profile synchronous CPU-bound code outside the async runtime (`spawn_blocking`, RocksDB operations, shard data structure manipulation), but these are a small slice of overall execution in a tokio-based server like FrogDB.

---

## 2. Goal

A causal profiler that answers: **"What if all polls of `handle_get` (or any tracing span) were X% faster — how much would end-to-end throughput/latency improve?"**

This operates at task/span granularity rather than thread/source-line granularity, providing actionable insight for optimizing an async server.

---

## 3. Core Mechanism

The critical design choice is how to "delay" non-target tasks. There are two options, and only one preserves the coz invariant.

### `tokio::time::sleep` (task yields) — BREAKS THE INVARIANT

The task yields to the executor, which picks up other tasks on the same worker thread. No wall-clock time is wasted — the thread stays busy doing other work. You're not making the system relatively slower; you're just rescheduling.

If task A's code got genuinely faster, the thread would be freed *sooner* for tasks B/C/D — the opposite of delaying them.

### `std::thread::sleep` (thread blocks) — PRESERVES THE INVARIANT

The OS thread actually stalls. No task runs on it. Wall-clock time *is* wasted, just like coz's thread delays. The relative timing shift is real.

This is the cardinal sin of async Rust — blocking a worker thread. But for a profiler that's *intentionally distorting timing to measure causal effects*, it's exactly correct. The overhead is the point.

**The profiler must use `std::thread::sleep`, not `tokio::time::sleep`.**

---

## 4. Design Sketch

### `CausalInstrumented<F>` Wrapper Future

```rust
struct CausalInstrumented<F> {
    inner: F,
    span_id: SpanId,
}

impl<F: Future> Future for CausalInstrumented<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // Before polling: check if this task should be delayed
        // (because another task is in the "speed up" region)
        let delay = GLOBAL_PROFILER.check_delay(self.span_id);
        if delay > Duration::ZERO {
            std::thread::sleep(delay); // Intentionally block the OS thread
        }

        // Poll the inner future, tracking entry/exit of the target region
        GLOBAL_PROFILER.enter_task(self.span_id);
        let result = self.inner.poll(cx);
        GLOBAL_PROFILER.exit_task(self.span_id);

        result
    }
}
```

Delays are injected at task poll boundaries. When a task enters the target span, the global profiler signals all worker threads to accumulate delays on non-target task polls.

### Global Profiler Coordinator

The profiler maintains global state coordinating the experiment:

- **Current target span**: which tracing span is being "virtually sped up" this experiment round
- **Speedup percentage**: randomly sampled 0–100% for each experiment
- **Delay counter**: when a task in the target span is polled, increment a global counter; non-target tasks check this counter and sleep proportionally
- **Progress point measurements**: track throughput/latency counters to measure the effect of each virtual speedup

Coordination follows coz's approach: a global atomic counter that non-target tasks check before each poll.

### Experiment Loop

Adapted from coz's experiment structure:

1. Select a target span (e.g., `handle_get`, `wal_write`, `snapshot`)
2. Select a random virtual speedup percentage
3. Run the experiment for a measurement window
4. Record progress point measurements
5. Repeat with different speedup percentages
6. Move to the next target span
7. After all spans are profiled, output the causal profile: for each span, the relationship between virtual speedup and progress improvement

---

## 5. Key Differences from Coz

| Aspect | Coz | Tokio Causal Profiler |
|--------|-----|----------------------|
| Granularity | Source line | Tracing span / poll boundary |
| Target identification | Debug info (DWARF) | `tracing` span names |
| Scheduling level | OS thread | Async task |
| Delay injection | Arbitrary points within thread execution | Between polls only |
| What it answers | "What if this line were faster?" | "What if all polls of this span were faster?" |

The span-level granularity is actually *more useful* for an async server than line-level. The question "what if `handle_get` were 30% faster?" is more actionable than "what if line 247 of store.rs were 30% faster?" — it maps directly to optimization targets.

The tradeoff is that you cannot profile *within* a single poll. If a poll does expensive synchronous work internally, you'd need to break it into smaller spans or use traditional profiling to drill down.

---

## 6. Pitfalls & Open Questions

### Causal Validity Under M:N Scheduling

**This is the fundamental hard problem — theoretical, not just engineering.**

Coz's causal inference proof assumes independent threads: delaying thread X reproduces the relative effect of speeding up thread X. With M:N scheduling, this assumption breaks:

- Tasks share threads — delaying one task's poll delays all tasks queued behind it on the same worker
- Work-stealing rebalances tasks unpredictably after delays
- The cascading effects may confound causal measurements

Whether the coz invariant still holds under these cascading effects is an open research question. Nobody has proven this — [SCOZ](https://onlinelibrary.wiley.com/doi/full/10.1002/spe.2930) extended coz to multi-process systems but not async runtimes. A rigorous answer here is potentially publishable.

### Work-Stealing Migration

A task might migrate between worker threads mid-execution. Delay accounting must follow the *task*, not the *thread*. This is solvable (attach delay state to the `CausalInstrumented` wrapper) but adds bookkeeping complexity.

### Granularity Limitations

Cooperative scheduling means you can only inject delays between polls, not at arbitrary points within a poll. If a task does 10ms of synchronous work in a single poll, you cannot profile sub-portions of that work with this technique — you'd need traditional profiling (flamegraphs, perf) to drill down.

### Coordination Overhead

When a task enters the target span, the profiler must signal all worker threads. The global atomic counter approach from coz should work, but the overhead of checking it on every poll needs to be measured. In a system doing millions of polls/second, even a few nanoseconds of overhead per check matters.

---

## 7. Effort Estimates

| Scope | Estimate | Notes |
|-------|----------|-------|
| Proof of concept | 2–4 weeks | Task delays + measurement, no correctness guarantees. Requires comfort with tokio internals and the coz paper. |
| Research-quality implementation | 3–6 months | Validated causal inference for M:N scheduling. Potentially publishable. |
| Production-quality tool | 6+ months | Robustness, low overhead, ergonomic API, documentation. |

---

## 8. References

- **Coz: Finding Code that Counts with Causal Profiling** — [Paper](http://arxiv.org/abs/1608.03676), [Implementation](https://github.com/plasma-umass/coz), [Rust bindings](https://github.com/alexcrichton/coz-rs)
- **SCOZ: A System-level Causal Profiler** — [Paper](https://onlinelibrary.wiley.com/doi/full/10.1002/spe.2930) (extends coz to multi-process, not async)
- **tokio-metrics** — [`TaskMonitor`](https://docs.rs/tokio-metrics/latest/tokio_metrics/struct.TaskMonitor.html) for per-task busy/idle/scheduled timing
- **tracing** — [Tokio tracing guide](https://tokio.rs/tokio/topics/tracing) for span-based instrumentation
- **Community discussion** — [Thoughts about profiling Rust/Tokio applications](https://users.rust-lang.org/t/thoughts-about-profiling-rust-tokio-applications/120069)
- **Async profiling in practice** — [Async Rust in Practice: Performance, Pitfalls, Profiling](https://www.scylladb.com/2022/01/12/async-rust-in-practice-performance-pitfalls-profiling/)
- **tracing-coz** — [Implementation](https://github.com/GregBowyer/tracing-coz) (abandoned, 2019). Translates tracing spans to `coz::begin!`/`coz::end!` and events to `coz::progress!`, but is a thin shim — coz still profiles at the OS thread level, so the M:N scheduling problem remains unsolved. Cannot compose with other tracing layers (implements raw `Subscriber`, not `Layer`).
