# FrogDB (Sunbird?)- Agent Context

FrogDB is a Redis-compatible memory-first database written in Rust. Its goals are to to be correct,
very very fast/efficient, memory safe, durable, scalable, and easy to operate in the wild. It intends to supplant prior
solutions by trying to be better in every way.

It is wire-compatible with Redis v6+ (RESP2 + RESP3 compatible) so you can use it with existing
Redis clients. [Planned] If you don't need redis compatibility It includes a custom high-performance network protocol with clients for major languages built around

[Planned] It includes a migration mode + tooling to migrate your existing Redis/Valkey/DragonflyDB
deployment to a FrogDB deployment with zero or close-to-zero downtime with no data loss.

It supports many configurable options for performance tuning with sensible defaults.

Operationally it supports Prometheus as well as OpenTelemetry metrics, tracing, and logging.

To manage FrogDB in cluster mode you can use a Kubernetes operator, TKTK.

## Goals

- _Correctness_: clear guarantees about consistency and failure modes that are verified using
  comprehensive testing, including Jepsen(tm) testing, as well as extensive testing of Redis
  compatibility.
- _Fastness/Efficiency_: Extensive benchmarking to ensure the performance cost of every change or
  feature detail across memory, compute and I/O are understood and kept to a minimum.
- _Memory/Thread Safety_: Using Rust while avoiding usages of `unsafe` as much as possible to minimize
  bugs/crashes and security vulnerabilities.
- _Durability_: supports multiple modes of persistence that are tunable to balance performance and safety
  depending on use case.
- _Scalable_: Built with clustered operation in mind from the start. Scales vertically with additional cores.
- _Easy to operate_: Provide those responsible for running the software with the information they need
  to diagnose problems and take action to resolve them as much as possible. Integrate with existing
  CNCF and other ecosystems to make integration easy. Online cluster resizing, recovery tools, and
  more.
