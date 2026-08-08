# roadmap — future work and unimplemented designs

State: active

Roadmap material migrated from the retired top-level `todo/` directory (2026-08-08).
Everything here is **future/unimplemented** work — none of it describes current behavior
(the doc-sync skill treats this directory as out of scope for drift checks).

| path | what |
|---|---|
| [CLUSTER_REBALANCING.md](CLUSTER_REBALANCING.md) | high-throughput slot migration + auto-rebalancing (Valkey 9-style) |
| [SPLIT_BRAIN_REPLAY.md](SPLIT_BRAIN_REPLAY.md) | divergent-write replay after partition heal: `SPLITBRAIN` command, CLI tool |
| [ROLLING_UPGRADE.md](ROLLING_UPGRADE.md) | zero-downtime rolling upgrades — partially implemented, audit before resuming |
| [NEW_FEATURES.md](NEW_FEATURES.md) | competitive analysis + unimplemented feature proposals |
| [POTENTIAL.md](POTENTIAL.md) | speculative ideas extracted from spec docs |
| [optimizations/](optimizations/INDEX.md) | perf roadmap: io_uring, arena allocator, SIMD, single-shard mode |
| [compat/](compat/INDEX.md) | Redis 8.6.0 compat — two deferred areas (14d/14e) + permanent exclusions record |

Not an issue tracker: these are design docs, not `Status:`-tracked issues. When work starts
on one, open a `.scratch/<feature>/` directory per
[`docs/agents/issue-tracker.md`](../../docs/agents/issue-tracker.md) and link back here.

The retired `todo/` directory's implemented material (62 architecture proposals across 7
rounds, the compat action-item breakdown, historical audits) lives in git history — see the
commit that removed `todo/`.
