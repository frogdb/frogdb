# `INFO server` and `INFO cpu` report fabricated runtime facts (uptime 0, CPU 0, tcp_port 6379)

Status: ready-for-agent
Type: bug (introspection accuracy)
Area: info

## Problem

Both INFO renderers emit constants where a live value exists. This is the exact failure mode
[ADR-0005](../../../../adr/0005-truthful-redis-86-surface.md) ruling 3 forbids — a fabricated
answer, not a modest one.

| field | emitted | real source |
|---|---|---|
| `uptime_in_seconds`, `uptime_in_days` | `0` | `server/src/server/subsystems.rs:78` holds `start_time = clock::now()` |
| `used_cpu_sys`, `used_cpu_user`, `*_children`, `*_main_thread` | `0.000000` | `getrusage(RUSAGE_SELF/RUSAGE_CHILDREN)` |
| `tcp_port` | `6379` literal | the bound listener port in config |
| `executable` | `/usr/local/bin/frogdb` literal | `std::env::current_exe()` |
| `config_file` | `""` | the config path the server was started with |
| `lru_clock` | `0` | the clock seam already used for `idle`/`age` |

Sites (both renderers carry the same literals — see [issue 28](../../../redis-feel/issues/) for
why the duplication keeps producing this bug):

- `frogdb-server/crates/server/src/info/sections.rs:97-107` (`ServerSection`), `:517-531`
  (`CpuSection`)
- `frogdb-server/crates/server/src/commands/info.rs:208-216` (server block), `:558-566`
  (`build_cpu_info`)

Impact is not cosmetic: `uptime_in_seconds` is how every Redis dashboard detects a restart, and
`used_cpu_*` is a standard `redis_exporter` scrape. A permanent `0` reads as "just restarted,
using no CPU" forever.

`run_id` is also a constant, but its truthful value needs a ruling — tracked in
[issue 29](../../../redis-feel/issues/), not here.

## Ruling

Report the real value for every row in the table. `used_cpu_*_children` is honestly `0` on a
server that forks no children — keep it, and keep `used_cpu_*_main_thread` only if an honest
per-thread number is obtainable; otherwise drop the field rather than echo the process-wide one.

## Acceptance criteria

- [ ] `uptime_in_seconds` advances with wall time and `uptime_in_days` derives from it
- [ ] `used_cpu_sys`/`used_cpu_user` are non-zero after sustained traffic and monotonic
- [ ] `tcp_port` matches the actually-bound port when the server runs on a non-6379 port
- [ ] `executable`/`config_file` match how the process was launched
- [ ] Regression test pins each field against its live source, not against a literal — a test that
      asserts `uptime_in_seconds:0` is the bug, not the contract
- [ ] Both renderers are covered (or the duplication is removed per issue 28)

Size: S-M
