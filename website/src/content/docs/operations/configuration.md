---
title: "Configuration"
description: "FrogDB's four configuration surfaces — TOML file, CLI flags, environment variables, and runtime CONFIG SET — and how they combine."
---

FrogDB reads configuration from four surfaces: a TOML file, `frogdb-server` command-line
flags, `FROGDB_`-prefixed environment variables, and the runtime `CONFIG GET`/`CONFIG SET`
commands. This page explains each surface, how they combine at startup, and which
parameters can change while the server is running. For the exhaustive parameter list —
every field's type, default, and runtime-mutability — see
[Configuration reference](/reference/configuration/).

## The four surfaces

| Surface | How you set it | Applies | Typical use |
|---|---|---|---|
| TOML file | `--config PATH`, or implicit `frogdb.toml` in the working directory | Startup only | The primary way to configure a deployment |
| CLI flags | `frogdb-server --bind 0.0.0.0 ...` | Startup only | One-off overrides, container `command:`/`args:` |
| Environment variables | `FROGDB_SECTION__FIELD=value` | Startup only | Container/orchestrator injection (Docker, Kubernetes, systemd `Environment=`) |
| `CONFIG SET` | `CONFIG SET param value` over the RESP protocol | Runtime, in-memory | Adjusting a live server without a restart |

## Precedence

Everything except `CONFIG SET` is merged once, at startup, by `Config::load` in
`frogdb-server/crates/server/src/config/loader.rs`, which layers the four sources on
[figment](https://docs.rs/figment). Lowest to highest precedence:

1. **Built-in defaults** — the field defaults on `Config`.
2. **TOML file** — `--config PATH` if given, else an implicit `frogdb.toml` in the
   current directory. A missing implicit file is a warning and FrogDB falls back to
   defaults; a missing *explicit* `--config` path is a startup error.
3. **Environment variables** — `FROGDB_`-prefixed, merged over the TOML values.
4. **CLI flags** — applied imperatively after the file/env merge, so a flag always wins
   over the same field set in the file or the environment.

For example, if `server.bind` is `"0.0.0.0"` in `frogdb.toml`, `FROGDB_SERVER__BIND` is
also set in the environment, and `--bind 127.0.0.1` is passed on the command line, the
server binds to `127.0.0.1` — the CLI flag wins.

`CONFIG SET` is a **separate axis**, applied to the already-running process. It does not
participate in this startup merge and does not itself touch the TOML file — it mutates
in-memory state only. A parameter changed with `CONFIG SET` reverts to whatever the
file/env/CLI merge produces the next time the server starts, unless you persist it first
with `CONFIG REWRITE` (see the Runtime configuration section below) or update the
file/environment/CLI invocation yourself.

## TOML configuration file

The file is located with `--config PATH`, or implicitly as `frogdb.toml` in the current
working directory if `--config` is omitted. Top-level sections are kebab-case (`[server]`,
`[persistence]`, `[tiered-storage]`, and so on — see
[Configuration reference](/reference/configuration/) for the full section list). The root
`Config` struct in `frogdb-server/crates/config/src/lib.rs` is annotated
`#[serde(deny_unknown_fields)]`: an unrecognized key or section anywhere in the file
aborts startup instead of being silently ignored, so typos fail fast rather than being
misconfigured at runtime.

A short example:

```toml
[server]
bind = "0.0.0.0"
port = 6379

[persistence]
enabled = true
data-dir = "/var/lib/frogdb/data"
durability-mode = "periodic"
```

This is illustrative, not exhaustive. For a generated, runnable template see
[Example config](/reference/example-config/) (produced by `frogdb-server --generate-config`,
described below); for every field with its type, default, and description see
[Configuration reference](/reference/configuration/).

## Command-line flags

`frogdb-server` accepts a set of CLI flags that override a *subset* of fields at startup —
they are not a mirror of every TOML field. Flags cover the most commonly overridden
startup settings (bind address, port, shard count, log level/format, the admin and HTTP
listener addresses, and the TLS settings, which bundle into a single overrides struct).
The full, current flag list is generated from the `clap` definition in
`frogdb-server/crates/server/src/main.rs` — see
[frogdb-server CLI reference](/reference/frogdb-server/) rather than a hand-maintained
list here, which would drift.

One flag is worth calling out specifically: `--generate-config` prints a template TOML
file to stdout and exits — it does not write a file itself:

```bash
frogdb-server --generate-config > frogdb.toml
```

The emitted template is a hand-maintained string, generated separately from the `Config`
struct's own defaults, and it does not cover every valid section (some sections that are
otherwise fully configurable are simply absent from the template's output). Treat it as a
convenient starting point, not an exhaustive reference — for the complete section and
field list, use [Configuration reference](/reference/configuration/).

## Environment variables

Environment variables are merged over the TOML file, before CLI flags. Variable names
follow a fixed mapping from the TOML path: prefix `FROGDB_`, a double underscore (`__`)
between the section and the field, and a single underscore (`_`) within a field name maps
to a hyphen (`-`) to match the file's kebab-case field names. For example:

| Environment variable | TOML equivalent |
|---|---|
| `FROGDB_SERVER__BIND=0.0.0.0` | `server.bind = "0.0.0.0"` |
| `FROGDB_MEMORY__MAXMEMORY_POLICY=allkeys-lru` | `memory.maxmemory-policy = "allkeys-lru"` |
| `FROGDB_LATENCY_BANDS__ENABLED=true` | `latency-bands.enabled = true` |

This mapping is what makes environment variables convenient for containers and
orchestrators that inject configuration as env vars rather than mounting a file — see
[Deployment](/operations/deployment/) for Docker and Kubernetes examples. (`RUST_LOG` and
the flamegraph output path are read directly from their own environment variables outside
this mapping, and are not part of the `FROGDB_` merge.)

## Runtime configuration (CONFIG GET / CONFIG SET)

Once the server is running, `CONFIG GET`/`CONFIG SET` operate against a fixed registry of
named parameters (`config_param_registry()` in
`frogdb-server/crates/config/src/params.rs`), independent of the startup merge above.
Every registered parameter falls into one of three categories:

- **Runtime-mutable** — `CONFIG SET` changes take effect immediately on the running
  server (for example `maxmemory`, `maxmemory-policy`, `loglevel`, `requirepass`,
  `durability-mode`, `wal-failure-policy`).
- **Accepted but a no-op** — parameters kept for Redis client/tooling compatibility
  (things some Redis clients or `redis-cli` scripts expect to be able to read or set,
  such as `save` or `hz`) that FrogDB accepts without any internal effect. Under the
  `[compat] strict-config` setting, these are hidden from `CONFIG GET` and rejected by
  `CONFIG SET` instead of silently accepted.
  See [Configuration reference](/reference/configuration/) for the `strict-config` field.
- **Read-only** — startup-only parameters (for example `bind`, `port`, `num-shards`,
  `dir`, and the TLS listener settings). `CONFIG SET` on one of these returns an
  `ImmutableParameter` error rather than silently doing nothing.

The [Configuration reference](/reference/configuration/) tables include a runtime-mutable
column, generated from this same registry, for every parameter that has a `CONFIG`
name — check there rather than assuming a given field is live-editable.

`CONFIG SET` alone never writes back to the TOML file — it only mutates the running
process's in-memory state. To persist runtime values, run `CONFIG REWRITE`. It merges the
current value of every non-no-op parameter into the TOML document that was loaded at
startup and atomically rewrites the file (temp file, `fsync`, rename), using a
comment-preserving, order-preserving `toml_edit` merge — untouched keys, comments, and
formatting are left exactly as they were
(`frogdb-server/crates/server/src/config_persister.rs`). `CONFIG REWRITE` fails with `ERR
The server is running without a config file` if the server started without `--config` and
no implicit `frogdb.toml` was found. Parameters accepted as compatibility no-ops (see
above) are excluded from the rewrite. If you'd rather not rewrite the file this way,
update the TOML file, environment variables, or CLI invocation by hand and restart.

## See also

- [Configuration reference](/reference/configuration/) — the full parameter tables
  (type, default, section, runtime-mutability), generated from source.
- [Example config](/reference/example-config/) — the output of `frogdb-server --generate-config`.
- [frogdb-server CLI reference](/reference/frogdb-server/) — the complete flag list.
- [Deployment](/operations/deployment/) — how the Docker image, Debian package, and Helm
  chart each supply configuration.
- [Persistence](/operations/persistence/), [Security](/operations/security/) — semantics
  of the durability, TLS, and ACL parameters.
