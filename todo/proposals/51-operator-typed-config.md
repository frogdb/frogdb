# 51 — Operator generates unparseable server config (typed config generation)

**Status:** proposed (2026-07-16, round 6)
**Severity:** 🔴 Correctness (operator-deployed servers cannot boot; verified by running tests)
**Found:** round-6 deepening review fan-out over `frogdb-operator` (its own workspace, no CI job —
nobody runs its tests).

## Problem

`frogdb-operator/src/config_gen.rs` builds `frogdb.toml` with hand-written `format!` string
templates using **snake_case** keys (`num_shards`, `durability_mode`, `maxmemory_policy`,
`data_dir`, …). The server config schema (`frogdb-config`) is
`#[serde(deny_unknown_fields, rename_all = "kebab-case")]` — it accepts only `num-shards`,
`durability-mode`, `maxmemory-policy`. Every generated config is rejected at parse time:

```
unknown field `maxmemory_policy`, expected one of … `maxmemory-policy`
```

**Both guard tests in `frogdb-operator/tests/integration.rs:141-183`
(`generated_toml_parses_as_frogdb_config`, `generated_toml_custom_config_parses`) fail right
now.** The operator crate is its own workspace with no CI job, so the red tests were invisible.
An operator-deployed server cannot boot from the generated config. `cluster_env_toml`
(`config_gen.rs:55-69`) has the same disease (`cluster_bus_addr`, `election_timeout_ms`,
`heartbeat_interval_ms`, `auto_failover`).

Root cause is structural, not a typo: the generator is a parallel, convention-synced copy of the
config schema. Any server-side rename/addition silently breaks it again.

## Design

**Typed construction.** The operator already depends on `frogdb-config`, and the config section
structs already derive `Serialize`. `generate_toml` populates real `frogdb_config` section structs
and emits via `toml::to_string` — the `rename_all = "kebab-case"` renames then apply
automatically, and schema drift becomes a **compile error** instead of a runtime parse failure.

- CRD spec (`crd.rs`) keeps its k8s-facing shape; `config_gen` maps `FrogDBConfigSpec` →
  `frogdb_config::Config` (or the individual section structs) and serializes.
- Apply the same treatment to `cluster_env_toml`.
- Fixed operator choices (bind `0.0.0.0`, `data_dir = "/data"`, json logging to stdout) become
  struct-field assignments, still visible in one place.

**CI:** add an operator-test job to `.github/workflows/test.yml` running the existing
`just operator-test` recipe, so this crate's tests actually run.

## Tests

- Upgrade the two guard tests from "parses Ok" to **value round-trip** assertions: deserialize
  the generated TOML via `frogdb_config::Config` and assert
  `config.persistence.durability_mode == "async"`, `num_shards == 8`, `maxmemory-policy`,
  metrics port, etc. The two currently-failing tests must go green.
- Same round-trip for `cluster_env_toml` fields.
- Booting a server from the generated TOML: parse via `frogdb_config::Config` is the minimum bar
  (covered by round-trip).

## Verify

`just operator-test` green (red before on the two guard tests); new CI job present.
