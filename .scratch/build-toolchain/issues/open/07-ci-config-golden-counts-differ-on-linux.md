# 07 — CI: `frogdb-config` golden counts are one param short on Linux (46/125 vs 47/126)

Status: needs-triage
Type: AFK
Size: S
Origin: Test run on `build-toolchain/impl` @ c66fc0fb5 (https://github.com/nathanjordan/frogdb/actions/runs/33937564779); identical on the Sept 3 `main` run (76b2a6dae)

## Parent

`.scratch/build-toolchain/PRD.md`

## Summary

Two `frogdb-config` unit tests fail deterministically on `ubuntu-latest` and pass on macOS:

```
param_id::tests::id_counts_are_stable  (frogdb-server/crates/config/src/param_id.rs:389)
  assertion `left == right` failed  left: 46  right: 47      # ImmutableParamId::ALL.len()
params::tests::test_golden_snapshot_row_count  (frogdb-server/crates/config/src/params.rs:1527)
  assertion `left == right` failed  left: 125  right: 126    # GOLDEN_SNAPSHOT.len()
```

`test_registry_matches_golden_snapshot` passes on the same runner, so the registry itself is one
immutable param short on Linux — something is compiled out, not mis-snapshotted. The config crate
has no `cfg(target_os = ...)`; the only cfg-gated registry difference is `turmoil` (the
`SECTION_PARAMS` test is already cfg-aware for it, `params.rs:1697-1708`). Same `cargo nextest
run --all` invocation as `just test`, so it is not a feature-set difference between the two.

Not caused by `build-toolchain/impl` (no Rust changes vs its base); red on `main` since at least
Sept 3.

## What to build

1. Find the param that exists on macOS but not on Linux (likely a `#[section]` struct or a param
   defined under a platform cfg in a crate the derive walks). Name it in the issue.
2. Decide: register it unconditionally with a platform-specific "unsupported" default (keeps the
   golden snapshot and the wire-visible `CONFIG GET` surface identical across platforms —
   preferred), or make both count assertions cfg-aware like `test_section_params_counts_every_section`.
3. Regression: the golden tests must pass on both platforms; CI is the Linux proof.

## Acceptance criteria

- [ ] `just test frogdb-config` green locally
- [ ] `Unit Tests` on a `workflow_dispatch` run of `test.yml` no longer lists these two tests
- [ ] the missing param is named in the resolution

## Files likely touched

- `frogdb-server/crates/config/src/param_id.rs`
- `frogdb-server/crates/config/src/params.rs`
- the crate that defines the platform-gated param (unknown until step 1)

## Blocked by

None.

## Decisions

Pending — which of the two fixes in step 2.
