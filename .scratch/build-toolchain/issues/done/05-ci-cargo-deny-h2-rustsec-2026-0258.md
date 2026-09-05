# 05 — CI: `cargo deny` fails on RUSTSEC-2026-0258 (h2 0.4.13, unbounded empty DATA frames)

Status: done
Type: AFK
Size: XS
Origin: Test run on `build-toolchain/impl` @ c66fc0fb5 (https://github.com/nathanjordan/frogdb/actions/runs/33937564779), first run where the Lint job got past mise

## Parent

`.scratch/build-toolchain/PRD.md`

## Summary

With 03 landed, the Lint job's mise step, fmt and clippy are green; it now fails at
`Check licenses and advisories`:

```
error[vulnerability]: h2 unbounded empty DATA frames
    ┌─ Cargo.lock:204:1
204 │ h2 0.4.13 registry+https://github.com/rust-lang/crates.io-index
    ├ ID: RUSTSEC-2026-0258
    ├ Advisory: https://rustsec.org/advisories/RUSTSEC-2026-0258
    ├ The h2 crate, used internally by hyper, had a flaw that would accept and queue empty DATA frames without limit.
      Low severity.
advisories FAILED, bans ok, licenses ok, sources ok
```

Advisory (dated 2026-08-17): patched in `h2 >= 0.4.16`. Both lockfiles carry 0.4.13:
`Cargo.lock:2465` and `frogdb-operator/Cargo.lock:1920`.

## What to build

Bump `h2` to the latest 0.4.x (≥ 0.4.16) in both lockfiles:

```
cargo update -p h2
cargo update -p h2 --manifest-path frogdb-operator/Cargo.toml
```

Lockfile-only change: no `Cargo.toml` edits, no new `[advisories] ignore` entry in
`frogdb-server/deny.toml`. If `cargo update -p h2` drags anything other than `h2` along, say so in
the report.

## Acceptance criteria

- [ ] `just deny` green locally (runs `cargo deny check --config frogdb-server/deny.toml`)
- [ ] `h2` ≥ 0.4.16 in `Cargo.lock` and `frogdb-operator/Cargo.lock`
- [ ] `just check` green (workspace) and `cargo check --manifest-path frogdb-operator/Cargo.toml` green
- [ ] `frogdb-server/deny.toml` unchanged

## Files likely touched

- `Cargo.lock`
- `frogdb-operator/Cargo.lock`

## Blocked by

None.

## Decisions

None — routine dependency bump.

## Resolution

Landed on `build-toolchain/impl` at merge `8ae16cf63` (2026-09-04). One commit, `75faab1ab`:
`h2` 0.4.13 → 0.4.19 in both `Cargo.lock` and `frogdb-operator/Cargo.lock` — the only crate whose
version changed. Root lock also carries edge-only `windows-sys` repoints (all versions already
present). Operator lock additionally caught up to path-dependency manifests it was stale against
(`frogdb-memory`, `tikv-jemalloc-ctl`, `paste`, `hashbrown 0.16.1` edges — all already declared
in the crate manifests at the base commit; review verified). No `Cargo.toml` or `deny.toml`
change. `just deny` green (advisories/bans/licenses/sources), `just check` green, operator
`cargo check` green.
