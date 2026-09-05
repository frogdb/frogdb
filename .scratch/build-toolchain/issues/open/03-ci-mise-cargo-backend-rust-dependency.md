# 03 — CI mise step still red: `cargo:` backend demands a mise-managed `rust`; the 2026.8.16 pin from 02 does not help

Status: ready-for-agent

## Summary

Issue [02](../done/02-main-ci-red-toolchain-generators-docs.md) pinned mise to `2026.8.16`
on the theory that mise `2026.9.0` introduced the `cargo:` backend's hard dependency on a
mise-installed `rust`. The first CI run of `build-toolchain/impl` (Test workflow run
`33928155916`, 2026-09-04) disproves that: the generator/docs jobs are green, but Lint, Unit
Tests, Quint Typecheck & Smoke, Turmoil and Shuttle all still die in the mise step, under the
pinned `2026.8.16`:

```
mise ERROR Failed to install cargo:cargo-deny@0.19.0: tool 'cargo:cargo-deny@0.19.0' requires configured install dependency 'rust@1.92.0', but its selected version is not installed
hint: Run `mise install rust@1.92.0` before installing 'cargo:cargo-deny@0.19.0'. Remove the dependency from configuration to allow the install hook to rely on system PATH instead.
mise ERROR Version: 2026.8.16 linux-x64 (2026-08-31)
```

## Root cause

Two things, neither of which is the version pinned:

1. **The check is older than assumed.** The bail lives in `src/install_context.rs`
   (`InstallDependencyEnv::resolve`); the string is present in mise `2026.8.11` (2026-08-23,
   jdx/mise#12234 "unify install dependency environments") and absent in `2026.8.10`. Every
   release from `2026.8.11` on refuses to install a `cargo:` tool while `.mise.toml` declares
   `rust` and that `rust` is not mise-installed — which is exactly this repo's CI shape
   (`rust = "1.92.0"` in `.mise.toml`; Rust installed by `dtolnay/rust-toolchain` *after* the
   mise step, for the `rust-cache` reasons recorded at `RUST_TOOLCHAIN` in
   `workflow_gen/helpers.py`).
2. **CI stayed green for ten days because `jdx/mise-action`'s cache hid it.** The action
   restores `~/.local/share/mise` keyed on platform + config hash; on a hit `cargo-deny` is
   already installed, `mise install` skips it, and the dependency check never runs. Main's Lint
   cache entry was evicted (the repo's cache is dominated by `rust-cache` entries; the
   `mise-v1-…-a4f183…` key was re-created from scratch at 2026-09-02T22:08, the first red run).
   The next fresh install hit the check. That is why bisecting mise versions against green main
   runs pointed at `2026.9.x` — those runs never installed `cargo-deny` at all.

Reproduced locally with the `2026.9.1` macOS binary, an empty `MISE_DATA_DIR`, and a
`mise.toml` holding `rust = "1.92.0"` + `"cargo:rustfilt" = "0.2.1"`: plain `mise install
cargo:rustfilt` fails with the identical message; `MISE_DISABLE_TOOLS=rust mise install
cargo:rustfilt` installs via the `cargo` on `PATH` (exit 0, 2m30s). `--dry-run` does not
exercise the check, so it cannot be used as the probe.

## Options

1. **`MISE_DISABLE_TOOLS=rust` on every CI mise step** (`env:` on the `jdx/mise-action` step
   emitted by `mise_setup_step()`, plus the hand-written `test-unit-tests-testbox.yml`). mise
   then treats `rust` as not configured and the `cargo:` backend falls back to the runner's
   `PATH` cargo — the pre-`2026.8.11` behaviour, and what every cache-miss run before 2026-08-23
   did. `.mise.toml` unchanged for contributors. The `MISE_VERSION` pin becomes redundant and
   can be dropped (the comment already says to unpin once the install works) or kept as a
   deliberate-bump control.
2. **Pin mise ≤ `2026.8.10`.** Works, but freezes CI on a two-week-old mise indefinitely and
   the pin rationale in `helpers.py` must be rewritten to say why.
3. **Drop `rust` from `.mise.toml`.** Removes the "configured dependency" entirely. Changes the
   contributor setup story (`mise install` no longer provisions Rust; `rust-toolchain.toml`
   + rustup only) and `just sync-toolchain-check` loses its second operand.
4. **Add `rust` to `install_args`.** Rejected already by the `RUST_TOOLCHAIN` rationale
   (mise's rust + `Swatinem/rust-cache` on self-hosted runners).

## Decision (D4)

Option 1. `MISE_DISABLE_TOOLS=rust` on every CI mise step; drop the `MISE_VERSION` pin.

## What to build

- `.github/workflows/workflow_gen/src/workflow_gen/helpers.py`:
  - `mise_setup_step()` emits `env: {MISE_DISABLE_TOOLS: rust}` on the `jdx/mise-action` step
    (`Step.env`), for every caller — jobs that install no `cargo:` tool are unaffected by it.
  - Remove `MISE_VERSION` and the `version:` key from the step's `with:`. Replace the pin
    comment with a short one explaining the env var: mise ≥ 2026.8.11 refuses to install a
    `cargo:` tool while `.mise.toml` declares a `rust` that mise did not install; CI installs
    Rust with `dtolnay/rust-toolchain` after this step (see `RUST_TOOLCHAIN`), so `rust` is
    hidden from mise here and the `cargo:` backend uses the runner's PATH cargo. Point at
    this issue.
- `just workflow-gen` to regenerate every workflow; `just workflow-gen-check` clean.
- `.github/workflows/test-unit-tests-testbox.yml` (hand-written, `MANUAL_WORKFLOWS` in
  `render.py`): same env on its mise step, `version:` line and its pin comment removed.
- Nothing in `.mise.toml`, `Justfile`, or the rust-toolchain steps changes.

## Acceptance criteria

- [ ] every generated workflow's `Set up mise toolchain` step carries `env: MISE_DISABLE_TOOLS: rust` and no `version:` key (`grep -c "MISE_DISABLE_TOOLS" .github/workflows/*.yml` matches the count of mise steps; `grep -L` finds none without it)
- [ ] `test-unit-tests-testbox.yml` matches by hand
- [ ] `just workflow-gen-check`, `just generate-check`, `just lint-gates` green
- [ ] `MISE_VERSION` no longer exists in the generator; `grep -rn "2026.8.16" .github/` is empty
- [ ] local proof in the report: with a mise ≥ 2026.8.11 binary, an empty `MISE_DATA_DIR`, and this repo's `.mise.toml`, `MISE_DISABLE_TOOLS=rust mise install just cargo:cargo-deny --dry-run` lists both tools and `mise install` without the env fails with the dependency error (the real install is not required — it compiles cargo-deny)

## Files likely touched

- `.github/workflows/workflow_gen/src/workflow_gen/helpers.py`
- `.github/workflows/*.yml` (regenerated)
- `.github/workflows/test-unit-tests-testbox.yml`

## Affects

Every generated workflow job whose mise step installs a `cargo:` tool (`cargo:cargo-deny`,
`cargo:cargo-nextest`, …): Lint, Unit Tests, Quint, Turmoil, Shuttle, the testbox unit job.
Generator/docs-only jobs (`just` + python) are green. Local builds unaffected: contributors
run `mise install`, which installs `rust` first.

## Blocked by

None. Supersedes the mise-pin half of 02; the rest of 02 stands.
