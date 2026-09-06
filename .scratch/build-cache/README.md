# build-cache — sccache and the parallel-worktree build waste

State: active

Record of the 2026-09-06 re-investigation of "why do parallel Claude worktrees recompile
everything, and wasn't sccache supposed to fix that?". Start here before re-investigating;
the rejected alternatives at the bottom were each checked against the sccache/cargo source.

## Timeline

| when | what |
|---|---|
| 2026-03-09 `68c5a42f` | sccache adopted: Justfile exports `RUSTC_WRAPPER` when `sccache` is on `PATH` |
| 2026-03-19 `79e4101f` | fuzz recipes land with `RUSTC_WRAPPER=""` (no recorded reason; Homebrew-bottle era) |
| 2026-04-10 `7c14993b` | sccache moves to mise (`ubi:mozilla/sccache`, unpinned) — `which sccache` now resolves to the mise **shim** |
| 2026-07-31 `61e6cf27`, `5927dc82` | `lint-spec`, `loop-cost`, `spec-lint.py`, `coverage-depth.py` gain `RUSTC_WRAPPER=""` ("sccache is off on macOS" — false) |
| 2026-09-01 | concurrency-testing issue 19 filed: `lint-spec`'s uncached recompile starves sim suites |
| 2026-09-06 | this investigation; Tier 1 landed (see below) |

sccache was never turned off. It was default-on the whole time, and its cache had thousands of
fresh entries — it was just failing on the paths that mattered.

## Findings

### 1. The mise shim cannot be a `RUSTC_WRAPPER` (root cause of every bypass)

cargo runs `rustc` for a registry crate with cwd = `~/.cargo/registry/src/<index>/<crate>/`
(verified with a logging wrapper: every non-workspace unit is invoked from there). The mise shim
resolves its version from the cwd's `.mise.toml`; there is none under `~/.cargo`, so the first
registry crate of any fresh build dies with

    mise ERROR No version is set for shim: sccache

The Justfile now sets `RUSTC_WRAPPER` from `mise which sccache` (the real binary under
`~/.local/share/mise/installs/...`), which works from any cwd. The 2026-07-31 bypasses were
papering over this failure without diagnosing it; with the real binary, `spec-lint`,
`loop-cost`, `-C instrument-coverage` builds (`coverage-depth`: 27/27 hits on the warm run) and
`cargo +nightly fuzz build` all go through sccache.

### 2. What sccache can and cannot cache across worktrees (structural)

From the sccache source (`src/compiler/rust.rs`): the compile **cwd is part of the hash**
("this will wind up in the rlib"), and `-C incremental` is refused outright ("Incremental
compilation makes a mess of sccache's entire world view"). Workspace crates are compiled from
each worktree's absolute path with incremental on, so they **never** hit across worktrees.
What does hit: registry dependencies (compiled from the shared registry cwd) and C/C++ objects
via cc-rs's wrapper fallback (RocksDB, zstd, lz4, usearch). That is the ceiling; do not expect
more from sccache.

Two more hash inputs matter in practice (both from the same `generate_hash_key`):

- **Every `CARGO_*` environment variable** (except `CARGO_MAKEFLAGS`, `CARGO_REGISTRIES_*`,
  `CARGO_BUILD_JOBS`, `CARGO_ENCODED_RUSTFLAGS`) is hashed. Setting `CARGO_TARGET_DIR` in the
  environment therefore puts a checkout-specific path into every Rust unit's key. Measured with
  `docs-gen` into a fresh target dir: with `CARGO_TARGET_DIR=<new value>` 0 Rust hits; with
  `--target-dir <new value>` 334 Rust hits / 95 misses (the misses are the workspace crates,
  cwd-hashed as above). Rule: pass `--target-dir`, never export `CARGO_TARGET_DIR`
  (`coverage-depth.py` was changed accordingly).
- `--out-dir`, `-L`, `--extern` paths are *not* hashed as strings (the extern rlibs are hashed
  by content), which is why a different target dir alone does not break hits.

### 3. Where the waste actually is (measured 2026-09-06)

| item | measure |
|---|---|
| worktrees under `.claude/worktrees` | 47 (+3 nested) |
| sum of worktree `target/` | 529 GB (main: 22 GB); disk 124 GB free of 926 GB |
| of which `*/incremental/` | 304 GB (57 %) |
| vendored RocksDB `out/` dirs | 22 × ~1.5 GB = 33 GB, ~10 min CPU each |
| worktrees whose branch is merged into main | 1 of 51 |

- **RocksDB**: `ROCKSDB_LIB_DIR`/`SNAPPY_LIB_DIR` (Homebrew rocksdb) are a per-recipe
  interpolation (`{{rocksdb-env}}`), not exported. Any cargo call without it — `docs-gen`,
  `admin`, `bench-table`, `test-coz` (all now routed through `_cargo`), rust-analyzer, a bare
  `cargo` in a shell — builds vendored RocksDB from source on a fresh worktree. `librocksdb-sys`
  declares `rerun-if-env-changed=ROCKSDB_LIB_DIR`, so the next enved build re-runs the build
  script and links the system lib, but the 1.5 GB of `out/*.o` stays forever. `just clean-stale`
  now prunes those objects when `output` proves the system lib is in use.
- **Workspace crates**: every worktree's first build compiles all `frogdb-*` crates. Cargo's
  metadata hashes are path-independent (`.fingerprint/<crate>-<hash>` names are identical
  across worktrees), so a `target/` is relocatable; only the mtime freshness check for path
  crates stands in the way (a fresh checkout stamps every file "now"). That is Tier 2's basis.

## What landed (Tier 1)

- `_cargo` private recipe = the one cargo entry point in the Justfile; `scripts/cargo_env.py`
  = the one env prelude for Python scripts; `lint-cargo-env` (in `lint-gates`) pins both.
- rust-analyzer `cargo/check/runnables.extraEnv` in `contrib/vscode/root/settings.json`.
- `RUSTC_WRAPPER` from `mise which sccache`; sccache pinned to 0.17.0 in `.mise.toml`;
  `SCCACHE_CACHE_SIZE` default 40G (`sccache --stop-server` to apply); `sccache-stats` flags a
  freshly started server. Bypasses removed (issue 19 closed).
- `just clean-stale` prunes stale vendored RocksDB objects; `just worktree-prune [yes]` lists /
  removes merged worktrees under `.claude/worktrees` (never main, detached, or dirty).

## Rejected alternatives (checked, do not revisit)

- **Shared `CARGO_TARGET_DIR` across worktrees**: cargo holds `target/debug/.cargo-lock` for
  the whole build (agents serialize), and two branches with different content for the same
  crate ping-pong-rebuild the same artifact path.
- **`CARGO_INCREMENTAL=0` + sccache for workspace crates**: cwd hashing still misses across
  worktrees; loses incremental edit loops for nothing.
- **Replacing sccache**: nothing else caches registry deps + cc objects across worktrees for
  free. Keep it, with the shim fix.
- **Exporting `ROCKSDB_LIB_DIR` globally from the Justfile**: `just` exports empty values, and
  `librocksdb-sys/build.rs` treats empty-but-set as "link the system lib" — Linux builds would
  break. `rocksdb-env` stays the single definition, reached through `_cargo`.
- **Testbox-by-default for agents**: valid, costs a box per agent; user chose local-mode fixes.

## Tier 2 (seed a new worktree's `target/` from a clean-main snapshot)

Spike-gated; status recorded here when it runs. Prerequisite: runtime `CARGO_MANIFEST_DIR`
resolution (`frogdb_types::manifest_dir!`) so a fingerprint-fresh binary built in main never
reads/writes main's paths from a worktree.
