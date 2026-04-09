# Remove redundant cross-compilation path

The original Docker build path used `cargo-zigbuild` to cross-compile on the host, then copied the
resulting binary into a Debian container (`frogdb-server/docker/Dockerfile`). This was superseded by
`Dockerfile.builder`, which compiles inside an Alpine container using system RocksDB and is
significantly faster. The cross-compile path is not used in CI and only remains in local Justfile
recipes, one of which (`docker-build-bench`) references a `Dockerfile.bench` that no longer exists.

## Files to remove

- `frogdb-server/docker/Dockerfile` -- the cross-compile Dockerfile (copies pre-built binary into Debian)
- Justfile recipes: `cross-install`, `cross-build`, `cross-build-arm`, `cross-verify`, `docker-cross-build`, `docker-build-bench`

## Files to modify

- `.mise.toml` -- remove `zig` and `"cargo:cargo-zigbuild"` entries
- `rust-toolchain.toml` -- remove `targets = ["x86_64-unknown-linux-gnu"]` (only needed for zigbuild; macOS CI targets are added per-job via `rustup target add`)
- `Brewfile` -- drop zig if present (check whether it survived the mise PR)
- `shell.nix` -- same for zig

## What still works after removal

- `just docker-build-prod` / `just docker-build-debug` -- Dockerfile.builder, unchanged
- CI build.yml + release.yml Docker jobs -- Dockerfile.builder via buildx multi-arch
- CI release.yml macOS jobs -- native cargo build with rustup targets
- Local `cargo build` on Apple Silicon -- just works

## Notes

- `docker-build-bench` is already broken (references nonexistent `Dockerfile.bench`)
- zig is only used by cargo-zigbuild in this project; no other use
