# build-toolchain — build, CI-toolchain and release-path defects

State: active

Working directory for defects in the *build* itself rather than in FrogDB's behavior: the
cross-compilation path, vendored C/C++ dependency builds, the recipes that produce
distributable artifacts, and the CI toolchain and generated-artifact checks that gate `main`.

The distinguishing feature of this class of bug is that ordinary development stays green while
a shipping path is broken. Per [ADR-0005](../../adr/0005-truthful-redis-86-surface.md) ruling 1,
every distributable artifact builds `cmd-full`, while the dev default is the much smaller
`core-profile`. Anything reachable only under `cmd-full` — `usearch` and its vendored `simsimd`,
for one — is never compiled by `just check`, `just test` or `just lint`, so a break there is
invisible until someone builds an artifact.

Issues here should say explicitly which build path they affect and which ones stay green.
