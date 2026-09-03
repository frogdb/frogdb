# `just test-core [area]` recipe + empty `sig_*` binaries; delete `core-test` / `core-test-e2e`

Status: ready-for-agent

Size: S

## Why

The compact suite is a *location*, not tooling: two greppable sets selected by one nextest
filterset — the boundary crates entire, plus the `sig_*` test binaries. This issue creates the
selector and the empty binaries so issues 03–07 have somewhere to put tests, and deletes the two
recipes the new one replaces.

See [PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling) and decision Q9 in
[PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02).

## What to build

**1. Delete `core-test` and `core-test-e2e`** from the `Justfile` (currently around lines
321–345, under the "Locked core areas" banner). Both go away entirely; `test-core` replaces them.

**2. Add `test-core area=""`.** With no area it runs the whole compact filter:

```
cargo nextest run -E 'package(/^frogdb-(txn|vll|persistence|recovery|replication|replication-runtime|cluster|cluster-runtime)$/) | binary(/^sig_/)'
```

With an area it runs that area's crates plus that area's signature binary:

| area | filter |
|---|---|
| `txn` | `package(/^frogdb-(txn\|vll)$/) \| binary(=sig_txn)` |
| `persistence` | `package(/^frogdb-(persistence\|recovery)$/) \| binary(=sig_persistence)` |
| `replication` | `package(/^frogdb-(replication\|replication-runtime)$/) \| binary(=sig_replication)` |
| `cluster` | `package(/^frogdb-(cluster\|cluster-runtime)$/) \| binary(=sig_cluster)` |
| `blocking` | `binary(=sig_wake)` (no crates — the wake axis has no crate of its own; `specs/blocking.md` is its spec) |

An unknown area exits non-zero with the legal list, the way `core-test` did. Keep the
`{{dyld-env}} {{rocksdb-env}}` prefixes the deleted recipes used. `just test` is unchanged and
remains the full suite / merge gate; `redis-regression` stays outside both, on its own recipe.

**3. Create the six empty signature binaries.** `TestServer`-level, one per boundary:

- `frogdb-server/crates/server/tests/sig_persistence.rs`
- `frogdb-server/crates/server/tests/sig_replication.rs`
- `frogdb-server/crates/server/tests/sig_cluster.rs`
- `frogdb-server/crates/server/tests/sig_txn.rs`

Shard-driver-level (no socket), under `frogdb-server/crates/shard-harness/tests/` (that crate
also sets `autotests = false`, so each needs a `[[test]]` entry there):

- `frogdb-server/crates/shard-harness/tests/sig_persistence.rs`
- `frogdb-server/crates/shard-harness/tests/sig_wake.rs`

(PRD §5 writes these as `frogdb-server/tests/sig_*.rs`; the crate actually lives at
`frogdb-server/crates/server/`.)

`frogdb-server/crates/server/Cargo.toml` sets `autotests = false`, so each file needs its own
`[[test]] name = "sig_<area>" / path = "tests/sig_<area>.rs"` entry — follow the existing
`cluster_*` entries, including their comment style explaining why they are separate binaries.
Mirror the `#![cfg(not(feature = "turmoil"))]` gate the `cluster_*` binaries carry where it
applies.

Each file starts with a module doc comment (`//!`) that names the boundary it covers, states the
tag grammar (`// SIG-<NAME>` at the test's definition site, name per
[PRD §3.3](../../PRD.md#33-naming-is-mechanical)), names the default test level for that boundary
per [PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling) (persistence and wake at
`shard_driver` level; replication, cluster routing and txn at `TestServer` level), and points at
`specs/signatures.md` and this PRD. Empty of tests otherwise — issues 04–07 fill them.

Note for later issues: area issues may add further `sig_*` binaries under
`frogdb-server/crates/shard-harness/tests/` or `frogdb-server/crates/core/tests/` for
shard-driver-level tests (both crates also set `autotests = false`, so they need `[[test]]`
entries too). The `binary(/^sig_/)` filter picks those up wherever they live.

**4. Update references to the deleted recipes.** Grep the tree for `core-test` / `core-test-e2e`
and fix every live reference. Known hits at time of filing:

- `frogdb-server/crates/server/Cargo.toml:17` — comment citing `just core-test-e2e cluster`
- `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md:121` — a
  historical evidence line; leave closed-issue history alone unless it reads as an instruction
- `.scratch/replication-cluster-rework/issues/done/05-cluster-admin-gating-breaks-client-bootstrap.md:73`
  — same, historical evidence
- `.config/nextest.toml` comments that mention the recipe by name
- `scripts/loop-cost.py` — check its docstring and `AREAS` comments for the old recipe names and
  update if present

List in the commit what you found and what you changed. Do not rewrite closed-issue history; only
update live docs and comments that instruct someone to run the deleted recipes.

## Acceptance criteria

- [ ] `core-test` and `core-test-e2e` no longer exist in the `Justfile`
- [ ] `just test-core` runs the full compact filter; `just test-core <area>` runs that area's
      crates plus `binary(=sig_<area>)` for each of txn / persistence / replication / cluster,
      and `binary(=sig_wake)` alone for `blocking`
- [ ] `just test-core bogus` exits non-zero naming the legal areas
- [ ] The six `sig_*.rs` files exist, each with a module doc comment naming its boundary, the
      `// SIG-` tag grammar and its default level, and each registered as a `[[test]]` in its
      crate's `Cargo.toml` (`frogdb-server/crates/server/` for four, `shard-harness/` for two)
- [ ] `just test-core` runs green (zero signature tests is a pass at this point)
- [ ] No live reference to `core-test` / `core-test-e2e` remains outside closed-issue history
- [ ] `just lint` and `just scratch-check` green

## Blocked by

Issue 01 in this directory.
