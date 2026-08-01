# Decide: subprocess-SIGKILL crash primitive, or is truncation-level crash testing enough?

Status: needs-triage
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I2
LOE: 1–2 weeks, with CI-flake risk (measured)
Tier: C
Area: frogdb-test-harness / crash + durability testing
Asked by: 13 (F10), echoed by 11 and 14

## Context

Persistence, cluster and replication audits all want a test that kills the server the way
production does — `SIGKILL`, mid-fsync, OS buffers still in flight — and then restarts it and
asserts what survived. Today no such primitive exists, and building one is not a fixture: it
means giving the whole harness a second execution mode. The residue it would newly cover is
narrow, so this needs a decision before anyone commits two weeks to it.

## Evidence

- `TestServer` is **entirely in-process** — zero `Command::new` in `test-harness/src`,
  verified. A real SIGKILL means adding a subprocess execution mode to the whole harness:
  spawn the actual binary, pass config by file/args, discover the port, connect a client, and
  handle teardown plus orphan reaping in CI. Every existing `start_*` helper either has to
  work under it or be explicitly declared out of scope.
- `ClusterNode::kill()` (`cluster_harness.rs:912`) is a **graceful shutdown**. The name has
  probably already misled a test author.
- `CrashTestHarness` (`core/src/persistence/test_harness.rs`) already does byte-level
  truncation and covers torn-write recovery. It misses only "process dies mid-fsync with OS
  buffers still in flight."

## Options

> **Decision needed**: is that residue worth 1–2 weeks plus ongoing CI flake, or is
> truncation-level crash testing sufficient for production readiness?
>
> **If built**: 13 asks it live in `frogdb-test-harness` next to `TestServer`, **not** in
> `core/src/persistence/test_harness.rs`.

## What to build

1. Take the decision above and record it on this issue.
2. **Regardless of the decision**: rename `ClusterNode::kill()`
   (`cluster_harness.rs:912`) to say what it does — it is a graceful shutdown — and fix
   call sites. This is not blocked on the decision and should not wait for it.
3. If "build": a subprocess execution mode for `TestServer` in `frogdb-test-harness`,
   covering spawn-by-binary, config by file/args, port discovery, client connect, teardown
   and orphan reaping, with an explicit list of `start_*` helpers declared out of scope.

## Acceptance criteria

- [ ] `ClusterNode::kill()` is renamed to reflect graceful shutdown, and no test refers to it
      as a crash.
- [ ] A decision — build or decline — is recorded in a `## Resolution` section with the
      reasoning, and the issue is closed or converted to `ready-for-agent` accordingly.
- [ ] If built: the primitive lives in `frogdb-test-harness` beside `TestServer`, not in
      `core/src/persistence/test_harness.rs`.
- [ ] If built: each existing `start_*` helper is either supported under the subprocess mode
      or listed in the module docs as out of scope.

## Test boundary

Level 4 — a real SIGKILL requires a real process and a socket client, so nothing below server
integration can express it. The cheaper substitute (`CrashTestHarness`) already covers the
level-1/2 truncation slice, which is precisely why the residue is small.

## Depends on

Nothing.
