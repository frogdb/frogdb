# Virtual-time / injectable-timeout primitive for shuttle — VLL timeout paths are unexplorable

Status: needs-triage
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I8
LOE: 1–2 weeks (estimated)
Tier: C
Area: crates/testing / shuttle (VLL model)
Asked by: 12 (F7)

## Context

The VLL model's remaining nondeterminism is message-arrival order combined with timeouts.
Shuttle is already in `crates/testing` and already explores arrival order, but it has no way
to explore timeout firing, so the timeout-dependent VLL states are unreachable under the
model. The audit also settled the tool question — loom is the wrong tool here — so the only
open question is scope and ownership of the virtual-time layer.

## Evidence

- Round-1 issue 07 already established shuttle in `crates/testing` for the MultiWaiter
  exactly-once guard. The VLL model needs that plus deterministic timeout exploration, which
  does not exist.
- 12 explicitly concluded **loom is the wrong tool** for VLL — no atomics, no `UnsafeCell`, no
  interior mutability; state machines are `&mut self` single-owner and all cross-task comms
  are tokio channels. The nondeterminism is message-arrival order, which is shuttle's domain.

## What to build

1. A virtual-time / injectable-timeout layer usable from shuttle tests in `crates/testing`,
   so timeout firing becomes a scheduling choice the explorer can vary rather than wall-clock.
2. Apply it to the VLL model so timeout-dependent transitions are reachable under exploration.
3. **Build once and share** if any other area wants deterministic timeouts — the layer must
   not be VLL-private.

## Decision needed

Tier C: 1–2 weeks (estimated). Decide whether to build the shared virtual-time layer now or
defer the VLL timeout states until another area also needs deterministic timeouts. Overlaps
with the clock-seam scope question — see issue 03,
`.scratch/testing-improvements-round2/issues/`.

## Acceptance criteria

- [ ] A decision — build now or defer — is recorded in a `## Resolution` section.
- [ ] If built: the primitive lives in `crates/testing` as a shared facility, not inside the
      VLL model, and its docs say so.
- [ ] If built: a shuttle test drives a VLL timeout transition deterministically, with no
      wall-clock sleep and a reproducible failing schedule on regression.
- [ ] The conclusion that loom is the wrong tool for VLL is recorded where a future author
      will find it, so it is not re-litigated.

## Test boundary

Level 2 — shuttle explores crate-internal state machines and channel interleavings directly;
lifting VLL timeout exploration to a level-4 or level-5 harness would reintroduce exactly the
wall-clock nondeterminism the primitive exists to remove.

## Depends on

Nothing.
