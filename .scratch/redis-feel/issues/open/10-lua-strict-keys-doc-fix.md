# Docs claim strict Lua `KEYS[]` enforcement that the code doesn't implement

Status: ready-for-agent
Type: doc bug
Area: website / compatibility docs

## Problem

`website/src/content/docs/compatibility/overview.mdx:89-99` claims "Strict key declaration ...
Accessing an undeclared key returns an error." This isn't what the code does, and isn't what
FrogDB's scripting gate is deliberately designed to do.

## What the code actually does

`frogdb-server/crates/core/src/scripting/gate.rs`:

- Module doc (:24-44) and `CrossSlotTracker` (:66-90) describe the real, deliberate policy:
  **standalone mode enforces nothing**; **cluster mode enforces cross-slot cohesion only** (a
  script may only touch keys in the same cluster slot, mirroring Redis's own cluster-mode
  scripting restriction) — not a strict "every key access must be declared in `KEYS[]`" check.
  An opt-out shebang, `allow-cross-slot-keys`, exists for scripts that need to relax even that.

This is Redis-parity behavior, not a gap — the docs are describing a policy FrogDB never
intended to implement.

## Fix

Doc-only change: rewrite the `overview.mdx:89-99` section to describe the real policy — no
enforcement standalone, cross-slot-only enforcement in cluster mode, with the
`allow-cross-slot-keys` opt-out documented.

## Acceptance criteria

- [ ] `overview.mdx` no longer claims per-key `KEYS[]` declaration enforcement
- [ ] The rewritten section accurately describes: no enforcement in standalone mode, cross-slot
      cohesion enforcement in cluster mode, and the `allow-cross-slot-keys` shebang opt-out
- [ ] No code changes — this is a doc-only fix confirming the code's existing (correct,
      Redis-parity) behavior

Size: S
