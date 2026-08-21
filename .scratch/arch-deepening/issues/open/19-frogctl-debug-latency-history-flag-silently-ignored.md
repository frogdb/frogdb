# 19 — `frogctl debug latency --history` is accepted, documented, and silently ignored

Status: needs-triage

## What to build

`frogctl/src/commands/debug.rs:44-46` declares the flag on `DebugCommand::Latency`:

```rust
/// Periodic snapshot mode (every 15s)
#[arg(long)]
history: bool,
```

The dispatch arm at `frogctl/src/commands/debug.rs:396-402` destructures
`DebugCommand::Latency { subcommand, samples, interval, dist, .. }` — `history` falls into the
`..` at `:401` and is never bound. `run_latency` (`:444`) takes `samples`, `interval`, `dist` and
`ctx` only, and has no notion of a snapshot loop. The result: `frogctl debug latency --history`
parses cleanly, exits 0, and runs an ordinary one-shot latency measurement. The user is told the
flag means "periodic snapshot mode (every 15s)" by `--help` and by the generated CLI reference,
and gets none of it. There is no error, no warning, and no exit code — this is **silently wrong
on main today**, and it is operator-facing.

Because clap `..` swallows the binding, the compiler cannot see the drop: this is precisely the
declared-and-unread-option class, and `--history` is one of two known instances (the other,
`debug slowlog --all`, is being fixed in the proposal-73 round with an honest bail).

Two honest resolutions, and the choice is a CLI-surface ruling, not a code cleanup:

1. **Implement it** — a 15s snapshot loop around the existing measurement, rendering successive
   samples. Note `frogctl/src/ops/latency.rs:37` already carries a `latency_history` engine
   function, but the whole `ops/` layer is currently unreachable from `commands/` (zero `ops::`
   references anywhere under `frogctl/src/commands/`), which is the subject of proposal 73. If
   `ops/` gets wired, this becomes the adapter's job rather than a from-scratch feature.
2. **Remove the flag** — the smaller and more immediately honest fix, but it shrinks the shipped
   clap surface, which regenerates `frogctl-cli.json` and the rendered website reference. That
   surface belongs to proposal 75, which owns the declared-and-unread-option family.

Either way the interim state should not be "accepts and ignores". Proposal 73 explicitly declines
to hotfix this (unlike `slowlog --all`, there is no correct behavior to fall back to inside the
existing arm) and files it here so the discovery survives the round.

## Acceptance criteria

- [ ] `frogctl debug latency --history` either performs the documented 15s periodic snapshot
      mode, or the flag no longer exists on the command — it is never accepted-and-ignored
- [ ] If the flag is removed, `frogctl-cli.json` and the rendered `frogctl.mdx` reference are
      regenerated so the website stops advertising it
- [ ] Regression test `debug_latency_history_flag_is_not_silently_dropped` in the `frogctl` test
      suite asserts the chosen behavior (either observable snapshot output, or a clap parse
      failure for the unknown flag) so a future `..` cannot re-swallow it
- [ ] `just frogctl-test` green (`just test frogctl <pattern>` refuses — `frogctl` is excluded
      from the default nextest filter, `Justfile:81-83`, `:297-298`)

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 73
(`.scratch/arch-deepening/proposals/73-frogctl-ops-wiring.md`), defect **H3** — ruled
"AGREE → file as an issue, not a hotfix" (proposal `:891-899`); belongs to proposal 75's
declared-and-unread-option family.

## Comments
