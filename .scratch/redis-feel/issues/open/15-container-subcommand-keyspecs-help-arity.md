# Container commands need per-subcommand key specs (XGROUP/XINFO HELP rejected)

Status: needs-triage

## Origin

Wave-D1 arity cross-check (`02e776b3`): upstream models XGROUP/XINFO as
containers whose *subcommand rows* carry the key specs, with container arity
`-2` — so bare `XGROUP HELP` / `XINFO HELP` is legal. FrogDB models each
container as a single command with a key at index 1, and
`CommandSpec::validate` refuses an arity minimum that does not cover the key
(`ArityTooSmallForKeys`), so we demand at least 3 args and reject
`XGROUP HELP` / `XINFO HELP` with a wrong-arity error. D1 carries both as
explicit arity exemptions in `upstream_metadata_tests.rs`.

## What is wrong

Two entangled problems:

1. User-visible: `XGROUP HELP` and `XINFO HELP` fail where Redis answers with
   the help text.
2. Structural: single-spec containers cannot express "subcommand A has a key
   at 2, subcommand B has none" — which also degrades COMMAND INFO key-spec
   fidelity for these containers and forced the DENYOOM judgment to be
   per-container instead of per-subcommand (wave C: XGROUP carries the flag
   even though DESTROY/DELCONSUMER free memory).

## Candidate direction

Per-subcommand spec support for containers (dispatch-level subspec table:
key spec + arity + flags per subcommand, container validates against the
matched subcommand). That also unblocks vendoring upstream's subcommand rows
(the vendor script currently skips them) for INFO/DOCS fidelity. Sizeable
registry change — needs a ruling on scope before implementation.

## Acceptance

- `XGROUP HELP` / `XINFO HELP` return help text, matching Redis.
- D1 arity exemptions for XGROUP/XINFO removed (test enforces shrink-only).
- Per-subcommand key extraction truthful for at least XGROUP/XINFO; DENYOOM
  assignable per subcommand where the container mixes allocating and freeing
  subcommands.

## Comments

**2026-08-21 (compat target 8.6.0 → 8.6.1).** Upstream 8.6.1 adds
`src/commands/hotkeys-help.json` — `HOTKEYS HELP`, arity 2, flags `LOADING`
`STALE`, `since: 8.6.1` — closing a gap of its own (the 8.6.0 `HOTKEYS`
container shipped `START`/`STOP`/`GET`/`RESET` with no `HELP`). FrogDB's
dispatcher (`frogdb-server/crates/server/src/connection/hotkeys.rs`, the
`match subcommand_str` arm) answers `ERR unknown subcommand '<x>'. Try HOTKEYS
START|STOP|RESET|GET.` for `HELP`, so bare `HOTKEYS HELP` now diverges from the
compat target the same way `XGROUP HELP` / `XINFO HELP` do. Same class, same
fix — fold it into this issue's per-subcommand work rather than filing
separately. Note the vendor script skips subcommand rows, so `HOTKEYS HELP`
does not appear in `generated.rs` and no metadata test catches this; the
acceptance comparison against a live 8.6.1 server is the only detector today.
