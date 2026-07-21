# 13-01 — Promote-or-justify audit of the 123 `#[param(skip)]` config fields

Status: ready-for-agent

## What to build

Phase 2 of the config derive-macro migration (proposal
`.scratch/arch-deepening/proposals/13-config-param-single-registry.md`) put
`#[derive(ConfigParams)]` on every struct-backed config section and forced every field to
declare `#[param(...)]` or `#[param(skip)]`. To keep the migration behavior-preserving, every
field that was *not* already a registered CONFIG GET/SET row was annotated `#[param(skip)]` —
**123** fields across `frogdb-server/crates/config/src/*.rs`.

`skip` is now doing double duty: it marks both "genuinely internal, never a CONFIG param" and
"arguably should be a CONFIG param but nobody has wired it up yet". That ambiguity is a silent
hole: a knob a Redis client would reasonably expect under CONFIG GET (e.g.
`persistence.compression`, `persistence.write-buffer-size-mb`, `metrics.otlp-endpoint`,
`http.*`, `admin.*`, the `vll.*` timeouts) is indistinguishable from a field that must never be
exposed (e.g. `server.enable-debug-command`, `memory.doctor-*` internals).

Audit each `#[param(skip)]` field and decide, per field:
- **Promote**: add a real `#[param]` / `#[param(mutable)]` row (and the matching
  `build_typed_params` / `build_param_registry` entry in
  `frogdb-server/crates/server/src/runtime_config.rs`) if it should be reachable via CONFIG.
- **Justify**: keep `skip`, but record *why* it is intentionally not a CONFIG param.

Capture the justification durably. Options (pick one, note the choice): a short `// skip: <reason>`
convention on each skipped field; or a `#[param(skip)]`-adjacent doc line; or a table in the
config crate's CONTEXT.md. Compare against Redis/Valkey CONFIG surface where a directly analogous
parameter exists.

## Acceptance criteria

- [ ] Every one of the 123 `#[param(skip)]` fields is classified promote-or-justify
- [ ] Promoted fields have a metadata row + server registry entry + a test asserting CONFIG
      GET/SET round-trips; `test_param_registry_consistency` and the golden snapshot updated
      deliberately (not auto-recaptured)
- [ ] Justified skips carry a durable, greppable reason
- [ ] Redis/Valkey parity checked for each candidate; divergences noted

## Blocked by

None — Phase 2 (this proposal's implementation) is merged/green. Can start immediately.

## Source

Proposal 13 Implementation section, residual gap (c). Filed 2026-07-21.
