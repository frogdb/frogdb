# Formal specification for FrogDB state

State: active

Constructive, lintable specifications of FrogDB's state and legal transitions, replacing the
reactive failure-mode catalogs as the authority per area.

- [Design](2026-08-12-formal-state-spec-design.md) — the approved architecture (four
  verification layers, one authority) and the per-area migration sequencing.
- [Phase 1 scaffolding plan](2026-08-12-phase1-scaffolding-plan.md) — `specs/` layout, linter
  evolution, Quint toolchain, website generator.
- [Phase 2 cluster quint plan](2026-08-13-phase2-cluster-quint-plan.md) —
  the two cluster Quint models, the quint-connect conformance harness, and the nightly
  `quint verify` CI lane (design doc §7, "Cluster").

The specs themselves live at the repository root under `specs/`, not here: they are a
first-class artifact, published to the website and linted by `just lint-spec`.
