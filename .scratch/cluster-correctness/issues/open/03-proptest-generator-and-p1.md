# 03 — Proptest command-sequence generator + P1 (invariants always hold)

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W2; dev-dependency + nightly ruled in §8 D1.

## What to build

Add `proptest` as a dev-dependency of `frogdb-cluster`. Build
`arb_command_sequence(len)`: a weighted stateful strategy over all 18 `ClusterCommand`
variants that tracks live node ids / assigned slots / open migrations and biases ~80/20
toward commands valid in context — garbage retained deliberately, because a *rejected*
command must also preserve every invariant and the rejection path is where
validate-then-mutate bugs live.

P1: apply each generated sequence via `apply_local`, assert the invariant catalog clean
after every step. Moderate case count in the normal suite; `PROPTEST_CASES`-boosted pass
wired into the nightly.

## Acceptance criteria

- [ ] Generator produces stateful, biased sequences over all 18 variants
- [ ] P1 runs in the default suite at moderate cases; failure shrinks to a minimal
      sequence
- [ ] Nightly boosted pass wired (same test, env-raised cases)
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 02 (`.scratch/cluster-correctness/issues/`) — P1 asserts the catalog.
