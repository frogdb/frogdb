# 36: Self-fence window strictly precedes successor promotion — stated, enforced, backstopped

Status: ready-for-agent

## Origin

Distsys-review MAJ-8 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

A fencing design is only safe if the deposed primary stops accepting writes *before*
the successor starts. Neither the spec nor the config validator states or enforces the
required inequality. `ClusterConfigSection::validate()` (`config/src/cluster.rs:182-228`)
enforces only `heartbeat_interval_ms < election_timeout_ms`; `fail_threshold`
(default 5) is unconstrained relative to either. With defaults, detection takes
`5 × 250ms = 1250ms` while the election timeout is `1000ms` — the successor's election
can complete before the old primary's fence engages, and both nodes admit writes
simultaneously. A split-brain window shipped by the *default* configuration, flagged by
nothing.

Comparisons: Raft's safety argument requires election timeout ≫ broadcast time; etcd
states the inequality in its docs and warns on violation; CRDB's lease design makes the
stale-lease interval strictly precede the new lease start.

**Framing (ruled)**: the timing inequality is *defense-in-depth*, not the safety
argument. No clock inequality survives an arbitrary process stall, so the hard backstop
is epoch fencing at write admission — a deposed primary's writes must be rejectable
regardless of timing (Invariant 5 / existing epoch-fence rows). This issue makes the
window small and the config honest; it does not replace admission-time fencing.

## What to build (spec-first; cluster locked, gate 0.80)

1. Spec rows first:
   - Named precondition in `specs/cluster.md` (e.g. `PRE-CLUSTER-FENCE-1`):
     `fail_threshold × heartbeat_interval < election_timeout` (with the margin factor
     chosen and justified in the row) for all admissible configurations.
   - FM row: "self-fence engages before any successor can be promoted, for all
     admissible configurations" — Observable: config violating the precondition is
     refused at load/apply; Invariant cell states the epoch-fence backstop explicitly
     (timing bounds the window; admission fencing closes it).
2. Code: `ClusterConfigSection::validate()` gains the cross-constraint — refuse the
   config (pre-alpha, no soft-warn compat); fix the shipped defaults so they satisfy
   the precondition with margin (e.g. raise `election_timeout_ms` or lower
   `fail_threshold × heartbeat`), and check live-mutable config paths (params are
   live-mutable — the runtime apply path must run the same validation).
3. Forcing tests: `validate()` rejects the current defaults' shape if the inequality
   is violated (constructed violating config → error naming the precondition);
   defaults pass; live CONFIG SET violating the inequality is refused.
4. Docs: config reference pages for the three knobs cross-cite the precondition.

## Cross-references

- [Issue 27](27-self-fence-quorum-derives-from-raft-liveness-not-tcp-probes.md):
  reshapes what "detection" means (raft liveness, not TCP probes). Coordinate: if 27
  lands first, the precondition's left side is whatever 27 makes the detection window;
  the inequality survives either way.
- Epoch-fence rows (Invariant 5 family): named as the backstop in the FM row; do not
  weaken them on the strength of the timing bound.

## Acceptance criteria

- [ ] Named precondition + FM row landed; `just lint-spec` green
- [ ] `validate()` enforces the inequality on load and on live mutation; defaults
      comply with margin
- [ ] Forcing tests fail pre-fix (defaults currently violate), pass post-fix
- [ ] `just mutants-diff` on touched locked crates triaged

## Blocked by

None — can start immediately (coordinate wording with issue 27 if in flight).
