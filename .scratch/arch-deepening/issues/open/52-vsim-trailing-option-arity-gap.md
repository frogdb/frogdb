# 52 — `VSIM … EF` with no value is silently accepted where every other valued option errors

Status: needs-triage

> **PARKED** (orchestrator ruling, 2026-08-11): not itself a security defect, but the correct fix
> is written in the same hunk as the parked security issues 50 and 51 — implementing it separately
> would fragment that review. Land together with 50/51 when the user unparks the vectorset
> security set.

## What to build

`VSIM`'s option loop treats the value of `EF`, `FILTER-EF` and `EPSILON` as **optional**, so an
argument list Redis rejects is accepted and executed. On `origin/main` the hunk is
`frogdb-server/crates/commands/src/vectorset/vsim.rs:166-174` (the proposal cites the
pre-`d48e1b44` numbering `:160-168`; a `+6` doc-block insertion landed in every vectorset file):

```rust
} else if opt.eq_ignore_ascii_case(b"EF")
    || opt.eq_ignore_ascii_case(b"FILTER-EF")
    || opt.eq_ignore_ascii_case(b"EPSILON")
{
    // Accept but skip value argument (these tune search behavior).
    i += 1;
    if i < rest.len() {
        i += 1; // skip the value
    }
}
```

The `if i < rest.len()` is the bug: when the option is the last token there is no value to skip,
the loop exits, and `VSIM k ELE a EF` **succeeds and returns results**. Every other valued option
in the same loop rejects that shape — `COUNT` at `vsim.rs:133-137` (`"COUNT requires a value"`)
and `FILTER` at `vsim.rs:151-155` (`"FILTER requires an expression"`) both bounds-check before
reading `rest[i]`. `VSIM`'s spec arity is `Arity::AtLeast(3)` (`vsim.rs:27`), which cannot express
a trailing-option constraint either, so there is no backstop above the parser.

This is an **arity/grammar gap**, distinct from the adjacent finding that `VSIM` silently
*discards* an `EF`/`EPSILON` value that *was* supplied: here the value is not merely ignored, the
argument list is malformed and FrogDB answers anyway. Blast radius is small and it is not a
memory-safety or availability issue — the command runs with default search tuning — but it is a
wire-behavior divergence in the direction of accepting garbage, which is the direction that later
becomes a compat trap. **LIVE on `origin/main` today**; the family is behind
`#[cfg(feature = "vectorset")]` (`commands/src/lib.rs:59`), so it ships only in `full`/`cmd-full`
builds and is invisible to a default `just check`.

Fix direction: make the three options bounds-check their value the way `COUNT` and `FILTER` do,
producing an error in the same shape. **Disposition note:** proposal 99 recommends *not*
implementing this inside the file-collapse commit, on two grounds — it is a wire change (a
currently-succeeding command starts erroring, so it needs Redis/Valkey confirmation of the exact
error string first), and the correct fix is written in the same hunk as the parked security
defects, so landing it separately would fragment that review. Confirm the intended error text
against a live Redis before shipping.

## Acceptance criteria

- [ ] `VSIM k ELE a EF`, `VSIM k ELE a FILTER-EF` and `VSIM k ELE a EPSILON` (option as the last
      token, no value) return an error rather than results, in the shape confirmed against a live
      Redis/Valkey
- [ ] The supplied-value forms (`VSIM k ELE a EF 200`, etc.) keep working exactly as today
- [ ] Regression test `vsim_trailing_tuning_option_requires_value` in
      `frogdb-server/crates/redis-regression/tests/vectorset_regression.rs` covers all three
      options in both the missing-value and supplied-value shapes, asserted next to the existing
      `COUNT`/`FILTER` missing-value cases so the three grammars are pinned as uniform; it fails
      on today's code
- [ ] `just test frogdb-redis-regression vsim_trailing_tuning_option`

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 99 (`.scratch/arch-deepening/proposals/99-vectorset-file-collapse.md`),
adjacent finding 6 (VSIM trailing-arity gap).

## Comments
