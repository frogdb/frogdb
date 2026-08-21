# 50 — [SEC] Vectorset `VALUES`/`COUNT` arithmetic overflows on client-controlled input, panicking the shard

Status: needs-triage

**PARKED per user (security)** — do not implement without user ruling.

## What to build

Three arithmetic sites in the vectorset command family add or multiply a client-supplied
`usize` with no overflow check, so a single well-formed command wraps the guard that was
supposed to protect the allocation behind it. On `origin/main` the sites are (the proposal
cites the pre-`d48e1b44` numbering `vadd.rs:100`, `vsim.rs:99`, `vsim.rs:200`; a `+6` doc-block
insertion landed in every vectorset file, so the live cites are **`vadd.rs:106`**,
**`vsim.rs:105`**, **`vsim.rs:206`**, all under `frogdb-server/crates/commands/src/vectorset/`):

| Site (on `origin/main`) | Expression | Source of `count` |
|---|---|---|
| `vadd.rs:106` | `i + count >= rest.len()` | `parse_usize(&rest[i])` at `vadd.rs:103` |
| `vsim.rs:105` | `i + count > rest.len()` | `parse_usize(&rest[i])` at `vsim.rs:103` |
| `vsim.rs:206` | `count * 4 + 10` | `COUNT` value, `parse_usize` at `vsim.rs:138` (default `10usize`, `vsim.rs:123`) |

The family's local `parse_usize` (`vadd.rs:271-280`, byte-identical to `vsim.rs:261-270`) is a
bare `str::parse::<usize>()` with no ceiling, so `count` is fully attacker-controlled up to
`18446744073709551615`. Trace for `VADD k VALUES 18446744073709551614 a b`: in a **release**
build (`overflow-checks = false`) `i + count` wraps mod 2^64 to a small value, the guard at
`vadd.rs:106` *passes*, and control reaches `Vec::with_capacity(count)` at `vadd.rs:112`, which
panics with `capacity overflow` because the requested `count * 4` bytes exceeds `isize::MAX`. In
a **debug/test** build the panic happens one line earlier, at the `+` itself. `vsim.rs:206` is
the same shape reached from an otherwise *valid* command — `VSIM k VALUES 1 0.5 COUNT
4611686018427387904 FILTER '.x == 1'` multiplies into overflow **before** any search runs. It is
only evaluated when a `FILTER` is present (`vsim.rs:205`), which is why the ordinary `COUNT` path
looks safe; the clamp that would have made the value harmless
(`count.min(self.name_to_id.len())`, `frogdb-server/crates/types/src/vectorset.rs:342`) sits
*downstream* of the multiply and never gets the chance.

The shard panic guard catches this and converts it into `-ERR internal error` plus a
`ShardPanicsIsolated` increment, so it is survivable — but
`frogdb-server/crates/core/src/shard/panic_guard.rs:13-17` states the doctrine explicitly: the
catch is "the *structural backstop*, not a substitute for fixing the arithmetic that panics. **A
caught panic is always a bug.**" The release-build wrap is worse than the panic, because the guard
is bypassed rather than tripped. **LIVE on `origin/main` today**, reachable by any client permitted
to issue `VADD`/`VSIM` (the family is behind `#[cfg(feature = "vectorset")]` at
`commands/src/lib.rs:59`, so it ships only in `full`/`cmd-full` builds — that is the blast-radius
qualifier, not an exemption). Nothing in the test surface can see it: grep for the family's four
error literals across both regression files returns nothing, and
`testing/fuzz/fuzz_targets/vectorset_ops.rs` constructs `VectorSetValue` directly with
already-typed fields (`vector: [f32; 4]`, `count: u8`) — no RESP bytes, no dispatch, no argument
parsing.

Fix direction, when unparked: one `checked_add`/`checked_mul` inside a shared
`parse_values(rest, &mut i, reserve_trailing)` helper plus an explicit ceiling on the `VSIM`
`COUNT` value, so the two `VALUES` bounds checks (which legitimately differ — `VADD` reserves a
trailing element-name slot, `VSIM` does not) are written and reviewed once. Note that proposal 99's
own implementer rule forbids shipping the hardening with the file collapse: the arithmetic moves
byte-for-byte verbatim there, and the fix lands here.

## Acceptance criteria

- [ ] `VADD k VALUES 18446744073709551614 a b` returns a clean argument error (not `-ERR internal
      error`, not a panic) in both debug and release builds
- [ ] `VSIM k VALUES 1 0.5 COUNT 4611686018427387904 FILTER '.x == 1'` returns a clean argument
      error or a bounded result, with no multiply overflow, in both debug and release builds
- [ ] The `ShardPanicsIsolated` counter does not increment for any of the three inputs
- [ ] Regression test `vectorset_count_arithmetic_does_not_overflow` in
      `frogdb-server/crates/redis-regression/tests/vectorset_regression.rs` drives all three
      sites over the wire (VADD `VALUES` near-`usize::MAX`, VSIM `VALUES` near-`usize::MAX`, VSIM
      `COUNT` near `usize::MAX / 4` with a `FILTER`) and fails on today's code
- [ ] `just test frogdb-redis-regression vectorset_count_arithmetic`

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 99 (`.scratch/arch-deepening/proposals/99-vectorset-file-collapse.md`),
defect F1.

## Comments
