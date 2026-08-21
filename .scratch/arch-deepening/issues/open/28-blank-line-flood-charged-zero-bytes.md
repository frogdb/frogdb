# 28 — Blank-line flood is charged zero bytes and zero commands to the ACL rate limiter

Status: needs-triage

**PARKED per user (security)** — do not implement without user ruling.

## What to build

`FrogDbResp2::decode` (`frogdb-server/crates/server/src/connection/codec.rs:107-118`) drains leading
`\r\n` pairs before any framing happens:

```rust
// 1. Strip leading empty lines (\r\n with no prefix byte).
//    Redis silently ignores these — common with telnet-style clients.
if src.len() >= 2 && src[0] == b'\r' && src[1] == b'\n' {
    let _ = src.split_to(2);
    continue;
}
```

Those bytes never become a `ParsedCommand`, so they never reach `estimate_command_size`
(`crates/server/src/connection.rs:379-380`) and therefore never reach `check_rate_limit`
(`connection.rs:403` → `connection/guards.rs:145-163`). Neither the ACL byte quota nor the ACL
command-count quota is charged. A client can consume server read bandwidth, kernel-to-userspace
copies and decoder loop iterations at **zero quota cost**, indefinitely, and the quota it is
nominally subject to never advances. The direction matters: this is an **under**-charge, unlike the
inline-command over-charge in the same accounting seam, which fails safe.

**No other limiter covers raw read volume.** `frogdb-server/crates/protocol/src/limits.rs` defines
`PROTO_MAX_BULK_LEN` (`:21`, a per-bulk-string ceiling), `PROTO_MAX_MULTIBULK_LEN` (`:26`, a
per-array element count) and `MAX_INTERNAL_FRAME_LEN` (`:43`, an internal-transport ceiling).
Redis's `PROTO_MAX_QUERYBUF_LEN` — the accumulated-request cap, the only member of that family that
bounds read *volume* — appears in the tree exactly once, inside a doc comment at `limits.rs:35`
describing where `MAX_INTERNAL_FRAME_LEN`'s number came from. There is no constant, no config key
and no check.

**Severity is bounded, and the bound is why this is filed as a classification rather than an
incident.** The drain is `split_to(2); continue;` — it *consumes* two bytes per iteration and
re-enters the loop, so the read buffer does not accumulate and `query_buf_size`
(`crates/server/src/connection/lifecycle.rs`, `framed.read_buffer().len()`) stays flat under the
flood. There is no unbounded allocation and **no memory-exhaustion path**; the cost is CPU and
network bandwidth only — a generic connection-level flood of the kind a transport-layer limit
already covers, charged against no quota. Exposure is further bounded on the quota side:
`check_rate_limit` returns `None` before any accounting unless the connection is authenticated as a
user that carries an ACL rate limit (`guards.rs:146-150`), so an unlimited user is unaffected by
construction. The real defect is a completeness gap in the quota — a limiter that claims to bound a
client's byte rate has a byte channel it structurally cannot see.

Fix direction, if the ruling is to fix: charge consumed-but-unframed bytes to the same accounting
seam the framed path uses — measure bytes consumed by the decoder rather than bytes of a
reconstructed `ParsedCommand` — which also incidentally closes the inline-command over-charge.
A separate, larger question is whether FrogDB should carry a `proto-max-querybuf-len` analogue at
all; that is a design decision, not part of this fix.

## Acceptance criteria

- [ ] A flood of `\r\n` pairs on a connection authenticated as a rate-limited ACL user advances the
      byte quota in proportion to the bytes consumed, rather than leaving it at zero.
- [ ] The inline-command path and the framed path charge the same seam, so no reachable decode path
      consumes bytes that no quota sees.
- [ ] Regression test `test_blank_line_flood_charges_the_byte_quota`
      (`frogdb-server/crates/server/tests/`): a user with a small ACL byte-rate limit sends N KiB of
      `\r\n` and is refused (or the quota is observably consumed) rather than being able to repeat
      indefinitely. Fails against today's tree.
- [ ] Regression test asserting `query_buf_size` stays flat under the same flood, pinning the
      no-accumulation property so a future fix cannot introduce a buffering path.
- [ ] `just test frogdb-server blank_line_flood` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 86 (`.scratch/arch-deepening/proposals/86-resp3-egress-codec.md`),
§Security classification — rate-limit accounting gap, filed and parked per standing policy.

## Comments
