# The real RESP decoder lives in `server`, not `protocol` — relocate `FrogDbResp2`

Status: needs-triage
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: `INFRASTRUCTURE.md` "Two structural notes that change infra cost" · `MASTER.md` §6 structural note
Score: structural — changes the effort of 08/F2 and 08/F5 from 2 to 1
Area: frogdb-protocol / frogdb-server connection layer

## Context

`frogdb-protocol` reports 85.6% line coverage. That number describes the wrong code. The
decoder that actually parses bytes off a client socket — `FrogDbResp2` — is not in the
protocol crate at all; it lives in `server/src/connection/{codec,frame_io,util}.rs`. So the
protocol crate's coverage figure flatters it: the crate is well covered *and* the real parsing
surface is measured somewhere else, under a crate whose tests are about connections.

This is not itself a defect. It is a structural fact that raises the cost of several protocol
findings and makes one of the workspace's coverage numbers misleading. It was recorded as a
structural note rather than an `I<N>` item, which means no other issue owns it.

## Evidence

- `INFRASTRUCTURE.md`, "Two structural notes that change infra cost": *"**`FrogDbResp2` is not
  in the protocol crate.** The real decoder lives in `server/src/connection/{codec,frame_io,util}.rs`.
  Relocating it into `protocol` would drop 08/F2 and 08/F5 from effort 2 to 1, make the decoder
  fuzzable as one public surface without a server dependency, and make "protocol coverage" a
  meaningful number for the first time."*
- `MASTER.md` §6, closing structural note: *"the real RESP decoder is not in the protocol crate
  … which is why protocol's 85.6% flatters it."*
- `proposals/08-protocol.md` — F2 and F5 both carry effort 2 **because** exercising the decoder
  requires a server dependency; the same tests are effort 1 against a crate-local public surface.

## Options

- **(a) Relocate `FrogDbResp2` into `frogdb-protocol`.** The decoder becomes one public,
  crate-local surface: fuzzable without a server dependency, testable at boundary 1–2, and
  `protocol`'s coverage number starts describing the code it claims to. Cost is a cross-crate
  move touching the connection hot path, and `codec`/`frame_io`/`util` are currently free to
  reach into server-local types — the move will surface whatever coupling exists.
- **(b) Leave it and re-scope the protocol crate's identity** — document that `protocol` holds
  types and encoding while decoding lives in `server`, and stop quoting a `protocol` coverage
  number as if it covered parsing. Free, honest, and leaves 08/F2 and 08/F5 at effort 2 with the
  decoder reachable only through a server.
- **(c) Extract only the pure decode core**, leaving the tokio codec wiring in `server`. Gets
  the fuzz surface and the effort drop without moving the I/O plumbing; costs one more seam.

**No recommendation was recorded by the audit.** This needs a call from whoever owns the crate
boundary before 08/F2 and 08/F5 are scheduled — their effort scores depend on it.

## Acceptance criteria

- [ ] A decision is recorded here, in the issue, with its rationale.
- [ ] If (a) or (c): `FrogDbResp2`'s decode path is reachable from a `frogdb-protocol` test with
      no `frogdb-server` dev-dependency.
- [ ] If (a) or (c): a fuzz target exercises the decoder against the relocated public surface,
      and is registered alongside the existing targets (see issue 10,
      `.scratch/testing-improvements-round2/issues/`).
- [ ] If (b): `frogdb-protocol`'s crate docs state that decoding lives in
      `server/src/connection/`, and no doc or dashboard presents the protocol crate's coverage
      as parser coverage.
- [ ] Either way, `proposals/08` F2 and F5 are re-scored against the outcome before they are
      scheduled.

## Test boundary

1–2 if the decoder moves (crate-local public API over byte slices); today it is only reachable
at 4, over a socket through a full server. That gap *is* the finding — a decoder that can only
be tested end-to-end cannot be property-tested or fuzzed cheaply.

## Depends on

Nothing. Blocks the scheduling of findings 08/F2 and 08/F5, carried in issue 84,
`.scratch/testing-improvements-round2/issues/`.

## Re-triage 2026-08-06

**Verdict: still-valid**

Nothing moved and no decision was recorded. `FrogDbResp2` is still defined in
`frogdb-server/crates/server/src/connection/codec.rs` (1024 lines), still used only from
`server/src/connection.rs:79,103,260`, with `frame_io.rs` and `util.rs` alongside it — the paths in
the body are current. The campaign's `frogdb-net` crate is **not** a relocation of this code: it is
a 20-line conditional-networking shim that swaps tokio for turmoil types
(`crates/net/src/lib.rs`), so the "the campaign may have moved it incidentally" hypothesis is false.
What the campaign did add nearby is `frogdb-protocol`'s new `limits.rs` — `PROTO_MAX_BULK_LEN`,
`PROTO_MAX_MULTIBULK_LEN`, `MAX_INTERNAL_FRAME_LEN` — which centralises the wire-size ceilings the
codec enforces (round-2 issue 69, now in `done/`) and is a small precedent for option (c): the pure
limits moved to `protocol` while the tokio codec wiring stayed in `server`. Still needs a call from
whoever owns the crate boundary; `Status` left at `needs-triage`.
