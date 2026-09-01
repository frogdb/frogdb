# `XCFGSET` (Redis 8.6 stream IDMP config) is absent

Status: needs-triage
Type: gap (missing command) / ruling needed
Area: commands / stream

## Problem

`XCFGSET key ...` — Redis 8.6.0, group `stream`, arity -2, flags `WRITE FAST`, ACL `STREAM`, key
spec `RW UPDATE` at index 1, "Sets the IDMP configuration parameters for a stream" (vendored
metadata, `website/src/data/redis-commands-8x.json`) — is not registered. It arrived in the same
8.6 line FrogDB advertises as its compat target.

## Why `needs-triage`

IDMP (idempotent producer) is a *stream feature*, not just a config command: `XCFGSET` configures
per-stream deduplication parameters that only mean something if the surrounding producer-dedup
machinery exists. Registering `XCFGSET` alone would accept configuration for behavior FrogDB does
not implement — exactly the fabrication ADR-0005 forbids, and worse than the current
unknown-command error.

So the ruling needed is about the feature, not the command:

1. Does FrogDB intend to implement stream IDMP? If no, `XCFGSET` should be a registered
   `NotSupported` stub with a reason, or a documented exclusion — not silence.
2. If yes, `XCFGSET` lands with the feature, plus whatever `XADD` options and `XINFO STREAM` fields
   it adds.
3. Sweep the rest of the 8.6 stream surface for the same feature against the vendored metadata, so
   this is triaged once rather than command by command.

## Acceptance criteria (draft, pending ruling)

- [ ] Ruling recorded on stream IDMP support
- [ ] `XCFGSET` is implemented alongside the feature, registered as a truthful `NotSupported`, or
      listed as a documented exclusion with its reason
- [ ] The compat matrix note says which, instead of a bare "not implemented in FrogDB"

Size: unknown until the IDMP question is answered
