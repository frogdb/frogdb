# Advertise `redis_version:8.6.0` / HELLO `version 8.6.0`

Status: ready-for-agent
Type: bug (version fidelity)
Area: info / protocol

## Problem

`INFO` reports `redis_version:7.2.0`; `HELLO` reports `server frogdb`, `version 0.1.0`. Neither
matches the version FrogDB's own 2,298-test regression port actually validates against, and the
two disagree with each other even before comparing against Redis.

## Ruling (ADR-0005, ruling 2)

`INFO`'s `redis_version` field and `HELLO`'s `version` field both report **`8.6.0`** — the
tested compat target. `HELLO`'s `server` field stays `frogdb` (honest identity); `frogdb_version`
stays the product version (`0.1.0` or whatever it currently is). This is a Valkey-precedent
split: honest server identity, compat version in the version-negotiation fields, so
version-gating clients correctly enable 8.x-era feature checks instead of downgrading behavior
for a server that actually implements them.

## Also check

`website/src/data/versions.json` and any docs that mention `7.2` for client-version-detection
purposes — update references consistent with the new advertised version.

## Consequence to be aware of (informational, not blocking)

Per ADR-0005: claiming 8.6.0 makes every unadvertised gap a bug by definition against the compat
matrix. That's the intended effect — it's the same reasoning driving the rest of this PRD's
issues — not something this issue needs to additionally guard against.

## Acceptance criteria

- [ ] `INFO`'s server section shows `redis_version:8.6.0`
- [ ] `HELLO 2` and `HELLO 3` replies show `version 8.6.0`
- [ ] `HELLO`'s `server` field remains `frogdb`; `frogdb_version` remains the product version
- [ ] `website/src/data/versions.json` and any related docs updated for consistency
- [ ] `just docs-gen --check` stays green

Size: S
