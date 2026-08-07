# `durability_mode` is parsed twice, independently

Status: ready-for-agent
Type: bug (latent divergence)
Severity: likelihood 1/3 (needs a new mode or a spelling change to diverge), consequence 3/3 (the
periodic sync task silently not running is invisible until a crash) — score 3
Area: server boot / persistence config

## Problem

One config string decides two things, in two places, with no link between them:

- `server/src/server/util.rs:40-67` — `build_wal_config` maps the string to a `DurabilityMode`,
  with `unreachable!()` on an unknown value (sound only because `config/loader.rs:193` validated
  it first).
- `server/src/server/startup.rs:95` — an independent `durability_mode.to_lowercase() == "periodic"`
  decides whether `spawn_periodic_sync` runs.

Add a mode, rename one, or change the validator's accepted spellings, and the WAL can be
configured for periodic durability while the task that performs the periodic sync never starts.
The failure is silent: writes are acked, nothing syncs them, and the loss only shows up after a
crash.

## Fix

Derive the second decision from the `DurabilityMode` value `build_wal_config` already produced —
`matches!(mode, DurabilityMode::Periodic { .. })` — so there is one parse and one source of truth.
A follow-on W1 candidate: a lint for config values parsed from a string in more than one place.

## Forcing test

A boot test asserting that the periodic sync task exists if and only if the resolved
`DurabilityMode` is periodic, driven from the resolved mode rather than the string.

## Comments

Found by the campaign-2 durability-extraction survey, 2026-08-07.
