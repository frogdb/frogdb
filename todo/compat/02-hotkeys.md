# 2. HOTKEYS Subsystem (43 tests — `hotkeys_tcl.rs`)

**Status**: Not implemented
**Scope**: Implement `HOTKEYS START/STOP/RESET/GET` subcommands for key access frequency sampling.

## Work

- Design sampling infrastructure (per-command key access interception)
- Implement START with METRICS (CPU, NET), COUNT, DURATION, SAMPLE, SLOTS parameters
- Implement STOP, RESET, GET with correct RESP2/RESP3 response formats
- Cluster-aware slot filtering
- Session lifecycle management (one active session at a time)
- Error handling for all invalid parameter combinations

**Key files to modify**: New module under `connection/handlers/`, `commands/metadata.rs`

## Tests

- `HOTKEYS START - METRICS required`
- `HOTKEYS START - METRICS with CPU only`
- `HOTKEYS START - METRICS with NET only`
- `HOTKEYS START - METRICS with both CPU and NET`
- `HOTKEYS START - with COUNT parameter`
- `HOTKEYS START - with DURATION parameter`
- `HOTKEYS START - with SAMPLE parameter`
- `HOTKEYS START - with SLOTS parameter in cluster mode`
- `HOTKEYS START - Error: session already started`
- `HOTKEYS START - Error: invalid METRICS count`
- `HOTKEYS START - Error: METRICS count mismatch`
- `HOTKEYS START - Error: METRICS invalid metrics`
- `HOTKEYS START - Error: METRICS same parameter`
- `HOTKEYS START - Error: COUNT out of range`
- `HOTKEYS START - Error: SAMPLE ratio invalid`
- `HOTKEYS START - Error: SLOTS not allowed in non-cluster mode`
- `HOTKEYS START - Error: SLOTS count mismatch`
- `HOTKEYS START - Error: SLOTS already specified`
- `HOTKEYS START - Error: duplicate slots`
- `HOTKEYS START - Error: invalid slot - negative value`
- `HOTKEYS START - Error: invalid slot - out of range`
- `HOTKEYS START - Error: invalid slot - non-integer`
- `HOTKEYS START - Error: slot not handled by this node`
- `HOTKEYS STOP - basic functionality`
- `HOTKEYS RESET - basic functionality`
- `HOTKEYS RESET - Error: session in progress`
- `HOTKEYS GET - returns nil when not started`
- `HOTKEYS GET - sample-ratio field`
- `HOTKEYS GET - no conditional fields without selected slots`
- `HOTKEYS GET - no conditional fields with sample_ratio = 1`
- `HOTKEYS GET - conditional fields with sample_ratio > 1 and selected slots`
- `HOTKEYS GET - selected-slots field with individual slots`
- `HOTKEYS GET - selected-slots with unordered input slots are sorted`
- `HOTKEYS GET - selected-slots returns full range in non-cluster mode`
- `HOTKEYS GET - selected-slots returns node's slot ranges when no SLOTS specified in cluster mode`
- `HOTKEYS GET - selected-slots returns each node's slot ranges in multi-node cluster`
- `HOTKEYS GET - RESP3 returns map with flat array values for hotkeys`
- `HOTKEYS - nested commands`
- `HOTKEYS - commands inside MULTI/EXEC`
- `HOTKEYS - EVAL inside MULTI/EXEC with nested calls`
- `HOTKEYS - tracks only keys in selected slots`
- `HOTKEYS - multiple selected slots`
- `HOTKEYS detection with biased key access, sample ratio = $sample_ratio`
