# T10b full-sync battery driver

Drives the 204-row mutation battery behind `.scratch/formal-spec/2026-08-20-fullsync-battery.md`
over `specs/quint/replication_fullsync{,_types,_logic,_machine}.qnt`.

| file | role |
|---|---|
| `rows.py` | the pre-registered rows: id, file, exact single-site `old`→`new` replacement, expected catcher. Authored **before** any row was run. |
| `run_battery.py` | pass driver. Per row: assert `old` occurs exactly once, mutate, `quint test`, then sampled `quint run` at 500×20 over all 28 invariants (seeds `0x1`/`0x2`), attribute a CAUGHT-P by re-running one invariant at a time, restore byte-for-byte from `pristine/`, assert the restore is clean. |
| `escalate.py` | escalation pass: every row still green at 500×20 is re-run at 4000×40 for seeds `0x1`/`0x2`/`0x3` before it may be recorded MISSED. |
| `witness_rows.py` | witness-count evidence for the step-unwiring rows, which cannot violate a safety invariant (removing transitions only shrinks the reachable set) and are observable only as a witness collapsing to 0 traces. |
| `gen_table.py` | renders the verdict table, merging pass 1 (`fullsync_results.json` + `escalation.json`) with the post-closure pass 2 (`fullsync_results2.json` + `escalation2.json`) so a row that changed verdict prints `MISSED → **CAUGHT-T**`. |
| `coverage.py` | discrimination evidence: which `run` test / which invariant killed which rows. |
| `predictions.py` | pre-registration scoring: predicted catcher vs observed, split into exact hits, caught-by-a-different-oracle, correctly-predicted misses, pessimistic and optimistic predictions. |
| `assemble.py` | stitches `skeleton.md` + the `section_*.md` prose + the generated tables into the report. |

Raw verdicts are committed alongside the scripts so the report's table can be regenerated without
re-running the battery: `fullsync_results.json` + `escalation.json` (pass 1, model as committed in
`82a4ee22`), `fullsync_results2.json` + `escalation2.json` (pass 2, the same rows against the closed
model), `missed_ids.txt` (the 84 pass-1 misses, which is pass 2's input) and
`rows_prereg_snapshot.py` (the frozen pre-registration, for auditing predictions against outcomes).

Invoke with `python3 run_battery.py [ROW_ID ...]` (no args = every row not already in the results
JSON). `BATTERY_RESULTS` / `BATTERY_ESC` select the results files, which is how pass 2 re-runs the
pass-1 misses against the closed model without overwriting pass 1.

The scripts hard-code the scratch paths of the session that produced the report (`SCRATCH`, and the
`pristine/` copy of the model that every restore comes from), so drop them next to a `pristine/`
directory holding the four unmutated model files before re-running. Requires `quint` 0.32.0
(`eval "$(mise activate bash)"`, bash not zsh: `--invariants` takes a space-separated list and zsh
does not word-split an unquoted variable — the drivers pass an explicit `subprocess` argv instead).

Restores are verified by **byte comparison against `pristine/`**, not by `git diff`: `git diff`
compares the worktree against the index, and a concurrent lefthook in this shared tree can stage a
mid-battery snapshot. `git checkout -- specs/quint/` is never used — other agents have concurrent
work in that directory.
