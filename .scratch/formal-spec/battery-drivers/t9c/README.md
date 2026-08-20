# T9c feed-gate battery driver

Drives the 129-row mutation battery behind `.scratch/formal-spec/2026-08-19-feed-gate-battery.md`
over `specs/quint/replication_feed_gate{,_types,_logic,_machine}.qnt`: `rows.py` holds the
pre-registered rows (mutation id, file, exact single-line replacement, expected verdict),
`run_battery.py` mutates the working tree a row at a time and `run_battery_{sb,p2,p3}.py` are the
same protocol against a sandbox copy (`sb` = pass-1 tail rows, `p2`/`p3` = pass-2 re-runs of the
closed rows).

Invoke with `python3 run_battery.py [ROW_ID ...]` (no args = every row not already in
`results.jsonl`). The scripts hard-code the scratch paths of the session that produced the report
— `SCR`, and for the sandbox variants `SPEC`/`BACKUP`/the results file — so point those at a
pristine copy of the model before re-running. Requires `quint` on `PATH` (`eval "$(mise activate
bash)"`, bash not zsh: the `--invariants` list must word-split).
