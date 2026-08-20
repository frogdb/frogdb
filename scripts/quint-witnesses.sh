#!/usr/bin/env bash
# Derive a Quint model's `val witness*` reachability-witness names by scanning its
# source — the witness-lane twin of scripts/quint-invariants.sh, and for the same
# reason: Quint has no "list all witnesses" introspection command, and a
# hand-maintained per-model list has no gate keeping it in sync with the model, so a
# renamed or added witness would silently stop being checked while the runner still
# reports success (task-4 review finding I1).
#
# Witnesses are the only oracle that observes an action *unwired from `step`*: safety
# invariants are closed under removing transitions (the reachable set only shrinks) and
# the `run` tests drive actions directly rather than through `step`, so neither can
# fail. What does change is reachability — the witness that behaviour carried drops to
# 0 traces. See scripts/quint-witness-gate.sh, which turns that into a gate.
#
# Usage: scripts/quint-witnesses.sh <model.qnt>
# Prints a space-separated witness list on stdout.
#
# Exit codes:
#   0  witnesses printed
#   1  usage error (no model given)
#   2  the model declares zero `val witness*` values. Distinct from 1 because it is a
#      verdict, not a crash: a runnable model with no witness is ungateable (every
#      `--witnesses` check over it passes vacuously), so quint-witness-gate.sh treats it
#      as a hole that must be justified per-model in the exemption list — not as
#      something to report success for, and not as a broken invocation either.
set -uo pipefail

model="${1:?usage: quint-witnesses.sh <model.qnt>}"

witnesses=$(grep -oE '\bval[[:space:]]+witness[A-Za-z0-9_]+' "$model" | awk '{print $2}' | tr '\n' ' ')
witnesses="${witnesses% }"

if [ -z "$witnesses" ]; then
    echo "quint-witnesses.sh: $model declares no 'val witness*' reachability witnesses" >&2
    exit 2
fi

echo "$witnesses"
