#!/usr/bin/env bash
# Derive a Quint model's `val inv_*` invariant names by scanning its source,
# instead of hand-maintaining a duplicate list in the Justfile. Quint has no
# "list all invariants" introspection command, so this grep is the single
# source both `quint-run` (sampled smoke, PR lane) and `quint-verify-model`
# (exhaustive Apalache sweep, nightly) key off of — a hardcoded per-model list
# has no gate keeping it in sync with the model, so a renamed or added
# invariant would silently stop being checked by either tier while both still
# report success (task-4 review finding I1).
#
# Usage: scripts/quint-invariants.sh <model.qnt>
# Prints a space-separated invariant list on stdout. Exits 1 with a message on
# stderr if the model declares zero `val inv_*` invariants — a model with
# nothing to check is a configuration error, not something to silently run
# `quint run`/`quint verify` against with no `--invariant` (which defaults to
# the no-op `"true"` and reports success either way).
set -uo pipefail

model="${1:?usage: quint-invariants.sh <model.qnt>}"

invariants=$(grep -oE '\bval[[:space:]]+inv_[A-Za-z0-9_]+' "$model" | awk '{print $2}' | tr '\n' ' ')
invariants="${invariants% }"

if [ -z "$invariants" ]; then
    echo "quint-invariants.sh: $model declares no 'val inv_*' invariants" >&2
    exit 1
fi

echo "$invariants"
