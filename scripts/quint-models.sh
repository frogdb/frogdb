#!/usr/bin/env bash
# Enumerate the *main* (runnable) Quint models under a directory of `.qnt` files.
#
# The models are modular: each one is several files — a types+constants module, a
# pure-logic module, a machine module (`var`s + actions), and one main module that
# imports the rest and owns the witnesses, invariants and `run` tests. Only the main
# module is runnable: `quint test`/`quint run`/`quint verify` need `init`/`step` and an
# invariant to check, which the satellite modules deliberately do not have. `quint
# typecheck`, by contrast, applies to every file — so `quint-check` keeps globbing the
# whole directory while `quint-run`/`quint-verify-*` key off this script.
#
# A main model is *derived*, not listed: a file is one iff it declares at least one
# `val inv_*` invariant. That is the same fact scripts/quint-invariants.sh already keys
# off (and refuses to run against a file without), so the two scripts cannot disagree,
# and — following task-4 review finding I1's reasoning about the old hardcoded
# per-model `case` — there is no hand-maintained manifest here for a new or renamed
# model to silently fall out of. Putting an invariant in a satellite module would
# promote it to a main model and make `quint run` fail loudly on its missing
# `init`/`step`, rather than silently skipping anything.
#
# Usage: scripts/quint-models.sh [dir]   (default dir: specs/quint)
# Prints one model path per line. Exits 0 with no output when the directory holds no
# `.qnt` files at all (the pre-first-model state the Justfile recipes already handle);
# exits 1 if it holds `.qnt` files but none of them is a main model, which is a
# configuration error rather than something to report success for.
set -uo pipefail

dir="${1:-specs/quint}"

shopt -s nullglob
files=("$dir"/*.qnt)
if [ ${#files[@]} -eq 0 ]; then
    exit 0
fi

found=0
for file in "${files[@]}"; do
    if grep -qE '\bval[[:space:]]+inv_[A-Za-z0-9_]+' "$file"; then
        echo "$file"
        found=1
    fi
done

if [ "$found" -eq 0 ]; then
    echo "quint-models.sh: $dir holds .qnt files but none declares a 'val inv_*' invariant — no runnable main model" >&2
    exit 1
fi
