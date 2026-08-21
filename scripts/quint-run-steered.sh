#!/usr/bin/env bash
# Steered sampled walk — the **opt-in** counterpart of `quint-run`'s uniform one.
#
# Why a second lane
# -----------------
# `quint run`'s sampler picks uniformly among the enabled actions of `step`. In a model
# whose action set is dominated by cheap, always-enabled churn (identity reports, ticks,
# adds/removes), the interesting *sequences* — a migration driven to commit, a demotion
# chained onto a demotion — sit many specific choices deep and are essentially never
# sampled at a PR-lane budget. Issue 41's residue family is the worked example: three
# real defects (R8, R9a, R9b) sat in reachable states that 200x20 uniform sampling had
# not visited in months of green runs, and a steered walk found all three in one pass.
#
# Steering is a **sampling-distribution** change and nothing else: the steered relation
# groups the same actions into a nested `any` and gates the churn family behind a coin,
# so every steered trace is a legal trace of `step`. It cannot make a violation up. What
# it can do is change which legal traces get looked at, which is why it does not replace
# the uniform lane:
#
#   - `just quint-run` (PR lane), the witness-floor gate and `quint verify` stay on the
#     flat `step`. They are the deterministic contract: same tree, same verdict, and no
#     coupling between "which invariant is checked" and "how the sampler was tuned".
#   - this lane is the *finder*. It runs a deeper budget over pinned seeds, one invariant
#     per invocation so a red cell names its own invariant, and it is where a steering
#     tweak belongs.
#
# A model opts in by declaring `action stepSteered` (see
# specs/quint/cluster_migration_failover_machine.qnt); models without one are skipped
# rather than run twice, so this stays model-agnostic in the same way
# scripts/quint-models.sh and scripts/quint-invariants.sh do — no hand-maintained list.
#
# Determinism
# -----------
# Budget and seeds are pinned (500x40 over the four seeds the issue-41 walk used), so a
# red cell here reproduces exactly, locally and in CI. The reproduction command is
# printed with every red cell.
#
# Usage
# -----
#   scripts/quint-run-steered.sh [model.qnt ...]
#
# With no arguments, walks every steered main model. Environment overrides:
#
#   QUINT_STEERED_SAMPLES   --max-samples          (default 500)
#   QUINT_STEERED_STEPS     --max-steps            (default 40)
#   QUINT_STEERED_SEEDS     space-separated seeds  (default "2 777 12345 20260819")
#   QUINT_STEERED_STEP      steered action name    (default stepSteered)
set -uo pipefail

SAMPLES="${QUINT_STEERED_SAMPLES:-500}"
STEPS="${QUINT_STEERED_STEPS:-40}"
SEEDS="${QUINT_STEERED_SEEDS:-2 777 12345 20260819}"
STEP_NAME="${QUINT_STEERED_STEP:-stepSteered}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -gt 0 ]; then
    candidates=("$@")
else
    # No `mapfile`: it is a bash-4 builtin and macOS ships bash 3.2 as /bin/bash.
    candidates=()
    listing=$("$here/quint-models.sh") || exit 1
    while read -r m; do
        [ -n "$m" ] && candidates+=("$m")
    done <<< "$listing"
    if [ "${#candidates[@]}" -eq 0 ]; then
        echo "quint-run-steered: no models under specs/quint/ yet — nothing to walk"
        exit 0
    fi
fi

# A main model is *steered* iff its module family declares the steered action. The
# relation lives in the machine module, which the main model imports, so the whole family
# (`<base>*.qnt`) is what gets scanned — the same "derive it, do not list it" rule
# quint-models.sh follows.
declares_steered_step() {
    local model="$1"
    local base="${model%.qnt}"
    grep -qE "\baction[[:space:]]+${STEP_NAME}\b" "$base"*.qnt 2>/dev/null
}

models=()
for model in "${candidates[@]}"; do
    if declares_steered_step "$model"; then
        models+=("$model")
    elif [ "$#" -gt 0 ]; then
        # Explicitly named on the command line: silently skipping it would report success
        # for a walk that never happened.
        echo "::error::$model declares no 'action $STEP_NAME' — it has no steered lane to run" >&2
        exit 1
    fi
done

if [ "${#models[@]}" -eq 0 ]; then
    echo "quint-run-steered: no model declares 'action $STEP_NAME' — nothing to walk"
    exit 0
fi

tmp=$(mktemp -d) || exit 1
trap 'rm -rf "$tmp"' EXIT

status=0
cells=0
red=0

for model in "${models[@]}"; do
    invariants=$("$here/quint-invariants.sh" "$model") || { status=1; continue; }
    n_inv=$(echo "$invariants" | wc -w | tr -d ' ')
    echo "=== steered walk: $model (--step $STEP_NAME, $n_inv invariants, ${SAMPLES}x${STEPS}, seeds: $SEEDS)"
    for inv in $invariants; do
        for seed in $SEEDS; do
            cells=$((cells + 1))
            quint run "$model" --step="$STEP_NAME" --max-samples="$SAMPLES" \
                --max-steps="$STEPS" --seed="$seed" --invariant="$inv" \
                > "$tmp/run.log" 2>&1
            rc=$?
            # Two independent signals, because they mean different things and neither
            # implies the other: a `[violation]` line is a counterexample, while a
            # nonzero rc with no such line is a tool failure (a type error, a missing
            # action name). Both are red, and the message says which.
            if grep -q '\[violation\]' "$tmp/run.log"; then
                red=$((red + 1))
                status=1
                echo "::error::VIOLATION $inv on $model at seed $seed (steered walk) — reproduce with: quint run $model --step=$STEP_NAME --max-samples=$SAMPLES --max-steps=$STEPS --seed=$seed --invariant=$inv"
                cat "$tmp/run.log"
            elif [ $rc -ne 0 ]; then
                red=$((red + 1))
                status=1
                echo "::error::TOOL FAILURE (exit $rc) on $model, invariant $inv, seed $seed (steered walk)"
                cat "$tmp/run.log" >&2
            else
                printf '  ok       %-56s seed=%s\n' "$inv" "$seed"
            fi
        done
    done
done

echo "=== steered walk: $red red / $cells cell(s)"
if [ "$status" -ne 0 ]; then
    echo "quint-run-steered: FAILED — see the ::error:: lines above" >&2
fi
exit $status
