#!/usr/bin/env bash
# Witness-floor gate: fail when a declared `val witness*` is observed in **0** sampled
# traces.
#
# Why this exists
# ---------------
# The mutation batteries under .scratch/formal-spec/ found a class of edit that every
# other oracle is blind to *by construction*: unwiring an action from `step` (admission
# battery rows A57/A58, feed-gate rows M48/M49, fullsync rows M112-M114).
#
#   - Invariants cannot catch it. Every `inv_*` is a safety predicate over reachable
#     states, and removing a transition can only *shrink* the reachable set. No safety
#     property is falsifiable by removing behaviour.
#   - The `run` tests cannot catch it. They drive actions directly, so they never
#     consult `step` at all.
#
# The one observable is reachability: the witness that behaviour carried collapses to
# 0 traces (measured, e.g. fullsync M112 drove `witnessReappliedOverlap` 20 -> 0 and
# `witnessTailApplied` 63 -> 0; M114 zeroed one witness and quartered six others).
# `quint run --witnesses` reports exactly that number, and until this gate existed
# nothing in `just quint-*` failed on it — it was a hand-run lane, i.e. a documented
# detection hole. Gating it here closes the class for every model at once.
#
# Determinism
# -----------
# Sampling is random, so the gate pins `--seed` (default: two seeds, matching the
# batteries' 0x1/0x2 convention) and a fixed sample/step budget. CI and local runs see
# the same counts for the same tree. A witness passes if it is observed in >0 traces
# under **any** pinned seed — two seeds cost little and take the sting out of a witness
# that sits near the sampling floor.
#
# Exemptions
# ----------
# Some witnesses are legitimately unreachable *under sampling* at the PR-lane budget:
# either unreachable by construction (a model states the state a superseded design
# revision could reach, so that reverting the narrowing makes the witness fire), or
# reachable only down a long, specific choice sequence a uniform random walk
# essentially never takes — those carry a deterministic `run` test instead. Both are
# listed, one line each with a written reason, in:
#
#     specs/quint/witness-floor-exemptions.txt
#
# A main model that declares no witnesses at all is the same kind of hole one size
# larger, and is written down the same way: a row whose witness field is `*`.
#
# The list is kept honest in both directions, the same way `just lint-spec` keeps
# spec rows and tests honest: an exemption naming a model or witness that does not
# exist fails the gate, and so does an exemption for a witness that the run *did*
# observe (a stale exemption is a hole that has quietly closed — delete the line).
#
# Usage
# -----
#   scripts/quint-witness-gate.sh [model.qnt ...]
#
# With no arguments, gates every main model reported by scripts/quint-models.sh.
# Environment overrides (for escalating a lane by hand — the batteries run 1000x25 and
# 4000x40; note that raising the budget can make an exemption stale, which is a failure
# by design, not a surprise):
#
#   QUINT_WITNESS_SAMPLES      --max-samples          (default 2000)
#   QUINT_WITNESS_STEPS        --max-steps            (default 40)
#   QUINT_WITNESS_SEEDS        space-separated seeds  (default "0x1 0x2")
#   QUINT_WITNESS_EXEMPTIONS   exemption list path    (default specs/quint/witness-floor-exemptions.txt)
set -uo pipefail

SAMPLES="${QUINT_WITNESS_SAMPLES:-2000}"
STEPS="${QUINT_WITNESS_STEPS:-40}"
SEEDS="${QUINT_WITNESS_SEEDS:-0x1 0x2}"
EXEMPTIONS="${QUINT_WITNESS_EXEMPTIONS:-specs/quint/witness-floor-exemptions.txt}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -gt 0 ]; then
    models=("$@")
else
    # No `mapfile`: it is a bash-4 builtin and macOS ships bash 3.2 as /bin/bash.
    models=()
    listing=$("$here/quint-models.sh") || exit 1
    while read -r m; do
        [ -n "$m" ] && models+=("$m")
    done <<< "$listing"
    if [ "${#models[@]}" -eq 0 ]; then
        echo "quint-witness-gate: no models under specs/quint/ yet — nothing to gate"
        exit 0
    fi
fi

tmp=$(mktemp -d) || exit 1
trap 'rm -rf "$tmp"' EXIT

status=0

# Exemptions are keyed by the model's repo-relative path, so `./specs/...` and
# `specs/...` must not read as two different models.
strip_dot_slash() { echo "${1#./}"; }

# --- exemption list: parse once, and validate every row against the tree ------------
#
# Rows are `<model-path> <witnessName> <reason...>`; the reason is mandatory and free
# text. Keyed by model *path* rather than by bare witness name so that two models may
# each declare a witness of the same name (they already do: `witnessAckIgnored` exists
# in both replication models) without one model's exemption silently covering the other.
: > "$tmp/exemptions"
if [ -f "$EXEMPTIONS" ]; then
    while read -r line; do
        case "$line" in ''|'#'*) continue ;; esac
        ex_model=$(strip_dot_slash "$(echo "$line" | awk '{print $1}')")
        ex_witness=$(echo "$line" | awk '{print $2}')
        ex_reason=$(echo "$line" | awk '{$1=""; $2=""; sub(/^[[:space:]]+/, ""); print}')
        if [ -z "$ex_witness" ] || [ -z "$ex_reason" ]; then
            echo "::error::$EXEMPTIONS: malformed row (want '<model.qnt> <witnessName> <reason>'): $line" >&2
            status=1
            continue
        fi
        if [ ! -f "$ex_model" ]; then
            echo "::error::$EXEMPTIONS: dangling exemption — no such model '$ex_model' (witness $ex_witness)" >&2
            status=1
            continue
        fi
        # `*` is the model-level row: "this main model declares no witnesses at all, on
        # purpose". It is still a hole (an action unwired from its `step` is invisible),
        # so it has to be written down and justified like any other, and it goes stale
        # the moment the model grows a witness.
        if [ "$ex_witness" = '*' ]; then
            if grep -qE '\bval[[:space:]]+witness[A-Za-z0-9_]+' "$ex_model"; then
                echo "::error::$EXEMPTIONS: stale exemption — $ex_model declares witnesses now, so its '*' (no-witnesses) row must go" >&2
                status=1
                continue
            fi
            printf '%s\t%s\t%s\n' "$ex_model" "$ex_witness" "$ex_reason" >> "$tmp/exemptions"
            continue
        fi
        if ! grep -qE "\bval[[:space:]]+${ex_witness}[[:space:]]*:" "$ex_model"; then
            echo "::error::$EXEMPTIONS: dangling exemption — $ex_model declares no witness '$ex_witness'" >&2
            status=1
            continue
        fi
        printf '%s\t%s\t%s\n' "$ex_model" "$ex_witness" "$ex_reason" >> "$tmp/exemptions"
    done < "$EXEMPTIONS"
fi

# --- the gate itself ----------------------------------------------------------------
for model_arg in "${models[@]}"; do
    model=$(strip_dot_slash "$model_arg")
    witnesses=$("$here/quint-witnesses.sh" "$model" 2>/dev/null)
    case $? in
        0) ;;
        2)  # No witnesses declared: allowed only with a `<model> * <reason>` row.
            reason=$(awk -F'\t' -v m="$model" '$1 == m && $2 == "*" {print $3; exit}' "$tmp/exemptions")
            if [ -n "$reason" ]; then
                echo "=== witness floor: $model — skipped, declares no witnesses: $reason"
            else
                echo "::error::$model declares no 'val witness*' reachability witness, so nothing about it is gated — an action unwired from its 'step' would be invisible. Give it a witness, or record the model in $EXEMPTIONS as '$model * <reason>'."
                status=1
            fi
            continue
            ;;
        *)  echo "::error::scripts/quint-witnesses.sh failed on $model" >&2
            status=1
            continue
            ;;
    esac
    n_witnesses=$(echo "$witnesses" | wc -w | tr -d ' ')
    echo "=== witness floor: $model ($n_witnesses witnesses, ${SAMPLES}x${STEPS}, seeds: $SEEDS)"

    : > "$tmp/counts"
    run_failed=0
    for seed in $SEEDS; do
        # `--invariant=true` (the quint default) on purpose: the invariant lane is
        # `quint-run`'s own `--invariants` pass. Keeping this run's exit code free of
        # invariant verdicts means a nonzero rc here is unambiguously a *tool* failure.
        # shellcheck disable=SC2086  # witnesses is a deliberately word-split list
        if ! quint run "$model" --max-samples="$SAMPLES" --max-steps="$STEPS" \
             --seed="$seed" --invariant=true --witnesses $witnesses > "$tmp/run.log" 2>&1; then
            echo "::error::quint run failed on $model at seed $seed (witness lane):" >&2
            cat "$tmp/run.log" >&2
            run_failed=1
            status=1
            continue
        fi
        grep -oE '^witness[A-Za-z0-9_]+ was witnessed in [0-9]+' "$tmp/run.log" \
            | awk '{print $1, $5}' >> "$tmp/counts"
    done
    [ "$run_failed" -eq 1 ] && continue

    for witness in $witnesses; do
        # Max across seeds: a witness observed under any pinned seed is reachable.
        count=$(awk -v w="$witness" '$1 == w {print $2}' "$tmp/counts" | sort -n | tail -1)
        if [ -z "$count" ]; then
            # quint accepted the name but reported nothing for it — a tool/parse
            # mismatch, not a reachability verdict. Never silently treated as "seen".
            echo "::error::$model: quint reported no witness count for '$witness' (witness lane parse failure)" >&2
            status=1
            continue
        fi
        reason=$(awk -F'\t' -v m="$model" -v w="$witness" '$1 == m && $2 == w {print $3; exit}' "$tmp/exemptions")
        if [ "$count" -gt 0 ]; then
            if [ -n "$reason" ]; then
                echo "::error::$model: witness '$witness' is exempt in $EXEMPTIONS but was observed in $count trace(s) — the exemption is stale, delete the line"
                status=1
            else
                printf '  ok       %-46s %s trace(s)\n' "$witness" "$count"
            fi
        elif [ -n "$reason" ]; then
            printf '  exempt   %-46s %s\n' "$witness" "$reason"
        else
            echo "::error::$model: witness '$witness' (module $(basename "$model" .qnt)) was observed in 0 sampled traces (budget ${SAMPLES}x${STEPS}, seeds [$SEEDS]) — an unreachable witness means the behaviour it names is gone (an action unwired from 'step' looks exactly like this) or the witness is dead. Fix the model, or record it in $EXEMPTIONS with a written reason."
            status=1
        fi
    done
done

if [ "$status" -ne 0 ]; then
    echo "quint-witness-gate: FAILED — see the ::error:: lines above" >&2
fi
exit $status
