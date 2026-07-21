#!/usr/bin/env bash
# Warm up a Blacksmith testbox and record its ID so the SessionEnd hook
# (scripts/testbox-cleanup.sh) can stop it when the agent session ends.
# Usage: testbox-warmup.sh [workflow] [extra warmup flags...]
set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"

workflow="${1:-test-unit-tests-testbox.yml}"
shift || true
idle="${TESTBOX_IDLE_TIMEOUT:-5}"
idfile="$(git rev-parse --git-dir)/blacksmith-testboxes"

out=$(blacksmith testbox warmup "$workflow" --idle-timeout "$idle" "$@" | tee /dev/stderr)

id=$(printf '%s\n' "$out" | grep -oE 'tbx_[A-Za-z0-9]+' | head -1 || true)
if [ -z "$id" ]; then
    id=$(printf '%s\n' "$out" | grep -oE -- '--id [A-Za-z0-9_-]+' | head -1 | cut -d' ' -f2 || true)
fi
if [ -z "$id" ]; then
    echo "warning: could not parse testbox ID from warmup output above;" >&2
    echo "         record it manually: echo <id> >> $idfile" >&2
    exit 0
fi
printf '%s\n' "$id" >>"$idfile"
echo "recorded testbox $id in $idfile (auto-stopped at session end)"
