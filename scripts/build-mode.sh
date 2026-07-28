#!/usr/bin/env bash
# Get or set the build/test execution mode for this worktree.
#
#   build-mode.sh                # print the current mode ("local" when unset)
#   build-mode.sh local|testbox  # record the mode
#
# "local" builds and tests on this machine; "testbox" offloads heavy compute to
# a Blacksmith testbox (see the blacksmith-testbox skill). The mode is stored in
# the worktree's git dir, so worktrees do not share a mode.
set -euo pipefail

modefile="$(git rev-parse --git-dir)/build-mode"

if [ "$#" -eq 0 ] || [ -z "${1:-}" ]; then
    mode=$(cat "$modefile" 2>/dev/null || true)
    printf '%s\n' "${mode:-local}"
    exit 0
fi

case "$1" in
local | testbox) ;;
*)
    echo "usage: build-mode.sh [local|testbox]" >&2
    exit 2
    ;;
esac

printf '%s\n' "$1" >"$modefile"
echo "build mode: $1 (recorded in $modefile)"
