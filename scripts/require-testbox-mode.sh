#!/usr/bin/env bash
# Guard for the tb-* recipes: refuse to touch a testbox while the worktree is in
# local mode, so "local mode" actually means nothing runs remotely.
# BUILD_MODE=testbox overrides for a one-off run without switching the worktree.
set -euo pipefail

mode="${BUILD_MODE:-$("$(dirname "$0")"/build-mode.sh)}"
[ "$mode" = "testbox" ] || {
    echo "build mode is '$mode' — testbox commands are disabled." >&2
    echo "  switch:   just build-mode testbox" >&2
    echo "  one-off:  BUILD_MODE=testbox just tb-run \"...\"" >&2
    exit 1
}
