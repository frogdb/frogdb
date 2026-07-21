#!/usr/bin/env bash
# Stop any Blacksmith testboxes recorded for this worktree by
# scripts/testbox-warmup.sh. Runs as a Claude Code SessionEnd hook, but is
# safe to invoke manually (just tb-stop). Idempotent; never fails the hook.
set -uo pipefail
export PATH="$HOME/.local/bin:$PATH"

command -v blacksmith >/dev/null 2>&1 || exit 0
gitdir=$(git rev-parse --git-dir 2>/dev/null) || exit 0
idfile="$gitdir/blacksmith-testboxes"
[ -f "$idfile" ] || exit 0

while IFS= read -r id; do
    [ -n "$id" ] || continue
    blacksmith testbox stop --id "$id" >/dev/null 2>&1 || true
done <"$idfile"
rm -f "$idfile"
