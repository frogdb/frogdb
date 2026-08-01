#!/usr/bin/env bash
# SessionStart hook: tell the agent which build/test execution mode this
# worktree last used, and require it to confirm before doing build/test work.
# Stdout is injected into the session as context.
set -euo pipefail

mode=$("$(dirname "$0")"/build-mode.sh 2>/dev/null || echo local)

cat <<EOF
Build/test execution mode for this worktree: **$mode**.

Before the first build, test, lint, or benchmark command of this session, settle the mode:
- If the user's prompt names one ("local mode", "use the testbox"), use that and record it
  with \`just build-mode <mode>\`.
- Otherwise ask the user (local vs testbox) with the above as the pre-selected default.
  Do not guess, and do not switch modes mid-session on your own.

local  = build/test on this machine, testbox untouched (the default).
testbox = offload heavy compute via \`just tb-run\`; see the blacksmith-testbox skill.

Subagents inherit the session mode — state it explicitly in every dispatch prompt.
EOF
