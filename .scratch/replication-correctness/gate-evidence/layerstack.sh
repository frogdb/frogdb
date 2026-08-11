#!/usr/bin/env bash
# Layer-stack run for the retro-validation gate (replication-correctness issue 15).
#
# Usage: layerstack.sh <label> [layer ...]
#   label  — evidence subdir under $GATE_OUT (default /tmp/claude/gate-evidence)
#   layer  — any of L2a L2b L3 L4 L6; empty means all of them
#
# Logs are written raw (never piped) so exit codes survive; the harness's own
# output only carries the tail of each log.
LABEL="${1:?label}"; shift
ROOT=/Users/nathan/workspace/frogdb/.claude/worktrees/agent-a1c7ba1e1a34abbba
OUT="${GATE_OUT:-/tmp/claude/gate-evidence}/$LABEL"
mkdir -p "$OUT"
cd "$ROOT" || exit 1

LAYERS=("$@")
has() {
  local x
  [ "${#LAYERS[@]}" -eq 0 ] && return 0
  for x in "${LAYERS[@]}"; do [ "$x" = "$1" ] && return 0; done
  return 1
}

run() {
  local name="$1"; shift
  echo "########## BEGIN $name ##########"
  "$@" > "$OUT/$name.log" 2>&1
  local rc=$?
  echo "rc=$rc" > "$OUT/$name.rc"
  echo "########## END $name rc=$rc ##########"
  tail -60 "$OUT/$name.log"
}

has L2a && run L2a-repl just test frogdb-replication
has L2b && run L2b-repl-runtime just test frogdb-replication-runtime
has L4 && run L4-seeded-smoke just concurrency-turmoil replication_scheduler
has L6 && run L6-integration just test frogdb-server replication
# The escalation tier: a wider seed block than the seven-seed smoke, for a
# defect the smoke misses. SEEDS overrides the count.
has L4big && run L4-seeded-"${SEEDS:-500}" just replication-seeds "${SEEDS:-500}"
# The full model-checking budget (release build, ignored by default).
has L3big && run L3-model-full just replication-model-check
echo "ALL DONE $LABEL"
