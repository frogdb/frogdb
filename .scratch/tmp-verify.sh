#!/usr/bin/env bash
# Temporary (issue 23 verification): run the determinism gate N times while the
# host is saturated with competing CPU work.
cd "$(dirname "$0")/.." || exit 1
export DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib
N=${N:-10}
LOAD=${LOAD:-8}

pids=()
for _ in $(seq "$LOAD"); do
  yes > /dev/null &
  pids+=($!)
done
trap 'kill "${pids[@]}" 2>/dev/null' EXIT

BIN=$(ls -t target/debug/deps/main-* | grep -v '\.' | head -1)
echo "binary: $BIN  load=$LOAD runs=$N"
pass=0; fail=0
for i in $(seq "$N"); do
  if "$BIN" --test-threads 1 --exact simulation::scheduler::test_cluster_scheduler_same_seed_same_run > /tmp/claude-501/verify-$i.log 2>&1; then
    pass=$((pass + 1)); echo "run $i: PASS"
  else
    fail=$((fail + 1)); echo "run $i: FAIL"
  fi
done
echo "VERIFY pass=$pass fail=$fail"
