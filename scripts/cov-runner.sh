#!/usr/bin/env bash
# nextest target-runner wrapper that captures one *sparse* coverage profile per test.
#
# nextest forks a fresh process per test, so pointing LLVM_PROFILE_FILE at a
# per-test path is enough to separate profiles — no recompilation, no runtime
# cost beyond the profile write. Under plain `cargo test` every test in a binary
# shares one process and this would be impossible.
#
# Invoked by cargo/nextest as:
#     cov-runner.sh <test-binary> [args...]
#
# Required environment:
#     COV_PROFILE_DIR    directory to collect per-test .profdata into
#     COV_LLVM_PROFDATA  path to the llvm-tools-preview llvm-profdata binary
#
# Raw profiles are ~5-15 MB each; keeping 7000+ of them costs 40-100 GB. Merging
# each to a sparse .profdata immediately drops that to tens of KB per test with
# no information loss (llvm-profdata merge accepts .profdata as input, so the
# later aggregate merge is unaffected).

set -uo pipefail

if [[ $# -lt 1 ]]; then
    echo "cov-runner.sh: expected a test binary argument" >&2
    exit 64
fi

bin="$1"
shift

profile_dir="${COV_PROFILE_DIR:?cov-runner.sh: COV_PROFILE_DIR is not set}"
profdata_bin="${COV_LLVM_PROFDATA:?cov-runner.sh: COV_LLVM_PROFDATA is not set}"

bin_name="$(basename "$bin")"

# Classify the invocation. nextest runs a listing pass over every binary before
# executing anything; those profiles are meaningless and are thrown away.
# The test name is the first non-flag argument (nextest emits either
# `<name> --exact` or `--exact <name>` depending on version, so "first non-flag"
# is correct for both).
is_list=0
test_name=""
for arg in "$@"; do
    case "$arg" in
        --list) is_list=1 ;;
        -*) ;;
        *) [[ -z "$test_name" ]] && test_name="$arg" ;;
    esac
done

# Sanitize into a filename component. The exact, unsanitized name is recorded in
# the manifest so reports can show real test paths.
safe_name="${test_name:-_unnamed}"
safe_name="${safe_name//[^A-Za-z0-9._-]/_}"
# Keep the tail of very long names: the leaf test name is the discriminating part.
if (( ${#safe_name} > 160 )); then
    safe_name="${safe_name: -160}"
fi

if (( is_list )); then
    out_dir="$profile_dir/_list"
else
    out_dir="$profile_dir"
fi
mkdir -p "$out_dir"

stem="$out_dir/${bin_name}__${safe_name}__$$"

# Record the mapping *before* running the child. A test killed by nextest's
# slow-timeout never reaches the merge below; the orchestrator's sweep recovers
# its raw profiles, and this line is what tells the report the real test name.
# Short O_APPEND writes are atomic, so parallel test processes can share the
# manifest without locking. Entries whose .profdata never materializes are
# skipped by the report.
if (( ! is_list )); then
    printf '%s\t%s\t%s\n' \
        "$(basename "${stem}").profdata" "$bin_name" "${test_name:-_unnamed}" \
        >>"$profile_dir/manifest.tsv"
fi

# macOS SIP strips every DYLD_* variable when a protected binary is exec'd, and
# this script's `#!/usr/bin/env bash` shebang goes through one. Proc-macro test
# binaries link libstd dynamically via @rpath, so without a restored fallback
# path they abort at startup with "Library not loaded: @rpath/libstd-*.dylib".
# The orchestrator passes the path under a non-DYLD name that survives, and it is
# re-exported here — this script is not SIP-protected, so the child inherits it.
if [[ -n "${COV_DYLD_FALLBACK:-}" ]]; then
    export DYLD_FALLBACK_LIBRARY_PATH="${COV_DYLD_FALLBACK}${DYLD_FALLBACK_LIBRARY_PATH:+:$DYLD_FALLBACK_LIBRARY_PATH}"
fi

# %p expands to the writing process's pid. A test that spawns helper processes
# would otherwise have them all clobber a single file; with %p each gets its own
# and the merge below folds them together.
export LLVM_PROFILE_FILE="${stem}-%p.profraw"

# Deliberately not `exec`: the merge below has to run after the child exits.
"$bin" "$@"
status=$?

# Built by hand rather than by globbing into an array: macOS ships bash 3.2,
# where expanding an empty array under `set -u` is an error.
raws=()
for raw in "${stem}"-*.profraw; do
    [[ -e "$raw" ]] && raws+=("$raw")
done

if (( ${#raws[@]} == 0 )); then
    exit "$status"
fi

if (( is_list )); then
    rm -f "${raws[@]}"
    exit "$status"
fi

# On merge failure the .profraw files are left in place for the orchestrator's
# post-run sweep rather than discarded.
if "$profdata_bin" merge --sparse -o "${stem}.profdata" "${raws[@]}" 2>/dev/null; then
    rm -f "${raws[@]}"
fi

exit "$status"
