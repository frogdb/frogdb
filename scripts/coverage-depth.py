#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Coverage *depth*: per-line exec counts and per-function test diversity.

`just coverage` answers "is this line covered". This answers "how well tested is
this codepath", which is a different question with two signals:

  T1  exec counts + regions (hotness / reachability) — one aggregate
      `llvm-cov export` over the whole suite. Gives per-line counts, region
      coverage, and the "cold line" class (count == 1: executed exactly once
      across the entire suite, which is almost always an incidental touch).

  T2  per-function test diversity (breadth) — "N distinct tests execute this
      function, and here they are". Summed exec count is *not* a quality
      metric: a line in a hot loop touched by one test scores 1,000,000, a line
      touched once by 50 different tests scores 50. Breadth is the signal.

T2 is only affordable because the repo uses nextest, which forks one process per
test: per-test profiles cost nothing at runtime, only a different
LLVM_PROFILE_FILE per process (see `scripts/cov-runner.sh`). Post-processing
reads *function counters straight out of each per-test profile*
(`llvm-profdata show`, ~3 ms) and joins them against T1's `functions[]` table.
Running `llvm-cov export` per test would instead re-parse a 100 MB binary's
coverage map every time — hours, not minutes. Line-level *diversity* would need
exactly that, and is deliberately out of scope.

This drives the llvm-tools-preview binaries directly rather than going through
`cargo llvm-cov`, because `cargo llvm-cov report` insists on re-merging the
.profraw files this pipeline deliberately deletes and offers no way to hand it a
pre-merged aggregate. `just coverage` / `just coverage-lcov` are untouched.

Rust is pinned to stable 1.92.0, so branch coverage (`-Z coverage-options=branch`)
and MC/DC are unavailable. Region coverage is reported instead.

Subcommands:
    calibrate CRATE   measure the pipeline on one small crate (Phase 0 gate)
    run               build instrumented, pre-validate, run nextest, aggregate
    report            parse profiles + export, emit markdown / HTML / JSON
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import html
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Dedicated target dir: instrumented artifacts must not poison the normal
# `just build` / `just test` cache.
COV_TARGET = REPO / "target" / "covdepth"
PROFILE_DIR = COV_TARGET / "profiles"
AGGREGATE = COV_TARGET / "all.profdata"
OUT_DIR = REPO / "target" / "llvm-cov" / "depth"
EXPORT_JSON = OUT_DIR / "export.json"
OBJECTS_TXT = COV_TARGET / "objects.txt"
AUDIT_DIR = REPO / ".scratch" / "testing-improvements" / "audit"

# Matches what `cargo llvm-cov` reports on: dependency and toolchain sources are
# out of scope, and so are the test/bench/example targets' own sources — a test
# function trivially "covers itself", which would swamp both the totals
# (the cluster_*.rs suite alone is 14k lines) and the diversity classes. Keeping
# them out also makes the totals comparable to coverage-nightly.yml.
IGNORE_REGEX = (
    r"(^/rustc/|/\.cargo/registry/|/\.rustup/toolchains/|/target/"
    r"|/tests/|/benches/|/examples/)"
)

# Classification thresholds. Overridable on `report`.
DEFAULT_WELL_COVERED_TESTS = 5
DEFAULT_HOT_TESTS = 3
DEFAULT_HOT_EXEC_FLOOR = 1000
DEFAULT_TOP = 60
# Covering-test names kept inline per function in depth.json; the complete
# membership lives in tests.json.
TESTS_SAMPLE = 12

# Baseline anchors from .scratch/testing-improvements/audit/coverage-summary.md
# (2026-07-22). Used by `calibrate` to prove that never-executed code is visible
# in the export at all — if it is not, the tool would silently hide exactly the
# functions it exists to find, and the coverage RUSTFLAGS need -C link-dead-code.
DEAD_CODE_ANCHORS = [
    ("server/src/connection/builder.rs", "0.0% 0/175"),
    ("server/src/commands/info.rs", "0.8% 3/397"),
]


# --------------------------------------------------------------------------
# environment / toolchain
# --------------------------------------------------------------------------


def host_triple() -> str:
    out = subprocess.run(["rustc", "-vV"], capture_output=True, text=True, check=True).stdout
    for line in out.splitlines():
        if line.startswith("host:"):
            return line.split(":", 1)[1].strip()
    raise SystemExit("could not determine host triple from `rustc -vV`")


def llvm_bin_dir() -> Path:
    sysroot = Path(
        subprocess.run(
            ["rustc", "--print", "sysroot"], capture_output=True, text=True, check=True
        ).stdout.strip()
    )
    d = sysroot / "lib" / "rustlib" / host_triple() / "bin"
    if not (d / "llvm-profdata").exists():
        raise SystemExit(
            f"llvm-tools-preview not installed (missing {d / 'llvm-profdata'}).\n"
            "Run: rustup component add llvm-tools-preview"
        )
    return d


def build_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Cargo environment mirroring the Justfile's dyld/rocksdb prelude."""
    env = dict(os.environ)
    env.setdefault("LIBCLANG_PATH", "/opt/homebrew/opt/llvm/lib")
    if sys.platform == "darwin":
        env["DYLD_LIBRARY_PATH"] = "/opt/homebrew/opt/llvm/lib"
        env.setdefault("ROCKSDB_LIB_DIR", "/opt/homebrew/lib")
        env.setdefault("SNAPPY_LIB_DIR", "/opt/homebrew/lib")
    # sccache does not cache instrumented builds usefully and is disabled on
    # macOS in this repo anyway.
    env["RUSTC_WRAPPER"] = ""
    env["CARGO_TARGET_DIR"] = str(COV_TARGET)
    env["CARGO_INCREMENTAL"] = "0"
    env["RUSTFLAGS"] = (env.get("RUSTFLAGS", "") + " -C instrument-coverage").strip()
    if extra:
        env.update(extra)
    return env


def runner_env_var() -> str:
    return "CARGO_TARGET_" + host_triple().upper().replace("-", "_") + "_RUNNER"


def dyld_fallback() -> str:
    """Search path proc-macro test binaries need to find libstd at runtime.

    They link libstd dynamically via @rpath and carry no LC_RPATH, so cargo
    normally supplies this. macOS SIP strips DYLD_* across the exec of the
    `#!/usr/bin/env bash` target runner, so it has to be re-established there
    (see COV_DYLD_FALLBACK in scripts/cov-runner.sh).
    """
    sysroot = Path(
        subprocess.run(
            ["rustc", "--print", "sysroot"], capture_output=True, text=True, check=True
        ).stdout.strip()
    )
    return os.pathsep.join(
        [
            str(sysroot / "lib" / "rustlib" / host_triple() / "lib"),
            str(sysroot / "lib"),
            str(COV_TARGET / "debug" / "deps"),
            str(COV_TARGET / "debug"),
        ]
    )


def run(cmd, *, env=None, cwd=REPO, check=True, capture=False, quiet=False):
    if not quiet:
        print(f"  $ {' '.join(str(c) for c in cmd)}", flush=True)
    return subprocess.run(
        [str(c) for c in cmd],
        env=env,
        cwd=cwd,
        check=check,
        text=True,
        capture_output=capture,
    )


# --------------------------------------------------------------------------
# pipeline steps
# --------------------------------------------------------------------------


def cargo_scope(crate: str | None) -> list[str]:
    return ["-p", crate] if crate else ["--all"]


def build_instrumented(crate: str | None, env: dict[str, str]) -> list[Path]:
    """Build test binaries with instrumentation; return the executables."""
    cmd = ["cargo", "test", *cargo_scope(crate), "--no-run", "--message-format", "json"]
    print(f"  $ {' '.join(cmd)}", flush=True)
    proc = subprocess.Popen(cmd, cwd=REPO, env=env, stdout=subprocess.PIPE, text=True, bufsize=1)
    executables: list[Path] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("profile", {}).get("test") and msg.get("executable"):
            executables.append(Path(msg["executable"]))
    rc = proc.wait()
    if rc != 0:
        raise SystemExit(f"instrumented build failed (exit {rc})")
    # Same binary can be reported more than once across feature unification.
    seen, uniq = set(), []
    for e in executables:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    OBJECTS_TXT.parent.mkdir(parents=True, exist_ok=True)
    OBJECTS_TXT.write_text("\n".join(str(e) for e in uniq) + "\n")
    return uniq


def prevalidate(executables: list[Path], env: dict[str, str]) -> None:
    """Serially exec each fresh binary once before nextest storms them.

    ~40 freshly-built binaries hitting exec simultaneously is the documented
    `syspolicyd` wedge trigger on macOS (see CLAUDE.md): amfid crashes, every
    subsequent code-signature validation queues behind the wedged daemon, and
    processes hang at `_dyld_start` forever. One-at-a-time pre-validation pays
    the signature-check tax without the storm.
    """
    scratch = COV_TARGET / "_prevalidate"
    scratch.mkdir(parents=True, exist_ok=True)
    penv = dict(env)
    penv["LLVM_PROFILE_FILE"] = str(scratch / "discard-%p.profraw")
    penv["DYLD_FALLBACK_LIBRARY_PATH"] = dyld_fallback()
    failed = 0
    for i, exe in enumerate(executables, 1):
        print(f"  [{i}/{len(executables)}] validating {exe.name}", flush=True)
        try:
            proc = subprocess.run(
                [str(exe), "--list"],
                env=penv,
                cwd=REPO,
                capture_output=True,
                text=True,
                timeout=300,
            )
        except subprocess.TimeoutExpired:
            print(f"      WARNING: {exe.name} --list timed out", flush=True)
            failed += 1
            continue
        # A binary that cannot even list its tests will fail identically under
        # nextest and abort the whole run, so say so here rather than swallowing it.
        if proc.returncode != 0:
            failed += 1
            err = (proc.stderr or "").strip().splitlines()
            print(
                f"      WARNING: {exe.name} --list exited {proc.returncode}: "
                f"{err[0] if err else '(no stderr)'}",
                flush=True,
            )
    if failed:
        print(f"  {failed}/{len(executables)} binaries failed pre-validation", flush=True)
    shutil.rmtree(scratch, ignore_errors=True)


def nextest(crate: str | None, pattern: str | None, env: dict[str, str]) -> int:
    cmd = ["cargo", "nextest", "run", *cargo_scope(crate)]
    if pattern:
        cmd += ["-E", f"test(/{pattern}/)"]
    proc = run(cmd, env=env, check=False)
    if proc.returncode != 0:
        print(
            f"  NOTE: nextest exited {proc.returncode} (test failures do not "
            "invalidate the profiles; the report covers whatever ran)",
            flush=True,
        )
    return proc.returncode


def sweep_profraw(profdata_bin: Path) -> int:
    """Merge .profraw left behind by tests killed before their own merge ran."""
    leftovers = sorted(PROFILE_DIR.glob("*.profraw"))
    groups: dict[str, list[Path]] = defaultdict(list)
    for p in leftovers:
        # <stem>-<pid>.profraw  ->  <stem>
        groups[re.sub(r"-\d+\.profraw$", "", p.name)].append(p)
    recovered = 0
    for stem, raws in groups.items():
        out = PROFILE_DIR / f"{stem}.profdata"
        proc = subprocess.run(
            [str(profdata_bin), "merge", "--sparse", "-o", str(out), *map(str, raws)],
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            for r in raws:
                r.unlink(missing_ok=True)
            recovered += 1
        else:
            print(f"  WARNING: could not merge leftovers for {stem}", flush=True)
    shutil.rmtree(PROFILE_DIR / "_list", ignore_errors=True)
    return recovered


def aggregate(profdata_bin: Path) -> None:
    profiles = sorted(PROFILE_DIR.glob("*.profdata"))
    if not profiles:
        raise SystemExit(f"no per-test profiles in {PROFILE_DIR}")
    listing = COV_TARGET / "profiles.list"
    listing.write_text("\n".join(str(p) for p in profiles) + "\n")
    run(
        [profdata_bin, "merge", "--sparse", "-f", listing, "-o", AGGREGATE],
        quiet=False,
    )


def export(cov_bin: Path, executables: list[Path]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(cov_bin),
        "export",
        f"--instr-profile={AGGREGATE}",
        "--format=text",
        f"--ignore-filename-regex={IGNORE_REGEX}",
    ]
    for exe in executables:
        cmd += ["--object", str(exe)]
    print(f"  $ llvm-cov export ({len(executables)} objects) > {EXPORT_JSON}", flush=True)
    with EXPORT_JSON.open("w") as fh:
        proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise SystemExit(f"llvm-cov export failed:\n{proc.stderr}")
    if proc.stderr.strip():
        # Mismatched-function warnings are normal when a binary contributed no
        # profile; surface a sample rather than the whole flood.
        lines = proc.stderr.strip().splitlines()
        print(f"  llvm-cov warnings ({len(lines)}); first 3:", flush=True)
        for line in lines[:3]:
            print(f"    {line}", flush=True)


# --------------------------------------------------------------------------
# profile parsing (T2)
# --------------------------------------------------------------------------

_FN_LINE = re.compile(r"^  (\S+):$")
_COUNT_LINE = re.compile(r"^    Function count: (\d+)$")


def read_profile_functions(profdata_bin: str, path: str) -> dict[str, int]:
    """Function name -> entry count for one per-test profile.

    `llvm-profdata merge --sparse` already drops zero-valued counters, so a
    per-test profile lists only what that test actually executed.
    """
    proc = subprocess.run(
        [profdata_bin, "show", "--all-functions", "--counts", path],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return {}
    result: dict[str, int] = {}
    name: str | None = None
    for line in proc.stdout.splitlines():
        m = _FN_LINE.match(line)
        if m:
            name = m.group(1)
            continue
        if name is not None:
            c = _COUNT_LINE.match(line)
            if c:
                count = int(c.group(1))
                if count:
                    result[name] = result.get(name, 0) + count
                name = None
    return result


def read_manifest() -> list[tuple[str, str, str]]:
    manifest = PROFILE_DIR / "manifest.tsv"
    if not manifest.exists():
        raise SystemExit(f"missing {manifest} — run `coverage-depth.py run` first")
    seen: set[str] = set()
    rows: list[tuple[str, str, str]] = []
    for line in manifest.read_text().splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        fname, binary, test = parts
        if fname in seen:
            continue
        seen.add(fname)
        if not (PROFILE_DIR / fname).exists():
            continue  # test died before its profile was merged and swept
        rows.append((fname, binary, test))
    return rows


# --------------------------------------------------------------------------
# export parsing (T1)
# --------------------------------------------------------------------------


@dataclass
class FuncInfo:
    name: str
    filename: str
    line_start: int
    line_end: int
    regions: int
    regions_covered: int
    export_count: int


@dataclass
class FuncDepth:
    info: FuncInfo
    tests: list[str] = field(default_factory=list)
    suites: set[str] = field(default_factory=set)
    profile_exec_total: int = 0
    klass: str = "covered"


def crate_of(path: str) -> str:
    rel = path
    for marker in ("/frogdb-server/crates/", "/frogdb-server/ops/", "/frogdb-server/"):
        if marker in rel:
            tail = rel.split(marker, 1)[1]
            return tail.split("/", 1)[0]
    if "/frogctl/" in rel:
        return "frogctl"
    try:
        return str(Path(rel).relative_to(REPO)).split("/", 1)[0]
    except ValueError:
        return "external"


def short_path(path: str) -> str:
    try:
        rel = str(Path(path).relative_to(REPO))
    except ValueError:
        return path
    return rel.removeprefix("frogdb-server/crates/").removeprefix("frogdb-server/")


def line_counts(segments: list[list]) -> dict[int, int]:
    """Per-line execution counts, following llvm-cov's LineCoverageStats.

    Segments are [line, col, count, hasCount, isRegionEntry, isGapRegion]. This
    mirrors LLVM's `LineCoverageStats` constructor, including lines that carry no
    segment of their own but sit inside a region still open from an earlier line
    — those are mapped and counted too, so skipping them would under-report cold
    lines. `report` cross-checks the result against the export's own per-file
    line summary.
    """
    if not segments:
        return {}
    by_line: dict[int, list[list]] = defaultdict(list)
    for seg in segments:
        by_line[seg[0]].append(seg)
    counts: dict[int, int] = {}
    wrapped: list | None = None
    for line in range(min(by_line), max(by_line) + 1):
        segs = by_line.get(line, [])
        entries = [s for s in segs if s[3] and s[4] and not s[5]]
        starts_skipped = bool(segs) and not segs[0][3] and segs[0][4]
        mapped = (not starts_skipped) and (bool(entries) or (wrapped is not None and wrapped[3]))
        if mapped:
            count = wrapped[2] if wrapped is not None else 0
            for s in entries:
                count = max(count, s[2])
            counts[line] = count
        if segs:
            # the last segment on this line stays open across the following gap
            wrapped = segs[-1]
    return counts


def load_export() -> dict:
    if not EXPORT_JSON.exists():
        raise SystemExit(f"missing {EXPORT_JSON} — run `coverage-depth.py run` first")
    with EXPORT_JSON.open() as fh:
        return json.load(fh)


def export_filenames(export_data: dict) -> set[str]:
    return {f["filename"] for entry in export_data["data"] for f in entry.get("files", [])}


def index_functions(export_data: dict, keep: set[str]) -> dict[str, FuncInfo]:
    """Mangled name -> FuncInfo, folding a generic's instantiations together.

    `--ignore-filename-regex` prunes `files[]` but *not* `functions[]`, so the
    export still carries every dependency monomorphization instantiated into
    these binaries (24k of 26k entries for a single small crate). Restricting to
    files that survived the filter keeps the report about this repo's code.
    """
    out: dict[str, FuncInfo] = {}
    for entry in export_data["data"]:
        for fn in entry.get("functions", []):
            name = fn["name"]
            filenames = fn.get("filenames") or [""]
            if filenames[0] not in keep:
                continue
            regions = fn.get("regions", [])
            lines = [r[0] for r in regions] + [r[2] for r in regions]
            info = out.get(name)
            covered = sum(1 for r in regions if r[4] > 0)
            if info is None:
                out[name] = FuncInfo(
                    name=name,
                    filename=filenames[0],
                    line_start=min(lines) if lines else 0,
                    line_end=max(lines) if lines else 0,
                    regions=len(regions),
                    regions_covered=covered,
                    export_count=fn.get("count", 0),
                )
            else:
                info.line_start = min(info.line_start or 10**9, min(lines) if lines else 10**9)
                info.line_end = max(info.line_end, max(lines) if lines else 0)
                info.regions += len(regions)
                info.regions_covered += covered
                info.export_count = max(info.export_count, fn.get("count", 0))
    return out


def bin_stem(binary: str) -> str:
    return re.sub(r"-[0-9a-f]{16}$", "", binary)


def suite_of(binary: str, test: str) -> str:
    head = test.split("::", 1)[0] if "::" in test else ""
    stem = bin_stem(binary)
    return f"{stem}::{head}" if head else stem


# --------------------------------------------------------------------------
# demangling
# --------------------------------------------------------------------------


def demangle(names: list[str]) -> dict[str, str]:
    if not names:
        return {}
    # ~/.cargo/bin is not on PATH on this machine (cargo itself is a Homebrew
    # rustup shim), so fall back to the canonical cargo-install location.
    rustfilt = shutil.which("rustfilt")
    if not rustfilt:
        cand = Path.home() / ".cargo" / "bin" / "rustfilt"
        rustfilt = str(cand) if cand.exists() else None
    if not rustfilt:
        print("  NOTE: rustfilt not on PATH — reports will show mangled names", flush=True)
        return {n: n for n in names}
    proc = subprocess.run([rustfilt], input="\n".join(names), capture_output=True, text=True)
    if proc.returncode != 0:
        return {n: n for n in names}
    out = proc.stdout.splitlines()
    if len(out) != len(names):
        return {n: n for n in names}
    return dict(zip(names, out, strict=True))


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------


def build_depth(args) -> dict:
    profdata_bin = str(llvm_bin_dir() / "llvm-profdata")
    export_data = load_export()
    keep = export_filenames(export_data)
    all_export_names = {
        fn["name"] for entry in export_data["data"] for fn in entry.get("functions", [])
    }
    funcs = index_functions(export_data, keep)
    print(
        f"  export: {len(funcs)} in-repo functions "
        f"({len(all_export_names)} incl. dependency instantiations)",
        flush=True,
    )

    rows = read_manifest()
    print(f"  profiles: {len(rows)} per-test", flush=True)

    depth: dict[str, FuncDepth] = {n: FuncDepth(info=i) for n, i in funcs.items()}
    join_hits = 0
    join_total = 0

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = {
            pool.submit(read_profile_functions, profdata_bin, str(PROFILE_DIR / fname)): (
                binary,
                test,
            )
            for fname, binary, test in rows
        }
        done = 0
        for fut in concurrent.futures.as_completed(futures):
            binary, test = futures[fut]
            suite = suite_of(binary, test)
            label = f"{bin_stem(binary)}::{test}"
            for name, count in fut.result().items():
                # Hit-rate is measured against the *unfiltered* export: it asks
                # whether the two tools agree on symbol names, not whether the
                # symbol is in-repo.
                join_total += 1
                if name in all_export_names:
                    join_hits += 1
                d = depth.get(name)
                if d is None:
                    continue
                d.tests.append(label)
                d.suites.add(suite)
                d.profile_exec_total += count
            done += 1
            if done % 500 == 0:
                print(f"  ... {done}/{len(rows)} profiles read", flush=True)
    elapsed = time.time() - t0
    hit_rate = 100.0 * join_hits / max(1, join_total)
    print(
        f"  joined {len(rows)} profiles in {elapsed:.1f}s; name join hit-rate {hit_rate:.2f}%",
        flush=True,
    )

    # -- classification -----------------------------------------------------
    tested = [d for d in depth.values() if d.tests]
    exec_totals = sorted((d.info.export_count for d in tested), reverse=True)
    hot_floor = DEFAULT_HOT_EXEC_FLOOR
    if exec_totals:
        p90 = exec_totals[max(0, int(len(exec_totals) * 0.10) - 1)]
        hot_floor = max(args.hot_exec_floor, p90)

    for d in depth.values():
        n = len(set(d.tests))
        if n == 0:
            d.klass = "untested"
        elif n == 1:
            d.klass = "single-test"
        elif len(d.suites) == 1:
            d.klass = "monoculture"
        elif d.info.export_count >= hot_floor and n <= args.hot_tests:
            d.klass = "hot-but-shallow"
        elif n >= args.well_covered_tests and len(d.suites) >= 2:
            d.klass = "well-covered"
        else:
            d.klass = "covered"

    # -- per-file / per-crate rollups + cold lines --------------------------
    files_out = {}
    crate_totals: dict[str, dict[str, int]] = defaultdict(
        lambda: {"lines": 0, "lines_covered": 0, "regions": 0, "regions_covered": 0}
    )
    totals = {"lines": 0, "lines_covered": 0, "regions": 0, "regions_covered": 0}
    cold_lines: list[dict] = []
    # Cross-check: line_counts() reimplements llvm's LineCoverageStats, and its
    # output is verified to match `llvm-cov export --format=lcov` DA records
    # exactly (all 713 files, 2026-07-28). It does *not* match the export's own
    # per-file `summary`, and should not: that summary is the sum of per-function
    # line stats, so a line belonging to several function records (macro
    # expansions, generic instantiations) is counted once per function. The
    # recomputed figure is the de-duplicated per-file view; both are reported.
    recomputed = {"lines": 0, "lines_covered": 0, "files_diverged": 0}
    diverged: list[dict] = []

    for entry in export_data["data"]:
        for f in entry.get("files", []):
            path = f["filename"]
            s = f.get("summary", {})
            lc = line_counts(f.get("segments", []))
            crate = crate_of(path)
            ls, lcov = s["lines"]["count"], s["lines"]["covered"]
            rs, rcov = s["regions"]["count"], s["regions"]["covered"]
            crate_totals[crate]["lines"] += ls
            crate_totals[crate]["lines_covered"] += lcov
            crate_totals[crate]["regions"] += rs
            crate_totals[crate]["regions_covered"] += rcov
            for k, v in (
                ("lines", ls),
                ("lines_covered", lcov),
                ("regions", rs),
                ("regions_covered", rcov),
            ):
                totals[k] += v
            rec_lines = len(lc)
            rec_cov = sum(1 for c in lc.values() if c > 0)
            recomputed["lines"] += rec_lines
            recomputed["lines_covered"] += rec_cov
            if (rec_lines, rec_cov) != (ls, lcov):
                recomputed["files_diverged"] += 1
                diverged.append(
                    {
                        "file": short_path(path),
                        "export": [ls, lcov],
                        "recomputed": [rec_lines, rec_cov],
                    }
                )
            cold = sorted(line for line, c in lc.items() if c == 1)
            if cold:
                cold_lines.append({"file": short_path(path), "crate": crate, "lines": cold})
            files_out[short_path(path)] = {
                "path": short_path(path),
                "abs_path": path,
                "crate": crate,
                "lines": ls,
                "lines_covered": lcov,
                "line_percent": s["lines"]["percent"],
                "regions": rs,
                "regions_covered": rcov,
                "region_percent": s["regions"]["percent"],
                "cold_lines": len(cold),
                "line_counts": {str(k): v for k, v in sorted(lc.items())},
            }

    # -- serialize ----------------------------------------------------------
    interesting = [d for d in depth.values() if d.info.regions > 0]
    names = [d.info.name for d in interesting]
    dem = demangle(names)

    # Full function -> test membership is written to its own file: one hot
    # dispatch function can be entered by thousands of tests, and inlining every
    # name here made depth.json 500 MB (94 % of it repeated test-name strings).
    # Reports never need more than a handful per function, so depth.json keeps a
    # bounded sample and tests.json keeps the complete, interned membership.
    test_ids: dict[str, int] = {}
    membership: dict[str, list[int]] = {}

    fn_out = []
    for d in interesting:
        uniq_tests = sorted(set(d.tests))
        if uniq_tests:
            membership[d.info.name] = [test_ids.setdefault(t, len(test_ids)) for t in uniq_tests]
        fn_out.append(
            {
                "name": dem.get(d.info.name, d.info.name),
                "mangled": d.info.name,
                "file": short_path(d.info.filename),
                "crate": crate_of(d.info.filename),
                "line_start": d.info.line_start,
                "line_end": d.info.line_end,
                "regions": d.info.regions,
                "regions_covered": d.info.regions_covered,
                "exec_total": d.info.export_count,
                "profile_exec_total": d.profile_exec_total,
                "test_count": len(uniq_tests),
                "suite_count": len(d.suites),
                "suites": sorted(d.suites),
                "tests": uniq_tests[:TESTS_SAMPLE],
                "class": d.klass,
            }
        )
    fn_out.sort(key=lambda r: (r["file"], r["line_start"], r["name"]))

    class_counts: dict[str, int] = defaultdict(int)
    for r in fn_out:
        class_counts[r["class"]] += 1

    return {
        "membership": {
            "note": "written to tests.json, not depth.json",
            "tests": [t for t, _ in sorted(test_ids.items(), key=lambda kv: kv[1])],
            "functions": membership,
        },
        "generated": args.date,
        "toolchain": {
            "rustc": subprocess.run(
                ["rustc", "--version"], capture_output=True, text=True
            ).stdout.strip(),
            "branch_coverage": False,
            "note": "stable 1.92.0: -Z coverage-options=branch and MC/DC are nightly-only; region coverage reported instead",
        },
        "tests_profiled": len(rows),
        "join_hit_rate": hit_rate,
        "hot_exec_floor": hot_floor,
        "thresholds": {
            "well_covered_tests": args.well_covered_tests,
            "hot_tests": args.hot_tests,
            "hot_exec_floor": args.hot_exec_floor,
        },
        "totals": totals,
        "line_recompute": {
            **recomputed,
            "files_total": len(files_out),
            "worst": sorted(
                diverged,
                key=lambda r: -abs(r["export"][0] - r["recomputed"][0]),
            )[:10],
        },
        "crates": {k: v for k, v in sorted(crate_totals.items(), key=lambda kv: kv[0])},
        "class_counts": dict(class_counts),
        "files": files_out,
        "functions": fn_out,
        "cold_lines": sorted(cold_lines, key=lambda r: -len(r["lines"])),
    }


def pct(covered: int, total: int) -> float:
    return 100.0 * covered / total if total else 0.0


def render_markdown(data: dict, top: int) -> str:
    t = data["totals"]
    lines: list[str] = []
    w = lines.append
    w(f"# Coverage depth (per-line exec counts + per-function test diversity, {data['generated']})")
    w("")
    w(
        f"{data['tests_profiled']} per-test profiles joined against one aggregate "
        f"`llvm-cov export`; name join hit-rate {data['join_hit_rate']:.2f}%."
    )
    w(f"Toolchain: {data['toolchain']['rustc']} — {data['toolchain']['note']}.")
    w("")
    w("## Totals")
    w("")
    w(
        f"Lines {pct(t['lines_covered'], t['lines']):.1f}% "
        f"({t['lines_covered']}/{t['lines']}) · "
        f"Regions {pct(t['regions_covered'], t['regions']):.1f}% "
        f"({t['regions_covered']}/{t['regions']})"
    )
    w("")
    lr = data["line_recompute"]
    w(
        f"De-duplicated per-file line view: {lr['lines_covered']}/{lr['lines']} "
        f"({pct(lr['lines_covered'], lr['lines']):.1f}%). The totals above are llvm-cov's "
        f"own per-file summaries, which sum *per function*, so a line in several function "
        f"records is counted once per function; the two differ in "
        f"{lr['files_diverged']}/{lr['files_total']} files. The de-duplicated figure is "
        f"what the HTML gutter shows and matches `llvm-cov export --format=lcov` exactly."
    )
    w("")
    cc = data["class_counts"]
    order = ["untested", "single-test", "monoculture", "hot-but-shallow", "covered", "well-covered"]
    w("| class | functions | meaning |")
    w("|---|---:|---|")
    meanings = {
        "untested": "no test reaches it at all",
        "single-test": "one test is the entire safety net",
        "monoculture": "reached by >1 test but only one suite",
        "hot-but-shallow": f"exec_total >= {data['hot_exec_floor']} but <= {data['thresholds']['hot_tests']} tests",
        "covered": "middling breadth",
        "well-covered": f">= {data['thresholds']['well_covered_tests']} tests across >= 2 suites",
    }
    for k in order:
        if k in cc:
            w(f"| `{k}` | {cc[k]} | {meanings[k]} |")
    w("")
    w("## Per-crate")
    w("")
    w("| crate | lines | line % | regions | region % |")
    w("|---|---:|---:|---:|---:|")
    for crate, c in sorted(
        data["crates"].items(), key=lambda kv: pct(kv[1]["lines_covered"], kv[1]["lines"])
    ):
        w(
            f"| {crate} | {c['lines_covered']}/{c['lines']} | "
            f"{pct(c['lines_covered'], c['lines']):.1f}% | "
            f"{c['regions_covered']}/{c['regions']} | "
            f"{pct(c['regions_covered'], c['regions']):.1f}% |"
        )
    w("")

    fns = data["functions"]

    def section(title: str, rows: list[dict], why: str, cols: str) -> None:
        w(f"## {title}")
        w("")
        w(why)
        w("")
        if not rows:
            w("_(none)_")
            w("")
            return
        w(cols)
        w("|---|---|---:|---:|---|")
        for r in rows[:top]:
            tests = ", ".join(r["tests"][:3])
            if r["test_count"] > 3:
                tests += f", +{r['test_count'] - 3} more"
            w(
                f"| `{r['name'][:110]}` | {r['file']}:{r['line_start']} | "
                f"{r['exec_total']} | {r['test_count']} | {tests or '—'} |"
            )
        if len(rows) > top:
            w("")
            w(f"_showing {top} of {len(rows)}; full list in `depth.json`._")
        w("")

    untested = sorted(
        (r for r in fns if r["class"] == "untested"),
        key=lambda r: -r["regions"],
    )
    section(
        "Untested functions",
        untested,
        "`test_count == 0` — instrumented, instantiated, never entered by any test. "
        "Ranked by region count (bigger = more untested logic).",
        "| function | location | exec | tests | covering tests |",
    )

    single = sorted((r for r in fns if r["class"] == "single-test"), key=lambda r: -r["regions"])
    section(
        "Single-test functions",
        single,
        "One test is the entire safety net. Deleting or weakening that test silently "
        "removes all coverage of this function.",
        "| function | location | exec | tests | covering tests |",
    )

    mono = sorted((r for r in fns if r["class"] == "monoculture"), key=lambda r: -r["test_count"])
    section(
        "Monoculture functions",
        mono,
        "Reached by several tests, but all from a single suite. High line coverage here "
        "hides the fact that only one angle of attack is represented.",
        "| function | location | exec | tests | covering tests |",
    )

    hot = sorted(
        (r for r in fns if r["class"] == "hot-but-shallow"), key=lambda r: -r["exec_total"]
    )
    section(
        "Hot but shallow",
        hot,
        "The class that justifies this whole exercise: enormous exec counts, almost no "
        "test breadth. Both today's coverage percentage and raw exec counts report these "
        "as healthy.",
        "| function | location | exec | tests | covering tests |",
    )

    w("## Cold lines (`count == 1`)")
    w("")
    w(
        "Lines executed exactly once across the entire suite — almost always an incidental "
        "touch on the way to something else, not a tested path."
    )
    w("")
    w("| file | cold lines | first few |")
    w("|---|---:|---|")
    for r in data["cold_lines"][:top]:
        sample = ", ".join(str(x) for x in r["lines"][:10])
        if len(r["lines"]) > 10:
            sample += ", …"
        w(f"| {r['file']} | {len(r['lines'])} | {sample} |")
    if len(data["cold_lines"]) > top:
        w("")
        w(f"_showing {top} of {len(data['cold_lines'])} files; full list in `depth.json`._")
    w("")
    return "\n".join(lines) + "\n"


HTML_CSS = """
:root { --bg:#fff; --fg:#1c1c1e; --muted:#6b7280; --line:#e5e7eb; --accent:#2563eb;
        --untested:#dc2626; --single:#ea580c; --mono:#ca8a04; --hot:#7c3aed; --well:#16a34a; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#111317; --fg:#e5e7eb; --muted:#9ca3af; --line:#2a2f3a; --accent:#60a5fa; }
}
* { box-sizing: border-box; }
body { margin:0; padding:1.5rem; background:var(--bg); color:var(--fg);
       font:14px/1.5 ui-sans-serif,-apple-system,Segoe UI,sans-serif; }
h1 { font-size:1.4rem; margin:0 0 .25rem; }
h2 { font-size:1.05rem; margin:2rem 0 .5rem; border-bottom:1px solid var(--line); padding-bottom:.25rem; }
.sub { color:var(--muted); margin:0 0 1rem; }
table { border-collapse:collapse; width:100%; font-size:13px; }
th,td { text-align:left; padding:.3rem .5rem; border-bottom:1px solid var(--line); vertical-align:top; }
th { cursor:pointer; user-select:none; position:sticky; top:0; background:var(--bg); }
th:after { content:" ⇅"; color:var(--muted); font-size:10px; }
td.num, th.num { text-align:right; font-variant-numeric:tabular-nums; }
.wrap { overflow-x:auto; }
code, .mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }
.badge { display:inline-block; padding:0 .4rem; border-radius:.5rem; font-size:11px; color:#fff; }
.b-untested{background:var(--untested)} .b-single-test{background:var(--single)}
.b-monoculture{background:var(--mono)} .b-hot-but-shallow{background:var(--hot)}
.b-well-covered{background:var(--well)} .b-covered{background:var(--muted)}
.controls { display:flex; gap:.5rem; flex-wrap:wrap; margin:.5rem 0 1rem; }
input,select { padding:.3rem .5rem; border:1px solid var(--line); border-radius:.35rem;
               background:var(--bg); color:var(--fg); }
.fname { cursor:pointer; color:var(--accent); }
pre.src { margin:0; overflow-x:auto; border:1px solid var(--line); border-radius:.35rem; }
pre.src .ln { display:block; white-space:pre; }
pre.src .g { display:inline-block; width:4.5rem; text-align:right; padding-right:.5rem;
             color:var(--muted); border-right:1px solid var(--line); margin-right:.5rem; }
pre.src .t { display:inline-block; width:3rem; text-align:right; padding-right:.5rem; color:var(--muted); }
.zero { background:rgba(220,38,38,.14); }
.cold { background:rgba(202,138,4,.18); }
.hide { display:none; }
"""

HTML_JS = """
function sortable(tbl) {
  tbl.querySelectorAll('th').forEach((th, i) => th.addEventListener('click', () => {
    const body = tbl.tBodies[0];
    const rows = [...body.rows];
    const dir = th.dataset.dir === 'asc' ? -1 : 1;
    tbl.querySelectorAll('th').forEach(o => delete o.dataset.dir);
    th.dataset.dir = dir === 1 ? 'asc' : 'desc';
    rows.sort((a, b) => {
      const x = a.cells[i].dataset.v ?? a.cells[i].textContent;
      const y = b.cells[i].dataset.v ?? b.cells[i].textContent;
      const nx = parseFloat(x), ny = parseFloat(y);
      if (!isNaN(nx) && !isNaN(ny)) return (nx - ny) * dir;
      return String(x).localeCompare(String(y)) * dir;
    });
    rows.forEach(r => body.appendChild(r));
  }));
}
document.querySelectorAll('table.sortable').forEach(sortable);

function filterFns() {
  const q = document.getElementById('fq').value.toLowerCase();
  const k = document.getElementById('fk').value;
  document.querySelectorAll('#fns tbody tr').forEach(tr => {
    const okK = !k || tr.dataset.k === k;
    const okQ = !q || tr.dataset.s.includes(q);
    tr.classList.toggle('hide', !(okK && okQ));
  });
}
document.getElementById('fq').addEventListener('input', filterFns);
document.getElementById('fk').addEventListener('change', filterFns);

function showFile(p) {
  const box = document.getElementById('srcbox');
  const src = SOURCES[p];
  if (!src) { box.innerHTML = '<p class="sub">No source embedded for ' + p + '.</p>'; return; }
  const counts = FILES[p].line_counts || {};
  const fnLines = FNLINES[p] || [];
  const testAt = n => { for (const f of fnLines) if (n >= f[0] && n <= f[1]) return f[2]; return ''; };
  let out = '';
  src.split('\\n').forEach((text, i) => {
    const n = i + 1;
    const c = counts[n];
    const cls = c === undefined ? '' : (c === 0 ? ' zero' : (c === 1 ? ' cold' : ''));
    const t = testAt(n);
    out += '<span class="ln' + cls + '"><span class="g">' + (c === undefined ? '' : c) +
           '</span><span class="t">' + (t === '' ? '' : t + 'T') + '</span>' +
           text.replace(/&/g, '&amp;').replace(/</g, '&lt;') + '</span>';
  });
  box.innerHTML = '<h3 class="mono">' + p + '</h3><pre class="src">' + out + '</pre>';
  box.scrollIntoView({behavior: 'smooth'});
}
document.querySelectorAll('.fname').forEach(e =>
  e.addEventListener('click', () => showFile(e.dataset.p)));
"""


def render_html(data: dict, source_files: int, max_rows: int) -> str:
    t = data["totals"]
    # Embed sources only for the files that actually carry findings; the whole
    # repo would be tens of MB of HTML. The cap is stated in the page.
    interest: dict[str, int] = defaultdict(int)
    for r in data["functions"]:
        if r["class"] in ("untested", "single-test", "monoculture", "hot-but-shallow"):
            interest[r["file"]] += 1
    for r in data["cold_lines"]:
        interest[r["file"]] += len(r["lines"]) // 10
    chosen = [f for f, _ in sorted(interest.items(), key=lambda kv: -kv[1])][:source_files]

    sources = {}
    for rel in chosen:
        info = data["files"].get(rel)
        if not info:
            continue
        p = Path(info["abs_path"])
        if p.exists() and p.stat().st_size < 2_000_000:
            try:
                sources[rel] = p.read_text(errors="replace")
            except OSError:
                pass

    fnlines: dict[str, list[list]] = defaultdict(list)
    for r in data["functions"]:
        if r["file"] in sources:
            fnlines[r["file"]].append([r["line_start"], r["line_end"], r["test_count"]])

    files_min = {
        k: {"line_counts": v["line_counts"]} for k, v in data["files"].items() if k in sources
    }

    esc = html.escape
    out: list[str] = []
    w = out.append
    # Explicit charset: the page is opened straight off disk (file://) as often as
    # over HTTP, and without this the browser falls back to windows-1252 and
    # mangles every non-ASCII character in the report.
    w("<meta charset='utf-8'>")
    w("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    w(f"<title>Coverage depth — {esc(data['generated'])}</title>")
    w(f"<style>{HTML_CSS}</style>")
    w(f"<h1>Coverage depth — {esc(data['generated'])}</h1>")
    w(
        f"<p class='sub'>{data['tests_profiled']} per-test profiles · "
        f"lines {pct(t['lines_covered'], t['lines']):.1f}% ({t['lines_covered']}/{t['lines']}) · "
        f"regions {pct(t['regions_covered'], t['regions']):.1f}% "
        f"({t['regions_covered']}/{t['regions']}) · join hit-rate "
        f"{data['join_hit_rate']:.2f}%<br>{esc(data['toolchain']['rustc'])} — "
        f"{esc(data['toolchain']['note'])}</p>"
    )

    w(
        "<h2>Per-crate</h2><div class='wrap'><table class='sortable'><thead><tr>"
        "<th>crate</th><th class='num'>lines</th><th class='num'>line %</th>"
        "<th class='num'>regions</th><th class='num'>region %</th></tr></thead><tbody>"
    )
    for crate, c in sorted(data["crates"].items()):
        w(
            f"<tr><td>{esc(crate)}</td>"
            f"<td class='num' data-v='{c['lines']}'>{c['lines_covered']}/{c['lines']}</td>"
            f"<td class='num'>{pct(c['lines_covered'], c['lines']):.1f}</td>"
            f"<td class='num' data-v='{c['regions']}'>{c['regions_covered']}/{c['regions']}</td>"
            f"<td class='num'>{pct(c['regions_covered'], c['regions']):.1f}</td></tr>"
        )
    w("</tbody></table></div>")

    w(
        "<h2>Per-file</h2><div class='wrap'><table class='sortable'><thead><tr>"
        "<th>file</th><th>crate</th><th class='num'>line %</th><th class='num'>region %</th>"
        "<th class='num'>cold lines</th></tr></thead><tbody>"
    )
    for rel, f in sorted(data["files"].items(), key=lambda kv: kv[1]["line_percent"]):
        clickable = rel in sources
        name = (
            f"<span class='fname mono' data-p='{esc(rel)}'>{esc(rel)}</span>"
            if clickable
            else f"<span class='mono'>{esc(rel)}</span>"
        )
        w(
            f"<tr><td>{name}</td><td>{esc(f['crate'])}</td>"
            f"<td class='num'>{f['line_percent']:.1f}</td>"
            f"<td class='num'>{f['region_percent']:.1f}</td>"
            f"<td class='num'>{f['cold_lines']}</td></tr>"
        )
    w("</tbody></table></div>")

    w("<h2>Functions</h2>")
    w(
        "<div class='controls'><input id='fq' placeholder='filter by name / file / test…' "
        "size='40'><select id='fk'><option value=''>all classes</option>"
        + "".join(
            f"<option value='{k}'>{k} ({v})</option>"
            for k, v in sorted(data["class_counts"].items(), key=lambda kv: -kv[1])
        )
        + "</select></div>"
    )
    # Every class gets a share of the row budget. A straight priority sort would
    # not: `untested` alone is ~15k functions, so it would fill the whole table
    # and `hot-but-shallow` — 13 functions, and the reason this tool exists —
    # would never render. Classes are filled in priority order, each capped at an
    # equal share, and whatever budget the small classes leave over is handed
    # back to the larger ones.
    by_class: dict[str, list[dict]] = defaultdict(list)
    for r in data["functions"]:
        by_class[r["class"]].append(r)
    for rows in by_class.values():
        rows.sort(key=lambda r: (-r["regions"], -r["exec_total"]))
    priority = ["hot-but-shallow", "monoculture", "single-test", "untested", "covered"]
    order = [k for k in priority if k in by_class] + [k for k in by_class if k not in priority]

    shown, budget, remaining_classes = [], max_rows, len(order)
    per_class_note = []
    for klass in order:
        rows = by_class[klass]
        share = max(1, budget // remaining_classes) if remaining_classes else 0
        take = rows[: min(share, len(rows))]
        shown.extend(take)
        budget -= len(take)
        remaining_classes -= 1
        per_class_note.append(f"{klass} {len(take)}/{len(rows)}")
    total_fns = len(data["functions"])
    if len(shown) < total_fns:
        w(
            f"<p class='sub'>Showing {len(shown)} of {total_fns} functions — an equal share "
            f"per class, largest first within each, so no class is crowded out "
            f"({', '.join(per_class_note)}). The complete set is in <code>depth.json</code>, "
            f"and the full function→test membership in <code>tests.json</code>.</p>"
        )
    w(
        "<div class='wrap'><table id='fns' class='sortable'><thead><tr><th>function</th>"
        "<th>location</th><th class='num'>exec</th><th class='num'>tests</th>"
        "<th class='num'>suites</th><th>class</th><th>covering tests</th></tr></thead><tbody>"
    )
    for r in shown:
        tests = r["tests"][:6]
        more = r["test_count"] - len(tests)
        tip = esc("\n".join(r["tests"]))
        if more > 0:
            tip += esc(f"\n… +{more} more (see tests.json)")
        tests_cell = esc(", ".join(tests)) + (f" +{more}" if more > 0 else "")
        search = esc(f"{r['name']} {r['file']} {' '.join(r['tests'])}".lower())
        w(
            f"<tr data-k='{r['class']}' data-s=\"{search}\">"
            f"<td class='mono' title=\"{esc(r['mangled'])}\">{esc(r['name'][:130])}</td>"
            f"<td class='mono'>{esc(r['file'])}:{r['line_start']}</td>"
            f"<td class='num'>{r['exec_total']}</td>"
            f"<td class='num'>{r['test_count']}</td>"
            f"<td class='num'>{r['suite_count']}</td>"
            f"<td><span class='badge b-{r['class']}'>{r['class']}</span></td>"
            f'<td title="{tip}">{tests_cell}</td></tr>'
        )
    w("</tbody></table></div>")

    w(
        f"<h2>Source view</h2><p class='sub'>Gutter: left = aggregate exec count, right = "
        f"number of distinct tests reaching the enclosing function. Red = never executed, "
        f"amber = executed exactly once. Sources embedded for the {len(sources)} highest-signal "
        f"files of {len(data['files'])} total (kept self-contained on purpose); the rest are "
        f"listed above without a source view.</p><div id='srcbox'></div>"
    )

    w("<script>")
    w("const SOURCES=" + json.dumps(sources) + ";")
    w("const FILES=" + json.dumps(files_min) + ";")
    w("const FNLINES=" + json.dumps(fnlines) + ";")
    w(HTML_JS)
    w("</script>")
    return "\n".join(out)


# --------------------------------------------------------------------------
# subcommands
# --------------------------------------------------------------------------


def cmd_run(args) -> int:
    llvm = llvm_bin_dir()
    env = build_env()

    if not args.keep_profiles:
        shutil.rmtree(PROFILE_DIR, ignore_errors=True)
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    print("== build (instrumented) ==", flush=True)
    t0 = time.time()
    executables = build_instrumented(args.crate, env)
    print(f"  {len(executables)} test binaries in {time.time() - t0:.1f}s", flush=True)

    print("== pre-validate binaries (macOS syspolicyd guard) ==", flush=True)
    prevalidate(executables, env)

    print("== nextest (per-test profiles) ==", flush=True)
    renv = dict(env)
    renv[runner_env_var()] = str(REPO / "scripts" / "cov-runner.sh")
    renv["COV_PROFILE_DIR"] = str(PROFILE_DIR)
    renv["COV_LLVM_PROFDATA"] = str(llvm / "llvm-profdata")
    renv["COV_DYLD_FALLBACK"] = dyld_fallback()
    t0 = time.time()
    nextest(args.crate, args.pattern, renv)
    print(f"  nextest wall time {time.time() - t0:.1f}s", flush=True)

    print("== sweep leftover raw profiles ==", flush=True)
    recovered = sweep_profraw(llvm / "llvm-profdata")
    profiles = list(PROFILE_DIR.glob("*.profdata"))
    stray = list(PROFILE_DIR.glob("*.profraw"))
    print(
        f"  {len(profiles)} per-test profiles ({recovered} recovered from timeouts), "
        f"{len(stray)} stray .profraw remaining",
        flush=True,
    )

    print("== aggregate merge (T1) ==", flush=True)
    t0 = time.time()
    aggregate(llvm / "llvm-profdata")
    print(
        f"  {AGGREGATE} ({AGGREGATE.stat().st_size / 1e6:.1f} MB) in {time.time() - t0:.1f}s",
        flush=True,
    )

    print("== llvm-cov export (T1) ==", flush=True)
    t0 = time.time()
    export(llvm / "llvm-cov", executables)
    print(
        f"  {EXPORT_JSON} ({EXPORT_JSON.stat().st_size / 1e6:.1f} MB) in {time.time() - t0:.1f}s",
        flush=True,
    )
    print("\nNext: ./scripts/coverage-depth.py report", flush=True)
    return 0


def cmd_report(args) -> int:
    data = build_depth(args)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    lr = data["line_recompute"]
    t = data["totals"]
    print(
        f"  lines: {t['lines_covered']}/{t['lines']} summed per function, "
        f"{lr['lines_covered']}/{lr['lines']} de-duplicated per file "
        f"({lr['files_diverged']}/{lr['files_total']} files where the two differ)",
        flush=True,
    )
    if lr["lines"] > t["lines"]:
        # The de-duplicated view can never exceed the per-function sum; if it
        # does, line_counts() is mapping lines llvm-cov does not.
        print("  WARNING: de-duplicated line total exceeds the export summary", flush=True)

    membership = data.pop("membership")
    tests_path = OUT_DIR / "tests.json"
    tests_path.write_text(json.dumps(membership, separators=(",", ":")))
    print(f"  Tests    {tests_path} ({tests_path.stat().st_size / 1e6:.1f} MB)", flush=True)

    json_path = Path(args.json) if args.json else OUT_DIR / "depth.json"
    # Compact: this file is machine-readable backing data, and per-line count
    # maps make a pretty-printed version several times larger for no benefit.
    json_path.write_text(json.dumps(data, separators=(",", ":")))
    print(f"  JSON     {json_path} ({json_path.stat().st_size / 1e6:.1f} MB)", flush=True)

    md_path = Path(args.md) if args.md else AUDIT_DIR / f"coverage-depth-{data['generated']}.md"
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_markdown(data, args.top))
    print(f"  Markdown {md_path}", flush=True)

    html_path = Path(args.html) if args.html else OUT_DIR / "index.html"
    html_path.write_text(render_html(data, args.source_files, args.max_html_rows))
    print(f"  HTML     {html_path} ({html_path.stat().st_size / 1e6:.1f} MB)", flush=True)
    return 0


def cmd_calibrate(args) -> int:
    """Phase 0 gate: measure the pipeline on one small crate.

    Prints the real per-unit costs that decide whether a full-suite run is
    affordable, plus the two checks that are genuine risks rather than
    formalities: the profile<->export name join hit-rate, and whether
    never-executed code is visible in the export at all.
    """
    llvm = llvm_bin_dir()
    env = build_env()
    timings: list[tuple[str, str]] = []

    shutil.rmtree(PROFILE_DIR, ignore_errors=True)
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"== calibrating on {args.crate} ==", flush=True)
    t0 = time.time()
    executables = build_instrumented(args.crate, env)
    timings.append(
        ("instrumented build", f"{time.time() - t0:.1f} s ({len(executables)} binaries)")
    )

    prevalidate(executables, env)

    renv = dict(env)
    renv[runner_env_var()] = str(REPO / "scripts" / "cov-runner.sh")
    renv["COV_PROFILE_DIR"] = str(PROFILE_DIR)
    renv["COV_LLVM_PROFDATA"] = str(llvm / "llvm-profdata")
    renv["COV_DYLD_FALLBACK"] = dyld_fallback()
    t0 = time.time()
    nextest(args.crate, args.pattern, renv)
    timings.append(("nextest run (incl. per-test merges)", f"{time.time() - t0:.1f} s"))

    sweep_profraw(llvm / "llvm-profdata")
    profiles = sorted(PROFILE_DIR.glob("*.profdata"))
    stray = list(PROFILE_DIR.glob("*.profraw"))
    if not profiles:
        raise SystemExit("calibration produced no profiles")
    sizes = [p.stat().st_size for p in profiles]
    timings.append(("per-test profiles", f"{len(profiles)} (stray .profraw: {len(stray)})"))
    timings.append(
        (
            "per-test sparse profdata size",
            f"mean {sum(sizes) / len(sizes) / 1024:.1f} KiB, max {max(sizes) / 1024:.1f} KiB",
        )
    )

    sample = profiles[: min(100, len(profiles))]
    t0 = time.time()
    per_test_fns = [read_profile_functions(str(llvm / "llvm-profdata"), str(p)) for p in sample]
    per_show = (time.time() - t0) / len(sample)
    timings.append(("llvm-profdata show (per profile)", f"{per_show * 1000:.1f} ms"))

    t0 = time.time()
    aggregate(llvm / "llvm-profdata")
    timings.append(
        (
            "aggregate profdata merge",
            f"{time.time() - t0:.1f} s -> {AGGREGATE.stat().st_size / 1024:.0f} KiB",
        )
    )

    t0 = time.time()
    export(llvm / "llvm-cov", executables)
    timings.append(
        (
            "llvm-cov export (one aggregate call)",
            f"{time.time() - t0:.1f} s -> {EXPORT_JSON.stat().st_size / 1e6:.1f} MB",
        )
    )

    export_data = load_export()
    export_names = {
        fn["name"] for entry in export_data["data"] for fn in entry.get("functions", [])
    }
    prof_names: set[str] = set()
    for d in per_test_fns:
        prof_names |= set(d)
    hits = len(prof_names & export_names)
    hit_rate = 100.0 * hits / max(1, len(prof_names))

    zero_fns = sum(
        1
        for entry in export_data["data"]
        for fn in entry.get("functions", [])
        if fn.get("count", 0) == 0
    )

    print("\n== calibration ==")
    width = max(len(k) for k, _ in timings)
    for k, v in timings:
        print(f"  {k.ljust(width)}  {v}")
    print()
    print(f"  join hit-rate (profile fn -> export fn): {hits}/{len(prof_names)} = {hit_rate:.2f}%")
    print(f"  export functions with count == 0:        {zero_fns}/{len(export_names)}")

    print("\n== dead-code visibility (baseline anchors, coverage-summary.md 2026-07-22) ==")
    files = {
        f["filename"]: f["summary"] for entry in export_data["data"] for f in entry.get("files", [])
    }
    any_anchor = False
    for suffix, expected in DEAD_CODE_ANCHORS:
        match = [p for p in files if p.endswith(suffix)]
        if not match:
            print(f"  {suffix}: not in this export (crate not built in this calibration)")
            continue
        any_anchor = True
        s = files[match[0]]["lines"]
        print(f"  {suffix}: {s['percent']:.1f}% {s['covered']}/{s['count']}  (baseline {expected})")
    if not any_anchor:
        print(
            "  NOTE: run `just coverage-calibrate frogdb-server` to check these anchors.\n"
            "  If they are absent from a server export, add `-C link-dead-code` to RUSTFLAGS\n"
            "  in build_env() — otherwise never-instantiated functions are silently hidden."
        )

    n_tests = 7258
    print("\n== extrapolation to the full suite ==")
    print(
        f"  {n_tests} tests x {per_show * 1000:.1f} ms profdata show = {n_tests * per_show / 60:.1f} min"
    )
    print(
        f"  {n_tests} x {sum(sizes) / len(sizes) / 1024:.1f} KiB profiles     = {n_tests * (sum(sizes) / len(sizes)) / 1e9:.2f} GB"
    )
    print("  (llvm-cov export runs exactly once regardless of test count)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="build, pre-validate, run nextest, aggregate")
    r.add_argument("--crate", default=None)
    r.add_argument("--pattern", default=None)
    r.add_argument("--keep-profiles", action="store_true", help="append to existing profiles")
    r.set_defaults(fn=cmd_run)

    c = sub.add_parser("calibrate", help="measure the pipeline on one crate (Phase 0 gate)")
    c.add_argument("crate")
    c.add_argument("--pattern", default=None)
    c.set_defaults(fn=cmd_calibrate)

    p = sub.add_parser("report", help="parse profiles + export, emit markdown/HTML/JSON")
    p.add_argument("--md", default=None)
    p.add_argument("--html", default=None)
    p.add_argument("--json", default=None)
    p.add_argument("--top", type=int, default=DEFAULT_TOP, help="rows per ranked list in markdown")
    p.add_argument("--source-files", type=int, default=60, help="files to embed source for in HTML")
    p.add_argument(
        "--max-html-rows", type=int, default=8000, help="max function rows in the HTML table"
    )
    p.add_argument("--jobs", type=int, default=min(16, (os.cpu_count() or 4)))
    p.add_argument("--well-covered-tests", type=int, default=DEFAULT_WELL_COVERED_TESTS)
    p.add_argument("--hot-tests", type=int, default=DEFAULT_HOT_TESTS)
    p.add_argument("--hot-exec-floor", type=int, default=DEFAULT_HOT_EXEC_FLOOR)
    p.add_argument("--date", default=str(datetime.date.today()))
    p.set_defaults(fn=cmd_report)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
