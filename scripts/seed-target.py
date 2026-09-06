#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Seed a fresh worktree's `target/` from a clean-main snapshot.

Background: `.scratch/build-cache/README.md`. Every parallel worktree's first
build recompiles every workspace crate. sccache cannot help (it hashes the
compile cwd and refuses incremental units), but a cargo `target/` is
relocatable: fingerprint hashes are path-independent and dep-info paths are
stored relative to the package/target roots. The only thing that stops cargo
from reusing a relocated `target/` is the mtime freshness check for path
crates — `git worktree add` stamps every file "now", newer than every
dep-info file, so every unit looks dirty.

    refresh   in the main checkout (clean tree): build through `just` so the
              env/flags match agent builds, then clone `target/` (minus
              incremental/ and everything not `debug/`) into
              $FROGDB_SEED_DIR (default ~/.cache/frogdb/seed) and record
              {sha, stamp} in seed.json.
    apply     in a worktree with no `target/debug`: clone the seed in, then set
              every tracked file that is unchanged since the seed sha to
              `stamp` (older than every fingerprint file in the seed, so cargo
              sees it as fresh). Changed, untracked, and
              `env!("CARGO_MANIFEST_DIR")`-bearing files keep their checkout
              mtime and rebuild.
    status    one line for the SessionStart hook: seed sha, age, commits
              behind the local `main`.
    maybe-refresh
              lefthook post-merge/post-checkout entry point: in the main
              checkout only, spawn a detached `refresh` when the seed is stale
              and no rustc/cargo is running; otherwise say why not.

Invariant the whole scheme rests on: `stamp` < mtime of every file under the
seed's `debug/.fingerprint` (computed as min-1 at refresh time). Cargo marks a
unit dirty when any source it lists is newer than the unit's dep-info file;
stamping unchanged sources below every dep-info mtime makes every seeded unit
fresh, and anything that really changed keeps a "now" mtime and rebuilds.
"""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import fcntl
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

MANIFEST_DIR_MACRO = 'env!("CARGO_MANIFEST_DIR")'
# What a seed carries. Top level: only the host debug profile (agents build
# debug) and cxxbridge (usearch's generated C++, path-independent). Foreign
# triples, release/, mutants/, tmp/, coverage/, probe/ are dropped.
TOP_LEVEL_KEEP = ("debug", "cxxbridge", ".rustc_info.json", "CACHEDIR.TAG")
# Under debug/: everything but incremental/ (57% of a worktree's target, and
# sccache-style unshareable: rustc keys it on absolute paths).
DEBUG_SKIP = ("incremental",)
STALE_AFTER_S = 30 * 60
BUILD_PROCS = {"rustc", "cargo", "cargo-nextest", "clippy-driver", "rustdoc", "cargo-clippy"}
# The three builds an agent's first commands need. Through `just` so
# rocksdb-env / sccache / rustflags match what a worktree build will hash.
REFRESH_BUILDS = (
    ["just", "check"],
    ["just", "build"],
    ["just", "_cargo", "nextest", "run", "--all", "--no-run"],
)


def seed_root() -> Path:
    return Path(os.environ.get("FROGDB_SEED_DIR") or Path.home() / ".cache" / "frogdb" / "seed")


def git(*args: str, cwd: Path | None = None) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def read_seed(root: Path) -> dict | None:
    meta = root / "seed.json"
    if not meta.is_file():
        return None
    info = json.loads(meta.read_text())
    if not (root / info["dir"]).is_dir():
        return None
    return info


def clone_tree(src: Path, dst: Path) -> None:
    """Copy-on-write clone of a directory tree (one clonefile(2) on APFS)."""
    if platform.system() == "Darwin":
        libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
        libc.clonefile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint32]
        if libc.clonefile(bytes(src), bytes(dst), 0) == 0:
            return
        err = ctypes.get_errno()
        print(
            f"clonefile({src}) failed: {os.strerror(err)}; falling back to cp -Rc", file=sys.stderr
        )
        subprocess.run(["cp", "-R", "-c", str(src), str(dst)], check=True)
    else:
        subprocess.run(["cp", "-a", "--reflink=auto", str(src), str(dst)], check=True)


def cargo_lock_held(target: Path) -> bool:
    lock = target / "debug" / ".cargo-lock"
    if not lock.exists():
        return False
    with open(lock) as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        fcntl.flock(fh, fcntl.LOCK_UN)
    return False


def min_fingerprint_mtime(debug: Path) -> float:
    fp = debug / ".fingerprint"
    lo = None
    for dirpath, _dirs, files in os.walk(fp):
        for f in files:
            m = os.stat(os.path.join(dirpath, f)).st_mtime
            lo = m if lo is None or m < lo else lo
    if lo is None:
        sys.exit(f"no fingerprints under {fp}: nothing to seed")
    return lo


def build_running() -> bool:
    out = subprocess.run(["ps", "-Ao", "comm="], capture_output=True, text=True).stdout
    return any(os.path.basename(line.strip()) in BUILD_PROCS for line in out.splitlines())


def is_main_checkout(cwd: Path) -> bool:
    return git("rev-parse", "--git-dir", cwd=cwd) == git("rev-parse", "--git-common-dir", cwd=cwd)


# --------------------------------------------------------------------------
# refresh
# --------------------------------------------------------------------------


def cmd_refresh(args: argparse.Namespace) -> int:
    repo = Path(git("rev-parse", "--show-toplevel"))
    os.chdir(repo)
    if git("status", "--porcelain"):
        sys.exit("refresh: working tree is not clean; the seed must match a commit exactly")
    sha = git("rev-parse", "HEAD")
    if not is_main_checkout(repo):
        print(f"refresh: note: running from a linked worktree ({repo}), seed sha {sha[:8]}")
    target = repo / "target"
    if cargo_lock_held(target):
        sys.exit("refresh: target/debug/.cargo-lock is held — a build is running; retry later")

    t0 = time.time()
    if not args.no_build:
        for cmd in REFRESH_BUILDS:
            print("refresh:", " ".join(cmd), flush=True)
            subprocess.run(cmd, check=True)
    if cargo_lock_held(target):
        sys.exit("refresh: a build started during the refresh; not snapshotting")
    debug = target / "debug"
    if not debug.is_dir():
        sys.exit(f"refresh: {debug} missing")
    stamp = min_fingerprint_mtime(debug) - 1.0

    root = seed_root()
    root.mkdir(parents=True, exist_ok=True)
    building = root / "target.building"
    if building.exists():
        shutil.rmtree(building)
    building.mkdir()
    t1 = time.time()
    for name in TOP_LEVEL_KEEP:
        src = target / name
        if not src.exists():
            continue
        if name == "debug":
            (building / "debug").mkdir()
            for entry in sorted(os.listdir(src)):
                if entry in DEBUG_SKIP:
                    continue
                clone_tree(src / entry, building / "debug" / entry)
        else:
            clone_tree(src, building / name)
    final_name = f"target.{sha[:12]}"
    final = root / final_name
    if final.exists():
        shutil.rmtree(final)
    building.rename(final)
    meta = {
        "sha": sha,
        "dir": final_name,
        "stamp": stamp,
        "created": time.time(),
        "build_seconds": round(t1 - t0, 1),
        "clone_seconds": round(time.time() - t1, 1),
    }
    tmp = root / "seed.json.tmp"
    tmp.write_text(json.dumps(meta, indent=2) + "\n")
    tmp.rename(root / "seed.json")
    # Prune every other generation; the seed is a cache, not history.
    for entry in root.iterdir():
        if entry.is_dir() and entry.name.startswith("target.") and entry.name != final_name:
            shutil.rmtree(entry)
    size = subprocess.run(["du", "-sh", str(final)], capture_output=True, text=True).stdout.split()[
        0
    ]
    print(
        f"refresh: seed {sha[:12]} at {final} ({size}; build {meta['build_seconds']}s, "
        f"clone {meta['clone_seconds']}s, stamp {time.strftime('%F %T', time.localtime(stamp))})"
    )
    return 0


# --------------------------------------------------------------------------
# apply
# --------------------------------------------------------------------------


def changed_since(sha: str, cwd: Path) -> set[str]:
    """Tracked paths that may differ from the seed's sources.

    diff seed..HEAD (committed changes), status (uncommitted + untracked), and
    every file that bakes CARGO_MANIFEST_DIR in at compile time: a seeded
    binary built in another checkout would carry that checkout's path, so
    those units must rebuild here regardless of content.
    """
    changed: set[str] = set()
    changed.update(git("diff", "--no-renames", "--name-only", sha, "HEAD", cwd=cwd).splitlines())
    for line in git("status", "--porcelain", "--untracked-files=all", cwd=cwd).splitlines():
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        changed.add(path.strip('"'))
    macro_hits = subprocess.run(
        ["git", "grep", "-l", "--fixed-strings", MANIFEST_DIR_MACRO, "--", "*.rs"],
        cwd=cwd,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    changed.update(macro_hits)
    return changed


def stamp_tracked(stamp: float, skip: set[str], cwd: Path) -> tuple[int, int]:
    out = subprocess.run(["git", "ls-files", "-z"], cwd=cwd, check=True, capture_output=True).stdout
    stamped = kept = 0
    for raw in out.split(b"\0"):
        if not raw:
            continue
        rel = raw.decode("utf-8", "surrogateescape")
        if rel in skip:
            kept += 1
            continue
        try:
            os.utime(cwd / rel, (stamp, stamp))
            stamped += 1
        except OSError:
            kept += 1
    return stamped, kept


def cmd_apply(args: argparse.Namespace) -> int:
    repo = Path(git("rev-parse", "--show-toplevel"))
    target = repo / "target"
    if (target / "debug").exists():
        if not args.quiet:
            print("seed-target: target/debug exists; nothing to do")
        return 0
    info = read_seed(seed_root())
    if info is None:
        if not args.quiet:
            print("seed-target: no seed (run `just seed-refresh` in the main checkout); cold build")
        return 0
    sha = info["sha"]
    if subprocess.run(
        ["git", "cat-file", "-e", f"{sha}^{{commit}}"], cwd=repo, capture_output=True
    ).returncode:
        print(f"seed-target: seed commit {sha[:12]} is not in this repository; cold build")
        return 0

    t0 = time.time()
    seed = seed_root() / info["dir"]
    target.mkdir(exist_ok=True)
    for entry in sorted(os.listdir(seed)):
        dst = target / entry
        if dst.exists():
            continue
        tmp = target / f".{entry}.seed-tmp"
        if tmp.exists():
            shutil.rmtree(tmp) if tmp.is_dir() else tmp.unlink()
        clone_tree(seed / entry, tmp)
        tmp.rename(dst)
    t1 = time.time()
    changed = changed_since(sha, repo)
    stamped, kept = stamp_tracked(info["stamp"], changed, repo)
    behind = git("rev-list", "--count", f"{sha}..HEAD", cwd=repo)
    print(
        f"seed-target: seeded target/ from {sha[:12]} (HEAD is {behind} commit(s) ahead): "
        f"{stamped} files marked fresh, {kept} changed/untracked/manifest-dir files left to rebuild; "
        f"clone {t1 - t0:.1f}s, stamp {time.time() - t1:.1f}s"
    )
    return 0


# --------------------------------------------------------------------------
# status / maybe-refresh
# --------------------------------------------------------------------------


def cmd_status(_args: argparse.Namespace) -> int:
    info = read_seed(seed_root())
    if info is None:
        print("Build seed: none (`just seed-refresh` in the main checkout builds one)")
        return 0
    age_h = (time.time() - info["created"]) / 3600
    try:
        behind = git("rev-list", "--count", f"{info['sha']}..main")
    except subprocess.CalledProcessError:
        behind = "?"
    print(
        f"Build seed: {info['sha'][:12]}, {age_h:.1f}h old, main is {behind} commit(s) ahead of it"
    )
    return 0


def cmd_maybe_refresh(_args: argparse.Namespace) -> int:
    repo = Path(git("rev-parse", "--show-toplevel"))
    if not is_main_checkout(repo):
        return 0
    if git("rev-parse", "--abbrev-ref", "HEAD") != "main":
        return 0
    if git("status", "--porcelain"):
        print("seed: not refreshing (working tree dirty)")
        return 0
    head = git("rev-parse", "HEAD")
    info = read_seed(seed_root())
    if info and info["sha"] == head:
        return 0
    if info and time.time() - info["created"] < STALE_AFTER_S:
        print(
            f"seed: not refreshing (seed is {(time.time() - info['created']) / 60:.0f} min old; run `just seed-refresh` to force)"
        )
        return 0
    if build_running():
        print(
            "seed: not refreshing (a rustc/cargo build is running; run `just seed-refresh` later)"
        )
        return 0
    root = seed_root()
    root.mkdir(parents=True, exist_ok=True)
    log = open(root / "refresh.log", "ab")
    subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "refresh"],
        cwd=repo,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    print(f"seed: refreshing in the background from {head[:12]} (log: {root / 'refresh.log'})")
    return 0


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("refresh")
    r.add_argument(
        "--no-build",
        action="store_true",
        help="snapshot the current target/ without building first",
    )
    r.set_defaults(fn=cmd_refresh)
    a = sub.add_parser("apply")
    a.add_argument("--quiet", action="store_true", help="say nothing when there is nothing to do")
    a.set_defaults(fn=cmd_apply)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    sub.add_parser("maybe-refresh").set_defaults(fn=cmd_maybe_refresh)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
