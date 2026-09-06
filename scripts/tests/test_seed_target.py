#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Tests for scripts/seed-target.py, the worktree target/ seeding.

Run: ./scripts/tests/test_seed_target.py   (or `just test-seed-target`)

Everything here runs against a scratch git repo and a scratch seed directory
(`FROGDB_SEED_DIR`), never the real ones. What is pinned is the mtime rule the
whole scheme rests on: after `apply`, every tracked file unchanged since the
seed commit carries the seed's `stamp`, and every file that may differ from
what the seed was built from — committed after the seed, modified, untracked,
or baking `env!("CARGO_MANIFEST_DIR")` outside the relocating helper — keeps
its checkout mtime so cargo rebuilds its unit.

No test framework: pure-stdlib assert script, nonzero on the first failure.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "seed-target.py"
HELPER = "frogdb-server/crates/types/src/manifest_dir.rs"
MACRO = 'env!("CARGO_MANIFEST_DIR")'


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def run(repo: Path, seed_dir: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ, FROGDB_SEED_DIR=str(seed_dir))
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], cwd=repo, env=env, capture_output=True, text=True
    )


def write(repo: Path, rel: str, text: str) -> Path:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def make_repo(root: Path) -> Path:
    repo = root / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    write(repo, "Cargo.toml", "[workspace]\n")
    write(repo, ".gitignore", "target\n")
    write(repo, "a/src/lib.rs", "pub fn a() {}\n")
    write(repo, "b/src/lib.rs", "pub fn b() {}\n")
    write(repo, "later/src/lib.rs", "pub fn later() {}\n")
    write(repo, "dirty/src/lib.rs", "pub fn dirty() {}\n")
    write(repo, "baked/src/lib.rs", f"const D: &str = {MACRO};\n")
    write(repo, HELPER, f"const D: &str = {MACRO};\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "seed commit")
    return repo


def make_seed(root: Path, sha: str, stamp: float) -> Path:
    seed_dir = root / "seed"
    tdir = seed_dir / f"target.{sha[:12]}"
    fp = tdir / "debug" / ".fingerprint" / "a-0123"
    fp.mkdir(parents=True)
    (fp / "dep-lib-a").write_text("")
    os.utime(fp / "dep-lib-a", (stamp + 1, stamp + 1))
    (tdir / "cxxbridge").mkdir()
    (tdir / "cxxbridge" / "x.h").write_text("")
    (seed_dir / "seed.json").write_text(
        json.dumps(
            {
                "sha": sha,
                "dir": tdir.name,
                "stamp": stamp,
                "created": time.time(),
                "build_seconds": 0,
                "clone_seconds": 0,
            }
        )
    )
    return seed_dir


def mtime(repo: Path, rel: str) -> float:
    return os.stat(repo / rel).st_mtime


def test_apply_without_seed_is_a_cold_build(root: Path) -> None:
    repo = make_repo(root)
    seed_dir = root / "seed"
    r = run(repo, seed_dir, "apply")
    assert r.returncode == 0, r.stderr
    assert "no seed" in r.stdout, r.stdout
    assert not (repo / "target" / "debug").exists()


def test_apply_stamps_only_what_the_seed_already_built(root: Path) -> None:
    repo = make_repo(root)
    sha = git(repo, "rev-parse", "HEAD")
    stamp = time.time() - 3600
    seed_dir = make_seed(root, sha, stamp)

    # A commit after the seed, a modified tracked file, and an untracked file.
    write(repo, "later/src/lib.rs", "pub fn later2() {}\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "after seed")
    write(repo, "dirty/src/lib.rs", "pub fn dirty2() {}\n")
    write(repo, "new/src/lib.rs", "pub fn new() {}\n")
    # Something that already sits in target/ must be left alone.
    (repo / "target" / "probe").mkdir(parents=True)

    before = {rel: mtime(repo, rel) for rel in ("later/src/lib.rs", "dirty/src/lib.rs")}
    r = run(repo, seed_dir, "apply")
    assert r.returncode == 0, r.stderr
    assert f"seeded target/ from {sha[:12]}" in r.stdout, r.stdout
    assert "HEAD is 1 commit(s) ahead" in r.stdout, r.stdout

    # Cloned in, per top-level entry, leaving what was there.
    assert (repo / "target" / "debug" / ".fingerprint" / "a-0123" / "dep-lib-a").is_file()
    assert (repo / "target" / "cxxbridge" / "x.h").is_file()
    assert (repo / "target" / "probe").is_dir()
    assert not list((repo / "target").glob(".*.seed-tmp"))

    # Unchanged since the seed sha: stamped, and older than the seed's dep-info.
    for rel in ("Cargo.toml", "a/src/lib.rs", "b/src/lib.rs", HELPER):
        assert abs(mtime(repo, rel) - stamp) < 0.01, (rel, mtime(repo, rel), stamp)
    assert stamp < os.stat(repo / "target/debug/.fingerprint/a-0123/dep-lib-a").st_mtime

    # Everything that may differ from the seed's sources keeps its checkout mtime.
    for rel in ("later/src/lib.rs", "dirty/src/lib.rs"):
        assert mtime(repo, rel) == before[rel], rel
    assert mtime(repo, "new/src/lib.rs") > stamp + 1
    assert mtime(repo, "baked/src/lib.rs") > stamp + 1, "macro-bearing file must rebuild"


def test_apply_is_a_no_op_once_target_debug_exists(root: Path) -> None:
    repo = make_repo(root)
    sha = git(repo, "rev-parse", "HEAD")
    seed_dir = make_seed(root, sha, time.time() - 3600)
    (repo / "target" / "debug").mkdir(parents=True)
    before = mtime(repo, "a/src/lib.rs")
    r = run(repo, seed_dir, "apply")
    assert r.returncode == 0, r.stderr
    assert "nothing to do" in r.stdout, r.stdout
    assert mtime(repo, "a/src/lib.rs") == before
    assert run(repo, seed_dir, "apply", "--quiet").stdout == ""


def test_apply_skips_a_seed_from_an_unknown_commit(root: Path) -> None:
    repo = make_repo(root)
    seed_dir = make_seed(root, "f" * 40, time.time() - 3600)
    before = mtime(repo, "a/src/lib.rs")
    r = run(repo, seed_dir, "apply")
    assert r.returncode == 0, r.stderr
    assert "not in this repository" in r.stdout, r.stdout
    assert not (repo / "target" / "debug").exists()
    assert mtime(repo, "a/src/lib.rs") == before


def test_status_reports_seed_and_distance(root: Path) -> None:
    repo = make_repo(root)
    assert "Build seed: none" in run(repo, root / "seed", "status").stdout
    sha = git(repo, "rev-parse", "HEAD")
    seed_dir = make_seed(root, sha, time.time() - 3600)
    write(repo, "a/src/lib.rs", "pub fn a2() {}\n")
    git(repo, "commit", "-qam", "one more")
    out = run(repo, seed_dir, "status").stdout
    assert f"Build seed: {sha[:12]}" in out, out
    assert "main is 1 commit(s) ahead" in out, out


def test_refresh_refuses_a_dirty_tree(root: Path) -> None:
    repo = make_repo(root)
    write(repo, "a/src/lib.rs", "pub fn a2() {}\n")
    r = run(repo, root / "seed", "refresh", "--no-build")
    assert r.returncode != 0
    assert "not clean" in r.stderr, r.stderr


def test_refresh_no_build_snapshots_debug_minus_incremental(root: Path) -> None:
    repo = make_repo(root)
    sha = git(repo, "rev-parse", "HEAD")
    fp = repo / "target" / "debug" / ".fingerprint" / "a-0123"
    fp.mkdir(parents=True)
    (fp / "dep-lib-a").write_text("")
    t = time.time() - 100
    os.utime(fp / "dep-lib-a", (t, t))
    (repo / "target" / "debug" / "incremental" / "a-xyz").mkdir(parents=True)
    (repo / "target" / "release").mkdir()
    (repo / "target" / "tmp").mkdir()
    seed_dir = root / "seed"
    r = run(repo, seed_dir, "refresh", "--no-build")
    assert r.returncode == 0, r.stderr
    info = json.loads((seed_dir / "seed.json").read_text())
    assert info["sha"] == sha
    assert abs(info["stamp"] - (t - 1)) < 0.01, (info["stamp"], t)
    tdir = seed_dir / info["dir"]
    assert (tdir / "debug" / ".fingerprint" / "a-0123" / "dep-lib-a").is_file()
    assert not (tdir / "debug" / "incremental").exists()
    assert not (tdir / "release").exists()
    assert not (tdir / "tmp").exists()
    # A second refresh at the same sha replaces the generation in place.
    r = run(repo, seed_dir, "refresh", "--no-build")
    assert r.returncode == 0, r.stderr
    assert [p.name for p in seed_dir.iterdir() if p.is_dir()] == [info["dir"]]


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        root = Path(tempfile.mkdtemp(prefix="seed-target-test-"))
        try:
            t(root)
        finally:
            shutil.rmtree(root, ignore_errors=True)
        print(f"ok  {t.__name__}")
    print(f"{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
