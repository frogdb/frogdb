#!/usr/bin/env python3
"""Restore tracked files' mtimes to their last-touching commit's timestamp.

Replacement for the git-restore-mtime package (2022.12), which parses
`git whatchanged` — a command git 2.51 turned into a hard failure, so the
packaged tool silently updates nothing. Walks `git log` newest-first with
merge diffs included and stamps each tracked file the first time it appears.

Requires full history: run `git fetch --unshallow` first on shallow clones.
Files never matched (e.g. quoted non-ASCII paths) keep their checkout mtime,
which only costs a rebuild of whatever depends on them.
"""

import os
import subprocess

out = subprocess.run(["git", "ls-files", "-z"], capture_output=True, check=True).stdout
pending = {p for p in out.decode("utf-8", "surrogateescape").split("\0") if p}
total = len(pending)

proc = subprocess.Popen(
    ["git", "log", "-m", "--no-renames", "--format=\x01%ct", "--name-only"],
    stdout=subprocess.PIPE,
)
assert proc.stdout is not None
mtime = 0
updated = 0
for raw in proc.stdout:
    line = raw.decode("utf-8", "surrogateescape").rstrip("\n")
    if not line:
        continue
    if line.startswith("\x01"):
        mtime = int(line[1:])
    elif line in pending:
        pending.discard(line)
        try:
            os.utime(line, (mtime, mtime))
            updated += 1
        except OSError:
            pass
        if not pending:
            proc.stdout.close()
            break
proc.wait()
print(f"restored mtimes: {updated}/{total} updated, {len(pending)} unmatched")
