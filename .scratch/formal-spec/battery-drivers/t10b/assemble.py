#!/usr/bin/env python3
"""Assemble the final battery report from the skeleton + section files + generated tables."""

import io
import os
import subprocess
import sys

SCRATCH = os.path.dirname(os.path.abspath(__file__))
REPORT = "/Users/nathan/workspace/frogdb/.scratch/formal-spec/2026-08-20-fullsync-battery.md"
SKEL = os.path.join(SCRATCH, "skeleton.md")


def run(script):
    r = subprocess.run(
        [sys.executable, os.path.join(SCRATCH, script)], capture_output=True, text=True
    )
    if r.returncode != 0:
        raise SystemExit(f"{script} failed:\n{r.stderr}")
    return r.stdout


def sect(name):
    return open(os.path.join(SCRATCH, name)).read().rstrip() + "\n"


table = run("gen_table.py")
# strip the trailing Totals/closed lines from the table output — they are prose material
lines = table.splitlines()
cut = next(i for i, ln in enumerate(lines) if ln.startswith("Totals:"))
table_md = "\n".join(lines[:cut]).rstrip() + "\n"
tail = "\n".join(lines[cut:])
open(os.path.join(SCRATCH, "table_tail.txt"), "w").write(tail)

cov = run("coverage.py")
cov_md = cov.split("Tests firing:")[0].rstrip() + "\n"

wit = open(os.path.join(SCRATCH, "witness_table.md")).read().rstrip() + "\n"

body = sect("skeleton.md")
body = body.replace("<!--TABLE-->", table_md)
analyses = (
    sect("section_analyses_static.md").replace("<!--WITNESS-TABLE-->", wit)
    + "\n"
    + sect("section_analyses_rows.md")
)
body = body.replace("<!--ANALYSES-->", analyses)
body = body.replace("<!--COVERAGE-SECTION-->", sect("section_coverage.md"))
body = body.replace("<!--COVERAGE-TABLES-->", cov_md)
body = body.replace("<!--CLOSURE-->", sect("section_closure.md"))
body = body.replace("<!--GATES-->", sect("section_gates.md"))
open(REPORT, "w").write(body)
print("wrote", REPORT, len(body.splitlines()), "lines")
print(tail)
