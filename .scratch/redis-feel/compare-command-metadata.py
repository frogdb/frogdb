#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Compare COMMAND INFO / COMMAND DOCS between FrogDB and a reference Redis.

Usage: compare-command-metadata.py <frogdb-port> <redis-port>

Speaks RESP2 over a raw socket so it has no client-library dependency and sees
the exact wire shape both servers emit. Reports per-command diff classes rather
than a raw dump so deliberate divergences can be recognised and grouped.
"""

from __future__ import annotations

import socket
import sys
from collections import defaultdict


class Resp:
    def __init__(self, host: str, port: int) -> None:
        self.sock = socket.create_connection((host, port), timeout=30)
        self.buf = b""

    def _line(self) -> bytes:
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("connection closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n: int) -> bytes:
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("connection closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2 :]
        return data

    def _read(self):
        line = self._line()
        tag, rest = line[:1], line[1:]
        if tag == b"+":
            return rest.decode()
        if tag == b"-":
            return Err(rest.decode())
        if tag == b":":
            return int(rest)
        if tag == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n).decode("utf-8", "replace")
        if tag == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise ValueError(f"unexpected RESP tag {line!r}")

    def cmd(self, *args: str):
        out = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = a.encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(out))
        return self._read()


class Err(str):
    pass


def pairs(flat) -> dict:
    """RESP2 map -> dict, preserving order."""
    return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}


# ---------------------------------------------------------------- INFO


def info_rows(conn: Resp) -> dict:
    rows = {}
    for row in conn.cmd("COMMAND"):
        if not row:
            continue
        rows[row[0].upper()] = row
    return rows


INFO_FIELDS = [
    "name",
    "arity",
    "flags",
    "first_key",
    "last_key",
    "key_step",
    "acl_categories",
    "tips",
    "key_specs",
    "subcommands",
]


def info_diff(name: str, ours: list, theirs: list) -> list[tuple[str, str]]:
    out = []
    for idx, field in enumerate(INFO_FIELDS):
        a = ours[idx] if idx < len(ours) else "<missing>"
        b = theirs[idx] if idx < len(theirs) else "<missing>"
        if field == "subcommands":
            # Compared separately: we deliberately never emit nested subcommands.
            continue
        if a != b:
            out.append((f"info.{field}", f"ours={a!r} theirs={b!r}"))
    return out


# ---------------------------------------------------------------- DOCS


def docs_rows(conn: Resp) -> dict:
    flat = conn.cmd("COMMAND", "DOCS")
    rows = {}
    for i in range(0, len(flat), 2):
        rows[flat[i].upper()] = pairs(flat[i + 1])
    return rows


# Fields FrogDB sources from its own CommandSpec, not from the vendored dump.
OURS_BY_DESIGN = {"summary", "since", "group", "complexity"}
# Fields FrogDB never emits by design.
NEVER_EMITTED = {"module", "subcommands"}


def docs_diff(ours: dict, theirs: dict) -> list[tuple[str, str]]:
    out = []
    keys = list(dict.fromkeys(list(ours) + list(theirs)))
    for k in keys:
        if k in NEVER_EMITTED:
            if k in ours:
                out.append((f"docs.{k}", "emitted but should not be"))
            continue
        a, b = ours.get(k, "<missing>"), theirs.get(k, "<missing>")
        if a == b:
            continue
        if k in OURS_BY_DESIGN:
            out.append((f"docs.{k}.prose", f"ours={a!r} theirs={b!r}"))
            continue
        if k == "arguments":
            sub = args_diff(a, b, "docs.arguments")
            out.extend(sub if sub else [(f"docs.{k}", "differs (unclassified)")])
            continue
        out.append((f"docs.{k}", f"ours={a!r} theirs={b!r}"))
    # field ordering
    shared = [k for k in ours if k in theirs and k not in NEVER_EMITTED]
    theirs_order = [k for k in theirs if k in ours and k not in NEVER_EMITTED]
    if shared != theirs_order:
        out.append(("docs.field_order", f"ours={shared} theirs={theirs_order}"))
    return out


def args_diff(a, b, path: str) -> list[tuple[str, str]]:
    if a == "<missing>" or b == "<missing>":
        return [(path, f"ours={'present' if a != '<missing>' else 'absent'} theirs={'present' if b != '<missing>' else 'absent'}")]
    if len(a) != len(b):
        return [(f"{path}.len", f"ours={len(a)} theirs={len(b)}")]
    out = []
    for i, (x, y) in enumerate(zip(a, b)):
        dx, dy = pairs(x), pairs(y)
        if list(dx) != list(dy):
            out.append((f"{path}[{i}].field_order", f"ours={list(dx)} theirs={list(dy)}"))
        for k in dict.fromkeys(list(dx) + list(dy)):
            va, vb = dx.get(k, "<missing>"), dy.get(k, "<missing>")
            if va == vb:
                continue
            if k == "arguments":
                out.extend(args_diff(va, vb, f"{path}[{i}].arguments"))
            else:
                out.append((f"{path}[{i}].{k}", f"ours={va!r} theirs={vb!r}"))
    return out


def main() -> int:
    ours_port, theirs_port = int(sys.argv[1]), int(sys.argv[2])
    ours_c = Resp("127.0.0.1", ours_port)
    theirs_c = Resp("127.0.0.1", theirs_port)

    ours_info, theirs_info = info_rows(ours_c), info_rows(theirs_c)
    ours_docs, theirs_docs = docs_rows(ours_c), docs_rows(theirs_c)

    shared = sorted(set(ours_info) & set(theirs_info))
    only_ours = sorted(set(ours_info) - set(theirs_info))
    only_theirs = sorted(set(theirs_info) - set(ours_info))

    print(f"commands: ours={len(ours_info)} theirs={len(theirs_info)} shared={len(shared)}")
    print(f"ours-only ({len(only_ours)}): {' '.join(only_ours)}")
    print(f"theirs-only ({len(only_theirs)}): {' '.join(only_theirs)}")
    print()

    classes: dict[str, list[str]] = defaultdict(list)
    detail: dict[str, list[tuple[str, str]]] = {}
    for name in shared:
        rows = info_diff(name, ours_info[name], theirs_info[name])
        if name in ours_docs and name in theirs_docs:
            rows += docs_diff(ours_docs[name], theirs_docs[name])
        elif name in theirs_docs:
            rows.append(("docs.missing", "no COMMAND DOCS entry from ours"))
        if rows:
            detail[name] = rows
            for cls, _ in rows:
                classes[cls].append(name)

    print(f"commands with diffs: {len(detail)}/{len(shared)}")
    print("\n=== diff classes ===")
    for cls, names in sorted(classes.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        print(f"{len(names):4d}  {cls}")
        print(f"        {' '.join(sorted(names))}")

    if "-v" in sys.argv:
        print("\n=== per-command detail ===")
        for name, rows in detail.items():
            print(f"\n{name}")
            for cls, msg in rows:
                print(f"  {cls}: {msg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
