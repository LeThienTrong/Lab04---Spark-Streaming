#!/usr/bin/env python3
"""
Task 6 helper - deterministic replay target management.
=======================================================

The replay demonstration edits exactly one file. For the demonstration to be
*deterministic* (re-runnable with identical before/after numbers), the file
must start from a known-clean baseline every time.

Default mode  : RESET - remove the lab edit (header comment + marker block)
                if present, restoring the original upstream content.
--add-marker  : ensure the lab edit IS present (header comment that shifts
                every line number, plus the marker function at EOF).

Both modes are idempotent: running them any number of times converges on the
same content, and the resulting sha256 is printed so the caller can record the
generation hash.

Usage:
    python scripts/reset_replay_target.py [--repo ./optimum]
        [--rel-path optimum/version.py] [--add-marker]
"""
from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path

HEADER = "# lab04 replay edit: shifts every line number below\n"
MARKER = (
    "\n\ndef _lab_replay_marker(value):\n"
    "    temporary_value = value + 1\n"
    "    return temporary_value\n"
)
# Matches any historical variant of the marker block (old x/y body included),
# with surrounding blank lines, so reset also cleans files edited by earlier
# versions of this lab.
MARKER_RE = re.compile(r"\n*def _lab_replay_marker\([^)]*\):\n(?:[ \t]+.*\n?)*")


def clean(text: str) -> str:
    if text.startswith(HEADER):
        text = text[len(HEADER):]
    text = MARKER_RE.sub("", text)
    return text.rstrip("\n") + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="./optimum")
    ap.add_argument("--rel-path", default="optimum/version.py")
    ap.add_argument("--add-marker", action="store_true",
                    help="ensure the lab edit is present instead of removing it")
    args = ap.parse_args()

    path = Path(args.repo) / args.rel_path
    original = path.read_text(encoding="utf-8")
    base = clean(original)

    if args.add_marker:
        new = HEADER + base.rstrip("\n") + "\n" + MARKER
        state = "marker present (added)" if new != original else "marker present (already)"
    else:
        new = base
        state = "baseline (cleaned)" if new != original else "baseline (already clean)"

    if new != original:
        path.write_text(new, encoding="utf-8")

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    print(f"file      : {args.rel_path}")
    print(f"state     : {state}")
    print(f"file_hash : {digest}")


if __name__ == "__main__":
    main()
