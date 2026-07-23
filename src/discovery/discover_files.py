"""
Task 1 - Repository Cloning & File Discovery
============================================

Enumerates every `.py` source file inside a cloned repository, optionally
excluding test / setup / auto-generated files (recommended by the lab), and
writes a manifest that later stages of the pipeline consume.

The manifest is deterministic (sorted by path) so that a replay produces the
same ordering and the same file identities every run.

Usage:
    python discover_files.py --repo ./optimum --out ./reports/file_manifest.json
    python discover_files.py --repo ./optimum --limit 15   # demo subset
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
from collections import Counter
from pathlib import Path

# Patterns we treat as non-source for CPG purposes. Excluding these is
# "optional but recommended" per the lab statement.
EXCLUDE_DIR_PARTS = {"tests", "test", ".git", "__pycache__", "build", "dist"}
EXCLUDE_FILE_GLOBS = [
    "test_*.py",
    "*_test.py",
    "conftest.py",
    "setup.py",
    "_version.py",
    "*_pb2.py",      # generated protobuf
    "*_pb2_grpc.py",
]


def _is_excluded(path: Path, repo_root: Path) -> bool:
    rel_parts = path.relative_to(repo_root).parts
    if any(part in EXCLUDE_DIR_PARTS for part in rel_parts[:-1]):
        return True
    name = path.name
    return any(fnmatch.fnmatch(name, g) for g in EXCLUDE_FILE_GLOBS)


def _content_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def discover(repo: str, apply_exclusions: bool = True, limit: int | None = None) -> dict:
    repo_root = Path(repo).resolve()
    if not repo_root.is_dir():
        raise SystemExit(f"Repo path not found: {repo_root}")

    all_py = sorted(repo_root.rglob("*.py"))
    included, excluded = [], []
    for p in all_py:
        (excluded if apply_exclusions and _is_excluded(p, repo_root) else included).append(p)

    if limit is not None:
        included = included[:limit]

    files = []
    dir_counter: Counter[str] = Counter()
    for p in included:
        rel = str(p.relative_to(repo_root))
        top = rel.split(os.sep)[0]
        dir_counter[top] += 1
        files.append({
            "file_id": hashlib.sha1(rel.encode()).hexdigest()[:16],  # stable per path
            "rel_path": rel,
            "size_bytes": p.stat().st_size,
            "content_hash": _content_hash(p),  # drives incremental change detection
        })

    return {
        "repo_root": str(repo_root),
        "total_py_found": len(all_py),
        "excluded_count": len(excluded),
        "included_count": len(files),
        "by_top_level_dir": dict(dir_counter.most_common()),
        "files": files,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--out", default="reports/file_manifest.json")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-exclude", action="store_true")
    args = ap.parse_args()

    manifest = discover(args.repo, apply_exclusions=not args.no_exclude, limit=args.limit)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(manifest, indent=2))

    print(f"Repository        : {manifest['repo_root']}")
    print(f"Total .py found   : {manifest['total_py_found']}")
    print(f"Excluded          : {manifest['excluded_count']}")
    print(f"Included (source) : {manifest['included_count']}")
    print("By top-level dir  :")
    for d, n in manifest["by_top_level_dir"].items():
        print(f"    {d:<20} {n}")
    print(f"Manifest written  -> {args.out}")


if __name__ == "__main__":
    main()
