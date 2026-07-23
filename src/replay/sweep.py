"""
Task 6 helper - automated generation sweep.
===========================================

After a file is reprocessed, elements of that file that belonged to the PREVIOUS
version still sit in Neo4j (they were never re-emitted, so MERGE never touched
them). They are stale/orphaned.

Every element carries the file's sha256 as `file_hash`, which acts as a
generation marker. This script:

  1. computes the CURRENT hash of the given file,
  2. deletes CpgNode rows for that file whose file_hash differs,
  3. reports before/after counts so the notebook can show real evidence.

Usage:
    python sweep.py --repo ./optimum --rel-path optimum/version.py \
        --uri bolt://localhost:7687 --user neo4j --password password

Requires:  pip install neo4j
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

COUNT_ALL = "MATCH (n:CpgNode {file_id:$fid}) RETURN count(n) AS c"
COUNT_STALE = ("MATCH (n:CpgNode {file_id:$fid}) WHERE n.file_hash <> $h "
               "RETURN count(n) AS c")
SWEEP = ("MATCH (n:CpgNode {file_id:$fid}) WHERE n.file_hash <> $h "
         "DETACH DELETE n")
DUPES = ("MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c WHERE c > 1 "
         "RETURN count(id) AS c")


def file_id_for(rel_path: str) -> str:
    """Must match src/parser/ids.py exactly."""
    return hashlib.sha1(rel_path.encode("utf-8")).hexdigest()[:16]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--rel-path", required=True)
    ap.add_argument("--uri", default="bolt://localhost:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", default="password")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be deleted, delete nothing")
    args = ap.parse_args()

    from neo4j import GraphDatabase  # imported lazily

    src = (Path(args.repo) / args.rel_path).read_bytes()
    current_hash = hashlib.sha256(src).hexdigest()
    fid = file_id_for(args.rel_path)

    print(f"file      : {args.rel_path}")
    print(f"file_id   : {fid}")
    print(f"file_hash : {current_hash[:16]}... (current generation)")

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    with driver.session() as s:
        total = s.run(COUNT_ALL, fid=fid).single()["c"]
        stale = s.run(COUNT_STALE, fid=fid, h=current_hash).single()["c"]
        print(f"\nnodes for this file : {total}")
        print(f"stale (old gen)     : {stale}")

        if args.dry_run:
            print("\n[dry-run] nothing deleted")
        else:
            s.run(SWEEP, fid=fid, h=current_hash)
            after = s.run(COUNT_ALL, fid=fid).single()["c"]
            print(f"after sweep         : {after}  (deleted {total - after})")

        dupes = s.run(DUPES).single()["c"]
        print(f"\nduplicate node ids in DB : {dupes}  "
              f"{'PASS (idempotent)' if dupes == 0 else 'FAIL'}")
    driver.close()


if __name__ == "__main__":
    main()
