"""
Task 6 helper - automated generation sweep.
===========================================

After a file is reprocessed, elements of that file that belonged to the PREVIOUS
version still sit in Neo4j (they were never re-emitted, so MERGE never touched
them). They are stale/orphaned.

Every element - node AND relationship - carries the file's sha256 as
`file_hash`, which acts as a generation marker. This script:

  1. computes the CURRENT hash of the given file,
  2. deletes stale CPG_EDGE relationships of that file first (a stale edge
     between two surviving nodes would outlive a node-only sweep),
  3. then deletes stale CpgNode rows (DETACH DELETE catches their edges too),
  4. reports stale/duplicate counts for both nodes and edges as evidence.

Usage:
    python sweep.py --repo ./optimum --rel-path optimum/version.py \
        --uri bolt://localhost:7687 --user neo4j --password password

Requires:  pip install neo4j
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

COUNT_NODES = "MATCH (n:CpgNode {file_id:$fid}) RETURN count(n) AS c"
COUNT_EDGES = ("MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = $fid "
               "RETURN count(r) AS c")
STALE_NODES = ("MATCH (n:CpgNode {file_id:$fid}) WHERE n.file_hash <> $h "
               "RETURN count(n) AS c")
STALE_EDGES = ("MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = $fid "
               "AND r.file_hash <> $h RETURN count(r) AS c")
SWEEP_EDGES = ("MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = $fid "
               "AND r.file_hash <> $h DELETE r")
SWEEP_NODES = ("MATCH (n:CpgNode {file_id:$fid}) WHERE n.file_hash <> $h "
               "DETACH DELETE n")
DUPE_NODES = ("MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c WHERE c > 1 "
              "RETURN count(id) AS c")
DUPE_EDGES = ("MATCH ()-[r:CPG_EDGE]->() WITH r.id AS id, count(*) AS c "
              "WHERE c > 1 RETURN count(id) AS c")


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
    ap.add_argument("--json-out", default=None,
                    help="also write the result as JSON to this path")
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
        def one(q, **kw):
            return s.run(q, **kw).single()["c"]

        nodes = one(COUNT_NODES, fid=fid)
        edges = one(COUNT_EDGES, fid=fid)
        stale_n = one(STALE_NODES, fid=fid, h=current_hash)
        stale_e = one(STALE_EDGES, fid=fid, h=current_hash)
        print(f"\nnodes for this file : {nodes}   (stale old-gen: {stale_n})")
        print(f"edges for this file : {edges}   (stale old-gen: {stale_e})")

        if args.dry_run:
            print("\n[dry-run] nothing deleted")
        else:
            # relationships first: a stale edge between two surviving nodes
            # is untouched by a node-only sweep
            s.run(SWEEP_EDGES, fid=fid, h=current_hash)
            s.run(SWEEP_NODES, fid=fid, h=current_hash)
            after_n = one(COUNT_NODES, fid=fid)
            after_e = one(COUNT_EDGES, fid=fid)
            print(f"after sweep         : {after_n} nodes (deleted {nodes - after_n}), "
                  f"{after_e} edges (deleted {edges - after_e})")
            nodes, edges = after_n, after_e
            stale_n = one(STALE_NODES, fid=fid, h=current_hash)
            stale_e = one(STALE_EDGES, fid=fid, h=current_hash)

        dupe_n = one(DUPE_NODES)
        dupe_e = one(DUPE_EDGES)
        ok = dupe_n == 0 and dupe_e == 0 and stale_n == 0 and stale_e == 0
        print(f"\nstale nodes / edges (this file) : {stale_n} / {stale_e}")
        print(f"duplicate node ids in DB        : {dupe_n}")
        print(f"duplicate edge ids in DB        : {dupe_e}")
        print(f"RESULT: {'PASS (idempotent)' if ok else 'FAIL'}")

        if args.json_out:
            payload = {
                "rel_path": args.rel_path, "file_id": fid,
                "file_hash": current_hash, "nodes": nodes, "edges": edges,
                "stale_nodes": stale_n, "stale_edges": stale_e,
                "duplicate_nodes": dupe_n, "duplicate_edges": dupe_e,
                "dry_run": args.dry_run,
            }
            Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
            Path(args.json_out).write_text(json.dumps(payload, indent=2))
            print(f"json      : {args.json_out}")
    driver.close()


if __name__ == "__main__":
    main()
