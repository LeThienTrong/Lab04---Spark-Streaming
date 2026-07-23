#!/usr/bin/env python3
"""
Task 6 helper - capture a verifiable state snapshot for the replay target.
==========================================================================

Queries Neo4j (bolt) and MongoDB (docker exec mongosh) for everything the
replay verification needs, and writes it as JSON so the before/after states
are durable evidence rather than scrollback:

  file_id, file_hash (from disk), per-file node/edge counts, DB totals,
  duplicate node/edge id counts, stale node/edge counts, and the MongoDB
  document (_id, file_hash, count) for the file.

Usage:
    python scripts/replay_evidence.py --repo ./optimum \
        --rel-path optimum/version.py --out reports/evidence/task6_before.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

Q = {
    "nodes_file": "MATCH (n:CpgNode {file_id:$fid}) RETURN count(n) AS c",
    "edges_file": ("MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = $fid "
                   "RETURN count(r) AS c"),
    "nodes_total": "MATCH (n:CpgNode) RETURN count(n) AS c",
    "edges_total": "MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS c",
    "duplicate_nodes": ("MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c "
                        "WHERE c > 1 RETURN count(id) AS c"),
    "duplicate_edges": ("MATCH ()-[r:CPG_EDGE]->() WITH r.id AS id, "
                        "count(*) AS c WHERE c > 1 RETURN count(id) AS c"),
    "stale_nodes": ("MATCH (n:CpgNode {file_id:$fid}) "
                    "WHERE n.file_hash <> $h RETURN count(n) AS c"),
    "stale_edges": ("MATCH ()-[r:CPG_EDGE]->() WHERE r.file_id = $fid "
                    "AND r.file_hash <> $h RETURN count(r) AS c"),
}


def file_id_for(rel_path: str) -> str:
    """Must match src/parser/ids.py exactly."""
    return hashlib.sha1(rel_path.encode("utf-8")).hexdigest()[:16]


def mongo_eval(expr: str) -> str:
    out = subprocess.run(
        ["docker", "exec", "mongodb", "mongosh", "--quiet", "cpg",
         "--eval", expr],
        capture_output=True, text=True, check=True)
    return out.stdout.strip()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="./optimum")
    ap.add_argument("--rel-path", default="optimum/version.py")
    ap.add_argument("--uri", default="bolt://localhost:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", default="password")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    from neo4j import GraphDatabase  # imported lazily

    src = (Path(args.repo) / args.rel_path).read_bytes()
    current_hash = hashlib.sha256(src).hexdigest()
    fid = file_id_for(args.rel_path)

    snap: dict = {"rel_path": args.rel_path, "file_id": fid,
                  "file_hash_on_disk": current_hash}

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    with driver.session() as s:
        for key, q in Q.items():
            snap[key] = s.run(q, fid=fid, h=current_hash).single()["c"]
    driver.close()

    doc = mongo_eval(
        "JSON.stringify(db.file_metadata.findOne("
        f"{{rel_path:'{args.rel_path}'}},"
        "{rel_path:1,file_hash:1,num_ast_nodes:1}))")
    snap["mongo_document"] = json.loads(doc) if doc and doc != "null" else None
    snap["mongo_count_for_file"] = int(mongo_eval(
        f"db.file_metadata.countDocuments({{rel_path:'{args.rel_path}'}})"))
    snap["mongo_total_documents"] = int(mongo_eval(
        "db.file_metadata.countDocuments({})"))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(snap, indent=2))
    print(json.dumps(snap, indent=2))
    print(f"\nwritten -> {out}")


if __name__ == "__main__":
    main()
