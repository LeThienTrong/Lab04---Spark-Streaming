"""
Parser Service (Task 2 + Task 3 producer side).
===============================================

Reads the file manifest from Task 1 and processes files ONE AT A TIME
(bounded memory - never holds more than a single file's tree). For each file it
extracts the CPG and produces node / edge / metadata events to Kafka, or, in
--offline mode, writes them to JSONL files so the pipeline can be demonstrated
without a running broker.

Topics (see Task 3):
    cpg.nodes      cpg.edges      cpg.metadata      cpg.errors

Run (Kafka):
    python parser_service.py --manifest reports/file_manifest.json \
        --repo ./optimum --bootstrap localhost:9092

Run (offline demo, no broker):
    python parser_service.py --manifest reports/file_manifest.json \
        --repo ./optimum --offline --outdir reports/events
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from cpg import extract_cpg
from events import edge_event, error_event, metadata_event, node_event

TOPICS = {
    "node": "cpg.nodes",
    "edge": "cpg.edges",
    "metadata": "cpg.metadata",
    "error": "cpg.errors",
}


class OfflineSink:
    """Writes events to per-topic JSONL files (broker-free demonstration)."""

    def __init__(self, outdir: str):
        self.outdir = Path(outdir)
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.fh = {t: (self.outdir / f"{name}.jsonl").open("w")
                   for t, name in TOPICS.items()}
        self.counts = {t: 0 for t in TOPICS}

    def send(self, kind: str, key: str, value: str):
        self.fh[kind].write(json.dumps({"key": key, "value": json.loads(value)}) + "\n")
        self.counts[kind] += 1

    def close(self):
        for fh in self.fh.values():
            fh.close()


class KafkaSink:
    """Produces to real Kafka topics via kafka-python."""

    def __init__(self, bootstrap: str):
        from kafka import KafkaProducer  # imported lazily
        common = dict(
            bootstrap_servers=bootstrap,
            key_serializer=lambda k: k.encode(),
            value_serializer=lambda v: v.encode(),
            acks="all",
        )
        try:
            self.p = KafkaProducer(enable_idempotence=True, **common)
        except AssertionError:
            # kafka-python-ng (Python 3.12) rejects enable_idempotence;
            # pipeline idempotency does not depend on it (structural ids + MERGE)
            self.p = KafkaProducer(**common)
        self.counts = {t: 0 for t in TOPICS}

    def send(self, kind: str, key: str, value: str):
        self.p.send(TOPICS[kind], key=key, value=value)
        self.counts[kind] += 1

    def close(self):
        self.p.flush()
        self.p.close()


def process_file(repo_root: Path, entry: dict, sink) -> None:
    rel = entry["rel_path"]
    source = (repo_root / rel).read_text(encoding="utf-8", errors="replace")
    try:
        res = extract_cpg(rel, source)
    except SyntaxError as exc:
        k, v = error_event(rel, entry["file_id"], exc)
        sink.send("error", k, v)
        return

    for node in res.nodes:
        k, v = node_event(res, node)
        sink.send("node", k, v)
    for edge in res.edges:
        k, v = edge_event(res, edge)
        sink.send("edge", k, v)
    k, v = metadata_event(res)
    sink.send("metadata", k, v)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--outdir", default="reports/events")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--only", help="process a single rel_path (used for replay)")
    args = ap.parse_args()

    manifest = json.loads(Path(args.manifest).read_text())
    repo_root = Path(args.repo).resolve()
    files = manifest["files"]
    if args.only:
        files = [f for f in files if f["rel_path"] == args.only]
        if not files:
            raise SystemExit(f"--only path not in manifest: {args.only}")

    sink = OfflineSink(args.outdir) if args.offline else KafkaSink(args.bootstrap)
    try:
        for i, entry in enumerate(files, 1):
            process_file(repo_root, entry, sink)
            if i % 10 == 0 or i == len(files):
                print(f"  processed {i}/{len(files)} files")
    finally:
        sink.close()

    print("Event counts:", sink.counts)


if __name__ == "__main__":
    main()
