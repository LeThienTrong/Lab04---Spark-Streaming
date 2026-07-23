"""
Event envelope builders (Task 3 message contract).

Every message carries:
  * schema_version - forward-compatibility field required by the lab.
  * event_time     - ISO-8601 UTC event timestamp required by the lab.
  * file_id / file_hash - identity + generation marker for idempotent cleanup.

The Kafka *key* for each message is the stable element id (node id / edge id /
file id). Keying by stable id guarantees that all versions of the same element
land on the same partition (preserving per-element order) and makes the topic
safe for log compaction.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

SCHEMA_VERSION = "1.0.0"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def node_event(res, node: dict) -> tuple[str, str]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "node",
        "event_time": _now(),
        "file_id": res.file_id,
        "rel_path": res.rel_path,
        "file_hash": res.file_hash,
        "node": node,
    }
    return node["id"], json.dumps(payload)


def edge_event(res, edge: dict) -> tuple[str, str]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "edge",
        "event_time": _now(),
        "file_id": res.file_id,
        "file_hash": res.file_hash,
        "edge": edge,
    }
    return edge["id"], json.dumps(payload)


def metadata_event(res) -> tuple[str, str]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "metadata",
        "event_time": _now(),
        **res.metadata,
    }
    return res.file_id, json.dumps(payload)


def error_event(rel_path: str, file_id: str, err: Exception) -> tuple[str, str]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_type": "error",
        "event_time": _now(),
        "file_id": file_id,
        "rel_path": rel_path,
        "error_type": type(err).__name__,
        "message": str(err)[:500],
    }
    return file_id, json.dumps(payload)
