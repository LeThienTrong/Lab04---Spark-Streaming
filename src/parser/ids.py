"""
Stable identifier scheme for CPG elements.
==========================================

WHY THIS MATTERS (the whole idempotency of the pipeline rests here):

A naive ID = hash(file, line, col). It is fatal: inserting a blank line at the
top of a file shifts every downstream line number, so every node gets a NEW id
on replay, and Neo4j fills up with duplicate/orphan nodes. That directly loses
Task 6.

Our IDs are therefore **structural**, not positional:

    node_id = sha1( file_id | structural_path )

`structural_path` is the chain of (ast_type, field_name, sibling_index) from the
module root down to the node. It is independent of absolute line numbers - moving
a function down the file does not change the path of unrelated code. Line/column
are still emitted, but only as *properties*, never as identity.

We additionally stamp every element with `file_hash` (sha256 of file content).
This acts as a **generation marker**: when a file is reprocessed after an edit,
new elements carry the new file_hash, and a single sweep can delete elements of
that file whose file_hash != current (stale-node cleanup), while unchanged
elements simply MERGE onto themselves.
"""
from __future__ import annotations

import hashlib


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def file_id_for(rel_path: str) -> str:
    """Stable per file path (path is the file's logical identity)."""
    return _sha1(rel_path)[:16]


def node_id_for(file_id: str, structural_path: str) -> str:
    return _sha1(f"{file_id}|{structural_path}")[:24]


def edge_id_for(edge_type: str, src_id: str, dst_id: str, extra: str = "") -> str:
    return _sha1(f"{edge_type}|{src_id}|{dst_id}|{extra}")[:24]
