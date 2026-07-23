"""
Task 2 - Incremental CPG extractor (single file).
=================================================

Uses the Python standard-library `ast` module (no external deps, fully
reproducible) to build a Code Property Graph for ONE file:

  * AST nodes  - every syntactic node, with a stable structural id.
  * AST edges  - parent -> child containment (the AST backbone).
  * CFG edges  - simplified intra-procedural control flow:
                 sequential statement order + branch entry into
                 if/for/while/try bodies and function entry.
  * DFG edges  - simplified reaching-definition data flow: an assignment to a
                 name (Store) links to later reads of that name (Load) in the
                 same function scope.
  * CALL edges - each Call site links to its callee; if the callee is a
                 function defined in the same file it links to that FunctionDef
                 node, otherwise to a synthetic external-symbol node.

CFG/DFG are intentionally simplified and documented as approximations - the lab
asks us to *represent* these edge categories, not to reimplement a full compiler.

The extractor is a generator-friendly, bounded-memory design: it holds only one
file's tree at a time and returns plain dicts ready to become Kafka events.
"""
from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass, field

from ids import edge_id_for, file_id_for, node_id_for


@dataclass
class CpgResult:
    file_id: str
    rel_path: str
    file_hash: str
    nodes: list = field(default_factory=list)
    edges: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


def _label(node: ast.AST) -> str:
    """Human-readable name for a node, when it has one."""
    for attr in ("name", "id", "arg", "attr"):
        v = getattr(node, attr, None)
        if isinstance(v, str):
            return v
    if isinstance(node, ast.Constant):
        return repr(node.value)[:40]
    return ""


def _snippet(src_lines: list[str], node: ast.AST) -> str:
    lineno = getattr(node, "lineno", None)
    if not lineno or lineno > len(src_lines):
        return ""
    return src_lines[lineno - 1].strip()[:120]


def extract_cpg(rel_path: str, source: str) -> CpgResult:
    file_hash = hashlib.sha256(source.encode("utf-8")).hexdigest()
    fid = file_id_for(rel_path)
    res = CpgResult(file_id=fid, rel_path=rel_path, file_hash=file_hash)

    try:
        tree = ast.parse(source, filename=rel_path)
    except SyntaxError as exc:  # surfaced by caller as a parser-error event
        raise exc

    src_lines = source.splitlines()

    # ------------------------------------------------------------------ #
    # 1) AST nodes + AST containment edges (structural walk)
    # ------------------------------------------------------------------ #
    # id_of maps each ast node object -> its stable structural id.
    id_of: dict[int, str] = {}
    # For CFG/DFG we need to know, per statement, which function scope it's in
    # and its ordering. We record that during the same walk.
    func_scopes: dict[int, list[ast.AST]] = {}  # scope-node-id -> ordered stmts
    name_defs: list[tuple[str, str, int]] = []   # (scope_id, name, node_ptr) Store
    name_uses: list[tuple[str, str, int]] = []   # (scope_id, name, node_ptr) Load
    call_sites: list[tuple[str, ast.Call]] = []  # (caller_scope_id, Call node)
    func_defs: dict[str, str] = {}               # func name -> node id (this file)

    def walk(node: ast.AST, path: str, scope_id: str):
        nid = node_id_for(fid, path)
        id_of[id(node)] = nid
        ntype = type(node).__name__
        res.nodes.append({
            "id": nid,
            "type": ntype,
            "label": _label(node),
            "code": _snippet(src_lines, node),
            "start_line": getattr(node, "lineno", None),
            "end_line": getattr(node, "end_lineno", None),
            "scope_id": scope_id,
        })

        # Track function definitions declared in this file (for call resolution)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_defs.setdefault(node.name, nid)

        # New scope for functions/module
        child_scope = nid if isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)
        ) else scope_id

        # Data-flow name tracking (store the *stable node id* directly)
        if isinstance(node, ast.Name):
            if isinstance(node.ctx, ast.Store):
                name_defs.append((child_scope, node.id, nid))
            elif isinstance(node.ctx, ast.Load):
                name_uses.append((child_scope, node.id, nid))
        elif isinstance(node, ast.arg):  # function parameters are definitions too
            name_defs.append((child_scope, node.arg, nid))
        if isinstance(node, ast.Call):
            call_sites.append((child_scope, node))

        # Recurse over child fields, building AST containment edges
        for field_name, value in ast.iter_fields(node):
            items = value if isinstance(value, list) else [value]
            for idx, child in enumerate(items):
                if isinstance(child, ast.AST):
                    child_path = f"{path}/{field_name}[{idx}]:{type(child).__name__}"
                    walk(child, child_path, child_scope)
                    res.edges.append({
                        "id": edge_id_for("AST", nid, id_of[id(child)], field_name),
                        "type": "AST",
                        "src_id": nid,
                        "dst_id": id_of[id(child)],
                        "field": field_name,
                    })

    root_path = "Module"
    walk(tree, root_path, node_id_for(fid, root_path))

    # ------------------------------------------------------------------ #
    # 2) CFG edges - sequential flow between consecutive statements in a
    #    block, plus function-entry edges.
    # ------------------------------------------------------------------ #
    def add_cfg_for_block(stmts):
        for a, b in zip(stmts, stmts[1:]):
            sa, sb = id(a), id(b)
            if sa in id_of and sb in id_of:
                res.edges.append({
                    "id": edge_id_for("CFG", id_of[sa], id_of[sb]),
                    "type": "CFG",
                    "src_id": id_of[sa],
                    "dst_id": id_of[sb],
                    "kind": "sequential",
                })

    for node in ast.walk(tree):
        # Any node carrying a `body` (module, function, if, for, while, with...)
        for attr in ("body", "orelse", "finalbody"):
            block = getattr(node, attr, None)
            if isinstance(block, list) and block:
                add_cfg_for_block(block)
        # branch-entry edge: header stmt -> first stmt of its body
        if isinstance(node, (ast.If, ast.For, ast.While, ast.With, ast.Try,
                             ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and id(node) in id_of and id(body[0]) in id_of:
                res.edges.append({
                    "id": edge_id_for("CFG", id_of[id(node)], id_of[id(body[0])], "enter"),
                    "type": "CFG",
                    "src_id": id_of[id(node)],
                    "dst_id": id_of[id(body[0])],
                    "kind": "enter",
                })

    # ------------------------------------------------------------------ #
    # 3) DFG edges - def (Store) -> use (Load) of same name in same scope.
    #    Approximation: links every def to every later use of that name in
    #    the scope (no path sensitivity). Clearly documented as such.
    # ------------------------------------------------------------------ #
    defs_by_scope_name: dict[tuple[str, str], list[str]] = {}
    for scope, name, def_nid in name_defs:
        defs_by_scope_name.setdefault((scope, name), []).append(def_nid)
    for scope, name, use_nid in name_uses:
        for def_nid in defs_by_scope_name.get((scope, name), []):
            if def_nid and use_nid and def_nid != use_nid:
                res.edges.append({
                    "id": edge_id_for("DFG", def_nid, use_nid, name),
                    "type": "DFG",
                    "src_id": def_nid,
                    "dst_id": use_nid,
                    "var": name,
                })

    # ------------------------------------------------------------------ #
    # 4) CALL edges - call site -> callee (resolved in-file or external).
    # ------------------------------------------------------------------ #
    ext_symbols: dict[str, str] = {}
    for scope, call in call_sites:
        callee = call.func
        if isinstance(callee, ast.Name):
            cname = callee.id
        elif isinstance(callee, ast.Attribute):
            cname = callee.attr
        else:
            cname = "<dynamic>"
        call_nid = id_of.get(id(call))
        if not call_nid:
            continue
        if cname in func_defs:                      # resolved in this file
            dst = func_defs[cname]
        else:                                       # external / synthetic node
            if cname not in ext_symbols:
                ext_nid = node_id_for(fid, f"__external__/{cname}")
                ext_symbols[cname] = ext_nid
                res.nodes.append({
                    "id": ext_nid, "type": "ExternalSymbol", "label": cname,
                    "code": "", "start_line": None, "end_line": None,
                    "scope_id": fid,
                })
            dst = ext_symbols[cname]
        res.edges.append({
            "id": edge_id_for("CALL", call_nid, dst, cname),
            "type": "CALL",
            "src_id": call_nid,
            "dst_id": dst,
            "callee": cname,
        })

    # ------------------------------------------------------------------ #
    # 5) Metadata (this document goes to MongoDB)
    # ------------------------------------------------------------------ #
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            imports += [f"{mod}.{a.name}" for a in node.names]

    edge_counts: dict[str, int] = {}
    for e in res.edges:
        edge_counts[e["type"]] = edge_counts.get(e["type"], 0) + 1

    res.metadata = {
        "file_id": fid,
        "rel_path": rel_path,
        "file_hash": file_hash,
        "loc": len(src_lines),
        "num_ast_nodes": len(res.nodes),
        "num_edges": len(res.edges),
        "edge_counts": edge_counts,
        "num_functions": sum(
            isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            for n in ast.walk(tree)
        ),
        "num_classes": sum(isinstance(n, ast.ClassDef) for n in ast.walk(tree)),
        "imports": sorted(set(imports))[:100],
    }
    return res
