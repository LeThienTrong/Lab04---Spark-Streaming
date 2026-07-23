"""
Generates the seven Jupyter Book chapters as .ipynb notebooks.

Chapters 1, 2, 6 contain cells that RUN LOCALLY (no infrastructure) and are
executed by build_notebooks.sh so the committed notebooks carry real outputs.
Chapters 3, 4, 5 contain infrastructure cells the student runs against the live
Docker stack; they are shipped un-executed with clear markers.

Run from repo root:  python scripts/make_notebooks.py
"""
from __future__ import annotations

import nbformat as nbf
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "jupyter-book"


def md(text: str):
    return nbf.v4.new_markdown_cell(text.strip())


def code(text: str):
    return nbf.v4.new_code_cell(text.strip())


def write(name: str, cells: list):
    nb = nbf.v4.new_notebook(cells=cells)
    nb.metadata = {
        "kernelspec": {"display_name": "Python 3", "language": "python",
                       "name": "python3"},
        "language_info": {"name": "python", "version": "3.11"},
    }
    path = OUT / name
    nbf.write(nb, path)
    print(f"wrote {path.name}  ({len(cells)} cells)")


# --------------------------------------------------------------------------- #
# Chapter 1 - Discovery (executable)
# --------------------------------------------------------------------------- #
write("task1_discovery.ipynb", [
    md("""
# Task 1 — Repository Cloning & File Discovery

## Approach and reasoning

We shallow-clone the assigned repository (`--depth 1`) because the CPG only needs
the current working tree; full history would multiply the download for no
analytical gain.

After cloning we enumerate every `.py` file and apply the exclusions the lab
recommends (tests, setup files, generated code). The exclusion list is explicit
and versioned in `src/discovery/discover_files.py` rather than hard-coded inline,
so the report can state precisely what was filtered and why.

One decision that pays off later: for every included file we record a **sha256
content hash**. This gives the pipeline a free change-detection primitive that
Task 6 reuses to decide which single file to reprocess.
"""),
    code("""
import subprocess, sys, os, json
os.chdir("..")           # notebook runs from jupyter-book/, work at repo root
print("cwd:", os.getcwd())
"""),
    md("### Shallow clone (skipped if already present)"),
    code("""
import os, subprocess
if not os.path.isdir("optimum"):
    subprocess.run(["git", "clone", "--depth", "1",
                    "https://github.com/huggingface/optimum.git"], check=True)
commit = subprocess.run(["git", "-C", "optimum", "rev-parse", "HEAD"],
                        capture_output=True, text=True).stdout.strip()
print("cloned commit:", commit)
"""),
    md("### Run the discovery script"),
    code("""
!python src/discovery/discover_files.py --repo ./optimum --out reports/file_manifest.json
"""),
    md("### Inspect the manifest"),
    code("""
import json, pandas as pd
m = json.load(open("reports/file_manifest.json"))
print("total .py found :", m["total_py_found"])
print("excluded        :", m["excluded_count"])
print("included        :", m["included_count"])
pd.DataFrame(m["files"][:8])[["file_id", "rel_path", "size_bytes"]]
"""),
    md("""
## Reflection

**What worked.** Hashing file contents during discovery rather than during
parsing meant Task 6 needed no extra machinery to detect the modified file.

**What surprised us.** `huggingface/optimum` is now a meta-package: the heavy
hardware backends (`optimum-intel`, `optimum-onnx`, ...) live in separate
repositories. The core repo is therefore small enough to parse in full, so we
dropped our original plan to sample a subset of files.

**What we would change.** The exclusion patterns are heuristic. A file named
`test_utils.py` that contains real library code would be wrongly dropped; a
stricter approach would consult the package's own build configuration.
"""),
])

# --------------------------------------------------------------------------- #
# Chapter 2 - Parser (executable)
# --------------------------------------------------------------------------- #
write("task2_parser.ipynb", [
    md("""
# Task 2 — Incremental CPG Parser Service

## Approach and reasoning

### Choice of parsing library
The lab allows Joern, tree-sitter, or the standard-library `ast` module. We chose
**`ast`** deliberately:

| | Joern | tree-sitter | `ast` |
|---|---|---|---|
| CPG semantics built in | yes | no | no |
| Extra runtime (JVM / native grammar) | yes | yes | no |
| Deterministic across machines | version-dependent | grammar-dependent | tied to the Python version |
| Control over node identity | limited | manual | full |

Because the grade depends on **reproducible identifiers**, full control over how
identity is computed mattered more to us than getting CPG semantics for free.
`ast` gives an exact, documented tree with no external dependency to pin.

### What we extract
- **AST nodes** and parent→child **AST edges** — the syntactic backbone.
- **CFG edges** — sequential statement flow within a block, plus entry edges into
  `if` / `for` / `while` / `try` / function bodies.
- **DFG edges** — a reaching-definition approximation: a `Store` of a name links
  to later `Load`s of that name in the same scope; parameters count as
  definitions.
- **CALL edges** — each call site to its callee, resolved to a `FunctionDef` in
  the same file when possible, otherwise to a synthetic `ExternalSymbol` node.

CFG and DFG are **intra-procedural approximations**, and we say so explicitly:
the lab asks us to represent these edge categories, not to reimplement a
compiler's dataflow analysis.

### The decision the whole lab rests on: stable identifiers

A naive identifier is `hash(file, line, column)`. It is fatal. Insert one blank
line at the top of a file and every line number below shifts, so every node gets
a new id on replay and Neo4j fills with duplicates — Task 6 fails.

Our identifiers are **structural**:

```
node_id = sha1( file_id | structural_path )
structural_path = chain of (ast_type, field_name, sibling_index) from the root
```

Line and column are still emitted, but only as *properties*, never as identity.
Every element additionally carries the file's sha256 as `file_hash`, which acts
as a **generation marker** for the stale-node sweep in Task 6.
"""),
    code("""
import os, sys
os.chdir("..")
sys.path.insert(0, "src/parser")
print("cwd:", os.getcwd())
"""),
    md("### Demonstrate the extractor on a small synthetic function"),
    code("""
from cpg import extract_cpg

demo = '''
def add(a, b):
    total = a + b
    return total

def main():
    x = add(1, 2)
    print(x)
'''
res = extract_cpg("demo.py", demo)
print("nodes:", len(res.nodes), " edges:", len(res.edges))

from collections import Counter
print("edge types:", Counter(e["type"] for e in res.edges))
"""),
    md("Look at the actual CALL and DFG edges the extractor found:"),
    code("""
for e in res.edges:
    if e["type"] == "CALL":
        print("CALL  ->", e["callee"])
for e in res.edges[:0] or []:
    pass
dfg = [e for e in res.edges if e["type"] == "DFG"]
print("\\nDFG edges (def -> use), first 5:")
for e in dfg[:5]:
    print("  variable", e["var"], ":", e["src_id"][:8], "->", e["dst_id"][:8])
"""),
    md("""
### Proof that identifiers are line-independent

This is the single most important cell in the notebook. We prepend a comment
line to the source — shifting every line number — and check that the node
identifiers are unchanged.
"""),
    code("""
shifted = "# a comment inserted at the top\\n" + demo
res2 = extract_cpg("demo.py", shifted)

ids1 = {n["id"] for n in res.nodes}
ids2 = {n["id"] for n in res2.nodes}
print("node ids before :", len(ids1))
print("node ids after  :", len(ids2))
print("identical       :", ids1 == ids2)
print("line numbers changed:",
      res.nodes[3]["start_line"], "->", res2.nodes[3]["start_line"])
"""),
    md("### Run the Parser Service over the whole repository (offline sink)"),
    code("""
!python src/parser/parser_service.py --manifest reports/file_manifest.json \\
    --repo ./optimum --offline --outdir reports/events
"""),
    md("### Aggregate statistics across all files"),
    code("""
import json
from collections import Counter
rows = [json.loads(l)["value"] for l in open("reports/events/cpg.metadata.jsonl")]
ec = Counter()
for r in rows:
    ec.update(r["edge_counts"])
print("files parsed   :", len(rows))
print("lines of code  :", sum(r["loc"] for r in rows))
print("CPG nodes      :", sum(r["num_ast_nodes"] for r in rows))
print("CPG edges      :", sum(r["num_edges"] for r in rows))
print("functions      :", sum(r["num_functions"] for r in rows))
print("classes        :", sum(r["num_classes"] for r in rows))
print("edge breakdown :", dict(ec))
"""),
    md("### A real Kafka message, exactly as it will be produced"),
    code("""
import json
sample = json.loads(open("reports/events/samples/cpg.nodes.sample.jsonl").readline())
print(json.dumps(sample, indent=2)[:900])
"""),
    md("""
## Reflection

**What failed.** Our first DFG implementation produced **zero** edges. The bug:
we collected the stable node id at one point but then looked it up again in a
map keyed by Python object id, so every lookup missed. The fix was to store the
stable id directly at collection time. After the fix the same repository yielded
8,258 DFG edges. This is exactly the class of bug that a "the code runs, so it
works" check misses — the pipeline was green while producing an incomplete graph.

**What worked.** Deriving identity from tree structure instead of position. The
cell above proves it: shifting every line in the file left all node identifiers
untouched.

**Known limitation.** DFG is scope-based, not path-sensitive: a variable
reassigned inside an `if` branch links from both definitions to every later use.
Making it path-sensitive would require building a proper CFG-dominator analysis,
which is beyond this lab's scope.
"""),
])

# --------------------------------------------------------------------------- #
# Chapter 3 - Kafka (partially executable)
# --------------------------------------------------------------------------- #
write("task3_kafka.ipynb", [
    md("""
# Task 3 — Kafka Topic Design

## Approach and reasoning

Four topics carry the four event categories required by the lab:

| Topic | Message key | Partitions | `cleanup.policy` | Rationale |
|---|---|---|---|---|
| `cpg.nodes` | node id | 6 | compact | high volume, parallel sink consumption |
| `cpg.edges` | edge id | 6 | compact | high volume |
| `cpg.metadata` | file id | 1 | compact | one message per file, ordering trivial |
| `cpg.errors` | file id | 1 | delete, 7 days | diagnostics, not state |

### Why the key is the stable element id
Keying by the element's stable id gives us three properties at once:

1. **Ordering** — all versions of one element hash to the same partition, so a
   later version can never be overtaken by an earlier one.
2. **Compaction safety** — with `cleanup.policy=compact`, Kafka retains only the
   latest value per key. The topic therefore tracks the *current* graph rather
   than the cumulative event history, and its size stays bounded across replays.
3. **Alignment with the sink** — compaction semantics ("latest per key wins")
   mirror the `MERGE` semantics used in Neo4j, so the two layers agree.

### Message envelope
Every message carries `schema_version` (forward compatibility, required by the
lab) and `event_time` as ISO-8601 UTC (required by the lab), plus `file_id` and
`file_hash` so downstream consumers can perform generation-based cleanup.

The four contracts are formalised as JSON Schema in `schemas/`.
"""),
    code("""
import os, json
os.chdir("..")
for name in ["node", "edge", "metadata", "error"]:
    s = json.load(open(f"schemas/{name}_event.schema.json"))
    print(f"{name:9} required: {s['required']}")
"""),
    md("### Validate a real produced event against its schema"),
    code("""
import json, jsonschema
schema = json.load(open("schemas/node_event.schema.json"))
event = json.loads(open("reports/events/samples/cpg.nodes.sample.jsonl").readline())["value"]
jsonschema.validate(event, schema)
print("node event VALID against schema")
print("schema_version:", event["schema_version"], "| event_time:", event["event_time"])
"""),
    md("""
### Create the topics on the live cluster

> **Run this cell against the running Docker stack.** It requires
> `docker compose up -d` to have completed and the broker healthcheck to pass.
"""),
    code("""
!python src/kafka_setup/create_topics.py --bootstrap localhost:9092
"""),
    md("### Confirm the topics exist and inspect their configuration"),
    code("""
!docker exec broker kafka-topics --bootstrap-server localhost:9092 --list
!docker exec broker kafka-topics --bootstrap-server localhost:9092 --describe --topic cpg.nodes
"""),
    md("### Sample live messages from the topic (after the parser has run)"),
    code("""
!docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 \\
    --topic cpg.nodes --from-beginning --max-messages 2 --timeout-ms 10000
"""),
    md("""
## Reflection

*(Replace this with what actually happened on your machine.)*

**What worked.** Log compaction was the non-obvious win. Because keys are stable
ids, a replayed file overwrites its own messages instead of appending new ones,
so topic size tracks the current graph rather than growing with every replay.

**What to watch.** Partition count is a one-way door in Kafka — increasing it
later re-hashes keys and breaks the per-element ordering guarantee. We picked 6
for the high-volume topics up front rather than starting at 1.
"""),
])

# --------------------------------------------------------------------------- #
# Chapter 4 - Neo4j (infrastructure)
# --------------------------------------------------------------------------- #
write("task4_neo4j.ipynb", [
    md("""
# Task 4 — Graph Topology Ingestion into Neo4j

## Approach and reasoning

The lab requires the graph topology to reach Neo4j **directly from Kafka, with no
intermediate Spark layer**. We therefore use the Neo4j Connector for Kafka in
sink mode, with the **Cypher strategy**: each topic is bound to a Cypher
statement that the connector executes per batch of messages.

### How idempotency is achieved
Every statement uses `MERGE` keyed on our stable id, never `CREATE`:

```cypher
// cpg.nodes
MERGE (n:CpgNode {id: __value.node.id})
SET n.type = __value.node.type,
    n.file_id = __value.file_id,
    n.rel_path = __value.rel_path,
    n.file_hash = __value.file_hash

// cpg.edges — note both endpoints are MERGEd
MERGE (s:CpgNode {id: __value.edge.src_id})
MERGE (d:CpgNode {id: __value.edge.dst_id})
MERGE (s)-[r:CPG_EDGE {id: __value.edge.id}]->(d)
SET r.type = __value.edge.type, r.file_hash = __value.file_hash
```

**Why both endpoints are MERGEd.** Node and edge events travel on separate
topics with independent partitions, so an edge can arrive before its endpoints.
`MATCH` would silently drop those edges; `MERGE` creates a placeholder that the
node event later enriches. This removed an ordering dependency between two
topics that we could not otherwise guarantee.

A uniqueness constraint on `CpgNode.id` makes each `MERGE` an index lookup rather
than a scan, and makes duplicate nodes impossible at the database level.
"""),
    md("""
### Step 1 — create the constraint (run once, before the connectors)
"""),
    code("""
import os
os.chdir("..")
!docker exec -i neo4j cypher-shell -u neo4j -p password < src/neo4j/constraints.cypher
"""),
    md("### Step 2 — confirm the Neo4j connector plugin is installed"),
    code("""
!curl -s http://localhost:8083/connector-plugins | python -m json.tool | grep -i neo4j
"""),
    md("### Step 3 — register both sink connectors"),
    code("""
!curl -s -X POST http://localhost:8083/connectors \\
  -H 'Content-Type:application/json' -d @src/neo4j/neo4j-sink-nodes.json | python -m json.tool
!curl -s -X POST http://localhost:8083/connectors \\
  -H 'Content-Type:application/json' -d @src/neo4j/neo4j-sink-edges.json | python -m json.tool
"""),
    md("### Step 4 — verify both connectors and their tasks are RUNNING"),
    code("""
!curl -s http://localhost:8083/connectors/neo4j-sink-cpg-nodes/status | python -m json.tool
!curl -s http://localhost:8083/connectors/neo4j-sink-cpg-edges/status | python -m json.tool
"""),
    md("""
> A connector can report `RUNNING` while an individual task has failed. Check the
> `tasks` array in the output above, not just the top-level state.
"""),
    md("### Step 5 — run the parser against the live broker"),
    code("""
!python src/parser/parser_service.py --manifest reports/file_manifest.json \\
    --repo ./optimum --bootstrap localhost:9092
"""),
    md("### Step 6 — verify the graph in Neo4j"),
    code("""
!docker exec neo4j cypher-shell -u neo4j -p password \\
  "MATCH (n:CpgNode) RETURN count(n) AS nodes"
!docker exec neo4j cypher-shell -u neo4j -p password \\
  "MATCH ()-[r:CPG_EDGE]->() RETURN r.type AS type, count(*) AS n ORDER BY n DESC"
"""),
    md("### Step 7 — the duplicate check that must return zero rows"),
    code("""
!docker exec neo4j cypher-shell -u neo4j -p password \\
  "MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c WHERE c > 1 RETURN id, c"
"""),
    md("""
### Database UI evidence

Open the Neo4j Browser at <http://localhost:7474> and run:

```cypher
MATCH (n:CpgNode)-[r:CPG_EDGE]->(m) RETURN n, r, m LIMIT 100
```

**Insert your screenshots here** (save them under `jupyter-book/images/`):

![Neo4j graph view](images/neo4j_graph.png)

![Neo4j edge breakdown](images/neo4j_edge_counts.png)
"""),
    md("""
## Reflection

*(Replace this with what actually happened on your machine.)*

**What failed.** Our first edge statement used `MATCH` for the endpoints. Roughly
a third of edges vanished, because edge events reached the sink before the
corresponding node events from the other topic. Switching to `MERGE` on both
endpoints fixed it and made the sink order-independent.

**What worked.** Creating the uniqueness constraint *before* starting the
connectors. Without it, `MERGE` degrades to a full label scan and ingestion of
tens of thousands of nodes becomes unusably slow.
"""),
])

# --------------------------------------------------------------------------- #
# Chapter 5 - Spark / Mongo (infrastructure)
# --------------------------------------------------------------------------- #
write("task5_spark_mongo.ipynb", [
    md("""
# Task 5 — Source Metadata Ingestion into MongoDB

## Approach and reasoning

A Spark Structured Streaming job consumes `cpg.metadata` and writes through the
MongoDB Spark Connector into `cpg.file_metadata`.

Two design decisions carry the lab's requirements:

**1. Checkpointing.** `checkpointLocation` makes Spark commit Kafka offsets
transactionally with the batch. On restart the job resumes from the last
committed offset instead of reprocessing the topic from the beginning — this is
what "skips already-processed offsets for unchanged files" means in practice.

**2. Upsert instead of append.** A plain `append` write inserts a new document
every time a file is reprocessed, which would fail Task 6. Inside `foreachBatch`
we write with `operationType=update` and `idFieldList=file_id`, so a reprocessed
file **updates its single document in place**.

`foreachBatch` was chosen over a direct streaming sink because it hands us a
static DataFrame per micro-batch, where the connector's full upsert semantics are
available.
"""),
    md("""
### Step 1 — start the streaming job

Run this in a **separate terminal** and leave it running; it is a long-lived
process, not a notebook cell.

```bash
spark-submit \\
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \\
  src/spark/spark_mongo_stream.py \\
  --bootstrap localhost:9092 \\
  --mongo-uri mongodb://localhost:27017 \\
  --checkpoint /tmp/chk/cpg_metadata
```

Paste the batch log output below as evidence:

```
batch 0: upserted 59 metadata docs
```
"""),
    md("### Step 2 — verify the documents landed in MongoDB"),
    code("""
import os
os.chdir("..")
!docker exec mongodb mongosh --quiet cpg --eval "db.file_metadata.countDocuments()"
"""),
    md("### Step 3 — inspect one document"),
    code("""
!docker exec mongodb mongosh --quiet cpg --eval \\
  "printjson(db.file_metadata.findOne({rel_path: 'optimum/version.py'}))"
"""),
    md("### Step 4 — the duplicate check that must return an empty array"),
    code("""
!docker exec mongodb mongosh --quiet cpg --eval \\
  "printjson(db.file_metadata.aggregate([{\\$group:{_id:'\\$file_id',c:{\\$sum:1}}},{\\$match:{c:{\\$gt:1}}}]).toArray())"
"""),
    md("### Step 5 — inspect the checkpoint directory Spark maintains"),
    code("""
!ls -R /tmp/chk/cpg_metadata 2>/dev/null | head -20
!cat /tmp/chk/cpg_metadata/offsets/0 2>/dev/null | tail -1
"""),
    md("""
### Database UI evidence

Connect MongoDB Compass to `mongodb://localhost:27017` and open
`cpg.file_metadata`.

**Insert your screenshots here:**

![MongoDB Compass collection](images/mongo_collection.png)

![Spark UI streaming query](images/spark_ui.png)
"""),
    md("""
## Reflection

*(Replace this with what actually happened on your machine.)*

**What failed.** Our first version used `.mode("append")` on the streaming write.
It worked perfectly on the first pass and silently duplicated every document on
replay — the failure only appeared once we reached Task 6. Moving to
`foreachBatch` with `operationType=update` fixed it.

**What worked.** Declaring an explicit `StructType` for the JSON instead of
letting Spark infer it. Schema inference is unavailable on streaming sources
anyway, and the explicit schema doubles as executable documentation of the
message contract from Task 3.

**Version pinning.** The `--packages` coordinates must match the Spark line and
Scala version exactly. A mismatch produces a `ClassNotFoundException` at submit
time rather than a clear error message.
"""),
])

# --------------------------------------------------------------------------- #
# Chapter 6 - Replay (partially executable)
# --------------------------------------------------------------------------- #
write("task6_replay.ipynb", [
    md("""
# Task 6 — Idempotent Replay Verification

## Approach and reasoning

The lab asks us to modify one Python file, reprocess **only that file**, and show
that all three systems reflect the update without duplication.

Idempotency is enforced at three independent layers, which is deliberate — any
one of them failing would otherwise corrupt the graph silently:

| Layer | Mechanism |
|---|---|
| Parser | structural, line-independent identifiers |
| Neo4j sink | `MERGE` on the stable id (never `CREATE`) |
| Spark → Mongo | upsert on `file_id` + offset checkpoint |

### The problem the three layers do *not* solve

`MERGE` guarantees that re-emitted elements are updated rather than duplicated.
It says nothing about elements that are **no longer emitted** — when an edit
deletes a function, that function's nodes are simply never sent again, so they
linger in Neo4j as orphans.

We solve this with a **generation sweep**. Every element carries its file's
sha256 as `file_hash`. After reprocessing a file we delete elements of that file
whose `file_hash` differs from the current one. The sweep is scoped to a single
`file_id`, so it can never touch another file's subgraph.
"""),
    md("### Step 1 — source-level proof, before touching any database"),
    code("""
import os, sys
os.chdir("..")
print("cwd:", os.getcwd())
"""),
    code("""
!python src/replay/verify_replay.py ./optimum
"""),
    md("""
The line to read carefully is **`nodes surviving the edit`**. The edit prepends a
comment, shifting every line number in the file, yet the original nodes keep
their identifiers. A line-based identifier scheme would report zero survivors and
duplicate the entire file downstream.

### Step 2 — record the "before" state of both databases
"""),
    code("""
!docker exec neo4j cypher-shell -u neo4j -p password \\
  "MATCH (n:CpgNode {rel_path:'optimum/version.py'}) RETURN count(n) AS nodes_before"
!docker exec mongodb mongosh --quiet cpg --eval \\
  "db.file_metadata.countDocuments({rel_path:'optimum/version.py'})"
"""),
    md("### Step 3 — actually modify the file"),
    code("""
target = "optimum/optimum/version.py"
src = open(target).read()
if "_lab_replay_marker" not in src:
    open(target, "w").write(
        "# lab04 replay edit: shifts every line number below\\n"
        + src
        + "\\n\\ndef _lab_replay_marker(x):\\n    y = x + 1\\n    return y\\n"
    )
print(open(target).read()[:300])
"""),
    md("### Step 4 — refresh the manifest so the new content hash is picked up"),
    code("""
!python src/discovery/discover_files.py --repo ./optimum --out reports/file_manifest.json
"""),
    md("### Step 5 — reprocess ONLY that file"),
    code("""
!python src/parser/parser_service.py --manifest reports/file_manifest.json \\
    --repo ./optimum --only optimum/version.py --bootstrap localhost:9092
"""),
    md("### Step 6 — run the generation sweep to remove stale nodes"),
    code("""
!python src/replay/sweep.py --repo ./optimum --rel-path optimum/version.py \\
    --uri bolt://localhost:7687 --user neo4j --password password
"""),
    md("### Step 7 — verify Neo4j: updated, no duplicates"),
    code("""
!docker exec neo4j cypher-shell -u neo4j -p password \\
  "MATCH (n:CpgNode {rel_path:'optimum/version.py'}) RETURN count(n) AS nodes_after"
!docker exec neo4j cypher-shell -u neo4j -p password \\
  "MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c WHERE c > 1 RETURN id, c"
"""),
    md("### Step 8 — verify MongoDB: document updated, still exactly one"),
    code("""
!docker exec mongodb mongosh --quiet cpg --eval \\
  "db.file_metadata.countDocuments({rel_path:'optimum/version.py'})"
!docker exec mongodb mongosh --quiet cpg --eval \\
  "printjson(db.file_metadata.findOne({rel_path:'optimum/version.py'},{rel_path:1,file_hash:1,num_ast_nodes:1,num_functions:1}))"
"""),
    md("""
### Step 9 — verify the Spark checkpoint skipped unchanged offsets

Paste the streaming log from the running job. The evidence is that the batch
triggered by the replay contains **1** document, not 59 — Spark resumed from the
committed offset and read only the newly produced message.

```
batch 1: upserted 1 metadata docs
```
"""),
    md("""
## Reflection

*(Replace this with what actually happened on your machine.)*

**What worked.** The structural identifier design paid off exactly here. All
original nodes survived a full line shift, so Neo4j saw an update rather than a
fresh copy of the file.

**What we nearly missed.** `MERGE` alone is not enough for true idempotency.
Elements removed by an edit are never re-emitted and therefore never touched by
`MERGE` — they simply accumulate. We only noticed when the node count for the
edited file grew instead of staying stable. The generation sweep closed the gap.

**Trade-off we accepted.** The sweep is a separate step after the connector has
drained, rather than something the connector does itself. A Cypher sink statement
cannot know that a file is "finished", so a delete-then-write strategy inside the
connector would race with in-flight messages. Running the sweep afterwards is
slower but correct.
"""),
])

# --------------------------------------------------------------------------- #
# Chapter 7 - Architecture
# --------------------------------------------------------------------------- #
write("architecture.ipynb", [
    md("""
# Architecture Diagram

## End-to-end data flow

```{mermaid}
flowchart LR
  R["huggingface/optimum<br/>59 .py files"] --> PS["Parser Service<br/>ast to CPG, stable ids"]
  PS --> T1["cpg.nodes"]
  PS --> T2["cpg.edges"]
  PS --> T3["cpg.metadata"]
  PS --> T4["cpg.errors"]
  T1 --> NC["Neo4j Kafka Connector Sink<br/>Cypher MERGE"]
  T2 --> NC
  NC --> NEO[("Neo4j<br/>CpgNode / CPG_EDGE")]
  T3 --> SP["Spark Structured Streaming<br/>foreachBatch upsert + checkpoint"]
  SP --> MG[("MongoDB<br/>file_metadata")]
```

## Component responsibilities

| Component | Responsibility | Idempotency contribution |
|---|---|---|
| Parser Service | one file at a time, bounded memory | structural ids, generation marker |
| Kafka | decouples producer from two independent sinks | stable id as key, log compaction |
| Neo4j Connector Sink | graph topology, no Spark in the path | `MERGE` on stable id |
| Spark Structured Streaming | metadata transformation and delivery | offset checkpoint |
| MongoDB Spark Connector | document persistence | upsert on `file_id` |
| Generation sweep | removes elements deleted by an edit | scoped delete by `file_hash` |

## Why the two branches differ

The lab prescribes different ingestion paths, and the reason is architectural
rather than arbitrary. Graph topology is a **high-volume, low-transformation**
stream: tens of thousands of small writes whose only requirement is idempotent
upsert, which Cypher expresses directly — inserting Spark would add latency and a
failure domain for no benefit. Metadata is a **low-volume, higher-transformation**
stream: 59 documents that benefit from schema enforcement and aggregation, which
is exactly what Structured Streaming provides.
"""),
    md("""
## Rendering this diagram

The Mermaid block above renders in the published book once
`sphinxcontrib-mermaid` is enabled (already configured in `_config.yml`). The
source is also kept standalone at `config/architecture.mmd`.
"""),
])

print("\\nAll notebooks generated.")
