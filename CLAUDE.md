# Lab 04 — Incremental CPG Streaming Pipeline

Big Data course lab. Read `RUNBOOK.md` first — it is the authoritative sequence
of phases and checkpoints. This file is context for you (Claude Code), not a
replacement for it.

## What this project is

Build an incremental Code Property Graph (CPG) from `huggingface/optimum`
(cloned locally at `./optimum/`), stream it through Kafka, and land it in two
databases: Neo4j (graph topology, via the Neo4j Kafka Connector, no Spark in
that path) and MongoDB (source metadata, via Spark Structured Streaming).

Grading is per-task (see the lab PDF the user has, not in this repo): file
discovery, parser, Kafka topic design, Neo4j ingestion, Mongo ingestion,
idempotent replay verification, architecture diagram — plus a Jupyter Book
published to GitHub Pages as the actual submission.

## Ground truth: what already works

- **Task 1 (discovery)** and **Task 2 (parser)** are fully verified. They run
  with no infrastructure at all:
  ```
  python src/discovery/discover_files.py --repo ./optimum --out reports/file_manifest.json
  python src/parser/parser_service.py --manifest reports/file_manifest.json --repo ./optimum --offline --outdir reports/events
  ```
  Known-good output on the reference commit: 59 source files, ~58.8k nodes,
  ~73.5k edges (AST/DFG/CFG/CALL all present, 0 parse errors).
- **Task 6's source-level proof** also runs standalone:
  `python src/replay/verify_replay.py ./optimum` must print `RESULT: PASS`.
  This does not touch Docker.
- Everything else (Tasks 3–5, and the DB-touching half of Task 6) needs the
  Docker stack up. That is where real bugs surface — see below.

## Environment

Ubuntu. Python 3.12 (not 3.11) — use `requirements-py312.txt`
(`kafka-python-ng`, not `kafka-python`, same `import kafka`). Java 17 required
for Spark 3.5, even though the OS default is Java 21
(`export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`). Docker Engine from
Docker's repo, not snap. `vm.max_map_count` must be ≥262144 for Neo4j.

`scripts/setup_ubuntu.sh` handles all of this. `scripts/healthcheck.sh` verifies
it. Run healthcheck before debugging anything else — most "mysterious" failures
turn out to be one of the six things it checks.

## Bugs already found and fixed — do not reintroduce these

1. **Neo4j sink connector task fails with `InvalidReplicationFactorException`
   ... "cannot be reached because only 1 broker(s) are registered"**, referring
   to the dead-letter-queue topic. Kafka Connect defaults DLQ replication to 3;
   this stack has 1 broker. Fixed by
   `"errors.deadletterqueue.topic.replication.factor": "1"` in both
   `src/neo4j/neo4j-sink-nodes.json` and `neo4j-sink-edges.json`. If you ever
   see `InvalidReplicationFactorException` anywhere, check for a Connect- or
   Kafka-managed topic that isn't pinned to replication factor 1 — the pattern
   recurs anywhere Connect creates a topic implicitly.

2. **Spark job dies at `DataStreamWriter.start()` with
   `org.apache.hadoop.ipc.Client ... Connection refused`** — despite this
   pipeline never using HDFS. Cause: a bare checkpoint path
   (`/tmp/chk/cpg_metadata`) resolves against `fs.defaultFS`, which on a Big
   Data course machine is often `hdfs://...` with no NameNode running. Fixed in
   `src/spark/spark_mongo_stream.py`: bare paths are normalised to `file://`
   URIs and `fs.defaultFS` is overridden to `file:///` unless `--use-hadoop-fs`
   is passed. If a stack trace bottoms out in `org.apache.hadoop.ipc.*` or
   `NetUtils.connect`, this is almost certainly the cause regardless of which
   Spark job triggered it.

3. **Neo4j connector `__value` binding** — the `__header`/`__key`/`__value`
   variables in Cypher-strategy statements only exist from connector 5.1.0
   onward; older versions expose only `event`. `src/neo4j/legacy/` has
   equivalent configs written against `event`, switchable via
   `bash scripts/reload_connectors.sh legacy`.

4. **jupyter-book must be v1.x.** v2 uses mystmd and silently ignores
   `_config.yml`/`_toc.yml`, producing "No file exports found" instead of a
   built book. `pip install "jupyter-book==1.0.2"` if this happens.

Full write-ups with symptom/cause/fix for all of these, plus more, are in
`TROUBLESHOOTING.md`. Read it before proposing a fix for anything Docker- or
Spark-related — the failure is very likely already documented there with the
exact trace to match against.

## The one design decision everything depends on

Node/edge identifiers in `src/parser/ids.py` are **structural**
(`sha1(file_id | ast_path)`), never based on line/column. This is what makes
`MERGE`-based ingestion idempotent. Do not "simplify" this to a
line/hash-based id — it would silently break Task 6, and the failure would only
show up much later as duplicate nodes after a replay, not as an error.

## Key scripts (all in `scripts/`, all shell-checked, treat as the interface)

| Script | Purpose |
|---|---|
| `setup_ubuntu.sh` | one-shot environment setup |
| `fetch_neo4j_connector.sh` | downloads the Neo4j Kafka connector jar |
| `healthcheck.sh` | pre-flight check, run before debugging anything |
| `run_pipeline.sh` | runs Tasks 1,3,4,2 in order with checkpoints |
| `diagnose_connector.sh <name>` | pattern-matches a failed connector's trace |
| `reload_connectors.sh [legacy]` | delete+re-register (Connect doesn't hot-reload) |
| `test_cypher.sh` | runs the sink's Cypher by hand, isolates Cypher vs. infra |
| `sweep.py` | Task 6 stale-node cleanup by generation (`file_hash`) |
| `verify_replay.py` | Task 6 source-level idempotency proof, no infra needed |
| `commit_by_task.sh` | one commit per task (grading requires incremental history) |

## Working rules

- **Prefer running the existing diagnostic scripts over guessing.** They exist
  because the first two production bugs in this project were both
  misdiagnosed before the real trace was read. `healthcheck.sh` and
  `diagnose_connector.sh` are the fast path to ground truth.
- **When a command fails, get the actual trace before proposing a fix.**
  `docker logs <container>`, `curl .../status`, the DLQ topic. Don't patch
  based on the exception class name alone — see bug #1 above, where the
  exception surfaced under a Neo4j connector but was purely a Kafka topic
  config issue.
- **Never fabricate command output.** If a Docker-dependent step can't be run
  in this environment, say so explicitly rather than writing an notebook cell
  output that looks executed.
- **Notebook outputs must come from real execution**, via
  `jupyter nbconvert --to notebook --execute --inplace`, not hand-written.
  Reflections in `jupyter-book/*.ipynb` describing "what failed" must match
  whatever actually failed on this machine — replace the placeholder text with
  real notes, don't invent generic ones.
- **Follow `RUNBOOK.md`'s phase order.** Constraint before connectors,
  connectors before parser, parser before Spark verification, etc. — the order
  encodes real dependencies (e.g. `MERGE` without the uniqueness constraint
  degrades to a full label scan at ~59k nodes).
- **Ask before anything destructive**: `docker compose down -v` (wipes
  volumes), force-pushing git history, deleting the cloned `optimum/` repo.
