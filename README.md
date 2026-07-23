# Lab 04 — Incremental CPG Streaming Pipeline

Incremental Code Property Graph construction over `huggingface/optimum`, streamed
through Apache Kafka into **Neo4j** (graph topology, via the Neo4j Kafka
Connector) and **MongoDB** (source metadata, via Spark Structured Streaming).

> **New here? Follow `RUNBOOK.md`** — it walks the whole lab end to end on
> Ubuntu with a checkpoint after each phase. This file is the reference; the
> runbook is the sequence.

## Published Jupyter Book

**<https://lethientrong.github.io/Lab04---Spark-Streaming/>** — the submission,
built from the notebooks in `jupyter-book/` on this branch and served from
`gh-pages`.

## Team and course

**Team BDS** — Lê Thiện Trọng, Hà Công Thuận, Đặng Ngọc Tiên, Nguyễn Tấn Đồng.
Big Data course, Lab 04 — Incremental CPG Streaming Pipeline.

**Assigned repository:** [`huggingface/optimum`](https://github.com/huggingface/optimum)
(cloned at build time into `./optimum/`, not committed).

**Stack:** Python 3.12 · Apache Kafka (KRaft, 1 broker) · Kafka Connect +
Neo4j Connector for Kafka 5.1.x · Neo4j 5 · Apache Spark 3.5.1 (Structured
Streaming) · MongoDB 7 · Docker Compose · Jupyter Book 1.x.

---

## Repository layout

```
docker-compose.yml            Kafka + Connect + Neo4j + MongoDB + Spark
requirements.txt              Python dependencies (Python 3.11 recommended)
TROUBLESHOOTING.md            Every failure worth anticipating, with fixes
RUNBOOK.md                    START HERE - full walkthrough with checkpoints
UBUNTU.md                     Ubuntu setup, traps and walkthrough
docker/connect/Dockerfile     Connect image with the Neo4j plugin baked in

src/discovery/                Task 1  clone + file discovery
src/parser/                   Task 2  CPG extraction, stable ids, Kafka producer
    ids.py                      identifier scheme (the core design decision)
    cpg.py                      AST / CFG / DFG / CALL extraction
    events.py                   message envelopes (schema_version, event_time)
    parser_service.py           one-file-at-a-time orchestrator
src/kafka_setup/              Task 3  topic creation
schemas/                      Task 3  four JSON Schema contracts
src/neo4j/                    Task 4  sink configs, constraints, sweep, queries
src/spark/                    Task 5  Structured Streaming job + Mongo checks
src/replay/                   Task 6  idempotency proof + generation sweep
config/architecture.mmd       Task 7  architecture diagram source

jupyter-book/                 The submission (published to GitHub Pages)
    intro.md, task*.ipynb, architecture.ipynb
    images/                     put your screenshots here
scripts/
    setup_ubuntu.sh             one-shot Ubuntu environment setup
    fetch_neo4j_connector.sh    downloads the Neo4j connector JAR
    healthcheck.sh              pre-flight check for the whole stack
    run_pipeline.sh             runs Tasks 1-5 in order, guarded
    reset_replay_target.py      Task 6: idempotent baseline reset / marker add
    replay_evidence.py          Task 6: before/after state snapshots (JSON)
    make_notebooks.py           regenerates the chapters
    commit_by_task.sh           creates an incremental commit history
reports/                      run artifacts (manifest, events, summary)
    evidence/                   Task 6 evidence: before/after/verification
                                JSONs, captured Spark log, connector status
```

---

## Quick start

### Phase 0 — Prepare

**On Ubuntu, use the setup script** — it handles Docker, JDK 17, PEP 668 and the
Python 3.12 Kafka issue in one pass. See `UBUNTU.md` for what it does and why.

```bash
bash scripts/setup_ubuntu.sh
# log out and back in (docker group), then:
source .venv/bin/activate
git clone --depth 1 https://github.com/huggingface/optimum.git
```

On other platforms:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # Python 3.12: requirements-py312.txt
git clone --depth 1 https://github.com/huggingface/optimum.git
```

### Phase 1 — Bring up the stack

The Connect image is built with the Neo4j connector JAR baked in, which removes
the most common failure in this lab (a silent `confluent-hub install` failure
leaving Connect with no plugin).

```bash
bash scripts/fetch_neo4j_connector.sh    # downloads the release JAR
docker compose build connect
docker compose up -d
bash scripts/healthcheck.sh              # every line must say OK
```

Do not continue until this prints `ALL CHECKS PASSED`.

### Phase 2 — Run the pipeline

```bash
# Task 1
python src/discovery/discover_files.py --repo ./optimum --out reports/file_manifest.json

# Task 3 - topics
python src/kafka_setup/create_topics.py --bootstrap localhost:9092

# Task 4 - constraint FIRST, then connectors
docker exec -i neo4j cypher-shell -u neo4j -p password < src/neo4j/constraints.cypher
curl -X POST http://localhost:8083/connectors -H 'Content-Type:application/json' -d @src/neo4j/neo4j-sink-nodes.json
curl -X POST http://localhost:8083/connectors -H 'Content-Type:application/json' -d @src/neo4j/neo4j-sink-edges.json
curl -s http://localhost:8083/connectors/neo4j-sink-cpg-nodes/status   # read tasks[], not just state

# Task 5 - leave this running in a second terminal
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
  src/spark/spark_mongo_stream.py --bootstrap localhost:9092 \
  --mongo-uri mongodb://localhost:27017 --checkpoint file:///tmp/chk/cpg_metadata

# Task 2 - produce to Kafka
python src/parser/parser_service.py --manifest reports/file_manifest.json \
    --repo ./optimum --bootstrap localhost:9092
```

### Phase 3 — Idempotent replay (Task 6, deterministic)

The replay is scripted so it can be re-run any number of times and always
produce the same before/after numbers:

```bash
# 0. reset the target to a clean baseline (idempotent), sync the databases
python scripts/reset_replay_target.py
python src/discovery/discover_files.py --repo ./optimum --out reports/file_manifest.json
python src/parser/parser_service.py --manifest reports/file_manifest.json \
    --repo ./optimum --only optimum/version.py --bootstrap localhost:9092
python src/replay/sweep.py --repo ./optimum --rel-path optimum/version.py

# 1. record the BEFORE state as evidence
python scripts/replay_evidence.py --out reports/evidence/task6_before.json

# 2. apply the lab edit exactly once (idempotent - never double-applies)
python scripts/reset_replay_target.py --add-marker

# 3. refresh the manifest, reprocess that ONE file
python src/discovery/discover_files.py --repo ./optimum --out reports/file_manifest.json
python src/parser/parser_service.py --manifest reports/file_manifest.json \
    --repo ./optimum --only optimum/version.py --bootstrap localhost:9092

# 4. sweep stale elements (relationships first, then nodes) and record AFTER
python src/replay/sweep.py --repo ./optimum --rel-path optimum/version.py \
    --json-out reports/evidence/task6_sweep.json
python scripts/replay_evidence.py --out reports/evidence/task6_after.json
```

The Task 6 notebook runs this same flow and computes a machine-checked verdict
(`reports/evidence/task6_verification.json`); every condition — changed file
hash, changed node/edge counts, stable Mongo `_id`, zero duplicate and stale
nodes *and* edges, a 1-document Spark replay batch — must hold for `PASS`.

### Phase 4 — Build and publish the book

```bash
# 1. edit jupyter-book/_config.yml   (title, author, repository url)
# 2. add screenshots to jupyter-book/images/   (see images/README.md)
# 3. re-run the notebooks so they carry YOUR outputs
jupyter nbconvert --to notebook --execute --inplace jupyter-book/task*.ipynb
# 4. build
jupyter-book build jupyter-book/
# 5. publish
ghp-import -n -p -f jupyter-book/_build/html
```

Then enable GitHub Pages on the `gh-pages` branch and **open the URL in a private
window** to confirm it is publicly reachable.

---

## Offline mode (no Docker required)

Tasks 1, 2 and the source-level half of Task 6 run with no infrastructure at all.
Useful while developing the parser, or if the stack is broken.

```bash
python src/parser/parser_service.py --manifest reports/file_manifest.json \
    --repo ./optimum --offline --outdir reports/events
python src/replay/verify_replay.py ./optimum      # prints RESULT: PASS
```

---

## Results from our run (commit `a6c775e`)

Two states exist by design. **Baseline** is the upstream tree as cloned.
**After replay** is the state once Task 6 has added the `_lab_replay_marker`
function (plus a line-shifting header comment) to `optimum/version.py` and
reprocessed that one file — that is the whole point of the replay
demonstration, and it accounts for every difference between the columns.

| Metric | Baseline | After replay |
|---|---:|---:|
| Python files (discovered / parsed) | 74 / 59 | 74 / 59 |
| Lines of code parsed | 13,725 | 13,731 |
| CPG nodes | 58,817 | 58,831 |
| CPG edges | 73,587 | 73,606 |
| — AST / DFG / CFG / CALL | 57,760 / 8,259 / 4,987 / 2,581 | 57,774 / 8,261 / 4,990 / 2,581 |
| Functions / classes | 522 / 153 | 523 / 153 |
| Parse errors | 0 | 0 |
| Duplicate node / edge ids | 0 / 0 | 0 / 0 |
| Replay verification | — | `RESULT: PASS` |

Machine-readable evidence for the replay lives in `reports/evidence/`
(`task6_before.json`, `task6_after.json`, `task6_verification.json`, the
captured `spark_stream.log`). Counts differ slightly against a newer upstream
commit; the hash is printed by the first cell of Task 1, so every number is
traceable to a specific tree.

---

## Design notes

**Identifiers are structural, not positional.** `node_id = sha1(file_id |
structural_path)`, where `structural_path` is the chain of
`(ast_type, field, sibling_index)` from the module root. Line and column are
properties, never identity. Inserting a line at the top of a file therefore
leaves every identifier unchanged — which is what makes replay idempotent rather
than duplicating the whole file.

**Idempotency is enforced at three layers**: stable ids at the parser, `MERGE`
in the Neo4j sink, upsert on `file_id` in the Spark to Mongo job.

**A fourth mechanism handles deletions.** `MERGE` never touches elements that
stopped being emitted, so an edit that removes a function leaves orphans behind.
Every element carries its file's sha256 as a generation marker, and
`src/replay/sweep.py` deletes elements of that file whose marker is stale. The
sweep is scoped by `file_id` and cannot affect another file's subgraph.

**CFG and DFG are documented approximations.** CFG covers sequential statement
flow plus branch entry; DFG is scope-based reaching definitions without path
sensitivity. The lab asks us to represent these edge categories, not to
reimplement a compiler's dataflow analysis.

---

## Known limitations

- **Single-broker Kafka.** Replication factor is 1 everywhere (including the
  Connect DLQ topics, which must be pinned explicitly — see
  `TROUBLESHOOTING.md` §9). Fine for a lab, not a production posture.
- **CFG/DFG approximations** as described above; CALL edges resolve
  intra-module names, not dynamic dispatch.
- **The sweep is a separate step**, not part of the connector: a Cypher sink
  statement cannot know when a file is "complete", so stale-generation cleanup
  runs after the sink has drained.
- **Producer-level idempotence is disabled** on Python 3.12: `kafka-python-ng`
  rejects `enable_idempotence` (`TROUBLESHOOTING.md` §11). Pipeline
  idempotency does not depend on it — structural ids plus `MERGE`/upsert
  absorb broker-side retries the same way they absorb full replays.
- **Low-RAM machines** (< 8 GB) should start the Spark job after the Neo4j
  ingestion finishes; the checkpoint makes the late start lossless.
