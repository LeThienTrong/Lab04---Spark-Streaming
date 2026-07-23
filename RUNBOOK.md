# RUNBOOK — Lab 04 on Ubuntu, start to finish

The one document to follow. Read `UBUNTU.md` if you want to understand *why* a
step exists; read `TROUBLESHOOTING.md` when something breaks. This file is the
sequence.

Verified on **Ubuntu 24.04.4 LTS**. Also valid for 22.04.

**Total time: 6–9 hours.** Phase 2 (stack) and Phase 5 (notebooks) dominate.

---

## Overview of the phases

| Phase | What | Time | Blocks |
|---|---|---|---|
| 1 | Environment setup | 30 min | everything |
| 2 | Docker stack + Neo4j plugin | 1–3 h | Tasks 4, 5 |
| 3 | Run the pipeline (Tasks 1–5) | 30 min | Task 6 |
| 4 | Idempotent replay (Task 6) | 30 min | — |
| 5 | Notebooks + screenshots | 2–3 h | submission |
| 6 | Publish + submit | 1 h | — |

Do not skip ahead. Each phase ends with a checkpoint you must pass.

---

# PHASE 1 — Environment (30 minutes)

## 1.1 Unpack and set up

```bash
unzip lab04_ubuntu.zip
cd lab04
bash scripts/setup_ubuntu.sh
```

The script installs base packages, JDK 17, Docker Engine from Docker's own
repository, raises `vm.max_map_count`, creates `.venv`, and installs the right
dependency set for your Python version.

## 1.2 Two things the script cannot do for you

**Log out and back in.** You were added to the `docker` group; group membership
is only picked up at login. Short-cut for the current shell only:

```bash
newgrp docker
```

**Set JAVA_HOME.** Add to `~/.bashrc`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Then `source ~/.bashrc`.

## 1.3 Clone the assigned repository

```bash
source .venv/bin/activate
git clone --depth 1 https://github.com/huggingface/optimum.git
```

## 1.4 Create the GitHub repository

It must be **public** — the lab requires the Jupyter Book to be served from a
public repository owned by your team.

```bash
git init
git remote add origin https://github.com/<your-team>/<your-repo>.git
bash scripts/commit_by_task.sh     # creates one commit per task
git branch -M main
git push -u origin main
```

`commit_by_task.sh` exists because the lab grades commit messages that reflect
incremental progress. A single bulk commit loses those marks.

## ✅ Checkpoint 1

```bash
docker run --rm hello-world     # must work WITHOUT sudo
java -version                   # must report 17
python -c "import kafka; print('ok')"
python -c "import jupyter_book; print(jupyter_book.__version__)"   # must be 1.x
```

If `jupyter_book` reports 2.x, fix it now — v2 uses mystmd and silently ignores
`_toc.yml`:

```bash
pip install "jupyter-book==1.0.2"
```

---

# PHASE 2 — Docker stack (1–3 hours; the hard part)

## 2.1 Fetch the Neo4j connector JAR

This is the step that removes the biggest single risk in the lab.

```bash
bash scripts/fetch_neo4j_connector.sh
ls -lh docker/connect/plugins/
```

If the script cannot reach the GitHub API it prints manual instructions. Do
that: open <https://github.com/neo4j/neo4j-kafka-connector/releases>, download
the self-contained `.jar` for **Neo4j Connector for Apache Kafka** — not the
Confluent `.zip` — into `docker/connect/plugins/`.

## 2.2 Build the Connect image

```bash
docker compose build connect
```

The build fails immediately if the plugins directory is empty. That is
deliberate: the alternative is a Connect container that starts happily and then
rejects your connectors with a confusing `ClassNotFound`.

## 2.3 Start everything

```bash
docker compose up -d
docker compose ps          # wait until broker/connect/neo4j/mongodb are healthy
```

Healthchecks take 60–90 seconds. `starting` is normal; `unhealthy` is not.

## 2.4 Verify

```bash
bash scripts/healthcheck.sh
```

## ✅ Checkpoint 2

`healthcheck.sh` must print **ALL CHECKS PASSED**. The line that matters most:

```
  OK    Neo4j connector plugin INSTALLED
```

If that says `MISSING`, go back to 2.1 — nothing downstream can work. Do not
proceed with a partially green stack.

---

# PHASE 3 — Run the pipeline (30 minutes)

## 3.1 Open three terminals

Each needs `source .venv/bin/activate`.

| Terminal | Role |
|---|---|
| **A** | the pipeline |
| **B** | the Spark job (long-running) |
| **C** | database queries |

## 3.2 Start the Spark job in terminal B

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
  src/spark/spark_mongo_stream.py --bootstrap localhost:9092 \
  --mongo-uri mongodb://localhost:27017 --checkpoint file:///tmp/chk/cpg_metadata
```

The first run downloads several hundred MB of jars — this is normal and only
happens once. Leave it running and printing batch logs.

**On 8 GB of RAM**, skip this for now and start it after 3.3 finishes. The job
reads the topic from the beginning, so starting late loses nothing.

## 3.3 Run the pipeline in terminal A

```bash
bash scripts/run_pipeline.sh
```

This executes Tasks 1, 3, 4 and 2 in the correct order with a checkpoint after
each. It stops at the first failure with a diagnosis rather than cascading.

The order is not arbitrary. The Neo4j **constraint is created before the
connectors**: without it, every `MERGE` degrades to a full label scan and
ingesting ~59k nodes goes from minutes to hours.

Running the individual commands by hand instead is fine — they are in
`README.md`, Phase 2.

## ✅ Checkpoint 3

The script prints the verification block. You need:

- Neo4j node count in the tens of thousands
- all four edge types present (AST, DFG, CFG, CALL)
- **duplicate check empty**
- MongoDB count equal to your file count (59 on our commit)

If MongoDB shows 0, Spark was not running. Start it now; the numbers appear
within a minute.

---

# PHASE 4 — Idempotent replay, Task 6 (30 minutes)

This is where the marks are won or lost, so do it deliberately.

## 4.1 Record the "before" state — in terminal C

```bash
docker exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CpgNode {rel_path:'optimum/version.py'}) RETURN count(n) AS before"
docker exec mongodb mongosh --quiet cpg --eval \
  "db.file_metadata.countDocuments({rel_path:'optimum/version.py'})"
```

**Screenshot this.** It is your before/after evidence.

## 4.2 Modify one file

```bash
cat >> optimum/optimum/version.py <<'EOF'

def _lab_replay_marker(x):
    y = x + 1
    return y
EOF
sed -i '1i # lab04 replay edit: shifts every line number below' optimum/optimum/version.py
```

The `sed` line is the point: it shifts every line number in the file. A
line-based identifier scheme would now duplicate the entire file.

## 4.3 Refresh the manifest, reprocess only that file

```bash
python src/discovery/discover_files.py --repo ./optimum --out reports/file_manifest.json
python src/parser/parser_service.py --manifest reports/file_manifest.json \
    --repo ./optimum --only optimum/version.py --bootstrap localhost:9092
```

## 4.4 Sweep the stale nodes

```bash
python src/replay/sweep.py --repo ./optimum --rel-path optimum/version.py --dry-run
python src/replay/sweep.py --repo ./optimum --rel-path optimum/version.py
```

`MERGE` updates elements that are re-emitted. It cannot touch elements that
**stopped** being emitted — an edit that deletes a function leaves orphans. The
sweep deletes elements of that one file whose generation marker (`file_hash`) is
stale.

## 4.5 Verify all three systems

```bash
# Neo4j: file updated, and NO duplicates anywhere
docker exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CpgNode {rel_path:'optimum/version.py'}) RETURN count(n) AS after"
docker exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c WHERE c > 1 RETURN id, c"

# MongoDB: still exactly ONE document, with the new hash
docker exec mongodb mongosh --quiet cpg --eval \
  "db.file_metadata.countDocuments({rel_path:'optimum/version.py'})"
docker exec mongodb mongosh --quiet cpg --eval \
  "printjson(db.file_metadata.findOne({rel_path:'optimum/version.py'},{rel_path:1,file_hash:1,num_ast_nodes:1}))"
```

In terminal B, the Spark log must show the replay batch containing **1**
document, not 59. That is your checkpoint evidence — Spark resumed from the
committed offset and read only the new message.

## ✅ Checkpoint 4

| Check | Required |
|---|---|
| Neo4j duplicate query | **zero rows** |
| Mongo document count for the file | **exactly 1** |
| Spark replay batch | **1 doc**, not 59 |

Also run the source-level proof, which needs no infrastructure:

```bash
python src/replay/verify_replay.py ./optimum      # must print RESULT: PASS
```

---

# PHASE 5 — Notebooks and screenshots (2–3 hours)

**The shipped notebooks contain outputs from a different machine. You must
re-run them.** This is the most commonly lost presentation mark, and it is
obvious to a marker when the outputs do not match the rest of the submission.

## 5.1 Re-run the two offline chapters

```bash
cd jupyter-book
jupyter nbconvert --to notebook --execute --inplace \
    task1_discovery.ipynb task2_parser.ipynb
cd ..
```

## 5.2 Run the infrastructure chapters interactively

```bash
jupyter lab
```

Open `task3_kafka.ipynb`, `task4_neo4j.ipynb`, `task5_spark_mongo.ipynb`,
`task6_replay.ipynb` and run the cells **while the stack is alive**. Run them
one at a time so a cell that fails does not abort the rest.

Task 6's notebook edits `version.py` again — that is fine and idempotent by
design, which is rather the point.

## 5.3 Screenshots

Save into `jupyter-book/images/` with **exactly** these names:

| File | Capture |
|---|---|
| `neo4j_graph.png` | Neo4j Browser, `MATCH (n:CpgNode)-[r:CPG_EDGE]->(m) RETURN n,r,m LIMIT 100` |
| `neo4j_edge_counts.png` | edge breakdown query, table view |
| `mongo_collection.png` | Compass showing `cpg.file_metadata` |
| `spark_ui.png` | <http://localhost:4040>, Structured Streaming tab |
| `connect_status.png` | connector status output |
| `neo4j_after_replay.png` | node count for the edited file, after the sweep |

Neo4j Browser: <http://localhost:7474>, login `neo4j` / `password`.
MongoDB Compass: download the `.deb` from mongodb.com, then
`sudo apt install ./mongodb-compass_*.deb`, connect to `mongodb://localhost:27017`.

The Spark UI only exists **while the job runs**. Capture it before you stop
Spark.

Region screenshot on Ubuntu: `gnome-screenshot -a`, or `PrtSc`.

## 5.4 Rewrite the reflections

Every chapter ends with a reflection, and chapters 3–6 contain a placeholder:

```
*(Replace this with what actually happened on your machine.)*
```

The text supplied there describes failures **we** hit — a DFG bug that produced
zero edges, `MATCH` dropping edges that arrived before their nodes, `append`
duplicating Mongo documents. If your machine failed differently, write what
actually happened. A marker reading a reflection that does not match the
notebook outputs will notice.

Keep notes as you work through Phases 2–4. That is the raw material.

## ✅ Checkpoint 5

```bash
grep -rl "Replace this with what actually happened" jupyter-book/
```

Must return nothing. Every placeholder replaced, every image present.

---

# PHASE 6 — Publish and submit (1 hour)

## 6.1 Fill in your details

Edit `jupyter-book/_config.yml`:

```yaml
title:  "Lab 04 - Incremental CPG Streaming Pipeline (huggingface/optimum)"
author: "Team <NAME> - <member 1>, <member 2>, <member 3>"
repository:
  url: https://github.com/<your-team>/<your-repo>
```

## 6.2 Build

```bash
jupyter-book build jupyter-book/
```

Warnings about `!` in code cells are cosmetic — pygments cannot lex shell
magics. `build succeeded` is what matters. Preview:

```bash
xdg-open jupyter-book/_build/html/index.html
```

## 6.3 Publish to GitHub Pages

```bash
git add -A && git commit -m "docs: executed notebooks with outputs and screenshots"
git push
ghp-import -n -p -f jupyter-book/_build/html
```

In the GitHub repository: **Settings → Pages → Source: branch `gh-pages`, folder
`/ (root)`**. Wait 2–3 minutes for the first deploy.

## 6.4 Verify it is actually public

```bash
firefox --private-window https://<your-team>.github.io/<your-repo>/
```

A private repository serves Pages only to you. Checking in a normal window is
how teams discover this after the deadline.

## ✅ Checkpoint 6

- URL loads in a private window
- all seven chapters appear in the sidebar
- notebook outputs are visible
- screenshots render
- architecture diagram renders

## 6.5 Submit

Moodle: **exactly one text entry** — the root URL of the published site. No zip,
no PDF, no Word.

---

# Quick reference

```bash
# every new terminal
cd lab04 && source .venv/bin/activate

# stack
docker compose up -d
docker compose ps
docker compose logs -f connect
docker compose down          # keeps data (named volumes)
docker compose down -v       # wipes data too

# health
bash scripts/healthcheck.sh

# full pipeline
bash scripts/run_pipeline.sh

# offline, no Docker needed
python src/parser/parser_service.py --manifest reports/file_manifest.json \
    --repo ./optimum --offline --outdir reports/events
python src/replay/verify_replay.py ./optimum

# start completely over
docker compose down -v
rm -rf /tmp/chk/cpg_metadata          # forgetting this is a classic
rm -rf optimum && git clone --depth 1 https://github.com/huggingface/optimum.git
```

---

# The three things most likely to cost you marks

1. **Neo4j plugin never installs** → Task 4 (2 points) impossible. Phase 2 exists
   entirely to prevent this. Do not proceed past Checkpoint 2.
2. **Submitting notebooks with our outputs** → obvious to a marker, and the
   reflections will not match. Phase 5.1–5.2.
3. **Repository left private** → the URL works for you and nobody else.
   Phase 6.4.
