#!/usr/bin/env bash
# Runs the pipeline (Tasks 1-5) in the correct order, stopping at the first
# failure with a diagnosis instead of cascading into confusing errors later.
#
#   bash scripts/run_pipeline.sh
#
# The Spark job is NOT started here - it is long-running and belongs in its own
# terminal. The script tells you when to start it.
set -uo pipefail

die()  { echo; echo "FAILED: $*"; echo "See TROUBLESHOOTING.md"; exit 1; }
step() { echo; echo "================ $* ================"; }
ok()   { echo "  -> $*"; }

[ -n "${VIRTUAL_ENV:-}" ] || die "virtualenv not active. Run: source .venv/bin/activate"
[ -d optimum ] || die "optimum/ not found. Run: git clone --depth 1 https://github.com/huggingface/optimum.git"

step "Pre-flight"
bash scripts/healthcheck.sh || die "healthcheck did not pass"

step "Task 1 - file discovery"
python src/discovery/discover_files.py --repo ./optimum --out reports/file_manifest.json \
  || die "discovery failed"
COUNT=$(python -c "import json;print(json.load(open('reports/file_manifest.json'))['included_count'])")
[ "$COUNT" -gt 0 ] || die "manifest contains no files"
ok "$COUNT source files in the manifest"

step "Task 3 - Kafka topics"
python src/kafka_setup/create_topics.py --bootstrap localhost:9092 || die "topic creation failed"
for t in cpg.nodes cpg.edges cpg.metadata cpg.errors; do
  docker exec broker kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep -qx "$t" || die "topic $t was not created"
done
ok "all four topics exist"

step "Task 4a - Neo4j constraint (MUST run before the connectors)"
docker exec -i neo4j cypher-shell -u neo4j -p password < src/neo4j/constraints.cypher \
  || die "could not create the constraint"
docker exec neo4j cypher-shell -u neo4j -p password "SHOW CONSTRAINTS" 2>/dev/null \
  | grep -qi cpg_node_id || die "constraint cpg_node_id is missing"
ok "uniqueness constraint in place"

step "Task 4b - register the sink connectors"
for cfg in src/neo4j/neo4j-sink-nodes.json src/neo4j/neo4j-sink-edges.json; do
  NAME=$(python -c "import json,sys;print(json.load(open('$cfg'))['name'])")
  if curl -sf "http://localhost:8083/connectors/$NAME" >/dev/null 2>&1; then
    ok "$NAME already registered, skipping"
  else
    RESP=$(curl -s -X POST http://localhost:8083/connectors \
             -H 'Content-Type:application/json' -d @"$cfg")
    echo "$RESP" | grep -q '"error_code"' && { echo "$RESP"; die "$NAME rejected"; }
    ok "$NAME registered"
  fi
done

echo "  waiting 15s for tasks to start..."
sleep 15
for NAME in neo4j-sink-cpg-nodes neo4j-sink-cpg-edges; do
  STATE=$(curl -s "http://localhost:8083/connectors/$NAME/status" \
          | python -c "
import json,sys
d=json.load(sys.stdin)
ts=[t['state'] for t in d.get('tasks',[])]
print(d['connector']['state'], '|tasks:', ','.join(ts) if ts else 'NONE')
")
  echo "  $NAME: $STATE"
  echo "$STATE" | grep -q FAILED && die "$NAME has a failed task. Inspect: curl -s http://localhost:8083/connectors/$NAME/status | python -m json.tool"
  echo "$STATE" | grep -q "tasks: NONE" && die "$NAME has no running tasks"
done
ok "both connectors running"

step "REMINDER - start the Spark job now, in a SECOND terminal"
cat <<'EOF'

    source .venv/bin/activate
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
      src/spark/spark_mongo_stream.py --bootstrap localhost:9092 \
      --mongo-uri mongodb://localhost:27017 --checkpoint /tmp/chk/cpg_metadata

  On a machine with 8 GB RAM you may instead start it AFTER this script
  finishes - it reads the topic from the beginning, so nothing is lost.

EOF
read -r -p "Press Enter when Spark is running (or to continue without it)... " _

step "Task 2 - run the parser against Kafka"
python src/parser/parser_service.py --manifest reports/file_manifest.json \
    --repo ./optimum --bootstrap localhost:9092 || die "parser failed"

step "Waiting 30s for the sinks to drain"
sleep 30

step "Verification"
echo "-- Neo4j node count --"
docker exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CpgNode) RETURN count(n) AS nodes"
echo "-- Neo4j edges by type --"
docker exec neo4j cypher-shell -u neo4j -p password \
  "MATCH ()-[r:CPG_EDGE]->() RETURN r.type AS type, count(*) AS n ORDER BY n DESC"
echo "-- duplicate check (MUST be empty) --"
docker exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c WHERE c > 1 RETURN id, c"
echo "-- MongoDB document count --"
docker exec mongodb mongosh --quiet cpg --eval "db.file_metadata.countDocuments()"

cat <<'EOF'

================ Pipeline complete ================

Expected: Neo4j node count in the tens of thousands, four edge types present,
the duplicate check EMPTY, and the Mongo count equal to your file count.

If Mongo shows 0, the Spark job was not running. Start it and the numbers
appear within a minute - the checkpoint makes this safe to do late.

Next: Task 6 (idempotent replay). See RUNBOOK.md, Phase 4.
EOF
