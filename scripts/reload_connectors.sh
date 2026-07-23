#!/usr/bin/env bash
# Deletes and re-registers both Neo4j sink connectors, so a config change takes
# effect. Kafka Connect does not reload a config on its own.
#
#   bash scripts/reload_connectors.sh            # current configs (__value)
#   bash scripts/reload_connectors.sh legacy     # for connectors older than 5.1.0
set -uo pipefail

DIR="src/neo4j"
[ "${1:-}" = "legacy" ] && DIR="src/neo4j/legacy" && echo "Using LEGACY configs (event binding)"

for NAME in neo4j-sink-cpg-nodes neo4j-sink-cpg-edges; do
  curl -s -X DELETE "http://localhost:8083/connectors/$NAME" >/dev/null 2>&1 \
    && echo "  deleted $NAME" || echo "  $NAME was not registered"
done
sleep 3

for CFG in "$DIR/neo4j-sink-nodes.json" "$DIR/neo4j-sink-edges.json"; do
  NAME=$(python3 -c "import json;print(json.load(open('$CFG'))['name'])")
  RESP=$(curl -s -X POST http://localhost:8083/connectors \
           -H 'Content-Type:application/json' -d @"$CFG")
  if echo "$RESP" | grep -q '"error_code"'; then
    echo "  REJECTED $NAME:"
    echo "$RESP" | python3 -m json.tool | head -20
    exit 1
  fi
  echo "  registered $NAME"
done

echo
echo "Waiting 15s for tasks to start..."
sleep 15
for NAME in neo4j-sink-cpg-nodes neo4j-sink-cpg-edges; do
  curl -s "http://localhost:8083/connectors/$NAME/status" | python3 -c "
import json,sys
d=json.load(sys.stdin)
ts=d.get('tasks',[])
states=[t['state'] for t in ts] or ['NO TASKS']
print(f\"  {d['name']}: connector={d['connector']['state']} tasks={','.join(states)}\")
if any(t['state']=='FAILED' for t in ts):
    print('    -> run: bash scripts/diagnose_connector.sh', d['name'])
"
done
