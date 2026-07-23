#!/usr/bin/env bash
# Diagnoses a failed Neo4j sink connector task.
#
#   bash scripts/diagnose_connector.sh [connector-name]
#
# Pulls the task trace, pattern-matches it against known causes, and prints the
# specific fix rather than a generic "check the logs".
set -uo pipefail

NAME="${1:-neo4j-sink-cpg-nodes}"

echo "=================================================================="
echo " Diagnosing: $NAME"
echo "=================================================================="

STATUS=$(curl -s "http://localhost:8083/connectors/$NAME/status" 2>/dev/null)
if [ -z "$STATUS" ]; then
  echo "Connect REST is not reachable. Is the container up?"
  echo "  docker compose ps ; docker logs connect | tail -50"
  exit 1
fi

echo
echo "--- States ---"
echo "$STATUS" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('connector:', d['connector']['state'])
for t in d.get('tasks',[]):
    print(f\"task {t['id']}: {t['state']}\")
"

TRACE=$(echo "$STATUS" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for t in d.get('tasks',[]):
    if t.get('trace'):
        print(t['trace'])
")

if [ -z "$TRACE" ]; then
  echo
  echo "No stack trace on the task. Falling back to container logs:"
  docker logs connect 2>&1 | grep -iE "ERROR|Exception|Caused by" | tail -30
  exit 0
fi

echo
echo "--- Root cause lines ---"
echo "$TRACE" | grep -E "Caused by|Exception|Error" | head -8

echo
echo "=================================================================="
echo " Diagnosis"
echo "=================================================================="

MATCHED=0
diag() { MATCHED=1; echo; echo ">>> $1"; echo; shift; printf '%s\n' "$@"; }

if echo "$TRACE" | grep -qiE "InvalidReplicationFactor|dead letter queue|replication factor of [0-9]+ cannot be reached"; then
  diag "CAUSE: the dead letter queue topic cannot be created." \
    "Kafka Connect defaults the DLQ replication factor to 3, but this lab runs" \
    "a single broker. The task dies during initialisation - before it ever" \
    "reaches Neo4j, so this is NOT a Neo4j problem." \
    "" \
    "FIX: the shipped configs now pin it to 1:" \
    "    \"errors.deadletterqueue.topic.replication.factor\": \"1\"" \
    "" \
    "Connect does not reload a changed file, so re-register:" \
    "    bash scripts/reload_connectors.sh" \
    "" \
    "Note: the same default bites any Connect topic setting on a single-broker" \
    "cluster. The worker's own internal topics are already pinned to 1 in" \
    "docker-compose.yml (CONNECT_*_STORAGE_REPLICATION_FACTOR)."
fi

if echo "$TRACE" | grep -qiE "routing|Unable to retrieve routing table|ServiceUnavailable.*routing"; then
  diag "CAUSE: the neo4j:// URI scheme requires a routing table (cluster)." \
    "A single Neo4j Community instance does not serve one." \
    "" \
    "FIX: use bolt:// instead. Already corrected in the shipped configs:" \
    "    \"neo4j.uri\": \"bolt://neo4j:7687\"" \
    "" \
    "Apply it:" \
    "    bash scripts/reload_connectors.sh"
fi

if echo "$TRACE" | grep -qiE "authentication|Unauthorized|AuthenticationRateLimit|client is unauthorized"; then
  diag "CAUSE: Neo4j rejected the credentials." \
    "" \
    "FIX: confirm the password matches NEO4J_AUTH in docker-compose.yml:" \
    "    docker exec neo4j cypher-shell -u neo4j -p password 'RETURN 1'" \
    "If that fails, the database kept an old password in its volume:" \
    "    docker compose down -v && docker compose up -d"
fi

if echo "$TRACE" | grep -qiE "Variable .__value. not defined|Unknown variable.*__value|__value"; then
  diag "CAUSE: the installed connector does not bind __value." \
    "The __header/__key/__value variables were introduced in connector 5.1.0." \
    "Older versions expose only 'event'." \
    "" \
    "FIX A (preferred): install connector 5.1.0 or newer:" \
    "    bash scripts/fetch_neo4j_connector.sh" \
    "    docker compose build connect && docker compose up -d connect" \
    "" \
    "FIX B: use the legacy configs that reference 'event' instead:" \
    "    bash scripts/reload_connectors.sh legacy"
fi

if echo "$TRACE" | grep -qiE "ConnectException.*Connection refused|ServiceUnavailable.*Connection refused|UnknownHost"; then
  diag "CAUSE: Connect cannot reach Neo4j over the network." \
    "" \
    "Inside Docker the host is 'neo4j', not 'localhost' - the connector runs" \
    "in the Connect container, not on your machine." \
    "" \
    "CHECK:" \
    "    docker exec connect getent hosts neo4j" \
    "    docker exec connect curl -s -o /dev/null -w '%{http_code}' http://neo4j:7474"
fi

if echo "$TRACE" | grep -qiE "SyntaxError|Invalid input|Neo.ClientError.Statement"; then
  diag "CAUSE: the Cypher statement is rejected by Neo4j." \
    "" \
    "The connector wraps your statement as:" \
    "    UNWIND \$events AS message" \
    "    WITH message.value AS event, ... , message.value AS __value" \
    "    <your statement>" \
    "" \
    "Test the full wrapped form by hand to see the real error:" \
    "    bash scripts/test_cypher.sh"
fi

if echo "$TRACE" | grep -qiE "ConfigException|Missing required configuration|Unknown configuration"; then
  diag "CAUSE: a configuration key is wrong or missing." \
    "" \
    "Print the exact rejected key:" \
    "    docker logs connect 2>&1 | grep -i configexception | tail -5" \
    "" \
    "Note: config keys are version sensitive. Confirm the installed version:" \
    "    ls docker/connect/plugins/"
fi

if [ "$MATCHED" -eq 0 ]; then
  echo
  echo "No known pattern matched. Full trace follows - the first 'Caused by'"
  echo "line is usually the real error."
  echo
  echo "$TRACE" | head -40
fi

echo
echo "--- Also worth checking: the dead letter queue ---"
# Read the DLQ topic from the connector's own config rather than guessing it
# from the connector name.
DLQ=$(curl -s "http://localhost:8083/connectors/$NAME/config" 2>/dev/null \
      | python3 -c "
import json,sys
try:
    print(json.load(sys.stdin).get('errors.deadletterqueue.topic.name',''))
except Exception:
    print('')
")
if [ -n "$DLQ" ]; then
  echo "docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 \\"
  echo "    --topic $DLQ --from-beginning --max-messages 3 --timeout-ms 8000"
else
  echo "  (no dead letter queue configured for this connector)"
fi
