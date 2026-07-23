#!/usr/bin/env bash
# Runs the sink's Cypher by hand, in exactly the wrapped form the connector
# generates, against a fake event. This isolates a Cypher problem from a
# connectivity or configuration problem in about ten seconds.
set -uo pipefail

echo "=== 1. Can Connect reach Neo4j at all? ==="
docker exec connect getent hosts neo4j || echo "  DNS for 'neo4j' failed inside the connect container"

echo
echo "=== 2. Do the credentials work? ==="
docker exec neo4j cypher-shell -u neo4j -p password "RETURN 1 AS ok" \
  || { echo "  Authentication failed - see diagnose_connector.sh"; exit 1; }

echo
echo "=== 3. Is the uniqueness constraint present? ==="
docker exec neo4j cypher-shell -u neo4j -p password "SHOW CONSTRAINTS" | grep -i cpg_node_id \
  || echo "  MISSING - run: docker exec -i neo4j cypher-shell -u neo4j -p password < src/neo4j/constraints.cypher"

echo
echo "=== 4. Node statement, wrapped exactly as the connector wraps it ==="
docker exec -i neo4j cypher-shell -u neo4j -p password <<'CYPHER'
UNWIND [{value: {
  file_id: "testfile", rel_path: "test/demo.py", file_hash: "deadbeef",
  node: {id: "TESTNODE1", type: "FunctionDef", label: "demo", code: "def demo():",
         start_line: 1, end_line: 3, scope_id: "TESTSCOPE"}
}}] AS message
WITH message.value AS event, message.value AS __value
WITH __value AS v
MERGE (n:CpgNode {id: v.node.id})
SET n.type = v.node.type, n.label = v.node.label, n.code = v.node.code,
    n.start_line = v.node.start_line, n.end_line = v.node.end_line,
    n.scope_id = v.node.scope_id, n.file_id = v.file_id,
    n.rel_path = v.rel_path, n.file_hash = v.file_hash
RETURN n.id AS created;
CYPHER

echo
echo "=== 5. Edge statement ==="
docker exec -i neo4j cypher-shell -u neo4j -p password <<'CYPHER'
UNWIND [{value: {
  file_id: "testfile", file_hash: "deadbeef",
  edge: {id: "TESTEDGE1", type: "AST", src_id: "TESTNODE1", dst_id: "TESTNODE2"}
}}] AS message
WITH message.value AS event, message.value AS __value
WITH __value AS v
MERGE (s:CpgNode {id: v.edge.src_id})
MERGE (d:CpgNode {id: v.edge.dst_id})
MERGE (s)-[r:CPG_EDGE {id: v.edge.id}]->(d)
SET r.type = v.edge.type, r.file_id = v.file_id, r.file_hash = v.file_hash
RETURN r.id AS created;
CYPHER

echo
echo "=== 6. Idempotency: run the node statement again, count must stay 1 ==="
docker exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CpgNode {id:'TESTNODE1'}) RETURN count(n) AS should_be_1"

echo
echo "=== 7. Clean up the test data ==="
docker exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:CpgNode) WHERE n.id STARTS WITH 'TESTNODE' DETACH DELETE n"

cat <<'EOF'

If steps 4 and 5 succeeded, the Cypher is fine and the failure is in the
connector configuration or its connection - run:
    bash scripts/diagnose_connector.sh

If they failed, the error printed above is the real one.
EOF
