#!/usr/bin/env bash
# Pre-flight check. Run AFTER `docker compose up -d`, BEFORE the pipeline.
# Every line must say OK.
set -u
pass() { echo "  OK    $1"; }
fail() { echo "  FAIL  $1"; FAILED=1; }
FAILED=0
USER_NAME="${USER:-$(id -un)}"

echo "== 0. Host environment (Ubuntu) =="
if groups "$USER_NAME" | grep -qw docker; then
  pass "$USER_NAME is in the docker group"
else
  fail "$USER_NAME not in docker group -> sudo usermod -aG docker $USER_NAME, then log out/in"
fi

if [ -n "${VIRTUAL_ENV:-}" ]; then
  pass "virtualenv active: $(basename "$VIRTUAL_ENV")"
else
  fail "no virtualenv active -> source .venv/bin/activate"
fi

JV=$(java -version 2>&1 | head -1 | grep -oP '"\K[0-9]+' || echo 0)
if [ "$JV" = "17" ] || [ "$JV" = "11" ] || [ "$JV" = "8" ]; then
  pass "Java $JV (supported by Spark 3.5)"
elif [ "$JV" = "0" ]; then
  fail "java not found -> sudo apt install openjdk-17-jdk"
else
  echo "  warn  Java $JV is outside Spark 3.5's supported range (8/11/17)."
  echo "        Basic jobs usually run, but if spark-submit throws"
  echo "        IllegalAccessError, switch: export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
fi

MMC=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
[ "$MMC" -ge 262144 ] && pass "vm.max_map_count=$MMC" \
  || fail "vm.max_map_count=$MMC too low -> sudo sysctl -w vm.max_map_count=262144"

FREE_GB=$(( $(free -m | awk '/^Mem:/{print $7}') / 1024 ))
[ "$FREE_GB" -ge 4 ] && pass "${FREE_GB} GB RAM available" \
  || echo "  warn  only ${FREE_GB} GB RAM available; run Spark after Neo4j ingestion finishes"

echo "== 1. Containers running =="
for c in broker connect neo4j mongodb; do
  if [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" = "true" ]; then
    pass "$c is up"
  else
    fail "$c is NOT running -> docker compose up -d $c ; docker logs $c"
  fi
done

echo "== 2. Kafka reachable =="
if docker exec broker kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  pass "broker answers"
  echo "  topics: $(docker exec broker kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | tr '\n' ' ')"
else
  fail "broker not answering -> docker logs broker"
fi

echo "== 3. Kafka Connect + Neo4j plugin =="
if curl -sf http://localhost:8083/connectors >/dev/null; then
  pass "Connect REST is up"
  if curl -s http://localhost:8083/connector-plugins | grep -qi "neo4j"; then
    pass "Neo4j connector plugin INSTALLED"
  else
    fail "Neo4j plugin MISSING -> bash scripts/fetch_neo4j_connector.sh && docker compose build connect && docker compose up -d connect"
  fi
else
  fail "Connect REST down -> docker logs connect"
fi

echo "== 4. Neo4j reachable =="
docker exec neo4j cypher-shell -u neo4j -p password "RETURN 1" >/dev/null 2>&1 \
  && pass "Neo4j bolt answers" || fail "Neo4j not answering -> docker logs neo4j"

echo "== 5. MongoDB reachable =="
docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1 \
  && pass "MongoDB answers" || fail "MongoDB not answering -> docker logs mongodb"

echo "== 6. Python dependencies =="
python -c "import kafka" 2>/dev/null && pass "kafka client importable" \
  || fail "kafka client broken -> pip install -r requirements-py312.txt"
python -c "import neo4j" 2>/dev/null && pass "neo4j driver importable" || fail "pip install neo4j"
python -c "import pyspark" 2>/dev/null && pass "pyspark importable" || fail "pip install pyspark==3.5.1"
JB=$(python -c "import jupyter_book; print(jupyter_book.__version__)" 2>/dev/null || echo none)
case "$JB" in
  1.*) pass "jupyter-book $JB" ;;
  none) fail "jupyter-book missing -> pip install jupyter-book==1.0.2" ;;
  *)   fail "jupyter-book $JB is v2 (uses mystmd, ignores _toc.yml) -> pip install 'jupyter-book==1.0.2'" ;;
esac

echo
if [ "$FAILED" -eq 0 ]; then
  echo "ALL CHECKS PASSED - safe to run the pipeline."
else
  echo "SOME CHECKS FAILED - fix them before running the pipeline."
  exit 1
fi
