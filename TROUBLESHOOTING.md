# Troubleshooting

The parsing side of this lab runs anywhere. Almost all real difficulty is in the
Docker stack. These are the failures worth anticipating, in rough order of how
often they bite.

---

## 1. `kafka-python` fails to import on Python 3.12

```
ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
```

`kafka-python==2.0.2` predates Python 3.12. Options, best first:

```bash
# Option A - use Python 3.11 (recommended)
python3.11 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

# Option B - stay on 3.12, use the maintained fork
pip uninstall kafka-python && pip install kafka-python-ng
```

The import name stays `kafka` either way, so no source change is needed.

---

## 2. The Neo4j connector plugin is not installed

Symptom: registering the connector returns

```
Failed to find any class that implements Connector and which name matches
org.neo4j.connectors.kafka.sink.Neo4jConnector
```

Check what Connect actually loaded:

```bash
curl -s http://localhost:8083/connector-plugins | grep -i neo4j
```

Empty output means the `confluent-hub install` step in `docker-compose.yml`
failed silently (usually a network restriction or a Confluent Hub rate limit).
Install the plugin manually:

```bash
# 1. Download the connector jar from the Neo4j release page onto the host
#    (look for "neo4j-kafka-connector-<version>.jar", the "kafka-connect" artifact)
# 2. Copy it into the container and restart
docker cp neo4j-kafka-connector-*.jar connect:/usr/share/confluent-hub-components/
docker restart connect
sleep 45
curl -s http://localhost:8083/connector-plugins | grep -i neo4j
```

A persistent alternative is to bake it into a small custom image:

```dockerfile
FROM confluentinc/cp-kafka-connect:7.6.1
COPY neo4j-kafka-connector-*.jar /usr/share/confluent-hub-components/
```

---

## 3. Connector reports RUNNING but nothing reaches Neo4j

A connector can report `RUNNING` at the top level while an individual **task**
has failed. Always read the `tasks` array:

```bash
curl -s http://localhost:8083/connectors/neo4j-sink-cpg-nodes/status | python -m json.tool
```

Common causes:

| Cause | Fix |
|---|---|
| Wrong bolt host | Inside Docker the host is `neo4j`, not `localhost`. The connector runs *in* the Connect container. |
| Value converter mismatch | We produce plain JSON, so `value.converter.schemas.enable` must be `false`. |
| Cypher syntax error | Check `docker logs connect` for the rejected statement. |
| Messages landing in the DLQ | `docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic cpg.nodes.dlq --from-beginning --max-messages 5` |

---

## 4. `spark-submit` fails with ClassNotFoundException

The `--packages` coordinates encode three things that must all match your Spark
installation: the Scala version (`_2.12` vs `_2.13`), the Spark version, and the
connector version.

```bash
# Check first
python -c "import pyspark; print(pyspark.__version__)"
spark-submit --version 2>&1 | grep -i scala
```

Then align the coordinates. For Spark 3.5.x on Scala 2.12:

```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
org.mongodb.spark:mongo-spark-connector_2.12:10.4.0
```

The first `spark-submit` downloads several hundred MB of jars. If your network
blocks Maven Central the job hangs with no useful message — pre-download with
`--packages` on a machine that has access, or use `--jars` with local files.

---

## 5. Spark cannot reach Kafka from inside Docker

If you run `spark-submit` on the **host**, use `localhost:9092`. If you run it
**inside a container**, use `broker:29092`. The compose file advertises both
listeners for exactly this reason. Mixing them produces a connection timeout
that looks like a broker failure.

---

## 6. Neo4j ingestion is extremely slow

If you started the connectors before creating the uniqueness constraint, every
`MERGE` degrades to a full label scan. With ~59k nodes this turns minutes into
hours.

```bash
# Verify the constraint exists
docker exec neo4j cypher-shell -u neo4j -p password "SHOW CONSTRAINTS"
```

If it is missing: stop the connectors, create it, wipe the graph, restart.

```bash
curl -X DELETE http://localhost:8083/connectors/neo4j-sink-cpg-nodes
curl -X DELETE http://localhost:8083/connectors/neo4j-sink-cpg-edges
docker exec -i neo4j cypher-shell -u neo4j -p password < src/neo4j/constraints.cypher
docker exec neo4j cypher-shell -u neo4j -p password "MATCH (n) DETACH DELETE n"
# then re-register the connectors and re-run the parser
```

---

## 7. Replay produces duplicates anyway

Work through the three layers in order — the failure is in exactly one of them:

```bash
# Layer 1: are the parser's ids stable?
python src/replay/verify_replay.py ./optimum      # must print RESULT: PASS

# Layer 2: is the sink MERGEing?
grep -o 'MERGE' src/neo4j/neo4j-sink-nodes.json   # must appear, CREATE must not

# Layer 3: is Mongo upserting?
grep operationType src/spark/spark_mongo_stream.py  # must be "update"
```

If all three pass but the node count for the edited file still grows, you have
**stale nodes**, not duplicates — elements removed by your edit that are no
longer emitted. That is what `src/replay/sweep.py` is for. Confirm with:

```bash
python src/replay/sweep.py --repo ./optimum --rel-path optimum/version.py --dry-run
```

---

## 8. Starting completely over

```bash
docker compose down -v          # -v also drops the volumes
rm -rf /tmp/chk/cpg_metadata    # Spark checkpoint, otherwise offsets persist
rm -rf optimum && git clone --depth 1 https://github.com/huggingface/optimum.git
docker compose up -d
```

Forgetting to delete the checkpoint is a classic: Spark resumes from the old
offsets against a brand-new Kafka cluster whose offsets restart at zero, and the
job silently processes nothing.
