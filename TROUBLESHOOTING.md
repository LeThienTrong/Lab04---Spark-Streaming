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

---

## 9. Neo4j sink connector task FAILED

`run_pipeline.sh` stops with:

```
FAILED: neo4j-sink-cpg-nodes has a failed task.
```

The connector registered (so the config parsed) but the task died at runtime.
Start here:

```bash
bash scripts/diagnose_connector.sh neo4j-sink-cpg-nodes
```

It pulls the task trace and matches it against the causes below. If you want to
isolate whether the problem is the Cypher or the connection:

```bash
bash scripts/test_cypher.sh
```

That runs the exact statement the connector generates, by hand, against a fake
event.

### Cause A — dead letter queue replication factor (CONFIRMED, most common)

Symptom:

```
ConnectException: Could not initialize dead letter queue with topic=cpg.nodes.dlq
Caused by: InvalidReplicationFactorException: The target replication factor of 3
cannot be reached because only 1 broker(s) are registered.
```

Kafka Connect creates the DLQ topic with **replication factor 3 by default**.
This lab runs a single broker, so the topic cannot be created and the task dies
during initialisation — before it ever contacts Neo4j. Despite appearing under
a "neo4j-sink" connector, this is not a Neo4j problem at all.

Fix — pin the factor to 1 (already present in the shipped configs):

```json
"errors.deadletterqueue.topic.replication.factor": "1"
```

Kafka Connect does not reload a changed config file on its own; the connector
must be deleted and re-registered:

```bash
bash scripts/reload_connectors.sh
```

The same default catches every Connect-managed topic on a single-broker
cluster. The worker's own internal topics are already pinned in
`docker-compose.yml` via `CONNECT_*_STORAGE_REPLICATION_FACTOR`, and the
pipeline topics via `create_topics.py --replication 1`. The DLQ was the one
that slipped through, because Connect creates it implicitly.

### Cause B — connection scheme

Symptom: `Unable to retrieve routing table`, `ServiceUnavailableException`.

The shipped configs use `bolt://neo4j:7687`. For a single Community instance
`neo4j://` normally also works (the server returns a routing table containing
only itself), so this is rarely the cause — but `bolt://` avoids the routing
round-trip and is the more precise choice for a non-clustered target.

### Cause C — `__value` is not bound

Symptom: `Variable '__value' not defined`.

The `__header` / `__key` / `__value` bindings were introduced in connector
**5.1.0**. Older versions expose only `event`. Upgrade the connector
(`bash scripts/fetch_neo4j_connector.sh`, then rebuild the connect image); as a
last resort the sink statements can be rewritten against `event` — the two
bindings carry the same payload, only the variable name differs.

### Cause D — cannot reach Neo4j

Symptom: `Connection refused`, `UnknownHostException`.

The connector runs **inside the Connect container**, so the host is `neo4j`,
not `localhost`:

```bash
docker exec connect getent hosts neo4j
```

### Cause E — authentication

Symptom: `The client is unauthorized due to authentication failure`.

```bash
docker exec neo4j cypher-shell -u neo4j -p password "RETURN 1"
```

If that fails, a previous run left an old password in the named volume:

```bash
docker compose down -v && docker compose up -d
```

### Cause F — Cypher syntax

Symptom: `Neo.ClientError.Statement.SyntaxError`.

The connector wraps your statement:

```
UNWIND $events AS message
WITH message.value AS event, message.key AS __key, message.value AS __value
<your statement>
```

Your statement is appended after a `WITH`, so it must be a valid continuation.
Starting it with `WITH __value AS v` (as the shipped configs do) is the safest
form — it works whether or not the preceding clause projected what you expect.

### After any fix

```bash
bash scripts/reload_connectors.sh
bash scripts/run_pipeline.sh
```

### Checking what was rejected

With `errors.tolerance: all`, individual bad messages go to a dead letter queue
instead of killing the task. If the task is running but Neo4j stays empty, look
there:

```bash
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic cpg.nodes.dlq --from-beginning --max-messages 3 --timeout-ms 8000
```

---

## 10. Spark dies at startup with `org.apache.hadoop.ipc.Client ... Connection refused`

Symptom — the stack trace ends in Hadoop RPC, not Kafka or Mongo:

```
at org.apache.spark.sql.streaming.DataStreamWriter.start(...)
Caused by: java.net.ConnectException: Connection refused
    at org.apache.hadoop.ipc.Client$Connection.setupConnection(Client.java:711)
    at org.apache.hadoop.ipc.Client.call(Client.java:1502)
```

**Cause.** Spark resolves a bare checkpoint path such as `/tmp/chk/cpg_metadata`
against `fs.defaultFS`. If this machine has Hadoop configured — very likely on a
Big Data course machine, via `HADOOP_CONF_DIR`, `core-site.xml`, or
`spark-defaults.conf` — that default is `hdfs://...`, so Spark tries to contact
a NameNode that is not running. Nothing in this pipeline uses HDFS; the path is
simply being resolved against the wrong filesystem.

**Fix.** Pin the checkpoint to the local disk with an explicit scheme:

```bash
--checkpoint file:///tmp/chk/cpg_metadata
```

`spark_mongo_stream.py` now does this automatically: a bare path is normalised
to a `file://` URI, and `fs.defaultFS` is overridden to `file:///` unless you
pass `--use-hadoop-fs`.

**Verify** the job printed its resolved location on startup:

```
checkpoint location : file:///tmp/chk/cpg_metadata
```

and that the directory really appears on local disk once a batch commits:

```bash
ls -R /tmp/chk/cpg_metadata      # commits/ metadata/ offsets/ sources/
```

**If you genuinely want HDFS** (not required for this lab), start it first and
pass `--use-hadoop-fs`.

### Related: check your Hadoop environment

```bash
echo "$HADOOP_CONF_DIR"
grep -r fs.defaultFS "$HADOOP_CONF_DIR" 2>/dev/null
cat "$SPARK_HOME/conf/spark-defaults.conf" 2>/dev/null | grep -i defaultFS
```

Anything pointing at `hdfs://` explains the failure.

---

## 11. Parser crashes at startup: `Unrecognized configs: {'enable_idempotence': True}`

```
File ".../kafka/producer/kafka.py", line 356, in __init__
    assert not configs, f'Unrecognized configs: {configs}'
AssertionError: Unrecognized configs: {'enable_idempotence': True}
```

**Cause.** On Python 3.12 this lab uses `kafka-python-ng` (see item 1). Its
`KafkaProducer` does not accept the `enable_idempotence` flag that upstream
`kafka-python` 2.1+ supports, and it asserts on any unknown config instead of
ignoring it.

**Fix.** `src/parser/parser_service.py` now tries the flag and falls back to a
producer with only `acks="all"` when the client rejects it. This is safe: the
pipeline's idempotency comes from structural ids plus `MERGE`/upsert in the
sinks, not from producer-level idempotence — a broker-side retry duplicate is
absorbed exactly like a full replay is.
