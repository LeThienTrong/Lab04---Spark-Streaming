"""
Task 5 - Source metadata ingestion into MongoDB via Spark Structured Streaming.
==============================================================================

Reads the `cpg.metadata` topic from Kafka and writes each file's metadata
document into MongoDB through the MongoDB Spark Connector (v10.x).

Idempotency (Task 6 requirement):
  * write operationType = "update" with idFieldList = "file_id"  -> UPSERT.
    Reprocessing a file updates its single document in place; it never inserts
    a duplicate.
  * checkpointLocation -> on restart Spark resumes from the last committed Kafka
    offset, so already-processed files for unchanged offsets are skipped.

Submit (packages pin the connector + Kafka source; adjust to your Spark line):
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
    spark_mongo_stream.py \
    --bootstrap localhost:9092 \
    --mongo-uri mongodb://localhost:27017 \
    --checkpoint /tmp/chk/cpg_metadata
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, IntegerType, MapType, StringType,
                               StructField, StructType)

METADATA_SCHEMA = StructType([
    StructField("schema_version", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("file_id", StringType()),
    StructField("rel_path", StringType()),
    StructField("file_hash", StringType()),
    StructField("loc", IntegerType()),
    StructField("num_ast_nodes", IntegerType()),
    StructField("num_edges", IntegerType()),
    StructField("edge_counts", MapType(StringType(), IntegerType())),
    StructField("num_functions", IntegerType()),
    StructField("num_classes", IntegerType()),
    StructField("imports", ArrayType(StringType())),
])


def build_stream(spark, bootstrap: str):
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", "cpg.metadata")
           .option("startingOffsets", "earliest")
           .load())

    return (raw
            .select(F.col("value").cast("string").alias("json"))
            .select(F.from_json("json", METADATA_SCHEMA).alias("m"))
            .select("m.*")
            .filter(F.col("file_id").isNotNull()))


def normalize_checkpoint(path: str) -> str:
    """Force a local-filesystem URI unless the user asked for a real scheme.

    Spark resolves a bare path like /tmp/chk against `fs.defaultFS`. On a
    machine with Hadoop configured (very common on a Big Data course machine),
    that is hdfs://..., so Spark tries to reach a NameNode and dies with

        org.apache.hadoop.ipc.Client ... java.net.ConnectException: Connection refused

    even though nothing in this pipeline uses HDFS. Prefixing the path with
    file:// pins it to the local disk regardless of the Hadoop configuration.
    """
    if "://" in path:
        return path
    return "file://" + os.path.abspath(path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--database", default="cpg")
    ap.add_argument("--collection", default="file_metadata")
    ap.add_argument("--checkpoint", default="/tmp/chk/cpg_metadata")
    ap.add_argument("--use-hadoop-fs", action="store_true",
                    help="do not override fs.defaultFS (only if you really want HDFS)")
    args = ap.parse_args()

    checkpoint = normalize_checkpoint(args.checkpoint)

    builder = (SparkSession.builder
               .appName("cpg-metadata-to-mongo")
               .config("spark.mongodb.write.connection.uri", args.mongo_uri))
    if not args.use_hadoop_fs:
        # Ignore any HDFS configuration present on this machine.
        builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"checkpoint location : {checkpoint}")
    print(f"kafka bootstrap     : {args.bootstrap}")
    print(f"mongo target        : {args.database}.{args.collection}")

    meta = build_stream(spark, args.bootstrap)

    def upsert_batch(batch_df, batch_id: int):
        # foreachBatch gives a static DataFrame we can write with UPSERT
        # semantics: operationType=update + idFieldList=file_id.
        (batch_df.write
         .format("mongodb")
         .mode("append")
         .option("connection.uri", args.mongo_uri)
         .option("database", args.database)
         .option("collection", args.collection)
         .option("operationType", "update")
         .option("idFieldList", "file_id")
         .option("upsertDocument", "true")
         .save())
        print(f"batch {batch_id}: upserted {batch_df.count()} metadata docs")

    query = (meta.writeStream
             .foreachBatch(upsert_batch)
             .option("checkpointLocation", checkpoint)
             .outputMode("append")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
