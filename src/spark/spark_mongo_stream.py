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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--database", default="cpg")
    ap.add_argument("--collection", default="file_metadata")
    ap.add_argument("--checkpoint", default="/tmp/chk/cpg_metadata")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("cpg-metadata-to-mongo")
             .config("spark.mongodb.write.connection.uri", args.mongo_uri)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

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
             .option("checkpointLocation", args.checkpoint)
             .outputMode("append")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
