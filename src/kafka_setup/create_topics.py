"""
Task 3 - Kafka Topic Design (topic creation).
=============================================

Creates the four topics that carry the pipeline's event categories:

    cpg.nodes      - one message per CPG node   (key = node id)
    cpg.edges      - one message per CPG edge    (key = edge id)
    cpg.metadata   - one message per source file (key = file id)
    cpg.errors     - one message per parse error (key = file id)

Design decisions
----------------
* Keying: the stable element id is the message key. Same element always hashes
  to the same partition -> per-element ordering is preserved and the topic is
  safe for log compaction (latest state per key wins).
* Partitions: nodes/edges are the high-volume streams (tens of thousands per
  repo) so they get more partitions for parallel sink consumption; metadata and
  errors are low-volume and use a single partition.
* cleanup.policy=compact on nodes/edges/metadata: keeping only the latest value
  per key mirrors the idempotent MERGE semantics downstream and bounds topic
  growth across replays.
"""
from __future__ import annotations

import argparse

from kafka.admin import KafkaAdminClient, NewTopic

TOPIC_SPECS = [
    ("cpg.nodes", 6, {"cleanup.policy": "compact"}),
    ("cpg.edges", 6, {"cleanup.policy": "compact"}),
    ("cpg.metadata", 1, {"cleanup.policy": "compact"}),
    ("cpg.errors", 1, {"cleanup.policy": "delete", "retention.ms": "604800000"}),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--replication", type=int, default=1)
    args = ap.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap)
    topics = [
        NewTopic(name, num_partitions=parts,
                 replication_factor=args.replication, topic_configs=cfg)
        for name, parts, cfg in TOPIC_SPECS
    ]
    from kafka.errors import TopicAlreadyExistsError

    for t in topics:
        try:
            admin.create_topics([t])
            print(f"created  {t.name}  partitions={t.num_partitions}")
        except TopicAlreadyExistsError:
            # idempotent re-run: the topic is already there with its config
            print(f"exists   {t.name}  (already created, skipping)")
        except Exception as exc:  # noqa: BLE001
            print(f"skip     {t.name}: {exc}")
    admin.close()


if __name__ == "__main__":
    main()
