# Screenshots

Place your screenshots here with EXACTLY these filenames (the notebooks
reference them):

| Filename | What to capture |
|---|---|
| `neo4j_graph.png` | Neo4j Browser: result of `MATCH (n:CpgNode)-[r:CPG_EDGE]->(m) RETURN n,r,m LIMIT 100` |
| `neo4j_edge_counts.png` | Neo4j Browser: edge breakdown query, table view |
| `mongo_collection.png` | MongoDB Compass: `cpg.file_metadata` collection listing |
| `spark_ui.png` | Spark UI (http://localhost:4040) Structured Streaming tab |
| `connect_status.png` | Output of the connector status curl, or Kafka UI |
| `neo4j_after_replay.png` | Neo4j node count for the edited file, after the sweep |
