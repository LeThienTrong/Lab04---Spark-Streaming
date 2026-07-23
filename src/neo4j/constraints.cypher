// Run ONCE before starting the sink connectors.
// Uniqueness constraint makes MERGE (n:CpgNode {id}) O(1) and guarantees no dup nodes.
CREATE CONSTRAINT cpg_node_id IF NOT EXISTS
  FOR (n:CpgNode) REQUIRE n.id IS UNIQUE;
CREATE INDEX cpg_node_file IF NOT EXISTS
  FOR (n:CpgNode) ON (n.file_id);
