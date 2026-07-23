// --- Verification queries for the lab report ---
// Total nodes / relationships
MATCH (n:CpgNode) RETURN count(n) AS nodes;
MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS edges;
// Edge breakdown by category (should mirror parser output)
MATCH ()-[r:CPG_EDGE]->() RETURN r.type AS type, count(*) AS n ORDER BY n DESC;
// Per-file node counts (spot-check a replayed file before/after)
MATCH (n:CpgNode {rel_path: 'optimum/version.py'}) RETURN count(n) AS nodes_in_file;
// Duplicate check: must return ZERO rows if idempotency holds
MATCH (n:CpgNode) WITH n.id AS id, count(*) AS c WHERE c > 1 RETURN id, c;
