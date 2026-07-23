// Task 6 stale-node cleanup ("generation sweep").
// After a file is reprocessed, delete nodes of that file whose file_hash is
// NOT the current one (orphans from the previous version). :param file_id and
// :param current_hash are supplied by the replay driver.
MATCH (n:CpgNode {file_id: $file_id})
WHERE n.file_hash <> $current_hash
DETACH DELETE n;
