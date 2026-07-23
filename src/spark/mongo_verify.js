// Mongo verification for the report (run: mongosh < mongo_verify.js)
use cpg;
print("total metadata docs:", db.file_metadata.countDocuments());
// A replayed file must have exactly ONE doc (upsert, not insert):
print("docs for version.py:",
  db.file_metadata.countDocuments({rel_path: "optimum/version.py"}));
// Show the updated document after a replay:
printjson(db.file_metadata.findOne({rel_path: "optimum/version.py"}));
// Duplicate check by file_id must be empty:
printjson(db.file_metadata.aggregate([
  {$group: {_id: "$file_id", c: {$sum: 1}}},
  {$match: {c: {$gt: 1}}}
]).toArray());
