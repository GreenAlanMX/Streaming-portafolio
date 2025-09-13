# Technology Choice — MongoDB vs CouchDB vs Firebase

**Chosen:** MongoDB

## Why MongoDB fits this streaming use‑case
- **Rich Aggregation Framework** ($unwind/$lookup/$facet/$unionWith) for analytics over arrays and nested fields.
- **Flexible document model** for `episodes_per_season`, multi‑genre, evolving content metadata.
- **Indexing** on arrays/compound keys enables Top‑N, geo, and time‑series groupings efficiently.

## CouchDB (alternative)
- Strengths: master‑master replication, offline‑first with PouchDB, append‑only storage.
- Trade‑offs: Map/Reduce views less expressive than Mongo pipelines; less convenient for ad‑hoc analytics with joins/arrays.

## Firebase (alternative)
- Strengths: serverless, real‑time sync (Firestore/RTDB), easy client integration.
- Trade‑offs: limited aggregation primitives; complex analytics requires exporting to BigQuery; indexing/sorting constraints for multi‑predicate queries.

**Conclusion:** MongoDB provides the best balance of **modeling flexibility + native analytics** for the required aggregations. For BI at warehouse scale, pair with a columnar DWH (e.g., BigQuery/Redshift) as described in *Performance_Comparison_1.2.md*.
