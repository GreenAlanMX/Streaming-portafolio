# 1.2 Performance Comparison: Relational vs NoSQL (MongoDB) — *Streaming* Platform

**Objective:** compare, with practical criteria, the **relational (SQL/columnar DWH)** vs **NoSQL (MongoDB)** approaches for typical *streaming* use cases (movies/series, viewing sessions, users).

---

## Executive Summary (when to use each)

| Use case | Best choice | Why |
|---|---|---|
| Top-N by `views` or `rating` in a **single entity** | **Tie** (Mongo or SQL) | Simple indexes + `SORT/LIMIT` without joins |
| Aggregations with **arrays/nested fields** (e.g., summing episodes) | **MongoDB** | `$unwind`, `$reduce`, `$addFields` simplify and avoid joins |
| **Multi-entity** KPIs (users + sessions + content) with heavy joins | **SQL/columnar DWH** | Optimized execution plans, hash/merge joins, efficient scans |
| Frequent schema evolution / semi-structured data | **MongoDB** | Document flexibility and no rigid migrations |
| Large cubes and slices by date/country/segment (BI reporting) | **SQL/columnar DWH** | Partitions, sort/cluster keys, column compression |
| Text search by `title`/`description` | **MongoDB** | `$text` indexes + `textScore` for quick simple relevance |

---

## Metrics to compare (if you benchmark)
- **p95/p99 latency** for key queries (Q1–Q3 below).  
- **Throughput (qps)** under concurrent load.  
- **CPU/IO cost and scanned volume** (especially in columnar DWH).  
- **Cold vs warm time** (caches).

---

## Representative queries (Q1–Q3)

**Q1. Top 10 titles by `views` (no joins)**  
- *MongoDB*: `db.movies.find().sort({views_count:-1}).limit(10)` (index on `views_count`).  
- *SQL*: `SELECT title, views_count FROM movies ORDER BY views_count DESC LIMIT 10;` (b-tree index).  
**→** Tie: both are O(log N) for ordering with an index; without an index, a full scan penalizes both.

**Q2. Daily views by country and plan (requires joining sessions+users)**  
- *MongoDB* (two options):  
  1) **Denormalize** country/plan inside `viewing_sessions` → fast reads, costlier writes.  
  2) `$lookup` with `users` → correct, but with millions of docs a relational DWH usually wins.  
- *SQL*: `JOIN viewing_sessions vs ON vs.user_id = users.user_id` + `GROUP BY date, country, plan`.  
**→** Advantage **SQL/DWH** for massive joins and day/country/plan aggregations.

**Q3. Series efficiency (views per total minute)**  
- *MongoDB*: `$addFields` for total episodes (`$reduce`), runtime, and metric → very straightforward.  
- *SQL*: needs a function/CTE to sum `episodes_per_season` (if normalized) or a UDF if it comes as array/JSON.  
**→** Advantage **MongoDB** when data lives in a single document with arrays.

---

## How to measure fairly (mini-plan)
1. **Data**: at least 1–5 M `viewing_sessions`; in SQL, normalized tables; in Mongo, a denormalized version (and optionally a `$lookup` variant).  
2. **Indexes/Keys**:  
   - Mongo: `{views_count:-1}`, `{rating:-1}`, `{genre:1,rating:-1}`, `{user_id:1}`, `{content_id:1}`.  
   - SQL/DWH: partition by `date`, sort/cluster keys (e.g., `(date, country)`), indexes on `views_count`, `user_id`, `content_id`.  
3. **Execution**: run Q1–Q3 10 times, record p50/p95/p99 with cold and warm cache.  
4. **Plans**: save **EXPLAIN (ANALYZE, BUFFERS)** in SQL and `explain("executionStats")` in Mongo to justify results.

---

## Practical recommendations for your repo
- Keep the **Mongo pipelines** (you already meet ≥3 stages each) for content/series KPIs with arrays.  
- Document (in SQL) the equivalent versions of Q1–Q3 if your instructor wants to see the relational side.  
- If you later ingest real sessions: consider **denormalizing** hot fields (e.g., `country`, `plan`) in `viewing_sessions` and keep the canonical source in `users`.

---

### Examples of “plan/diagnostics”
- **MongoDB**:  
  ```js
  db.movies.find().sort({views_count:-1}).limit(10).explain("executionStats")
  ```
- **SQL (PostgreSQL example):**  
  ```sql
  EXPLAIN (ANALYZE, BUFFERS)
  SELECT title, views_count
  FROM movies
  ORDER BY views_count DESC
  LIMIT 10;
  ```

> **Conclusion:** For single-entity analytics with arrays, MongoDB is agile and expressive; for extensive joins and cubes by date/country/segment, a relational/columnar DWH tends to offer lower latency and more stable costs at large scale.
