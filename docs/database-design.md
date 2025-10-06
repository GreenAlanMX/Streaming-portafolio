## Database Design (Integrated Summary)

This document consolidates relational (PostgreSQL) and NoSQL (MongoDB) designs from phases 1.1 and 1.2.

### Relational Model (PostgreSQL)
- Entities: users, content, viewing_sessions
- Keys and relationships: users 1..* viewing_sessions; content 1..* viewing_sessions
- See `activity-1.1-relational-model/diagrams/schema.png` and `sql/01_create_tables.sql` for DDL.

### NoSQL Model (MongoDB)
- Collection: content
- Subtypes: movies, series with unified schema
- Aggregations: popularity, completion metrics, top genres
- See `phase-1.2/scripts/aggregation_pipelines_unified.js` equivalent integrated into ETL transformations.

### Rationale and Normalization
- 3NF for transactional integrity (users, sessions, content)
- Denormalized aggregates produced downstream for analytics

### Notes
- Indexing recommended on `viewing_sessions(user_id, content_id, watch_date)` and `content(genre, content_type)`.


