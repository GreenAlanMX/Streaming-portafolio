# Schema Documentation — Activity 1.2 (NoSQL Model)

## Collections

### movies
- `content_id` (string, unique) — business key
- `title` (string)
- `genre` (array<string>)
- `duration_minutes` (int)
- `release_year` (int)
- `rating` (double)
- `views_count` (int)
- `production_budget` (long)

**Indexes**
- `{content_id:1}` unique
- `{genre:1}` multikey
- `{release_year:1}`
- `{rating:-1}`
- `{views_count:-1}`
- `{genre:1, release_year:1, rating:-1}` composite

### users
- `user_id` (string, unique)
- `age` (int)
- `gender` (string: "M"/"F"/"Other")
- `country` (string)
- `city` (string)
- `registration_date` (date)

**Indexes**
- `{user_id:1}` unique
- `{country:1, city:1}`
- `{age:1, gender:1}`
- `{registration_date:1}`

### viewing_sessions
- `session_id` (string, unique)
- `user_id` (string, FK→users.user_id)
- `content_id` (string, FK→movies.content_id)
- `start_time` (date) — used for time‑series
- `end_time` (date)
- `watch_duration` (int, minutes)
- `device_type` (string)
- `quality` (string: "SD"/"HD"/"4K")
- `completed` (bool)

**Indexes**
- `{session_id:1}` unique
- `{user_id:1, start_time:-1}`
- `{content_id:1, start_time:-1}`
- `{start_time:-1}` (time‑series)
- `{device_type:1, quality:1}`

## Normalization Strategy
- **Reference model** between `users` ⇄ `viewing_sessions` ⇄ `movies` via `user_id`/`content_id` keys.
- Optionally **denormalize** hot attributes (e.g., `country`, `plan`) into `viewing_sessions` to accelerate Geo/Plan aggregations, keeping canonical fields in `users`.

## Required Aggregations (where to find)
- **User engagement by demographics** — `init.sql` → *AGGREGATION 1* (lookup users, group by age buckets/gender).
- **Content performance analytics** — `init.sql` → *AGGREGATION 2* (lookup sessions, compute completion/ROI, group by genre).
- **Geographic distribution** — `init.sql` → *AGGREGATION 3* (group by country/city, devices).
- **Time‑series viewing trends** — `init.sql` → *AGGREGATION 4* (derive year/month/dow/hour from `start_time`).

## Query Optimization Notes
- Ensure **covered sorts** by aligning sort keys with index order (`start_time`, `views_count`, `rating`).
- Use **compound indexes** that match query predicates and sort (e.g., `{user_id:1, start_time:-1}`).
- For `$lookup` heavy workloads, consider **pre‑aggregation** or **denormalization** to reduce join costs.
