# 📊 ETL Pipeline – Streaming Analytics Project

This project implements a complete **ETL pipeline** for a *streaming analytics* scenario, integrating multiple data sources (CSV and JSON), applying data validation and cleaning rules, and generating a final dataset optimized in **Parquet** format.

## 🚀 Key Features
- **Extract:** Integration of multiple sources (`users.csv`, `viewing_sessions.csv`, `content.json`).
- **Transform:** 
  - Data cleaning (nulls, date formats, numeric conversion).
  - Validation rules (positive duration, valid release year, etc.).
  - Data enrichment by joining users, sessions, and content catalog.
- **Load:** 
  - Incremental load → only new `session_id` records are added.
  - Output in **Parquet** format optimized for large datasets.
- **Monitor:** 
  - Detailed logging in `monitoring/etl_log.log`.
  - Error handling with `try/except`.
- **Scale:** 
  - Chunked CSV reading (`chunksize`) for big files.
  - Parquet for reduced space and better performance.
- **Documentation:** Complete process documentation in `docs/etl-documentation.md`.

## Project Structure
```
etl/
 ├── data/
 │   ├── raw/                  # Input data
 │   │   ├── users.csv
 │   │   ├── viewing_sessions.csv
 │   │   └── content.json
 │   ├── processed/            # Processed output data
 │   │   └── processed_sessions.parquet
 ├── monitoring/
 │   └── etl_log.log           # Execution logs
 ├── pipelines/
 │   └── streaming_etl.py      # Main ETL pipeline script
 ├── docs/
 │   └── etl-documentation.md  # Technical documentation
 ├── venv/                     # Virtual environment
 └── README.md                 # This file
```

## Installation & Usage
1. Clone the repository or download the project.
2. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # Linux/Mac
   venv\Scripts\activate    # Windows
   pip install -r requirements.txt
   ```
3. Run the pipeline from the project root:
   ```bash
   python pipelines/streaming_etl.py
   ```
4. Outputs:
   - Final dataset → `data/processed/processed_sessions.parquet`
   - Execution logs → `monitoring/etl_log.log`

## Example Output
- **Number of rows:** 222,785 processed sessions (example).  
- **Final columns:**  
  `session_id, user_id, content_id, watch_date, watch_duration_minutes, completion_percentage, device_type, quality_level, age, country, subscription_type, registration_date, total_watch_time_hours, title, genre, duration_minutes, release_year, rating, views_count, production_budget`

## Scheduling
Currently, the pipeline can be scheduled using **cron** (Linux/Mac) or **Task Scheduler** (Windows).  
Example with cron (run daily at 2 AM):

```
0 2 * * * /usr/bin/python3 /path/to/project/etl/pipelines/streaming_etl.py >> /path/to/project/etl/monitoring/cron_etl.log 2>&1
```

For larger projects, consider using **Apache Airflow** or **Prefect**.

## 🛠️ Technologies
- **Language:** Python 3.12
- **Libraries:** Pandas, PyArrow/Fastparquet, Logging
- **Output format:** Parquet
- **Orchestration (documented):** cron, Airflow, Prefect

---
