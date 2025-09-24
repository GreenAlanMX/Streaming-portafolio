# 📊 ETL Pipeline – Streaming Analytics Project

This project implements a complete **ETL pipeline** for a *streaming analytics* scenario, integrating multiple data sources (CSV and JSON), applying data validation and cleaning rules, and generating a final dataset optimized in **Parquet** format.

## 🚀 Key Features
- **Extract:** Integration of multiple sources (`users.csv`, `viewing_sessions.csv`, `content.json`).
- **Transform:** 
  - Data cleaning (nulls, date formats, numeric conversion).
  - Validation rules (minimum age, valid release year, completion % 0–100, etc.).
  - Data enrichment by joining users, sessions, and content catalog.
- **Load:** 
  - Incremental load → only new `session_id` records are added.
  - Output in **Parquet** format optimized for large datasets.
- **Monitor:** 
  - Detailed logging in `monitoring/etl_log.log`.
  - Error handling with `try/except`.
  - Warnings when invalid values are corrected.
- **Scale:** 
  - Chunked CSV reading (`chunksize`) for big files.
  - Parquet for reduced space and better performance.
- **Documentation:** Complete process documentation in `docs/etl-documentation.md`.

## 📂 Project Structure
```
etl/
 ├── data/
 │   ├── raw/                  # Input data
 │   │   ├── users.csv
 │   │   ├── viewing_sessions.csv
 │   │   └── content.json
 │   └── processed/            # Processed output data (Parquet)
 ├── monitoring/
 │   └── etl_log.log           # Execution logs
 ├── docs/
 │   └── etl-documentation.md  # Technical documentation
 ├── etl_pipeline_enhanced.py  # 🚀 Main ETL pipeline
 ├── test_validation.py        # Unit tests (pytest)
 ├── requirements.txt          # Dependencies
 ├── README.md                 # This file
 └── venv/                     # Virtual environment (ignored in Git)
```

## ⚙️ Installation & Usage
1. Clone the repository or download the project.  
2. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # Linux/Mac
   venv\Scripts\activate      # Windows
   pip install -r requirements.txt
   ```
3. Run the pipeline from the project root:
   ```bash
   python3 etl_pipeline_enhanced.py
   ```
4. Outputs:
   - Final dataset → `data/processed/streaming_data.parquet`  
   - Execution logs → `monitoring/etl_log.log`

## 📝 Example Log Output
```
INFO     Users extracted: 5000 registros
INFO     Sessions extracted: 200000 registros
WARNING  Se corrigieron 100 valores inválidos en release_year
INFO     Transformación completada: 200000 registros
INFO     Datos cargados en data/processed/streaming_data.parquet, total 200000 registros
```

## ⏰ Scheduling
You can schedule the pipeline using **cron** (Linux/Mac) or **Task Scheduler** (Windows).  
Example with cron (run daily at 2 AM):

```
0 2 * * * /usr/bin/python3 /path/to/etl/etl_pipeline_enhanced.py >> /path/to/etl/monitoring/cron_etl.log 2>&1
```

For larger projects, consider using **Apache Airflow** or **Prefect**.

## 🛠️ Technologies
- **Language:** Python 3.12  
- **Libraries:** Pandas, PyArrow/Fastparquet, Prefect, Pytest, Logging  
- **Output format:** Parquet  
- **Orchestration:** Prefect (with option for Airflow/cron)  
