## ETL Documentation (Phase 4 Integrated)

### Overview
- Core script: `etl/etl_pipeline_enhanced.py` (Prefect flow `etl-pipeline`)
- Sources: PostgreSQL/Mongo via `.env` (SOURCE_MODE=database) or CSV/JSON files (SOURCE_MODE=files)
- Outputs: `etl/data/processed/` parquet and CSV analytics exports

### Key steps
1) Extract: users, viewing_sessions, content (DB or files)
2) Transform: cleaning, validation, data quality reports
3) Aggregate: user-level metrics
4) Cluster: KMeans k=3
5) Load: parquet (fastparquet) + analytics CSVs
6) Monitor: time, memory peak, CPU per etapa

### Configuration
- `.env` includes DB credentials, queries, and SOURCE_MODE
- Mongo auth supports username/password and authSource

### Running
```
cd etl
python etl_pipeline_enhanced.py
```

### Synthetic data
- Scripts in `benchmarking/` generate datasets and benchmarking reports


