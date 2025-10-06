# Streaming-portafolio – Integrated Portfolio (Phases 1.1 → 4)

This repository now integrates all phases into `main`: relational (1.1), NoSQL (1.2), statistical analysis (2), visualization (3), and ETL (4). It includes unified documentation, AI disclosure, and a demo workflow.

## Team
See `TEAM.md` for member names, roles, and profiles. Contributions are summarized per responsibility area.

## AI Disclosure
Summary: AI assisted code drafting, troubleshooting, and documentation. Human reviewed, executed, and validated all changes. Full disclosure in `AI_Disclosure.md` and per-session notes in `AI-assistence-1.md` to `AI-assistence-4.md`.

## Architecture Overview
- Data sources: PostgreSQL, MongoDB, CSV/JSON
- ETL orchestration: Prefect (`etl/etl_pipeline_enhanced.py`)
- Analytics: user aggregation and KMeans clustering
- Visualization: Streamlit dashboard (`notebooks/streamlit_dashboard.py`)

## Project Structure (Integrated)
```
docs/
 ├── database-design.md
 ├── statistical-analysis.md
 ├── visualization-guide.md
 └── etl-documentation.md
etl/
 ├── data/raw/            # Input data (or DB via .env)
 ├── data/processed/      # Parquet + analytics CSVs
 └── etl_pipeline_enhanced.py
notebooks/
 ├── Phase2_Statistical_Analysis.ipynb
 ├── Phase3_Interactive_Dashboards.ipynb
 ├── streamlit_dashboard.py
 └── output/
presentation/
 └── README.md            # slides/infographic placeholders
TEAM.md
AI_Disclosure.md
```

## Setup
1) Create and populate `.env` (see `.env.example`). Key vars: `POSTGRES_*`, `MONGO_*`, `SOURCE_MODE` (database|files), queries for users/sessions.
2) Start services:
```bash
docker compose up -d
```
3) Create venv and install:
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run ETL
```bash
cd etl
python etl_pipeline_enhanced.py
```
Outputs are written to `etl/data/processed/` and used by the dashboard.

## Run Dashboard
```bash
cd notebooks
streamlit run streamlit_dashboard.py --server.port 8501
```

## Benchmarking (optional)
See `benchmarking/README.md` and `demo_commands_fixed.md` to reproduce performance runs and the demo workflow.

## Notes
- All phase branches have been merged into `main` and reflected in `docs/`.
- Docker configuration relies on environment variables; do not commit secrets.
