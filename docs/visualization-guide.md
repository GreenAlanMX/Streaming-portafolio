## Visualization Guide (Phase 3)

### Dashboard
- Implementation: `notebooks/streamlit_dashboard.py`
- Data sources: `notebooks/output/user_aggregation_with_clusters.csv`, `cluster_profiles.csv`
- KPIs: sessions_count, avg_duration, avg_completion, unique_content

### How to run
```
cd notebooks
streamlit run streamlit_dashboard.py --server.port 8501
```

### Notes
- Ensure ETL has produced outputs in `etl/data/processed` and copied to `notebooks/output`.
- Mobile-responsive layout and Plotly charts.


