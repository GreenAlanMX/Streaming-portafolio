## Statistical Analysis (Phase 2 Synthesis)

This document synthesizes the analysis performed in `notebooks/Phase2_Statistical_Analysis.ipynb`.

### Dataset preparation
- Clean samples exported to `notebooks/output/*_clean_sample.csv`
- Key variables: duration_watched, completion_rate, sessions per user, unique content

### Descriptive statistics
- Central tendency and dispersion computed for core metrics
- Outliers identified using IQR, exported to ETL data quality reports

### Clustering
- Method: KMeans (k=3)
- Features: sessions_count, avg_duration, avg_completion, unique_content
- Outputs: `cluster_profiles.csv`, `user_aggregation_with_clusters.csv`

### Feature importance
- Derived from tree-based models in notebook; used to prioritize KPIs in dashboard

### Findings (high-level)
- Three distinct user segments: casual, engaged, active
- Completion and session frequency strongly correlate with engagement


