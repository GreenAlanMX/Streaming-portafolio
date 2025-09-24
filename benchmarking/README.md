# Database Migration Benchmarking (PostgreSQL vs MongoDB)

This module implements the benchmarking protocol defined in `Benchmarking.md`, adapted to compare CSV → PostgreSQL and JSON → MongoDB.

## Components
- `data_generator.py`: Generate synthetic datasets (CSV/JSON) in sizes: 1K, 10K, 100K (1M optional)
- `migrate_postgres.py`: Batch insert CSV into PostgreSQL
- `migrate_mongo.py`: Batch insert JSON into MongoDB
- `benchmark_runner.py`: Orchestrate runs and capture metrics (time, RPS, memory, CPU)
- `analyzer.py`: Plot and export performance results
- `tests/`: Integrity and performance unit tests
- `.env.example`: Connection settings

## Quickstart
```bash
# 1) Create and activate venv (optional)
python3 -m venv .venv && source .venv/bin/activate

# 2) Install deps
pip install -r requirements.txt

# 3) Configure environment
cp benchmarking/.env.example .env
# Edit DB credentials as needed

# 4) Generate datasets (1K, 10K, 100K)
python benchmarking/data_generator.py --sizes 1000 10000 100000

# 5) Initialize databases (tables/collections)
python benchmarking/migrate_postgres.py --init
python benchmarking/migrate_mongo.py --init

# 6) Run benchmarks
python benchmarking/benchmark_runner.py --sizes 1000 10000 100000 --modes sequential batch

# 7) Analyze results
python benchmarking/analyzer.py --results benchmark_results.csv
```

## Notes
- Uses batch size 1000 by default; configurable via CLI.
- For large datasets (1M), ensure enough disk/RAM and adjust batch size.
- See `Benchmarking.md` for success criteria and reporting template.
