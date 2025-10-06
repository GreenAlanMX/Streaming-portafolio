import argparse
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv


def load_env():
    env_local = Path('.env')
    env_example = Path('benchmarking/.env.example')
    if env_local.exists():
        load_dotenv(env_local)
    elif env_example.exists():
        load_dotenv(env_example)


@dataclass
class PgConfig:
    host: str
    port: int
    db: str
    user: str
    password: str


def get_pg_config() -> PgConfig:
    load_env()
    return PgConfig(
        host=os.getenv('PG_HOST', 'localhost'),
        port=int(os.getenv('PG_PORT', '5432')),
        db=os.getenv('PG_DB', 'benchmark_db'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD', 'postgres'),
    )


def connect_pg(cfg: PgConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.db,
        user=cfg.user,
        password=cfg.password,
    )


def init_schema():
    cfg = get_pg_config()
    with connect_pg(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255),
                    email VARCHAR(255),
                    age INTEGER,
                    salary NUMERIC(10,2),
                    created_at TIMESTAMP
                );
            """)
        conn.commit()
    print("PostgreSQL schema initialized.")


def migrate_csv(csv_path: Path, batch_size: int = 1000) -> int:
    cfg = get_pg_config()
    df = pd.read_csv(csv_path)
    total = len(df)

    with connect_pg(cfg) as conn:
        with conn.cursor() as cur:
            # Optional tuning
            cur.execute("SET synchronous_commit TO off;")

            cols = ['id','name','email','age','salary','created_at']
            from psycopg2.extras import execute_values
            insert_sql = f"INSERT INTO test_table ({', '.join(cols)}) VALUES %s ON CONFLICT (id) DO NOTHING"

            for i in range(0, total, batch_size):
                batch = df.iloc[i:i+batch_size]
                values = [tuple(row[c] for c in cols) for _, row in batch.iterrows()]
                execute_values(cur, insert_sql, values)
        conn.commit()

    return total


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--init', action='store_true')
    parser.add_argument('--csv', type=str, help='Path to CSV file')
    parser.add_argument('--batch', type=int, default=int(os.getenv('BATCH_SIZE', '1000')))
    args = parser.parse_args()

    if args.init:
        init_schema()
    elif args.csv:
        count = migrate_csv(Path(args.csv), batch_size=args.batch)
        print(f"Migrated {count} records to PostgreSQL")
    else:
        parser.print_help()
