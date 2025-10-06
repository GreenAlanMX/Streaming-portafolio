import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


def generate(size: int, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    ids = np.arange(1, size + 1)
    names = [f"User_{i}" for i in ids]
    emails = [f"user{i}@example.com" for i in ids]
    ages = np.random.randint(18, 80, size)
    salaries = np.round(np.random.uniform(30000, 150000, size), 2)
    created_at = [
        (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
        for _ in range(size)
    ]

    df = pd.DataFrame(
        {
            "id": ids,
            "name": names,
            "email": emails,
            "age": ages,
            "salary": salaries,
            "created_at": created_at,
        }
    )

    csv_path = out_dir / f"test_data_{size}.csv"
    json_path = out_dir / f"test_data_{size}.json"

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)
    print(f"Wrote {csv_path} and {json_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", nargs="+", type=int, default=[1000, 10000, 100000])
    parser.add_argument("--out", type=str, default="benchmarking/data")
    args = parser.parse_args()

    out_dir = Path(args.out)
    for s in args.sizes:
        generate(s, out_dir)
