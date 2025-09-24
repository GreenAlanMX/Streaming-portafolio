import argparse
import os
import shutil
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SYN_DATA_DIR = HERE / "data"
ETL_RAW = REPO / "etl" / "data" / "raw"
ETL_SCRIPT = REPO / "etl" / "etl_pipeline_enhanced.py"
ENV_FILE = REPO / ".env"


def run(cmd: list[str]):
    subprocess.run(cmd, check=True)


def generate_data(sizes: list[int]):
    run(["python", str(HERE / "data_generator.py"), "--sizes", *map(str, sizes), "--out", str(SYN_DATA_DIR)])


def backup_env():
    if ENV_FILE.exists():
        b = ENV_FILE.with_suffix(".synthetic.bak")
        shutil.copy2(ENV_FILE, b)
        return b
    return None


def restore_env(backup_path: Path | None):
    if backup_path and backup_path.exists():
        shutil.move(str(backup_path), str(ENV_FILE))


def set_source_mode_files():
    # Ensure .env exists
    if not ENV_FILE.exists():
        (REPO / ".env.example").replace(ENV_FILE)
    # Edit SOURCE_MODE=files
    text = ENV_FILE.read_text()
    lines = []
    found = False
    for line in text.splitlines():
        if line.startswith("SOURCE_MODE="):
            lines.append("SOURCE_MODE=files")
            found = True
        else:
            lines.append(line)
    if not found:
        lines.append("SOURCE_MODE=files")
    ENV_FILE.write_text("\n".join(lines) + "\n")


def stage_size_into_etl_raw(size: int):
    ETL_RAW.mkdir(parents=True, exist_ok=True)
    src_csv = SYN_DATA_DIR / f"test_data_{size}.csv"
    src_json = SYN_DATA_DIR / f"test_data_{size}.json"
    shutil.copy2(src_csv, ETL_RAW / "users.csv")
    shutil.copy2(src_csv, ETL_RAW / "viewing_sessions.csv")
    shutil.copy2(src_json, ETL_RAW / "content.json")


def main():
    parser = argparse.ArgumentParser(description="Run ETL with synthetic data")
    parser.add_argument("--size", type=int, default=10000, help="dataset size to stage (e.g., 1000, 10000, 100000)")
    parser.add_argument("--generate", action="store_true", help="generate synthetic datasets before running")
    parser.add_argument("--label", type=str, default="synthetic", help="metrics label")
    args = parser.parse_args()

    if args.generate:
        generate_data([args.size])

    backup = backup_env()
    try:
        set_source_mode_files()
        stage_size_into_etl_raw(args.size)
        run(["python", str(ETL_SCRIPT)])
        print("ETL with synthetic data finished. Check benchmarking/etl_metrics_" + args.label + ".csv and etl/data/processed/")
    finally:
        restore_env(backup)


if __name__ == "__main__":
    main()
