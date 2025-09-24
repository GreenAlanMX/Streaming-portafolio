"""
Enhanced ETL Pipeline for Streaming Analytics Project
----------------------------------------------------
Reflects Phases 1→3 and measures Phase 4:
- Extract from Postgres (users, sessions) and Mongo (content) or fallback to CSV/JSON
- Validate/Clean three datasets
- Transform (merge)
- Aggregate user-level metrics
- Cluster (KMeans) and cluster profiles
- Load incremental to Parquet
- Export CSV artifacts for dashboards (Phase 3)
- Instrument timings/memory/CPU per stage (Phase 4)
"""
from __future__ import annotations

import os
import time
import json
import logging
from pathlib import Path
from dataclasses import dataclass
from contextlib import contextmanager

import numpy as np
import pandas as pd

from prefect import flow
from dotenv import load_dotenv

# Optional ML imports
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
except Exception:  # noqa: BLE001
    StandardScaler = None
    KMeans = None

# Optional system metrics
try:
    import psutil
except Exception:  # noqa: BLE001
    psutil = None

# Optional DB clients
try:
    import psycopg2
except Exception:  # noqa: BLE001
    psycopg2 = None
try:
    from pymongo import MongoClient
except Exception:  # noqa: BLE001
    MongoClient = None

# ---------------- Paths & logging -----------------
BASE_DIR = Path(__file__).resolve().parent
RAW_PATH = BASE_DIR / "data" / "raw"
PROCESSED_PATH = BASE_DIR / "data" / "processed"
LOG_PATH = BASE_DIR / "monitoring"
BENCHMARK_PATH = BASE_DIR.parent / "benchmarking"

PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
LOG_PATH.mkdir(parents=True, exist_ok=True)
BENCHMARK_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH / "etl_log.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Load env from repo root if exists
env_file = Path.cwd() / ".env"
if env_file.exists():
    load_dotenv(env_file)

# ---------------- Instrumentation -----------------
METRICS: list[dict] = []
PROCESS = psutil.Process(os.getpid()) if psutil else None

@contextmanager
def track(stage_name: str, extra: dict | None = None):
    t0 = time.perf_counter()
    runtime_extra: dict = {}
    mem0 = PROCESS.memory_info().rss if PROCESS else 0
    cpu0 = PROCESS.cpu_times() if PROCESS else None
    try:
        yield runtime_extra
    finally:
        t1 = time.perf_counter()
        row = {"timestamp": pd.Timestamp.now().isoformat(), "stage": stage_name, "duration_s": round(t1 - t0, 3)}
        if PROCESS and cpu0 is not None:
            mem1 = PROCESS.memory_info().rss
            cpu1 = PROCESS.cpu_times()
            row.update({
                "mem_delta_mb": round((mem1 - mem0)/1024/1024, 2),
                "cpu_user_s": round(cpu1.user - cpu0.user, 3),
                "cpu_system_s": round(cpu1.system - cpu0.system, 3),
            })
        if runtime_extra:
            row.update(runtime_extra)
        if extra:
            row.update(extra)
        METRICS.append(row)

def export_metrics(dataset_label: str) -> Path:
    if not METRICS:
        return BENCHMARK_PATH / "etl_metrics_empty.csv"
    dfm = pd.DataFrame(METRICS)
    dfm["dataset"] = dataset_label
    out = BENCHMARK_PATH / f"etl_metrics_{dataset_label}.csv"
    dfm.to_csv(out, index=False)
    logging.info("Metrics exported to %s", out)
    return out

# ---------------- Validation rules ----------------

def validate_users(df: pd.DataFrame) -> pd.DataFrame:
    if "age" in df.columns:
        invalid_ages = (df["age"] < 13).sum()
        df.loc[df["age"] < 13, "age"] = pd.NA
        if invalid_ages:
            logging.warning("Se corrigieron %s edades menores a 13 años", invalid_ages)
    if "total_watch_time_hours" in df.columns:
        invalid_watch = (df["total_watch_time_hours"] < 0).sum()
        df.loc[df["total_watch_time_hours"] < 0, "total_watch_time_hours"] = pd.NA
        if invalid_watch:
            logging.warning("Se corrigieron %s valores negativos en total_watch_time_hours", invalid_watch)
    return df

def validate_sessions(df: pd.DataFrame) -> pd.DataFrame:
    if "completion_percentage" in df.columns:
        invalid_completion = (~df["completion_percentage"].between(0, 100)).sum()
        df.loc[~df["completion_percentage"].between(0, 100), "completion_percentage"] = pd.NA
        if invalid_completion:
            logging.warning("Se corrigieron %s valores inválidos en completion_percentage", invalid_completion)
    if "watch_duration_minutes" in df.columns:
        invalid_durations = (df["watch_duration_minutes"] < 0).sum()
        df.loc[df["watch_duration_minutes"] < 0, "watch_duration_minutes"] = pd.NA
        if invalid_durations:
            logging.warning("Se corrigieron %s duraciones negativas en watch_duration_minutes", invalid_durations)
    return df

def validate_content(df: pd.DataFrame) -> pd.DataFrame:
    if "release_year" in df.columns:
        invalid_years = (~df["release_year"].between(1900, 2100)).sum()
        df.loc[~df["release_year"].between(1900, 2100), "release_year"] = pd.NA
        if invalid_years:
            logging.warning("Se corrigieron %s valores inválidos en release_year", invalid_years)
    if "rating" in df.columns:
        invalid_ratings = (~df["rating"].between(0, 5)).sum()
        df.loc[~df["rating"].between(0, 5), "rating"] = pd.NA
        if invalid_ratings:
            logging.warning("Se corrigieron %s valores inválidos en rating", invalid_ratings)
    return df

# ---------------- Alerts --------------------------

def send_alert(message: str) -> None:
    logging.error("ALERT: %s", message)

# ---------------- Extract -------------------------

def _pg_conn():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no disponible; instala psycopg2-binary")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "streaming"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )

def _mongo_col():
    if MongoClient is None:
        raise RuntimeError("pymongo no disponible; instala pymongo")
    client = MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        username=os.getenv("MONGO_USER"),
        password=os.getenv("MONGO_PASSWORD"),
        authSource=os.getenv("MONGO_AUTH_SOURCE", "admin")
    )
    db = client[os.getenv("MONGO_DB", "streaming")]
    return db[os.getenv("MONGO_COLLECTION_CONTENT", "content")], client

def extract_users() -> pd.DataFrame:
    try:
        mode = os.getenv("SOURCE_MODE", "files").lower()
        if mode == "database":
            with _pg_conn() as conn:
                query = os.getenv("POSTGRES_USERS_QUERY", "SELECT * FROM users;")
                users = pd.read_sql_query(query, conn)
        else:
            users = pd.read_csv(RAW_PATH / "users.csv")
        logging.info("Users extracted: %s", len(users))
        return users
    except Exception as e:  # noqa: BLE001
        send_alert(f"Error extrayendo usuarios: {e}")
        raise

def extract_sessions() -> pd.DataFrame:
    try:
        mode = os.getenv("SOURCE_MODE", "files").lower()
        if mode == "database":
            with _pg_conn() as conn:
                query = os.getenv("POSTGRES_SESSIONS_QUERY", "SELECT * FROM viewing_sessions;")
                sessions = pd.read_sql_query(query, conn)
        else:
            chunks = pd.read_csv(RAW_PATH / "viewing_sessions.csv", chunksize=50000)
            sessions = pd.concat(chunks, ignore_index=True)
        logging.info("Sessions extracted: %s", len(sessions))
        return sessions
    except Exception as e:  # noqa: BLE001
        send_alert(f"Error extrayendo sesiones: {e}")
        raise

def extract_content() -> pd.DataFrame:
    try:
        mode = os.getenv("SOURCE_MODE", "files").lower()
        if mode == "database":
            col, client = _mongo_col()
            try:
                # Collection "content"; docs may include arrays movies/series
                docs = list(col.find({}, {"_id": 0}))
                frames = []
                for doc in docs:
                    if isinstance(doc.get("movies"), list):
                        frames.append(pd.DataFrame(doc["movies"]))
                    if isinstance(doc.get("series"), list):
                        frames.append(pd.DataFrame(doc["series"]))
                content = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(docs)
            finally:
                client.close()
        else:
            with open(RAW_PATH / "content.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            movies = pd.DataFrame(data.get("movies", []))
            series = pd.DataFrame(data.get("series", []))
            content = pd.concat([movies, series], ignore_index=True)
        logging.info("Content extracted: %s", len(content))
        return content
    except Exception as e:  # noqa: BLE001
        send_alert(f"Error extrayendo contenido: {e}")
        raise

# ---------------- Transform -----------------------

def transform(users: pd.DataFrame, sessions: pd.DataFrame, content: pd.DataFrame) -> pd.DataFrame:
    try:
        users = validate_users(users)
        sessions = validate_sessions(sessions)
        content = validate_content(content)
        merged = sessions.merge(users, on="user_id", how="left")
        merged = merged.merge(content, on="content_id", how="left")
        logging.info("Transformación completada: %s registros", len(merged))
        return merged
    except Exception as e:  # noqa: BLE001
        send_alert(f"Error en transformación: {e}")
        raise

# ---- Phase 2: Descriptives, DQ, Outliers --------

def _numeric_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

def generate_descriptive_stats(df: pd.DataFrame, name: str) -> Path:
    out = PROCESSED_PATH / f"{name}_descriptive_stats.csv"
    cols = _numeric_columns(df)
    (df[cols].describe() if cols else df.describe(include='all')).to_csv(out)
    logging.info("Descriptive stats saved: %s", out)
    return out

def generate_data_quality_report(df: pd.DataFrame, name: str) -> Path:
    out = PROCESSED_PATH / f"{name}_data_quality.csv"
    rows = []
    for col in df.columns:
        s = df[col]
        try:
            unique_count = int(s.nunique(dropna=True))
        except TypeError:
            unique_count = "N/A (contains lists)"
        rows.append({
            "column": col,
            "dtype": str(s.dtype),
            "null_count": int(s.isna().sum()),
            "null_pct": round(float(s.isna().mean())*100, 2),
            "unique_count": unique_count,
        })
    pd.DataFrame(rows).to_csv(out, index=False)
    logging.info("Data quality saved: %s", out)
    return out

def detect_outliers_iqr(df: pd.DataFrame, name: str) -> Path:
    out = PROCESSED_PATH / f"{name}_outliers_iqr.csv"
    cols = _numeric_columns(df)
    rows = []
    for col in cols:
        s = df[col].dropna()
        if s.empty:
            continue
        q1, q3 = np.percentile(s, [25, 75])
        iqr = q3 - q1
        lower, upper = q1 - 1.5*iqr, q3 + 1.5*iqr
        rows.append({"column": col, "lower": lower, "upper": upper, "outliers_count": int(((df[col] < lower) | (df[col] > upper)).sum())})
    pd.DataFrame(rows).to_csv(out, index=False)
    logging.info("Outliers report saved: %s", out)
    return out

# ---------------- Aggregate (user level) ----------

def aggregate_user_metrics(df: pd.DataFrame) -> pd.DataFrame:
    duration_col = "watch_duration_minutes"
    completion_col = "completion_percentage"
    grouped = df.groupby("user_id")
    user_agg = grouped.agg(
        sessions_count=("session_id", "count"),
        avg_duration=(duration_col, "mean"),
        duration_std=(duration_col, "std"),
        avg_completion=(completion_col, "mean"),
        completion_std=(completion_col, "std"),
        unique_content=("content_id", "nunique"),
        age=("age", "first"),
        subscription_type=("subscription_type", "first"),
        country=("country", "first"),
    ).reset_index().fillna(0)
    subscription_map = {"Basic": 1, "Standard": 2, "Premium": 3}
    user_agg["subscription_numeric"] = user_agg["subscription_type"].map(subscription_map).fillna(1).astype(int)
    return user_agg

# ---------------- Clustering ----------------------

def cluster_users(user_agg: pd.DataFrame, n_clusters: int = 3):
    if StandardScaler is None or KMeans is None:
        logging.warning("sklearn no disponible; se omite clustering")
        user_agg = user_agg.copy()
        user_agg["cluster_kmeans"] = 0
        return user_agg, pd.DataFrame()
    features = ["sessions_count", "avg_duration", "duration_std", "avg_completion", "completion_std", "unique_content", "subscription_numeric"]
    X = user_agg[features].astype(float).values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    kmeans = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
    labels = kmeans.fit_predict(X_scaled)
    user_agg = user_agg.copy()
    user_agg["cluster_kmeans"] = labels
    cluster_profiles = user_agg.groupby("cluster_kmeans")[features].mean().round(2).reset_index()
    return user_agg, cluster_profiles

# ---------------- Load & Exports ------------------

def load_incremental(df: pd.DataFrame) -> None:
    output_file = PROCESSED_PATH / "streaming_data.parquet"
    if output_file.exists():
        existing = pd.read_parquet(output_file)
        if "session_id" in df.columns and "session_id" in existing.columns:
            new_data = df[~df["session_id"].isin(existing["session_id"])]
            final = pd.concat([existing, new_data], ignore_index=True)
        else:
            final = pd.concat([existing, df], ignore_index=True)
    else:
        final = df
    
    # Convert date columns to datetime before saving to parquet
    date_columns = ['watch_date', 'created_at', 'updated_at', 'registration_date']
    for col in date_columns:
        if col in final.columns:
            final[col] = pd.to_datetime(final[col], errors='coerce')
    
    final.to_parquet(output_file, index=False, engine='fastparquet')
    logging.info("Datos cargados en %s, total %s registros", output_file, len(final))


def export_analysis_outputs(user_agg: pd.DataFrame, cluster_profiles: pd.DataFrame) -> None:
    user_out = PROCESSED_PATH / "user_aggregation_with_clusters.csv"
    prof_out = PROCESSED_PATH / "cluster_profiles.csv"
    user_agg.to_csv(user_out, index=False)
    logging.info("User aggregation with clusters exportado: %s", user_out)
    if not cluster_profiles.empty:
        cluster_profiles.to_csv(prof_out, index=False)
        logging.info("Cluster profiles exportado: %s", prof_out)

# ---------------- Flow ----------------------------
@flow
def etl_pipeline(dataset_label: str = "real") -> None:
    with track("extract_users") as m:
        users = extract_users()
        m["rows"] = len(users)
    with track("extract_sessions") as m:
        sessions = extract_sessions()
        m["rows"] = len(sessions)
    with track("extract_content") as m:
        content = extract_content()
        m["rows"] = len(content)

    with track("transform") as m:
        df = transform(users, sessions, content)
        m["rows"] = len(df)

    # Phase 2 reports
    with track("descriptive_stats_users"):
        generate_descriptive_stats(users, "users")
    with track("dq_users"):
        generate_data_quality_report(users, "users")
    with track("outliers_users"):
        detect_outliers_iqr(users, "users")

    with track("descriptive_stats_sessions"):
        generate_descriptive_stats(sessions, "sessions")
    with track("dq_sessions"):
        generate_data_quality_report(sessions, "sessions")
    with track("outliers_sessions"):
        detect_outliers_iqr(sessions, "sessions")

    with track("descriptive_stats_content"):
        generate_descriptive_stats(content, "content")
    with track("dq_content"):
        generate_data_quality_report(content, "content")
    with track("outliers_content"):
        detect_outliers_iqr(content, "content")

    with track("aggregate_user_metrics") as m:
        user_agg = aggregate_user_metrics(df)
        m["rows"] = len(user_agg)
    with track("cluster_users") as m:
        user_agg_with_clusters, cluster_profiles = cluster_users(user_agg)
        m["rows"] = len(user_agg_with_clusters)
    with track("export_analysis_outputs"):
        export_analysis_outputs(user_agg_with_clusters, cluster_profiles)

    with track("load_incremental") as m:
        load_incremental(df)
        m["rows"] = len(df)

    export_metrics(dataset_label)

if __name__ == "__main__":
    etl_pipeline("real")
