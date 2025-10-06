"""
Enhanced ETL Pipeline for Streaming Analytics Project
----------------------------------------------------
Includes:
- Extraction from CSV and JSON
- Transformation with validation rules
- Incremental loading to Parquet
- Logging and error handling
- Business rule validations
- Simple monitoring with email alerts
- Prefect scheduling flow
- Placeholder for PySpark scalability
"""

import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime

# Prefect for scheduling
from prefect import flow, task

# --------------------------------------------------
# Paths and logging setup
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
RAW_PATH = BASE_DIR / "data" / "raw"
PROCESSED_PATH = BASE_DIR / "data" / "processed"
LOG_PATH = BASE_DIR / "monitoring"

PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
LOG_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH / "etl_log.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# --------------------------------------------------
# Validation Rules
# --------------------------------------------------
def validate_users(df: pd.DataFrame):
    # Edad mínima
    invalid_ages = (df['age'] < 13).sum()
    df.loc[df['age'] < 13, 'age'] = pd.NA

    # Horas vistas no negativas
    invalid_watch = (df['total_watch_time_hours'] < 0).sum()
    df.loc[df['total_watch_time_hours'] < 0, 'total_watch_time_hours'] = pd.NA

    if invalid_ages > 0:
        logging.warning(f"Se corrigieron {invalid_ages} edades menores a 13 años")
    if invalid_watch > 0:
        logging.warning(f"Se corrigieron {invalid_watch} valores negativos en total_watch_time_hours")

    return df


def validate_sessions(df: pd.DataFrame):
    # Porcentaje de completitud
    invalid_completion = (~df['completion_percentage'].between(0, 100)).sum()
    df.loc[~df['completion_percentage'].between(0, 100), 'completion_percentage'] = pd.NA

    # Duraciones no negativas
    invalid_durations = (df['watch_duration_minutes'] < 0).sum()
    df.loc[df['watch_duration_minutes'] < 0, 'watch_duration_minutes'] = pd.NA

    if invalid_completion > 0:
        logging.warning(f"Se corrigieron {invalid_completion} valores inválidos en completion_percentage")
    if invalid_durations > 0:
        logging.warning(f"Se corrigieron {invalid_durations} duraciones negativas en watch_duration_minutes")

    return df


def validate_content(df: pd.DataFrame):
    # Años de lanzamiento
    invalid_years = (~df['release_year'].between(1900, 2100)).sum()
    df.loc[~df['release_year'].between(1900, 2100), 'release_year'] = pd.NA

    # Ratings fuera de 0–5
    invalid_ratings = (~df['rating'].between(0, 5)).sum()
    df.loc[~df['rating'].between(0, 5), 'rating'] = pd.NA

    if invalid_years > 0:
        logging.warning(f"Se corrigieron {invalid_years} valores inválidos en release_year")
    if invalid_ratings > 0:
        logging.warning(f"Se corrigieron {invalid_ratings} valores inválidos en rating")

    return df



# --------------------------------------------------
# Monitoring: simple email alert (placeholder)
# --------------------------------------------------
def send_alert(message: str):
    try:
        logging.error(f"ALERT: {message}")
        # Here you could integrate SMTP, Slack, etc.
    except Exception as e:
        logging.error(f"Error sending alert: {e}")

# --------------------------------------------------
# Extract
# --------------------------------------------------
def extract_users():
    try:
        users = pd.read_csv(RAW_PATH / "users.csv")
        logging.info(f"Users extracted: {len(users)} registros")
        return users
    except Exception as e:
        send_alert(f"Error extrayendo usuarios: {e}")
        raise

def extract_sessions():
    try:
        chunks = pd.read_csv(RAW_PATH / "viewing_sessions.csv", chunksize=50000)
        sessions = pd.concat(chunks)
        logging.info(f"Sessions extracted: {len(sessions)} registros")
        return sessions
    except Exception as e:
        send_alert(f"Error extrayendo sesiones: {e}")
        raise

def extract_content():
    try:
        with open(RAW_PATH / "content.json", "r", encoding="utf-8") as f:
            content_data = json.load(f)
        movies = pd.DataFrame(content_data.get("movies", []))
        series = pd.DataFrame(content_data.get("series", []))
        content = pd.concat([movies, series], ignore_index=True)
        logging.info(f"Content extracted: {len(content)} registros")
        return content
    except Exception as e:
        send_alert(f"Error extrayendo contenido: {e}")
        raise

# --------------------------------------------------
# Transform
# --------------------------------------------------
def transform(users, sessions, content):
    try:
        # Apply validation rules
        users = validate_users(users)
        sessions = validate_sessions(sessions)
        content = validate_content(content)

        # Merge datasets
        merged = sessions.merge(users, on="user_id", how="left")
        merged = merged.merge(content, on="content_id", how="left")

        logging.info(f"Transformación completada: {len(merged)} registros")
        return merged
    except Exception as e:
        send_alert(f"Error en transformación: {e}")
        raise

# --------------------------------------------------
# Load (incremental)
# --------------------------------------------------
def load(df: pd.DataFrame):
    try:
        output_file = PROCESSED_PATH / "streaming_data.parquet"

        if output_file.exists():
            existing = pd.read_parquet(output_file)
            new_data = df[~df["session_id"].isin(existing["session_id"])]
            final = pd.concat([existing, new_data])
        else:
            final = df

        final.to_parquet(output_file, index=False)
        logging.info(f"Datos cargados en {output_file}, total {len(final)} registros")
    except Exception as e:
        send_alert(f"Error en carga: {e}")
        raise

# --------------------------------------------------
# Prefect Flow
# --------------------------------------------------
@flow
def etl_pipeline():
    users = extract_users()
    sessions = extract_sessions()
    content = extract_content()
    df = transform(users, sessions, content)
    load(df)

if __name__ == "__main__":
    etl_pipeline()
