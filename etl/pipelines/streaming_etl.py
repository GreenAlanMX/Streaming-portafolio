import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime
import logging

# Ruta base: la carpeta padre de /pipelines/
BASE_DIR = Path(__file__).resolve().parent.parent

# Carpetas importantes
RAW_PATH = BASE_DIR / "data" / "raw"
PROCESSED_PATH = BASE_DIR / "data" / "processed"
LOG_PATH = BASE_DIR / "monitoring"

# Crear carpetas si no existen
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
LOG_PATH.mkdir(parents=True, exist_ok=True)

# Configuración de logging
logging.basicConfig(
    filename=LOG_PATH / "etl_log.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# ------------------- EXTRACT -------------------

def extract_users():
    try:
        users = pd.read_csv(RAW_PATH / "users.csv")
        logging.info(f"Users extracted: {len(users)} registros")
        return users
    except Exception as e:
        logging.error(f"Error extrayendo usuarios: {e}")
        raise

def extract_sessions():
    try:
        # chunksize para manejar datasets grandes
        chunks = pd.read_csv(RAW_PATH / "viewing_sessions.csv", chunksize=50000)
        sessions = pd.concat(chunks)
        logging.info(f"Sessions extracted: {len(sessions)} registros")
        return sessions
    except Exception as e:
        logging.error(f"Error extrayendo sesiones: {e}")
        raise

def extract_content():
    try:
        with open(RAW_PATH / "content.json", "r") as f:
            content = json.load(f)

        # Normalizar directamente la lista de películas
        df_content = pd.json_normalize(content["movies"])

        logging.info(f"Content extracted: {len(df_content)} registros")
        return df_content
    except Exception as e:
        logging.error(f"Error extrayendo contenido: {e}")
        raise


# ------------------- TRANSFORM -------------------

def transform(users, sessions, content):
    try:
        # Validaciones y limpieza de usuarios
        users = users.dropna(subset=["user_id"])  # ID obligatorio
        users["registration_date"] = pd.to_datetime(users["registration_date"], errors="coerce")

        # Validaciones de sesiones
        sessions = sessions.dropna(subset=["session_id", "user_id", "content_id"])
        if "start_time" in sessions.columns and "end_time" in sessions.columns:
            sessions["start_time"] = pd.to_datetime(sessions["start_time"], errors="coerce")
            sessions["end_time"] = pd.to_datetime(sessions["end_time"], errors="coerce")
            sessions["duration_minutes"] = (
                (sessions["end_time"] - sessions["start_time"]).dt.total_seconds() / 60
            )

        # Validaciones de contenido
        if "release_year" in content.columns:
            content["release_year"] = pd.to_numeric(content["release_year"], errors="coerce")

        # Enriquecimiento: unir datasets
        merged = sessions.merge(users, on="user_id", how="left")
        merged = merged.merge(content, on="content_id", how="left")

        logging.info(f"Transformación completada: {len(merged)} registros finales")
        return merged
    except Exception as e:
        logging.error(f"Error en transformación: {e}")
        raise

# ------------------- LOAD (INCREMENTAL) -------------------

def load_incremental(df):
    try:
        output_file = PROCESSED_PATH / "processed_sessions.parquet"

        if output_file.exists():
            existing = pd.read_parquet(output_file)
            before_count = len(existing)

            # Detectar solo nuevos registros (basado en session_id)
            new_data = df[~df["session_id"].isin(existing["session_id"])]
            combined = pd.concat([existing, new_data], ignore_index=True)

            combined.to_parquet(output_file, index=False)
            logging.info(
                f"Incremental load: {len(new_data)} nuevos registros añadidos. "
                f"Total ahora: {len(combined)} (antes {before_count})"
            )
        else:
            df.to_parquet(output_file, index=False)
            logging.info(f"Carga inicial completa: {len(df)} registros.")
    except Exception as e:
        logging.error(f"Error en carga incremental: {e}")
        raise

# ------------------- PIPELINE -------------------

def etl_pipeline():
    logging.info("==== INICIO DEL PIPELINE ETL ====")
    users = extract_users()
    sessions = extract_sessions()
    content = extract_content()
    transformed = transform(users, sessions, content)
    load_incremental(transformed)
    logging.info("==== FIN DEL PIPELINE ETL ====")

if __name__ == "__main__":
    etl_pipeline()
