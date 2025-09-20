import pandas as pd

df = pd.read_parquet("data/processed/processed_sessions.parquet")

print("Número de filas:", len(df))
print("Columnas:", df.columns.tolist())
print(df.head())   # primeras filas
