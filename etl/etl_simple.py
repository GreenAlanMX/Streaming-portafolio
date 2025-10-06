import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import os

def main():
    print("=== ETL SIMPLIFICADO ===")
    
    # Cargar datos
    print("Cargando datos...")
    users = pd.read_csv('data/raw/users.csv')
    sessions = pd.read_csv('data/raw/viewing_sessions.csv')
    content = pd.read_csv('data/raw/content.csv')
    
    print(f"Usuarios: {len(users)}")
    print(f"Sesiones: {len(sessions)}")
    print(f"Contenido: {len(content)}")
    
    # Procesar datos
    print("Procesando datos...")
    
    # Agregar métricas por usuario
    user_metrics = sessions.groupby('user_id').agg({
        'session_id': 'count',
        'duration_watched': ['mean', 'std'],
        'completion_rate': ['mean', 'std'],
        'content_id': 'nunique'
    }).round(2)
    
    user_metrics.columns = ['sessions_count', 'avg_duration', 'duration_std', 
                           'avg_completion', 'completion_std', 'unique_content']
    user_metrics = user_metrics.reset_index()
    
    # Unir con datos de usuarios
    user_aggregation = users.merge(user_metrics, on='user_id', how='left')
    
    # Clustering
    print("Aplicando clustering...")
    features = ['sessions_count', 'avg_duration', 'avg_completion', 'unique_content']
    X = user_aggregation[features].fillna(0)
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    kmeans = KMeans(n_clusters=3, random_state=42)
    user_aggregation['cluster_kmeans'] = kmeans.fit_predict(X_scaled)
    
    # Guardar resultados
    print("Guardando resultados...")
    os.makedirs('data/processed', exist_ok=True)
    
    user_aggregation.to_csv('data/processed/user_aggregation_with_clusters.csv', index=False)
    
    # Perfiles de clusters
    cluster_profiles = user_aggregation.groupby('cluster_kmeans')[features].mean().round(2)
    cluster_profiles.to_csv('data/processed/cluster_profiles.csv')
    
    print("✅ ETL completado exitosamente!")
    print(f"Usuarios procesados: {len(user_aggregation)}")
    print(f"Clusters: {user_aggregation['cluster_kmeans'].nunique()}")

if __name__ == "__main__":
    main()
