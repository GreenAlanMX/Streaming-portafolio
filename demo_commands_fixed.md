# ETL Pipeline Demo - Comandos de Ejecución (Corregidos)

## **Preparación**
```bash
# Verificar estructura del proyecto
ls -la

```

## **Fase 1: Base de Datos**
```bash
# Iniciar contenedores
docker-compose up -d

# Verificar MongoDB
docker exec -it video_streaming_mongodb mongo --eval "db.content.count()"

# Verificar PostgreSQL
docker exec -it video_streaming_postgres psql -U postgres -d video_streaming_platform -c "SELECT COUNT(*) FROM users;"
```

## **Fase 2: ETL Pipeline**
```bash
# Ejecutar ETL
cd etl
python etl_pipeline_enhanced.py

# Verificar resultados
ls -la data/processed/
ls -la data/raw/

# Mostrar datos procesados
head -5 data/processed/users_clean_sample.csv
head -5 data/processed/sessions_clean_sample.csv
```

## **Fase 3: Análisis Estadístico**
```bash
# Abrir notebook
cd ../notebooks
jupyter notebook Phase2_Statistical_Analysis.ipynb
```

## **Fase 4: Dashboard**
```bash
# Ejecutar dashboard
streamlit run streamlit_dashboard.py
```

## ⚡ **Fase 5: Benchmarking**
```bash
# Ejecutar benchmark
cd ../benchmarking
python final_benchmark.py

# Abrir reporte
open results/benchmark_report_with_images.html
```

##  **Fase 6: Pruebas**
```bash
# Ejecutar pruebas unitarias
cd ../tests
python run_unit_tests.py
```

## **Fase 7: Resumen**
```bash
# Ver archivos generados
cd ..
find . -name "*.parquet" -o -name "*.html" -o -name "*.png" | head -10

# Verificar métricas finales
ls -la benchmarking/results/
ls -la notebooks/output/
```

## **Comandos Alternativos de Verificación**
```bash
# Ver contenedores activos
docker ps

# Ver logs de MongoDB
docker logs video_streaming_mongodb

# Ver logs de PostgreSQL
docker logs video_streaming_postgres

# Conectar a MongoDB directamente
docker exec -it video_streaming_mongodb mongo

# Conectar a PostgreSQL directamente
docker exec -it video_streaming_postgres psql -U postgres -d video_streaming_platform
```
