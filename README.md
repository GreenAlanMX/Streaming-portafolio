# 🎬 Streaming Platform MongoDB Docker Setup

Este proyecto configura una base de datos MongoDB completa para una plataforma de streaming con datos de películas, series y analytics, utilizando Docker y Docker Compose.

## 📋 Requisitos Previos

- Docker (versión 20.10 o superior)
- Docker Compose (versión 2.0 o superior)
- Make (opcional, para usar los comandos automatizados)

## 🚀 Inicio Rápido

### 1. Estructura de Directorios

Crea la siguiente estructura de directorios:

```
streaming-platform/
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── README.md
├── scripts/
│   └── init-streaming-db.js
└── mongo-init/
```

### 2. Setup Inicial

```bash
# Crear directorios necesarios
mkdir -p streaming-platform/scripts streaming-platform/mongo-init

# Navegar al directorio
cd streaming-platform

# Configurar entorno (si tienes Make)
make dev-setup

# O manualmente
mkdir -p scripts mongo-init backups logs
```

### 3. Iniciar Servicios

**Opción A: Con Make (Recomendado)**
```bash
# Ver todos los comandos disponibles
make help

# Iniciar todos los servicios
make up
```

**Opción B: Con Docker Compose directamente**
```bash
# Iniciar MongoDB y Mongo Express
docker-compose up -d mongodb mongo-express

# Esperar 10 segundos y ejecutar el script
sleep 10
docker-compose up script-runner
```

## 🔧 Servicios Incluidos

### MongoDB (Puerto 27017)
- **Usuario:** admin
- **Contraseña:** streaming123
- **Base de datos:** streaming_platform
- **Colecciones:** content, analytics

### Mongo Express (Puerto 8081)
- **URL:** http://localhost:8081
- **Usuario:** admin
- **Contraseña:** streaming123
- **Interfaz web para administrar MongoDB**

## 📊 Estructura de Datos

### Colección `content`
Almacena información de películas y series:
- Movies: Data Adventures, Analytics Kingdom
- Series: Analytics Chronicles, Data Detectives

### Colección `analytics`
Datos de rendimiento y métricas:
- Visualizaciones diarias
- Ingresos
- Ratings de usuarios
- Datos demográficos

### Índices Optimizados
- Búsquedas por content_id, tipo, género
- Ordenamiento por rating y visualizaciones
- Búsqueda de texto completo
- Índices compuestos para consultas complejas

## 🎯 Pipelines de Agregación Incluidos

El script ejecuta automáticamente 3 pipelines de análisis:

1. **Top Contenido por Género** - Análisis de géneros más populares
2. **Análisis de ROI** - Rendimiento financiero por categoría de presupuesto
3. **Análisis Temporal** - Métricas de rendimiento a lo largo del tiempo

## 🛠️ Comandos Útiles

### Con Makefile

```bash
# Ver estado de servicios
make status

# Ver logs
make logs

# Conectar a MongoDB shell
make mongo-shell

# Conectar directamente a la BD
make mongo-shell-db

# Reiniciar servicios
make restart

# Ejecutar script de inicialización manualmente
make init-db

# Crear backup
make backup

# Restaurar backup
make restore BACKUP_DIR=./backups/streaming_platform_20240918_143000

# Verificar salud de servicios
make health-check

# Ejecutar consultas de prueba
make test-queries

# Limpiar todo (¡CUIDADO!)
make clean
```

### Comandos Docker Compose

```bash
# Ver logs de MongoDB específicamente
docker-compose logs -f mongodb

# Parar servicios
docker-compose down

# Parar y eliminar volúmenes
docker-compose down -v

# Ver estado
docker-compose ps
```

### Comandos de MongoDB

```bash
# Conectar a MongoDB
docker exec -it streaming_mongodb mongosh -u admin -p streaming123 --authenticationDatabase admin

# Usar la base de datos
use streaming_platform

# Ver colecciones
show collections

# Consultar contenido
db.content.find().pretty()

# Consultar analytics
db.analytics.find().pretty()

# Ejecutar pipeline de ejemplo
db.content.aggregate([{$group: {_id: "$type", count: {$sum: 1}}}])
```

## 🔍 Consultas de Ejemplo

### Top películas por rating
```javascript
db.content.find({type: "movie"}).sort({rating: -1}).limit(5)
```

### Series con más temporadas
```javascript
db.content.find({type: "series"}).sort({seasons: -1})
```

### Contenido por género
```javascript
db.content.aggregate([
  {$unwind: "$genre"},
  {$group: {_id: "$genre", count: {$sum: 1}}},
  {$sort: {count: -1}}
])
```

### Analytics por contenido
```javascript
db.analytics.aggregate([