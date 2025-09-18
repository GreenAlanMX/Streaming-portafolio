// =====================================================
// DISEÑO DE COLECCIONES MONGODB
// =====================================================

// 1. COLECCIÓN: content (Unificada para movies y series)
// Ventajas: Permite consultas unificadas y mejor escalabilidad
const contentSchema = {
  _id: ObjectId,
  content_id: String,        // Identificador único del contenido
  type: String,              // "movie" o "series"
  title: String,
  genre: [String],
  rating: Number,
  views_count: Number,       // Para movies: views_count, para series: total_views
  production_budget: Number,
  release_year: Number,      // Solo para movies
  duration_minutes: Number,  // Solo para movies
  seasons: Number,           // Solo para series
  episodes_per_season: [Number], // Solo para series
  avg_episode_duration: Number,  // Solo para series
  created_at: Date,
  updated_at: Date
};

// 2. COLECCIÓN: analytics (Para métricas y estadísticas)
const analyticsSchema = {
  _id: ObjectId,
  content_id: String,
  date: Date,
  daily_views: Number,
  revenue: Number,
  user_ratings: [Number],
  demographic_data: {
    age_groups: Object,
    regions: Object
  }
};

// =====================================================
// SCRIPTS DE INSERCIÓN CON DATOS
// =====================================================

// Conectar a MongoDB
use('streaming_platform');

// Limpiar colecciones existentes
db.content.drop();
db.analytics.drop();

// Insertar datos de contenido
const contentData = [
  // Movies
  {
    content_id: "M001",
    type: "movie",
    title: "Data Adventures",
    genre: ["Action", "Sci-Fi"],
    duration_minutes: 120,
    release_year: 2023,
    rating: 4.2,
    views_count: 15420,
    production_budget: 50000000,
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    content_id: "M002",
    type: "movie",
    title: "Analytics Kingdom",
    genre: ["Fantasy", "Adventure"],
    duration_minutes: 98,
    release_year: 2024,
    rating: 4.5,
    views_count: 23150,
    production_budget: 35000000,
    created_at: new Date(),
    updated_at: new Date()
  },
  // Series
  {
    content_id: "S001",
    type: "series",
    title: "Analytics Chronicles",
    genre: ["Drama", "Technology"],
    seasons: 3,
    episodes_per_season: [10, 12, 8],
    avg_episode_duration: 45,
    rating: 4.7,
    views_count: 89650,
    production_budget: 120000000,
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    content_id: "S002",
    type: "series",
    title: "Data Detectives",
    genre: ["Crime", "Mystery"],
    seasons: 2,
    episodes_per_season: [8, 10],
    avg_episode_duration: 52,
    rating: 4.3,
    views_count: 67420,
    production_budget: 85000000,
    created_at: new Date(),
    updated_at: new Date()
  }
];

// Insertar contenido
db.content.insertMany(contentData);

// Insertar datos de analytics (simulados)
const analyticsData = [
  {
    content_id: "M001",
    date: new Date("2024-01-15"),
    daily_views: 1250,
    revenue: 3750.00,
    user_ratings: [4, 5, 4, 3, 5],
    demographic_data: {
      age_groups: {"18-25": 30, "26-35": 45, "36-45": 25},
      regions: {"North America": 60, "Europe": 30, "Asia": 10}
    }
  },
  {
    content_id: "M002",
    date: new Date("2024-02-20"),
    daily_views: 1890,
    revenue: 5670.00,
    user_ratings: [5, 4, 5, 5, 4],
    demographic_data: {
      age_groups: {"18-25": 35, "26-35": 40, "36-45": 25},
      regions: {"North America": 55, "Europe": 35, "Asia": 10}
    }
  },
  {
    content_id: "S001",
    date: new Date("2024-03-10"),
    daily_views: 2890,
    revenue: 8670.00,
    user_ratings: [5, 5, 4, 5, 4],
    demographic_data: {
      age_groups: {"18-25": 25, "26-35": 50, "36-45": 25},
      regions: {"North America": 50, "Europe": 40, "Asia": 10}
    }
  },
  {
    content_id: "S002",
    date: new Date("2024-04-05"),
    daily_views: 2100,
    revenue: 6300.00,
    user_ratings: [4, 4, 5, 3, 4],
    demographic_data: {
      age_groups: {"18-25": 20, "26-35": 55, "36-45": 25},
      regions: {"North America": 65, "Europe": 25, "Asia": 10}
    }
  }
];

db.analytics.insertMany(analyticsData);

// =====================================================
// CREACIÓN DE ÍNDICES OPTIMIZADOS
// =====================================================

// Índices para la colección content
db.content.createIndex({ "content_id": 1 }, { unique: true }); // Búsquedas por ID
db.content.createIndex({ "type": 1 }); // Filtrar por tipo (movie/series)
db.content.createIndex({ "genre": 1 }); // Búsquedas por género
db.content.createIndex({ "rating": -1 }); // Ordenar por rating descendente
db.content.createIndex({ "views_count": -1 }); // Contenido más visto
db.content.createIndex({ "release_year": -1 }); // Películas más recientes
db.content.createIndex({ "title": "text" }); // Búsqueda de texto completo

// Índices compuestos para consultas complejas
db.content.createIndex({ "type": 1, "rating": -1 }); // Mejores por tipo
db.content.createIndex({ "genre": 1, "rating": -1 }); // Mejores por género
db.content.createIndex({ "production_budget": -1, "views_count": -1 }); // ROI analysis

// Índices para la colección analytics
db.analytics.createIndex({ "content_id": 1 });
db.analytics.createIndex({ "date": -1 });
db.analytics.createIndex({ "content_id": 1, "date": -1 });

print("✅ Base de datos, colecciones e índices creados exitosamente");

// =====================================================
// PIPELINE DE AGREGACIÓN 1: TOP CONTENIDO POR GÉNERO
// =====================================================

print("\n🚀 PIPELINE 1: Top contenido por género con estadísticas");

db.content.aggregate([
  // Etapa 1: Desenrollar géneros para análisis individual
  {
    $unwind: "$genre"
  },
  
  // Etapa 2: Agrupar por género y calcular métricas
  {
    $group: {
      _id: "$genre",
      total_content: { $sum: 1 },
      avg_rating: { $avg: "$rating" },
      total_views: { $sum: "$views_count" },
      avg_budget: { $avg: "$production_budget" },
      top_content: {
        $push: {
          title: "$title",
          rating: "$rating",
          views: "$views_count",
          type: "$type"
        }
      }
    }
  },
  
  // Etapa 3: Ordenar y formatear resultados
  {
    $project: {
      genre: "$_id",
      total_content: 1,
      avg_rating: { $round: ["$avg_rating", 2] },
      total_views: 1,
      avg_budget_millions: { $round: [{ $divide: ["$avg_budget", 1000000] }, 1] },
      top_rated: {
        $arrayElemAt: [
          {
            $sortArray: {
              input: "$top_content",
              sortBy: { rating: -1 }
            }
          },
          0
        ]
      }
    }
  },
  
  // Etapa 4: Ordenar por rating promedio
  {
    $sort: { avg_rating: -1 }
  }
]);

// =====================================================
// PIPELINE DE AGREGACIÓN 2: ANÁLISIS DE ROI Y RENDIMIENTO
// =====================================================

print("\n🚀 PIPELINE 2: Análisis de ROI y rendimiento financiero");

db.content.aggregate([
  // Etapa 1: Calcular métricas de rendimiento
  {
    $addFields: {
      views_per_million_budget: {
        $round: [
          { 
            $divide: [
              "$views_count", 
              { $divide: ["$production_budget", 1000000] }
            ]
          }, 
          2
        ]
      },
      budget_category: {
        $switch: {
          branches: [
            { case: { $lt: ["$production_budget", 50000000] }, then: "Low Budget" },
            { case: { $lt: ["$production_budget", 100000000] }, then: "Medium Budget" },
            { case: { $gte: ["$production_budget", 100000000] }, then: "High Budget" }
          ],
          default: "Unknown"
        }
      }
    }
  },
  
  // Etapa 2: Agrupar por categoría de presupuesto
  {
    $group: {
      _id: {
        budget_category: "$budget_category",
        type: "$type"
      },
      content_count: { $sum: 1 },
      avg_rating: { $avg: "$rating" },
      avg_views_per_million: { $avg: "$views_per_million_budget" },
      total_budget: { $sum: "$production_budget" },
      total_views: { $sum: "$views_count" },
      best_performer: {
        $max: {
          title: "$title",
          roi_metric: "$views_per_million_budget"
        }
      }
    }
  },
  
  // Etapa 3: Formatear y proyectar resultados finales
  {
    $project: {
      budget_category: "$_id.budget_category",
      content_type: "$_id.type",
      content_count: 1,
      avg_rating: { $round: ["$avg_rating", 2] },
      avg_views_per_million_budget: { $round: ["$avg_views_per_million", 0] },
      total_budget_millions: { $round: [{ $divide: ["$total_budget", 1000000] }, 1] },
      total_views: 1,
      efficiency_score: {
        $round: [
          { 
            $multiply: [
              { $divide: ["$avg_rating", 5] },
              { $divide: ["$avg_views_per_million", 1000] }
            ]
          }, 
          3
        ]
      }
    }
  },
  
  // Etapa 4: Ordenar por score de eficiencia
  {
    $sort: { efficiency_score: -1 }
  }
]);

// =====================================================
// PIPELINE DE AGREGACIÓN 3: ANÁLISIS TEMPORAL CON ANALYTICS
// =====================================================

print("\n🚀 PIPELINE 3: Análisis temporal con datos de analytics");

db.analytics.aggregate([
  // Etapa 1: Join con datos de contenido
  {
    $lookup: {
      from: "content",
      localField: "content_id",
      foreignField: "content_id",
      as: "content_info"
    }
  },
  
  // Etapa 2: Desenrollar y estructurar datos
  {
    $unwind: "$content_info"
  },
  
  // Etapa 3: Calcular métricas avanzadas por contenido
  {
    $group: {
      _id: {
        content_id: "$content_id",
        title: "$content_info.title",
        type: "$content_info.type",
        month: { $month: "$date" },
        year: { $year: "$date" }
      },
      total_daily_views: { $sum: "$daily_views" },
      total_revenue: { $sum: "$revenue" },
      avg_user_rating: { 
        $avg: { 
          $avg: "$user_ratings" 
        } 
      },
      performance_dates: {
        $push: {
          date: "$date",
          views: "$daily_views",
          revenue: "$revenue"
        }
      },
      demographics_summary: {
        $push: "$demographic_data"
      }
    }
  },
  
  // Etapa 4: Calcular métricas finales y rankings
  {
    $addFields: {
      revenue_per_view: {
        $round: [
          { $divide: ["$total_revenue", "$total_daily_views"] }, 
          2
        ]
      },
      performance_period: {
        $concat: [
          { $toString: "$_id.month" },
          "/",
          { $toString: "$_id.year" }
        ]
      }
    }
  },
  
  // Etapa 5: Proyección final y ordenamiento
  {
    $project: {
      content_id: "$_id.content_id",
      title: "$_id.title",
      type: "$_id.type",
      performance_period: 1,
      total_daily_views: 1,
      total_revenue: { $round: ["$total_revenue", 2] },
      revenue_per_view: 1,
      avg_user_rating: { $round: ["$avg_user_rating", 2] },
      profitability_score: {
        $round: [
          {
            $multiply: [
              "$revenue_per_view",
              { $divide: ["$avg_user_rating", 5] },
              { $divide: ["$total_daily_views", 1000] }
            ]
          },
          3
        ]
      }
    }
  },
  
  // Etapa 6: Ordenar por score de rentabilidad
  {
    $sort: { profitability_score: -1 }
  }
]);

print("\n✅ Todos los pipelines ejecutados exitosamente");
print("📊 Revisa los resultados de cada agregación para obtener insights sobre tu plataforma de streaming");