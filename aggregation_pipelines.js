// ==========================================
// AGGREGATION PIPELINES (MongoDB)
// Colecciones usadas: movies, series
// ==========================================


// PIPELINE A (5 etapas): Top géneros por "impacto" (rating promedio * log10(views))
//
// Etapas: $unwind -> $group -> $addFields -> $sort -> $limit
db.movies.aggregate([
  { $unwind: "$genre" },
  {
    $group: {
      _id: "$genre",
      avgRating: { $avg: "$rating" },
      totalViews: { $sum: "$views_count" },
      titles: { $addToSet: "$title" },
      items: { $sum: 1 }
    }
  },
  {
    $addFields: {
      impactScore: {
        $multiply: [
          "$avgRating",
          { $log10: { $add: ["$totalViews", 1] } }
        ]
      }
    }
  },
  { $sort: { impactScore: -1 } },
  { $limit: 10 }
]);


// PIPELINE B (4 etapas): Eficiencia de series (views por minuto total)
// Calcula episodios totales con $reduce, runtime total y eficiencia.
//
// Etapas: $addFields -> $addFields -> $project -> $sort
db.series.aggregate([
  {
    $addFields: {
      total_episodes: {
        $reduce: {
          input: "$episodes_per_season",
          initialValue: 0,
          in: { $add: ["$$value", "$$this"] }
        }
      }
    }
  },
  {
    $addFields: {
      total_runtime_minutes: { $multiply: ["$total_episodes", "$avg_episode_duration"] }
    }
  },
  {
    $project: {
      _id: 0,
      content_id: 1,
      title: 1,
      seasons: 1,
      total_episodes: 1,
      total_runtime_minutes: 1,
      total_views: 1,
      rating: 1,
      efficiency_views_per_min: {
        $divide: ["$total_views", { $max: ["$total_runtime_minutes", 1] }]
      }
    }
  },
  { $sort: { efficiency_views_per_min: -1, rating: -1 } }
]);


// PIPELINE C (5 etapas): Top contenido unificado (películas + series)
// Normaliza campos con $project, une con $unionWith, crea score y ordena.
//
// Etapas: $project -> $unionWith(subpipeline con $project) -> $addFields -> $sort -> $limit
db.movies.aggregate([
  {
    $project: {
      _id: 0,
      content_id: 1,
      title: 1,
      type: { $literal: "movie" },
      rating: 1,
      views: "$views_count",
      release_year: 1
    }
  },
  {
    $unionWith: {
      coll: "series",
      pipeline: [
        {
          $project: {
            _id: 0,
            content_id: 1,
            title: 1,
            type: { $literal: "series" },
            rating: 1,
            views: "$total_views",
            release_year: { $literal: null }
          }
        }
      ]
    }
  },
  {
    $addFields: {
      // "score" pondera rating y views; ajusta pesos si lo deseas
      score: { $add: [ { $multiply: ["$rating", 10000] }, "$views" ] }
    }
  },
  { $sort: { score: -1 } },
  { $limit: 10 }
]);


// PIPELINE D (3 etapas): Tendencia por década (películas)
// Agrupa por década para rating promedio y views totales.
//
// Etapas: $addFields -> $group -> $sort
db.movies.aggregate([
  {
    $addFields: {
      decade: {
        $concat: [
          {
            $toString: {
              $multiply: [{ $floor: { $divide: ["$release_year", 10] } }, 10]
            }
          },
          "s"
        ]
      }
    }
  },
  {
    $group: {
      _id: "$decade",
      avgRating: { $avg: "$rating" },
      totalViews: { $sum: "$views_count" },
      titles: { $addToSet: "$title" },
      count: { $sum: 1 }
    }
  },
  { $sort: { _id: 1 } }
]);


// PIPELINE E (4 etapas): Búsqueda de texto con relevancia
// Requiere índice de texto en "title" (ya lo creaste).
//
// Etapas: $match -> $addFields -> $sort -> $project
db.movies.aggregate([
  { $match: { $text: { $search: "Investigators Advanced" } } },
  { $addFields: { textScore: { $meta: "textScore" } } },
  { $sort: { textScore: -1, rating: -1 } },
  { $project: { _id: 0, title: 1, genre: 1, rating: 1, textScore: 1 } }
]);


// PIPELINE F (3 etapas): Eficiencia de presupuesto (películas)
// Views por dólar y ranking.
//
// Etapas: $match -> $addFields -> $sort
db.movies.aggregate([
  { $match: { production_budget: { $gt: 0 } } },
  {
    $addFields: {
      views_per_dollar: { $divide: ["$views_count", "$production_budget"] }
    }
  },
  { $sort: { views_per_dollar: -1, rating: -1 } }
]);


// OPCIONAL (multi-salida): Resumen en paralelo por $facet (≥1 etapa, subpipelines dentro)
// No cuenta como "varios pipelines", pero es útil para dashboard.
db.movies.aggregate([
  {
    $facet: {
      topByRating: [
        { $sort: { rating: -1 } },
        { $limit: 5 },
        { $project: { _id: 0, title: 1, rating: 1 } }
      ],
      topByViews: [
        { $sort: { views_count: -1 } },
        { $limit: 5 },
        { $project: { _id: 0, title: 1, views_count: 1 } }
      ]
    }
  }
]);
