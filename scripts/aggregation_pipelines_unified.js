// ==========================================
// MONGODB UNIFIED AGGREGATION PIPELINES - ACTIVITY 1.2
// Single collection approach for optimal scalability
// ==========================================

// ==========================================
// 1. USER ENGAGEMENT METRICS BY DEMOGRAPHICS
// (Adapted to use content performance as engagement proxy)
// ==========================================
print("=== 1. CONTENT ENGAGEMENT METRICS BY GENRE AND CONTENT TYPE ===")
db.content.aggregate([
   {
      // Stage 1: Unwind genres to analyze each genre separately
      $unwind: "$genre"
   },
   {
      // Stage 2: Add engagement metrics based on views and rating
      $addFields: {
         engagement_score: {
            $multiply: [
               "$rating",
               { $log10: { $add: ["$views_count", 1] } }
            ]
         },
         efficiency_metric: {
            $cond: [
               { $eq: ["$content_type", "movie"] },
               { $divide: ["$views_count", "$duration_minutes"] },
               { $divide: ["$views_count", "$total_runtime_minutes"] }
            ]
         }
      }
   },
   {
      // Stage 3: Group by genre and content type
      $group: {
         _id: {
            genre: "$genre",
            content_type: "$content_type"
         },
         total_content: { $sum: 1 },
         avg_rating: { $avg: "$rating" },
         total_views: { $sum: "$views_count" },
         avg_engagement_score: { $avg: "$engagement_score" },
         avg_efficiency: { $avg: "$efficiency_metric" },
         high_performing_titles: {
            $push: {
               $cond: [
                  { $gte: ["$engagement_score", 4.0] },
                  { 
                     title: "$title", 
                     rating: "$rating", 
                     views: "$views_count",
                     content_type: "$content_type"
                  },
                  null
               ]
            }
         }
      }
   },
   {
      // Stage 4: Filter out null values and add counts
      $addFields: {
         high_performing_count: {
            $size: {
               $filter: {
                  input: "$high_performing_titles",
                  cond: { $ne: ["$$this", null] }
               }
            }
         }
      }
   },
   {
      // Stage 5: Sort by engagement metrics
      $sort: { "avg_engagement_score": -1, "total_views": -1 }
   }
])

// ==========================================
// 2. CONTENT PERFORMANCE ANALYTICS
// ==========================================
print("\n=== 2. UNIFIED CONTENT PERFORMANCE ANALYTICS ===")
db.content.aggregate([
   {
      // Stage 1: Add unified performance metrics
      $addFields: {
         performance_score: {
            $add: [
               { $multiply: ["$rating", 10000] },
               "$views_count"
            ]
         },
         roi_estimate: {
            $cond: [
               { $gt: ["$production_budget", 0] },
               { $divide: ["$views_count", "$production_budget"] },
               0
            ]
         },
         runtime_minutes: {
            $cond: [
               { $eq: ["$content_type", "movie"] },
               "$duration_minutes",
               "$total_runtime_minutes"
            ]
         }
      }
   },
   {
      // Stage 2: Unwind genres for genre analysis
      $unwind: "$genre"
   },
   {
      // Stage 3: Group by genre and content type
      $group: {
         _id: {
            genre: "$genre",
            content_type: "$content_type"
         },
         total_content: { $sum: 1 },
         avg_rating: { $avg: "$rating" },
         total_views: { $sum: "$views_count" },
         avg_budget: { $avg: "$production_budget" },
         avg_roi: { $avg: "$roi_estimate" },
         avg_runtime: { $avg: "$runtime_minutes" },
         top_performers: {
            $push: {
               title: "$title",
               rating: "$rating",
               views: "$views_count",
               performance_score: "$performance_score",
               content_type: "$content_type"
            }
         }
      }
   },
   {
      // Stage 4: Sort top performers within each group
      $addFields: {
         top_performers: {
            $slice: [
               {
                  $sortArray: {
                     input: "$top_performers",
                     sortBy: { performance_score: -1 }
                  }
               },
               3
            ]
         }
      }
   },
   {
      // Stage 5: Sort groups by performance
      $sort: { "avg_rating": -1, "total_views": -1 }
   }
])

// ==========================================
// 3. GEOGRAPHIC DISTRIBUTION ANALYSIS
// (Adapted to analyze content by release year and genre distribution)
// ==========================================
print("\n=== 3. CONTENT DISTRIBUTION ANALYSIS BY RELEASE YEAR ===")
db.content.aggregate([
   {
      // Stage 1: Add year grouping and decade analysis
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
         },
         year_group: {
            $switch: {
               branches: [
                  { case: { $lt: ["$release_year", 2010] }, then: "2000s" },
                  { case: { $lt: ["$release_year", 2020] }, then: "2010s" },
                  { case: { $gte: ["$release_year", 2020] }, then: "2020s" }
               ],
               default: "Unknown"
            }
         }
      }
   },
   {
      // Stage 2: Unwind genres for distribution analysis
      $unwind: "$genre"
   },
   {
      // Stage 3: Group by year group, genre, and content type
      $group: {
         _id: {
            year_group: "$year_group",
            genre: "$genre",
            content_type: "$content_type"
         },
         content_count: { $sum: 1 },
         total_views: { $sum: "$views_count" },
         avg_rating: { $avg: "$rating" },
         total_budget: { $sum: "$production_budget" }
      }
   },
   {
      // Stage 4: Group by year group to get distribution
      $group: {
         _id: "$_id.year_group",
         content_distribution: {
            $push: {
               genre: "$_id.genre",
               content_type: "$_id.content_type",
               count: "$content_count",
               views: "$total_views",
               avg_rating: "$avg_rating"
            }
         },
         total_content: { $sum: "$content_count" },
         total_views: { $sum: "$total_views" }
      }
   },
   {
      // Stage 5: Sort distribution within each year group
      $addFields: {
         content_distribution: {
            $sortArray: {
               input: "$content_distribution",
               sortBy: { count: -1 }
            }
         }
      }
   },
   {
      // Stage 6: Sort by year group
      $sort: { "_id": 1 }
   }
])

// ==========================================
// 4. TIME-SERIES VIEWING TRENDS
// (Adapted to analyze trends by release year and rating evolution)
// ==========================================
print("\n=== 4. UNIFIED CONTENT TRENDS ANALYSIS BY RELEASE YEAR ===")
db.content.aggregate([
   {
      // Stage 1: Add trend analysis fields
      $addFields: {
         year: "$release_year",
         decade: {
            $concat: [
               {
                  $toString: {
                     $multiply: [{ $floor: { $divide: ["$release_year", 10] } }, 10]
                  }
               },
               "s"
            ]
         },
         rating_category: {
            $switch: {
               branches: [
                  { case: { $lt: ["$rating", 2.0] }, then: "Low" },
                  { case: { $lt: ["$rating", 3.5] }, then: "Medium" },
                  { case: { $gte: ["$rating", 3.5] }, then: "High" }
               ],
               default: "Unknown"
            }
         }
      }
   },
   {
      // Stage 2: Group by year and content type
      $group: {
         _id: {
            year: "$year",
            content_type: "$content_type"
         },
         content_count: { $sum: 1 },
         avg_rating: { $avg: "$rating" },
         total_views: { $sum: "$views_count" },
         avg_budget: { $avg: "$production_budget" },
         rating_distribution: {
            $push: {
               $switch: {
                  branches: [
                     { case: { $lt: ["$rating", 2.0] }, then: "Low" },
                     { case: { $lt: ["$rating", 3.5] }, then: "Medium" },
                     { case: { $gte: ["$rating", 3.5] }, then: "High" }
                  ],
                  default: "Unknown"
               }
            }
         }
      }
   },
   {
      // Stage 3: Calculate rating distribution percentages
      $addFields: {
         low_rating_count: {
            $size: {
               $filter: {
                  input: "$rating_distribution",
                  cond: { $eq: ["$$this", "Low"] }
               }
            }
         },
         medium_rating_count: {
            $size: {
               $filter: {
                  input: "$rating_distribution",
                  cond: { $eq: ["$$this", "Medium"] }
               }
            }
         },
         high_rating_count: {
            $size: {
               $filter: {
                  input: "$rating_distribution",
                  cond: { $eq: ["$$this", "High"] }
               }
            }
         }
      }
   },
   {
      // Stage 4: Calculate percentages and add trend indicators
      $addFields: {
         low_rating_pct: { $multiply: [{ $divide: ["$low_rating_count", "$content_count"] }, 100] },
         medium_rating_pct: { $multiply: [{ $divide: ["$medium_rating_count", "$content_count"] }, 100] },
         high_rating_pct: { $multiply: [{ $divide: ["$high_rating_count", "$content_count"] }, 100] },
         views_per_content: { $divide: ["$total_views", "$content_count"] }
      }
   },
   {
      // Stage 5: Sort by year for trend visualization
      $sort: { "_id.year": 1, "_id.content_type": 1 }
   }
])

// ==========================================
// 5. BONUS: UNIFIED EFFICIENCY ANALYSIS
// ==========================================
print("\n=== 5. UNIFIED CONTENT EFFICIENCY ANALYSIS ===")
db.content.aggregate([
   {
      // Stage 1: Add efficiency metrics for both content types
      $addFields: {
         runtime_minutes: {
            $cond: [
               { $eq: ["$content_type", "movie"] },
               "$duration_minutes",
               "$total_runtime_minutes"
            ]
         },
         episode_count: {
            $cond: [
               { $eq: ["$content_type", "series"] },
               "$total_episodes",
               1
            ]
         }
      }
   },
   {
      // Stage 2: Calculate efficiency metrics
      $addFields: {
         views_per_minute: { $divide: ["$views_count", "$runtime_minutes"] },
         views_per_episode: { $divide: ["$views_count", "$episode_count"] },
         budget_efficiency: {
            $cond: [
               { $gt: ["$production_budget", 0] },
               { $divide: ["$views_count", "$production_budget"] },
               0
            ]
         }
      }
   },
   {
      // Stage 3: Group by content type and genre
      $unwind: "$genre"
   },
   {
      // Stage 4: Group by genre and content type
      $group: {
         _id: {
            genre: "$genre",
            content_type: "$content_type"
         },
         content_count: { $sum: 1 },
         avg_rating: { $avg: "$rating" },
         total_views: { $sum: "$views_count" },
         avg_views_per_minute: { $avg: "$views_per_minute" },
         avg_views_per_episode: { $avg: "$views_per_episode" },
         avg_budget_efficiency: { $avg: "$budget_efficiency" },
         top_efficient_content: {
            $push: {
               title: "$title",
               rating: "$rating",
               views: "$views_count",
               views_per_minute: "$views_per_minute",
               content_type: "$content_type"
            }
         }
      }
   },
   {
      // Stage 5: Sort top efficient content within each group
      $addFields: {
         top_efficient_content: {
            $slice: [
               {
                  $sortArray: {
                     input: "$top_efficient_content",
                     sortBy: { views_per_minute: -1 }
                  }
               },
               2
            ]
         }
      }
   },
   {
      // Stage 6: Sort groups by efficiency
      $sort: { "avg_views_per_minute": -1, "content_count": -1 }
   }
])

// ==========================================
// 6. BONUS: CROSS-CONTENT TYPE COMPARISON
// ==========================================
print("\n=== 6. CROSS-CONTENT TYPE PERFORMANCE COMPARISON ===")
db.content.aggregate([
   {
      // Stage 1: Add unified metrics
      $addFields: {
         runtime_minutes: {
            $cond: [
               { $eq: ["$content_type", "movie"] },
               "$duration_minutes",
               "$total_runtime_minutes"
            ]
         },
         normalized_views: {
            $divide: ["$views_count", "$runtime_minutes"]
         }
      }
   },
   {
      // Stage 2: Group by content type
      $group: {
         _id: "$content_type",
         total_content: { $sum: 1 },
         avg_rating: { $avg: "$rating" },
         total_views: { $sum: "$views_count" },
         avg_runtime: { $avg: "$runtime_minutes" },
         avg_normalized_views: { $avg: "$normalized_views" },
         total_budget: { $sum: "$production_budget" },
         top_content: {
            $push: {
               title: "$title",
               rating: "$rating",
               views: "$views_count",
               runtime: "$runtime_minutes",
               normalized_views: "$normalized_views"
            }
         }
      }
   },
   {
      // Stage 3: Sort top content within each type
      $addFields: {
         top_content: {
            $slice: [
               {
                  $sortArray: {
                     input: "$top_content",
                     sortBy: { normalized_views: -1 }
                  }
               },
               5
            ]
         }
      }
   },
   {
      // Stage 4: Add efficiency metrics
      $addFields: {
         budget_efficiency: {
            $cond: [
               { $gt: ["$total_budget", 0] },
               { $divide: ["$total_views", "$total_budget"] },
               0
            ]
         }
      }
   },
   {
      // Stage 5: Sort by performance
      $sort: { "avg_normalized_views": -1 }
   }
])
