// ============================================
// MONGODB UNIFIED CONTENT ANALYTICS PROJECT
// Single collection approach for movies and series
// ============================================

// Switch to the database
use movieAnalyticsDB

// ============================================
// 1. UNIFIED COLLECTION DESIGN
// ============================================

// Create unified content collection
db.createCollection("content", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["content_id", "title", "genre", "content_type", "rating"],
         properties: {
            content_id: { bsonType: "string" },
            title: { bsonType: "string" },
            genre: { bsonType: "array", items: { bsonType: "string" } },
            content_type: { enum: ["movie", "series"] },
            rating: { bsonType: "double", minimum: 0, maximum: 5 },
            views_count: { bsonType: "int", minimum: 0 },
            production_budget: { bsonType: "long", minimum: 0 },
            // Movie-specific fields
            duration_minutes: { bsonType: "int", minimum: 1 },
            release_year: { bsonType: "int", minimum: 1900 },
            // Series-specific fields
            seasons: { bsonType: "int", minimum: 1 },
            episodes_per_season: { bsonType: "array", items: { bsonType: "int" } },
            avg_episode_duration: { bsonType: "int", minimum: 1 },
            total_episodes: { bsonType: "int", minimum: 1 },
            total_runtime_minutes: { bsonType: "int", minimum: 1 }
         }
      }
   }
})

// ============================================
// 2. OPTIMIZED INDEXES FOR UNIFIED COLLECTION
// ============================================

// Primary indexes
db.content.createIndex({ "content_id": 1 }, { unique: true })
db.content.createIndex({ "content_type": 1 })
db.content.createIndex({ "genre": 1 })
db.content.createIndex({ "rating": -1 })
db.content.createIndex({ "views_count": -1 })
db.content.createIndex({ "title": "text" })

// Compound indexes for common queries
db.content.createIndex({ "content_type": 1, "genre": 1, "rating": -1 })
db.content.createIndex({ "content_type": 1, "views_count": -1 })
db.content.createIndex({ "genre": 1, "release_year": 1 })
db.content.createIndex({ "content_type": 1, "release_year": 1 })

// Performance indexes
db.content.createIndex({ "production_budget": 1 })
db.content.createIndex({ "seasons": 1 })
db.content.createIndex({ "total_episodes": 1 })

print("Unified collection and indexes created successfully!")
print("Ready to import data from content.json using the conversion script.")
