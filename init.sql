// ============================================
// MONGODB MOVIE ANALYTICS PROJECT
// Complete implementation for all requirements
// ============================================

// ============================================
// 1. COLLECTION/DOCUMENT DESIGN
// ============================================

// Switch to the database
use movieAnalyticsDB

// Create collections with validation schemas
db.createCollection("movies", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["content_id", "title", "genre", "duration_minutes", "release_year", "rating"],
         properties: {
            content_id: { bsonType: "string" },
            title: { bsonType: "string" },
            genre: { bsonType: "array", items: { bsonType: "string" } },
            duration_minutes: { bsonType: "int", minimum: 1 },
            release_year: { bsonType: "int", minimum: 1900 },
            rating: { bsonType: "double", minimum: 0, maximum: 5 },
            views_count: { bsonType: "int", minimum: 0 },
            production_budget: { bsonType: "long", minimum: 0 }
         }
      }
   }
})

// Create users collection for demographics
db.createCollection("users", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["user_id", "age", "gender", "country", "registration_date"],
         properties: {
            user_id: { bsonType: "string" },
            age: { bsonType: "int", minimum: 13 },
            gender: { enum: ["M", "F", "Other"] },
            country: { bsonType: "string" },
            city: { bsonType: "string" },
            registration_date: { bsonType: "date" }
         }
      }
   }
})

// Create viewing sessions collection for analytics
db.createCollection("viewing_sessions", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: ["session_id", "user_id", "content_id", "start_time", "watch_duration"],
         properties: {
            session_id: { bsonType: "string" },
            user_id: { bsonType: "string" },
            content_id: { bsonType: "string" },
            start_time: { bsonType: "date" },
            end_time: { bsonType: "date" },
            watch_duration: { bsonType: "int", minimum: 0 },
            device_type: { enum: ["mobile", "tablet", "desktop", "smart_tv"] },
            quality: { enum: ["SD", "HD", "4K"] },
            completed: { bsonType: "bool" }
         }
      }
   }
})

// ============================================
// 2. DATA INSERTION SCRIPTS
// ============================================

// Insert movies data (based on your JSON structure)
db.movies.insertMany([
   {
      "content_id": "M001",
      "title": "Advanced World",
      "genre": ["Sci-Fi", "Horror", "Drama"],
      "duration_minutes": 179,
      "release_year": 2020,
      "rating": 3.5,
      "views_count": 66721,
      "production_budget": NumberLong(220088717)
   },
   {
      "content_id": "M002",
      "title": "Neural Signal",
      "genre": ["Animation", "Horror"],
      "duration_minutes": 146,
      "release_year": 2021,
      "rating": 2.8,
      "views_count": 11128,
      "production_budget": NumberLong(23593231)
   },
   {
      "content_id": "M003",
      "title": "Digital Dreams",
      "genre": ["Drama", "Thriller"],
      "duration_minutes": 132,
      "release_year": 2022,
      "rating": 4.2,
      "views_count": 89543,
      "production_budget": NumberLong(45000000)
   },
   {
      "content_id": "M004",
      "title": "Quantum Reality",
      "genre": ["Sci-Fi", "Action"],
      "duration_minutes": 158,
      "release_year": 2023,
      "rating": 4.7,
      "views_count": 156789,
      "production_budget": NumberLong(180000000)
   }
])

// Insert sample users data
db.users.insertMany([
   {
      "user_id": "U001",
      "age": 25,
      "gender": "M",
      "country": "Mexico",
      "city": "Mérida",
      "registration_date": new Date("2023-01-15")
   },
   {
      "user_id": "U002",
      "age": 32,
      "gender": "F",
      "country": "USA",
      "city": "New York",
      "registration_date": new Date("2022-08-20")
   },
   {
      "user_id": "U003",
      "age": 19,
      "gender": "Other",
      "country": "Canada",
      "city": "Toronto",
      "registration_date": new Date("2023-03-10")
   },
   {
      "user_id": "U004",
      "age": 45,
      "gender": "F",
      "country": "Mexico",
      "city": "CDMX",
      "registration_date": new Date("2022-12-05")
   },
   {
      "user_id": "U005",
      "age": 28,
      "gender": "M",
      "country": "Spain",
      "city": "Madrid",
      "registration_date": new Date("2023-06-18")
   }
])

// Insert sample viewing sessions
db.viewing_sessions.insertMany([
   {
      "session_id": "S001",
      "user_id": "U001",
      "content_id": "M001",
      "start_time": new Date("2024-01-15T19:30:00Z"),
      "end_time": new Date("2024-01-15T22:29:00Z"),
      "watch_duration": 179,
      "device_type": "smart_tv",
      "quality": "4K",
      "completed": true
   },
   {
      "session_id": "S002",
      "user_id": "U002",
      "content_id": "M002",
      "start_time": new Date("2024-02-10T20:00:00Z"),
      "end_time": new Date("2024-02-10T21:30:00Z"),
      "watch_duration": 90,
      "device_type": "mobile",
      "quality": "HD",
      "completed": false
   },
   {
      "session_id": "S003",
      "user_id": "U003",
      "content_id": "M003",
      "start_time": new Date("2024-03-05T18:45:00Z"),
      "end_time": new Date("2024-03-05T21:57:00Z"),
      "watch_duration": 132,
      "device_type": "desktop",
      "quality": "HD",
      "completed": true
   },
   {
      "session_id": "S004",
      "user_id": "U001",
      "content_id": "M004",
      "start_time": new Date("2024-04-20T21:15:00Z"),
      "end_time": new Date("2024-04-20T23:53:00Z"),
      "watch_duration": 158,
      "device_type": "smart_tv",
      "quality": "4K",
      "completed": true
   }
])

// ============================================
// 3. PROPER INDEXING STRATEGIES
// ============================================

// Indexes for movies collection
db.movies.createIndex({ "content_id": 1 }, { unique: true })
db.movies.createIndex({ "genre": 1 })
db.movies.createIndex({ "release_year": 1 })
db.movies.createIndex({ "rating": -1 })
db.movies.createIndex({ "views_count": -1 })
db.movies.createIndex({ "genre": 1, "release_year": 1, "rating": -1 }) // Compound index

// Indexes for users collection
db.users.createIndex({ "user_id": 1 }, { unique: true })
db.users.createIndex({ "country": 1, "city": 1 })
db.users.createIndex({ "age": 1, "gender": 1 })
db.users.createIndex({ "registration_date": 1 })

// Indexes for viewing_sessions collection
db.viewing_sessions.createIndex({ "session_id": 1 }, { unique: true })
db.viewing_sessions.createIndex({ "user_id": 1, "start_time": -1 })
db.viewing_sessions.createIndex({ "content_id": 1, "start_time": -1 })
db.viewing_sessions.createIndex({ "start_time": -1 }) // For time-series queries
db.viewing_sessions.createIndex({ "device_type": 1, "quality": 1 })

// ============================================
// 4. AGGREGATION PIPELINES (Minimum 3 stages each)
// ============================================

// AGGREGATION 1: User Engagement Metrics by Demographics
print("=== 1. USER ENGAGEMENT METRICS BY DEMOGRAPHICS ===")
db.viewing_sessions.aggregate([
   {
      // Stage 1: Lookup user demographics
      $lookup: {
         from: "users",
         localField: "user_id",
         foreignField: "user_id",
         as: "user_info"
      }
   },
   {
      // Stage 2: Unwind user info and filter valid sessions
      $unwind: "$user_info"
   },
   {
      // Stage 3: Add computed fields for engagement metrics
      $addFields: {
         age_group: {
            $switch: {
               branches: [
                  { case: { $lt: ["$user_info.age", 25] }, then: "18-24" },
                  { case: { $lt: ["$user_info.age", 35] }, then: "25-34" },
                  { case: { $lt: ["$user_info.age", 45] }, then: "35-44" },
                  { case: { $gte: ["$user_info.age", 45] }, then: "45+" }
               ],
               default: "Unknown"
            }
         },
         completion_rate: {
            $cond: [
               { $eq: ["$completed", true] },
               1,
               { $divide: ["$watch_duration", 100] }
            ]
         }
      }
   },
   {
      // Stage 4: Group by demographics and calculate metrics
      $group: {
         _id: {
            age_group: "$age_group",
            gender: "$user_info.gender",
            country: "$user_info.country"
         },
         total_sessions: { $sum: 1 },
         avg_watch_duration: { $avg: "$watch_duration" },
         avg_completion_rate: { $avg: "$completion_rate" },
         unique_users: { $addToSet: "$user_id" }
      }
   },
   {
      // Stage 5: Add user count and sort results
      $addFields: {
         unique_user_count: { $size: "$unique_users" }
      }
   },
   {
      // Stage 6: Sort by engagement metrics
      $sort: { "avg_completion_rate": -1, "total_sessions": -1 }
   }
])

// AGGREGATION 2: Content Performance Analytics
print("\n=== 2. CONTENT PERFORMANCE ANALYTICS ===")
db.movies.aggregate([
   {
      // Stage 1: Lookup viewing sessions for each movie
      $lookup: {
         from: "viewing_sessions",
         localField: "content_id",
         foreignField: "content_id",
         as: "sessions"
      }
   },
   {
      // Stage 2: Add computed performance metrics
      $addFields: {
         total_viewing_sessions: { $size: "$sessions" },
         total_watch_time: { $sum: "$sessions.watch_duration" },
         completed_sessions: {
            $size: {
               $filter: {
                  input: "$sessions",
                  cond: { $eq: ["$$this.completed", true] }
               }
            }
         }
      }
   },
   {
      // Stage 3: Calculate performance ratios and ROI
      $addFields: {
         completion_rate: {
            $cond: [
               { $gt: ["$total_viewing_sessions", 0] },
               { $divide: ["$completed_sessions", "$total_viewing_sessions"] },
               0
            ]
         },
         avg_session_duration: {
            $cond: [
               { $gt: ["$total_viewing_sessions", 0] },
               { $divide: ["$total_watch_time", "$total_viewing_sessions"] },
               0
            ]
         },
         roi_metric: {
            $cond: [
               { $gt: ["$production_budget", 0] },
               { $divide: ["$views_count", "$production_budget"] },
               0
            ]
         }
      }
   },
   {
      // Stage 4: Group by genre for category analysis
      $unwind: "$genre"
   },
   {
      // Stage 5: Calculate genre-based performance
      $group: {
         _id: "$genre",
         movies_in_genre: { $sum: 1 },
         avg_rating: { $avg: "$rating" },
         avg_completion_rate: { $avg: "$completion_rate" },
         total_views: { $sum: "$views_count" },
         avg_roi: { $avg: "$roi_metric" },
         top_movie: { $first: "$$ROOT" }
      }
   },
   {
      // Stage 6: Sort by performance metrics
      $sort: { "avg_completion_rate": -1, "avg_rating": -1 }
   }
])

// AGGREGATION 3: Geographic Distribution Analysis
print("\n=== 3. GEOGRAPHIC DISTRIBUTION ANALYSIS ===")
db.users.aggregate([
   {
      // Stage 1: Lookup viewing sessions for each user
      $lookup: {
         from: "viewing_sessions",
         localField: "user_id",
         foreignField: "user_id",
         as: "sessions"
      }
   },
   {
      // Stage 2: Filter active users and add metrics
      $match: {
         "sessions.0": { $exists: true }
      }
   },
   {
      // Stage 3: Add geographic and activity metrics
      $addFields: {
         total_sessions: { $size: "$sessions" },
         total_watch_time: { $sum: "$sessions.watch_duration" },
         avg_session_duration: { $avg: "$sessions.watch_duration" },
         device_preferences: "$sessions.device_type"
      }
   },
   {
      // Stage 4: Group by geographic location
      $group: {
         _id: {
            country: "$country",
            city: "$city"
         },
         user_count: { $sum: 1 },
         total_sessions: { $sum: "$total_sessions" },
         avg_watch_time_per_user: { $avg: "$total_watch_time" },
         avg_session_duration: { $avg: "$avg_session_duration" },
         all_devices: { $push: "$device_preferences" }
      }
   },
   {
      // Stage 5: Calculate geographic engagement metrics
      $addFields: {
         sessions_per_user: {
            $divide: ["$total_sessions", "$user_count"]
         },
         popular_devices: {
            $reduce: {
               input: "$all_devices",
               initialValue: [],
               in: { $concatArrays: ["$$value", "$$this"] }
            }
         }
      }
   },
   {
      // Stage 6: Sort by user engagement
      $sort: { "sessions_per_user": -1, "user_count": -1 }
   }
])

// AGGREGATION 4: Time-series Viewing Trends
print("\n=== 4. TIME-SERIES VIEWING TRENDS ===")
db.viewing_sessions.aggregate([
   {
      // Stage 1: Add time-based grouping fields
      $addFields: {
         year: { $year: "$start_time" },
         month: { $month: "$start_time" },
         day_of_week: { $dayOfWeek: "$start_time" },
         hour: { $hour: "$start_time" },
         date_only: {
            $dateToString: {
               format: "%Y-%m-%d",
               date: "$start_time"
            }
         }
      }
   },
   {
      // Stage 2: Lookup movie information for genre analysis
      $lookup: {
         from: "movies",
         localField: "content_id",
         foreignField: "content_id",
         as: "movie_info"
      }
   },
   {
      // Stage 3: Unwind movie info and filter
      $unwind: "$movie_info"
   },
   {
      // Stage 4: Group by time periods for trending analysis
      $group: {
         _id: {
            year: "$year",
            month: "$month",
            day_of_week: "$day_of_week",
            hour: "$hour"
         },
         sessions_count: { $sum: 1 },
         total_watch_time: { $sum: "$watch_duration" },
         avg_watch_duration: { $avg: "$watch_duration" },
         unique_users: { $addToSet: "$user_id" },
         unique_movies: { $addToSet: "$content_id" },
         completion_rate: {
            $avg: {
               $cond: [{ $eq: ["$completed", true] }, 1, 0]
            }
         },
         popular_genres: { $push: "$movie_info.genre" },
         device_distribution: { $push: "$device_type" }
      }
   },
   {
      // Stage 5: Add computed trend metrics
      $addFields: {
         unique_user_count: { $size: "$unique_users" },
         unique_movie_count: { $size: "$unique_movies" },
         day_name: {
            $switch: {
               branches: [
                  { case: { $eq: ["$_id.day_of_week", 1] }, then: "Sunday" },
                  { case: { $eq: ["$_id.day_of_week", 2] }, then: "Monday" },
                  { case: { $eq: ["$_id.day_of_week", 3] }, then: "Tuesday" },
                  { case: { $eq: ["$_id.day_of_week", 4] }, then: "Wednesday" },
                  { case: { $eq: ["$_id.day_of_week", 5] }, then: "Thursday" },
                  { case: { $eq: ["$_id.day_of_week", 6] }, then: "Friday" },
                  { case: { $eq: ["$_id.day_of_week", 7] }, then: "Saturday" }
               ],
               default: "Unknown"
            }
         }
      }
   },
   {
      // Stage 6: Sort by time for chronological trends
      $sort: {
         "_id.year": 1,
         "_id.month": 1,
         "_id.day_of_week": 1,
         "_id.hour": 1
      }
   }
])

// ============================================
// 5. PERFORMANCE OPTIMIZATION QUERIES
// ============================================

// Explain query plans for performance analysis
print("\n=== PERFORMANCE ANALYSIS ===")

// Check index usage for complex query
db.viewing_sessions.explain("executionStats").aggregate([
   {
      $match: {
         start_time: {
            $gte: new Date("2024-01-01"),
            $lt: new Date("2024-12-31")
         }
      }
   },
   {
      $lookup: {
         from: "users",
         localField: "user_id",
         foreignField: "user_id",
         as: "user_info"
      }
   },
   {
      $group: {
         _id: "$user_info.country",
         total_sessions: { $sum: 1 }
      }
   }
])

// ============================================
// 6. DATA VALIDATION AND MAINTENANCE
// ============================================

// Validate data integrity
print("\n=== DATA VALIDATION ===")

// Check for orphaned viewing sessions (sessions without valid users)
db.viewing_sessions.aggregate([
   {
      $lookup: {
         from: "users",
         localField: "user_id",
         foreignField: "user_id",
         as: "user_check"
      }
   },
   {
      $match: {
         user_check: { $size: 0 }
      }
   },
   {
      $count: "orphaned_sessions"
   }
])

// Check for orphaned viewing sessions (sessions without valid movies)
db.viewing_sessions.aggregate([
   {
      $lookup: {
         from: "movies",
         localField: "content_id",
         foreignField: "content_id",
         as: "movie_check"
      }
   },
   {
      $match: {
         movie_check: { $size: 0 }
      }
   },
   {
      $count: "sessions_with_invalid_movies"
   }
])

// ============================================
// 7. UTILITY QUERIES FOR ANALYSIS
// ============================================

// Get collection statistics
print("\n=== COLLECTION STATISTICS ===")
db.movies.stats()
db.users.stats()  
db.viewing_sessions.stats()

// Get index information
print("\n=== INDEX INFORMATION ===")
db.movies.getIndexes()
db.users.getIndexes()
db.viewing_sessions.getIndexes()

print("\n=== MONGODB SETUP COMPLETE ===")
print("All collections created with proper validation schemas")
print("Sample data inserted with appropriate indexing")
print("Four comprehensive aggregation pipelines implemented")
print("Performance optimization strategies applied")