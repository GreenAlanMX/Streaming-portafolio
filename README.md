# Activity 1.2: NoSQL Database Model (MongoDB)
## Streaming Platform Analytics - Unified Collection Approach

This activity implements a NoSQL database model using MongoDB for a streaming platform, using a **unified collection approach** for optimal scalability and query performance.

## Key Design Decision: Unified Collection

Instead of separate `movies` and `series` collections, we use a single `content` collection with a `content_type` field. This approach provides:

- **Better Scalability**: Single collection to manage and index
- **Unified Queries**: Cross-content type analytics without joins
- **Simplified Maintenance**: One schema to maintain and validate
- **Performance Optimization**: Fewer collections to query and aggregate

## Why JavaScript in MongoDB?

### Technical Justification

**JavaScript is the native language of MongoDB** for several fundamental reasons:

#### 1. **Native Design and Architecture**
- MongoDB was designed from the ground up to work with JavaScript
- The MongoDB Shell (`mongosh`) is built on JavaScript Engine
- It's the most natural and efficient way to interact with MongoDB

#### 2. **Data Structure Compatibility**
```javascript
// JavaScript handles JSON objects and arrays natively
{
  content_id: "M001",
  title: "Advanced World",
  genre: ["Sci-Fi", "Horror", "Drama"],  // Array handling
  rating: 3.5,
  views_count: 66721
}
```

#### 3. **Complex Aggregation Pipelines**
```javascript
// JavaScript enables sophisticated multi-stage aggregations
db.content.aggregate([
  { $unwind: "$genre" },                    // Stage 1: Array decomposition
  { $addFields: { engagement_score: { ... } } }, // Stage 2: Computed fields
  { $group: { _id: "$genre", count: { $sum: 1 } } }, // Stage 3: Grouping
  { $sort: { count: -1 } },                 // Stage 4: Sorting
  { $limit: 10 }                           // Stage 5: Limiting
])
```

#### 4. **Flexible Data Manipulation**
- **Array Operations**: Native support for `$unwind`, `$push`, `$addToSet`
- **Object Nesting**: Natural handling of embedded documents
- **Conditional Logic**: `$cond`, `$switch` for complex business rules
- **Mathematical Operations**: Built-in functions for calculations

#### 5. **Unified Collection Benefits**
```javascript
// JavaScript allows seamless handling of different content types
{
  $addFields: {
    runtime_minutes: {
      $cond: [
        { $eq: ["$content_type", "movie"] },
        "$duration_minutes",        // Movie runtime
        "$total_runtime_minutes"    // Series total runtime
      ]
    }
  }
}
```

#### 6. **Performance and Efficiency**
- **No Translation Layer**: Direct execution without language conversion
- **Optimized Execution**: MongoDB's aggregation engine is optimized for JavaScript syntax
- **Memory Efficiency**: Native handling reduces memory overhead
- **Index Utilization**: Better index usage with native JavaScript queries

### Comparison with Alternatives

#### SQL (Relational Databases)
```sql
-- SQL requires rigid table structure
CREATE TABLE movies (
  id INT PRIMARY KEY,
  title VARCHAR(255),
  genre VARCHAR(255)  -- Single genre only
);
```
**Limitations**: No native array support, rigid schema, complex joins for related data

#### Other Programming Languages
```python
# Python with PyMongo
collection.find({"age": {"$gt": 18}})
```
**Trade-offs**: Requires translation layer, less direct control over aggregation pipelines

#### MongoDB Compass (GUI)
**Advantages**: Visual interface, automatic code generation
**Limitations**: Less flexible for complex logic, not suitable for automation

## Deliverables

### 1. Collection/Document Design for MongoDB
- **Files**: 
  - `scripts/init_unified_content_auth.js` - Unified collection creation with validation schemas
  - `docs/SCHEMA_1.2.md` - Complete schema documentation
- **Collection**: `content` - Unified collection for both movies and series
- **Features**:
  - JSON schema validation for data integrity
  - `content_type` field to distinguish movies from series
  - Optimized indexing for unified queries
  - Support for both movie and series-specific fields

### 2. Data Insertion Scripts with Proper Indexing
- **File**: `scripts/init_unified_content_auth.js`
- **Features**:
  - Collection creation with authentication support
  - Comprehensive indexing strategy for both content types
  - Text search indexes for title searching
  - Compound indexes for cross-content type queries
- **Indexes Created**:
  - Unique indexes on content_id
  - Content type and genre indexes
  - Performance indexes for rating, views, and budget
  - Compound indexes for complex queries

### 3. Aggregation Pipelines (Minimum 3 Stages Each)
- **File**: `scripts/aggregation_pipelines_unified.js`
- **Required Aggregations**:

#### 3.1 User Engagement Metrics by Demographics
- **Adapted as**: Content Engagement Metrics by Genre and Content Type
- **Stages**: 5 stages
- **Features**: Genre analysis, engagement scoring, efficiency metrics for both content types

#### 3.2 Content Performance Analytics
- **Stages**: 5 stages
- **Features**: Unified performance scoring, ROI analysis, genre-based metrics for movies and series

#### 3.3 Geographic Distribution Analysis
- **Adapted as**: Content Distribution Analysis by Release Year
- **Stages**: 6 stages
- **Features**: Decade analysis, genre distribution over time, content type trends

#### 3.4 Time-series Viewing Trends
- **Adapted as**: Unified Content Trends Analysis by Release Year
- **Stages**: 5 stages
- **Features**: Rating evolution, view trends, performance indicators over time for both content types

#### 3.5 Bonus: Unified Efficiency Analysis
- **Stages**: 6 stages
- **Features**: Cross-content type efficiency metrics, views per minute, budget efficiency

#### 3.6 Bonus: Cross-Content Type Comparison
- **Stages**: 5 stages
- **Features**: Direct comparison between movies and series performance

### 4. Performance Comparison Between Relational and NoSQL Approaches
- **File**: `docs/Performance_Comparison_1.2.md`
- **Content**:
  - Detailed comparison of SQL vs MongoDB approaches
  - Unified collection vs separate collections analysis
  - Performance metrics and benchmarks
  - Use case recommendations

## Unified Data Structure

### Content Collection (Movies and Series)
```javascript
// Movie Example
{
  content_id: "M001",
  title: "Advanced World",
  genre: ["Sci-Fi", "Horror", "Drama"],
  content_type: "movie",
  duration_minutes: 179,
  release_year: 2020,
  rating: 3.5,
  views_count: 66721,
  production_budget: 220088717
}

// Series Example
{
  content_id: "S001",
  title: "The Investigators",
  genre: ["Crime"],
  content_type: "series",
  seasons: 7,
  episodes_per_season: [8, 14, 7, 17, 12, 13, 18],
  avg_episode_duration: 50,
  total_episodes: 89,
  total_runtime_minutes: 4450,
  rating: 1.3,
  views_count: 50315,
  production_budget: 44041273
}
```

## Usage Instructions

### 1. Setup MongoDB with Docker
```bash
# Start MongoDB container
docker-compose up -d

# Verify container is running
docker ps
```

### 2. Initialize Unified Database and Collection
```bash
# Run the unified initialization script with authentication
docker exec -i video_streaming_mongodb mongosh -u admin -p admin123 --authenticationDatabase admin video_streaming_platform < scripts/init_unified_content_auth.js
```

### 3. Convert and Load Complete Dataset
```bash
# Convert existing content.json to unified format
node scripts/convert_json_to_unified.js

# Import the unified content
mongoimport --db video_streaming_platform --collection content --file data/content_unified.json --jsonArray
```

### 4. Run Unified Aggregation Pipelines
```bash
# Execute unified aggregation pipelines
docker exec -i video_streaming_mongodb mongosh -u admin -p admin123 --authenticationDatabase admin video_streaming_platform < scripts/aggregation_pipelines_unified.js
```

### 5. Test Individual Queries
```bash
# Test basic aggregation
docker exec -i video_streaming_mongodb mongosh -u admin -p admin123 --authenticationDatabase admin video_streaming_platform --eval "db.content.aggregate([{\$group: {_id: '\$content_type', count: {\$sum: 1}}}])"
```

## File Structure
```
Streaming-portafolio/
├── README.md
├── scripts/
│   ├── init_unified_content_auth.js
│   ├── aggregation_pipelines_unified.js
│   ├── convert_json_to_unified.js
│   └── import_unified_content.js
├── data/
│   ├── content.json (original)
│   └── content_unified.json (converted)
├── docs/
│   ├── SCHEMA_1.2.md
│   └── Performance_Comparison_1.2.md
└── docker-compose.yml
```

## Technical Specifications

### Database: MongoDB
- **Version**: 7.0+
- **Database Name**: video_streaming_platform
- **Collection**: content (unified)
- **Authentication**: admin/admin123

### Key Features
- **Unified Schema**: Single collection for movies and series
- **Schema Validation**: JSON schema validation for data integrity
- **Optimized Indexing**: Indexes designed for cross-content type queries
- **Aggregation Pipelines**: Complex analytics with unified data access
- **Performance Optimization**: Compound indexes and query optimization

### Query Capabilities
- **Cross-Content Type Queries**: Compare movies and series in single queries
- **Unified Analytics**: Genre analysis across both content types
- **Performance Metrics**: ROI and efficiency calculations for all content
- **Text Search**: Full-text search across all content titles
- **Complex Aggregations**: Multi-stage pipelines for comprehensive analytics

## Performance Advantages of Unified Approach

### Scalability Benefits
- **Single Collection Management**: Easier to scale and maintain
- **Unified Indexing**: Fewer indexes to manage and optimize
- **Cross-Content Analytics**: No joins required for comparative analysis
- **Simplified Queries**: Single collection queries are faster

### Query Performance
- **No Joins**: Direct access to all content in single collection
- **Optimized Indexes**: Compound indexes for cross-content type queries
- **Efficient Aggregations**: Single collection aggregations are faster
- **Better Caching**: Single collection benefits from MongoDB's caching

### Maintenance Benefits
- **Single Schema**: One validation schema to maintain
- **Unified Operations**: Backup, restore, and maintenance on single collection
- **Simplified Monitoring**: Single collection to monitor and optimize
- **Easier Development**: Single collection reduces complexity

## Comparison with Separate Collections

### Unified Collection Advantages
- **Cross-Content Analytics**: Easy comparison between movies and series
- **Simplified Queries**: No need for $unionWith or complex joins
- **Better Performance**: Single collection queries are faster
- **Easier Maintenance**: One collection to manage

### Separate Collections Advantages
- **Schema Clarity**: Clear separation of movie vs series fields
- **Storage Efficiency**: Only store relevant fields for each type
- **Validation Specificity**: Type-specific validation rules

## Future Enhancements
- **Real-time Analytics**: Implement change streams for live metrics
- **Sharding Strategy**: Horizontal scaling for large unified collection
- **Advanced Indexing**: Partial indexes for content-type specific queries
- **API Integration**: RESTful API for unified content access
- **Machine Learning**: Unified feature extraction for recommendation systems

## Why JavaScript in MongoDB?

### Technical Justification

JavaScript is used in MongoDB for several fundamental reasons:

#### 1. Native Language Design
- **MongoDB Shell** (`mongosh`) is built on JavaScript Engine
- **Native Support**: JavaScript is the original and native language for MongoDB operations
- **Built-in Integration**: No translation layer needed between queries and data manipulation

#### 2. Data Structure Compatibility
```javascript
// JavaScript naturally handles MongoDB's document structure
{
  _id: ObjectId("..."),
  title: "Movie Title",
  genre: ["Action", "Drama"],  // Native array support
  ratings: {                   // Native object nesting
    imdb: 8.5,
    rotten: 85
  }
}
```

#### 3. Aggregation Pipeline Power
```javascript
// Complex aggregations with multiple stages
db.content.aggregate([
  { $unwind: "$genre" },           // Stage 1: Array decomposition
  { $addFields: { ... } },         // Stage 2: Field computation
  { $group: { ... } },             // Stage 3: Data grouping
  { $sort: { ... } },              // Stage 4: Result ordering
  { $limit: 10 }                   // Stage 5: Result limiting
])
```

#### 4. Flexible Data Manipulation
```javascript
// Conditional logic for unified collections
{
  $addFields: {
    runtime_minutes: {
      $cond: [
        { $eq: ["$content_type", "movie"] },
        "$duration_minutes",        // Movie field
        "$total_runtime_minutes"    // Series field
      ]
    }
  }
}
```

#### 5. Array and Object Operations
```javascript
// Native array handling
db.content.aggregate([
  { $unwind: "$genre" },  // Decompose genre arrays
  { $group: { _id: "$genre", count: { $sum: 1 } } }
])
```

### Comparison with Alternatives

#### JavaScript vs SQL
- **SQL**: Rigid table structure, requires joins for related data
- **JavaScript**: Flexible document structure, embedded objects and arrays
- **Result**: JavaScript better suited for NoSQL document databases

#### JavaScript vs Other Languages
- **Python/Java/C#**: Require drivers and translation layers
- **JavaScript**: Direct execution in MongoDB shell
- **Result**: JavaScript provides immediate, interactive database access

### Practical Benefits

#### Development and Testing
- **Interactive Shell**: Immediate query testing and data exploration
- **Rapid Prototyping**: Quick aggregation pipeline development
- **Debugging**: Step-by-step pipeline execution and result inspection

#### Administration
- **Database Management**: Schema validation, index creation, user management
- **Data Migration**: Script-based data transformation and import/export
- **Performance Analysis**: Query optimization and execution plan analysis

### Modern Alternatives

#### MongoDB Compass (GUI)
- Visual interface for query building
- Generates JavaScript code automatically
- Suitable for data exploration and visualization

#### Application Drivers
- **Python**: PyMongo for application development
- **Java**: MongoDB Java Driver for enterprise applications
- **Node.js**: Native JavaScript for web applications

### Conclusion

JavaScript in MongoDB provides:
- **Native Performance**: No translation overhead
- **Flexible Syntax**: Natural handling of JSON documents
- **Powerful Aggregations**: Complex data processing capabilities
- **Interactive Development**: Immediate feedback and testing
- **Administrative Tools**: Complete database management capabilities

For this project, JavaScript enables efficient implementation of complex aggregation pipelines that analyze unified movie and series data with optimal performance and maintainability.