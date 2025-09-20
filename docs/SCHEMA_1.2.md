# Schema Documentation — Activity 1.2 (NoSQL Model)
## Unified Collection Approach

## Collection Design

### content (Unified Collection)
- `content_id` (string, unique) — business key
- `title` (string)
- `genre` (array<string>)
- `content_type` (string: "movie"/"series") — discriminator field
- `rating` (double, 0-5)
- `views_count` (int) — unified field name
- `production_budget` (long)

**Movie-specific fields:**
- `duration_minutes` (int)
- `release_year` (int)

**Series-specific fields:**
- `seasons` (int)
- `episodes_per_season` (array<int>)
- `avg_episode_duration` (int)
- `total_episodes` (int) — calculated field
- `total_runtime_minutes` (int) — calculated field

**Indexes**
- `{content_id:1}` unique
- `{content_type:1}` — discriminator index
- `{genre:1}` multikey
- `{rating:-1}`
- `{views_count:-1}`
- `{title:"text"}` — full-text search
- `{content_type:1, genre:1, rating:-1}` composite
- `{content_type:1, views_count:-1}` composite
- `{genre:1, release_year:1}` composite
- `{content_type:1, release_year:1}` composite
- `{production_budget:1}`
- `{seasons:1}`
- `{total_episodes:1}`

## Unified Design Strategy

### Benefits of Unified Collection
- **Cross-Content Analytics**: Easy comparison between movies and series
- **Simplified Queries**: No joins or $unionWith required
- **Better Performance**: Single collection queries are faster
- **Unified Indexing**: Fewer indexes to manage and optimize
- **Scalability**: Single collection is easier to scale

### Data Consistency
- **Unified Field Names**: `views_count` for both movies and series
- **Calculated Fields**: `total_episodes` and `total_runtime_minutes` for series
- **Type Discrimination**: `content_type` field for filtering
- **Schema Validation**: JSON schema ensures data integrity

## Required Aggregations (Unified Approach)

### 1. Content Engagement Metrics by Genre and Content Type
- **Collection**: `content`
- **Approach**: Group by `genre` and `content_type`
- **Metrics**: Engagement score, efficiency metrics, high-performing content
- **Stages**: 5 stages with genre unwinding and performance calculations

### 2. Unified Content Performance Analytics
- **Collection**: `content`
- **Approach**: Cross-content type performance analysis
- **Metrics**: Performance score, ROI, genre-based metrics
- **Stages**: 5 stages with unified performance calculations

### 3. Content Distribution Analysis by Release Year
- **Collection**: `content`
- **Approach**: Year-based distribution with content type breakdown
- **Metrics**: Genre distribution, content trends over time
- **Stages**: 6 stages with decade analysis and distribution calculations

### 4. Unified Content Trends Analysis by Release Year
- **Collection**: `content`
- **Approach**: Time-series analysis for both content types
- **Metrics**: Rating evolution, view trends, performance indicators
- **Stages**: 5 stages with trend analysis and percentage calculations

## Query Optimization Notes

### Index Usage
- **Content Type Filtering**: Use `{content_type:1}` index for type-specific queries
- **Genre Analysis**: Use `{genre:1}` multikey index for genre-based aggregations
- **Performance Queries**: Use compound indexes for complex filtering and sorting
- **Text Search**: Use `{title:"text"}` index for title-based searches

### Aggregation Optimization
- **Early Filtering**: Use `$match` with `content_type` early in pipelines
- **Index Alignment**: Ensure sort keys align with index order
- **Memory Management**: Monitor memory usage for large unified collection
- **Pipeline Efficiency**: Minimize data movement between stages

### Performance Considerations
- **Unified Queries**: Single collection queries are faster than joins
- **Compound Indexes**: Use content_type in compound indexes for efficiency
- **Covered Queries**: Design indexes to avoid document retrieval when possible
- **Sharding Strategy**: Consider sharding by content_type for very large datasets

## Migration from Separate Collections

### Data Conversion
1. **Field Standardization**: Rename `total_views` to `views_count` for series
2. **Calculated Fields**: Add `total_episodes` and `total_runtime_minutes` for series
3. **Type Discrimination**: Add `content_type` field to all documents
4. **Schema Validation**: Apply unified JSON schema validation

### Query Migration
1. **Remove Joins**: Replace `$unionWith` with single collection queries
2. **Add Type Filtering**: Use `content_type` field for type-specific queries
3. **Optimize Indexes**: Update indexes for unified collection structure
4. **Update Aggregations**: Modify pipelines for single collection approach

## Future Enhancements

### Advanced Features
- **Partial Indexes**: Create content-type specific partial indexes
- **Change Streams**: Implement real-time analytics for unified collection
- **Sharding**: Consider sharding by content_type for horizontal scaling
- **Caching**: Implement Redis caching for frequently accessed unified data

### Performance Monitoring
- **Query Analysis**: Monitor performance of unified vs separate collection queries
- **Index Usage**: Track index utilization for optimization
- **Memory Usage**: Monitor aggregation memory consumption
- **Scalability Metrics**: Measure performance as collection grows
