# Normalization Justification and Optimization Documentation

## Activity 1.1: Relational Model - Streaming Platform

### Database Normalization Analysis

#### Current Normal Form: Third Normal Form (3NF)

Our streaming platform database is designed to be in **Third Normal Form (3NF)**, which ensures:
- **1NF**: All attributes contain atomic values (no repeating groups)
- **2NF**: All non-key attributes are fully functionally dependent on the primary key
- **3NF**: No transitive dependencies exist between non-key attributes

#### Table Analysis

##### Users Table
- **Primary Key**: `user_id` (VARCHAR(50))
- **Functional Dependencies**:
  - `user_id` → `age`, `country`, `subscription_type`, `registration_date`, `total_watch_time_hours`
- **Normalization Status**: 3NF
  - All attributes are directly dependent on the primary key
  - No transitive dependencies exist
  - No partial dependencies (single-column primary key)

##### Viewing Sessions Table
- **Primary Key**: `session_id` (VARCHAR(50))
- **Foreign Key**: `user_id` (references users.user_id)
- **Functional Dependencies**:
  - `session_id` → `user_id`, `content_id`, `watch_date`, `watch_duration_minutes`, `completion_percentage`, `device_type`, `quality_level`
- **Normalization Status**: 3NF
  - All attributes are directly dependent on the primary key
  - Foreign key relationship properly maintains referential integrity
  - No transitive dependencies

#### Why Not Higher Normal Forms?

**Boyce-Codd Normal Form (BCNF)**: Not applicable as we don't have composite candidate keys or overlapping candidate keys.

**Fourth Normal Form (4NF)**: Not applicable as we don't have multi-valued dependencies.

**Fifth Normal Form (5NF)**: Not applicable as we don't have join dependencies.

### Optimization Strategies

#### 1. Indexing Strategy
```sql
-- Primary indexes (automatically created)
PRIMARY KEY (user_id)
PRIMARY KEY (session_id)

-- Foreign key index
INDEX idx_user_id (user_id)

-- Query optimization indexes
INDEX idx_watch_date (watch_date)
INDEX idx_content_id (content_id)
INDEX idx_device_type (device_type)
INDEX idx_users_country (country)
INDEX idx_users_subscription (subscription_type)
INDEX idx_sessions_quality (quality_level)
INDEX idx_sessions_completion (completion_percentage)
```

#### 2. Data Type Optimization
- **VARCHAR lengths**: Carefully sized to balance storage efficiency and flexibility
- **DECIMAL precision**: Optimized for watch time and completion percentage storage
- **INT for durations**: Efficient storage for minute-based durations
- **DATE vs DATETIME**: Using DATE for dates without time requirements

#### 3. Constraint Optimization
- **CHECK constraints**: Ensure data integrity at database level
- **FOREIGN KEY constraints**: Maintain referential integrity with CASCADE options
- **NOT NULL constraints**: Prevent invalid data entry

#### 4. Denormalization Considerations

**Current Design Benefits**:
- Maintains data integrity
- Reduces storage redundancy
- Simplifies data maintenance
- Supports complex analytical queries

**Potential Denormalization** (if needed for performance):
- Could add `user_country` to `viewing_sessions` to avoid JOINs in frequent queries
- Could add `subscription_type` to `viewing_sessions` for subscription-based analytics
- **Trade-off**: Increased storage vs. query performance

#### 5. Query Performance Optimizations

**Join Optimization**:
- Foreign key indexes ensure efficient JOINs between users and viewing_sessions
- Composite indexes on frequently queried column combinations

**Aggregation Optimization**:
- Indexes on columns used in GROUP BY clauses
- Indexes on columns used in WHERE clauses for filtering

**Analytical Query Support**:
- Window functions supported for ranking and statistical analysis
- Subquery optimization through proper indexing

### Performance Monitoring Recommendations

1. **Query Execution Plans**: Monitor slow queries and optimize accordingly
2. **Index Usage**: Track index utilization and remove unused indexes
3. **Storage Growth**: Monitor table size growth and consider partitioning for large datasets
4. **Connection Pooling**: Implement connection pooling for high-concurrency scenarios

### Scalability Considerations

**Horizontal Scaling**:
- Database sharding by user_id or geographic region
- Read replicas for analytical queries

**Vertical Scaling**:
- Current design supports vertical scaling with proper indexing
- Consider partitioning for tables exceeding millions of rows

**Data Archiving**:
- Archive old viewing sessions based on date
- Maintain aggregated statistics for historical analysis

### Conclusion

The current database design achieves an optimal balance between:
- **Normalization**: 3NF ensures data integrity and reduces redundancy
- **Performance**: Strategic indexing supports efficient querying
- **Maintainability**: Clear relationships and constraints simplify maintenance
- **Scalability**: Design supports both horizontal and vertical scaling strategies

This design provides a solid foundation for a streaming platform that can handle both transactional operations and complex analytical queries while maintaining data integrity and performance.
