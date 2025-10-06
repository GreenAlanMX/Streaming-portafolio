# Activity 1.1: Relational Model - Streaming Platform

## Overview
This activity implements a relational database model for a streaming platform, including ER diagram, SQL scripts, complex queries, and normalization documentation.

## Deliverables

### 1. ER Diagram of the Relational Database
- **File**: `diagrams/erd_diagram.md`
- **Description**: Complete Entity-Relationship diagram showing the relationship between `users` and `viewing_sessions` tables
- **Relationship**: One-to-Many (1:N) between users and viewing sessions

### 2. SQL Scripts for Table Creation with Proper Constraints
- **Files**: 
  - `sql/01_create_tables.sql` - Table creation with constraints and indexes
  - `sql/02_sample_data.sql` - Sample data for testing
- **Features**:
  - Primary and foreign key constraints
  - CHECK constraints for data validation
  - Indexes for performance optimization
  - CASCADE DELETE/UPDATE for referential integrity

### 3. Complex SQL Queries for Analysis (6 queries with joins, subqueries, and aggregations)
- **File**: `queries/analysis_queries.sql`
- **Queries Include**:
  1. Average watch time by subscription type with user demographics
  2. Top performing content by device type and quality
  3. User engagement analysis with ranking
  4. Monthly growth analysis with year-over-year comparison
  5. Device preference analysis with user segmentation
  6. Content performance with user demographics correlation
- **Features**: JOINs, subqueries, window functions, aggregations, CASE statements

### 4. Normalization Justification and Optimization Documentation
- **File**: `docs/normalization_justification.md`
- **Content**:
  - Normalization analysis (3NF)
  - Optimization strategies
  - Indexing strategy
  - Performance monitoring recommendations
  - Scalability considerations

## Database Schema

### Tables
1. **users**: User profile and subscription information
2. **viewing_sessions**: Individual viewing session records

### Key Features
- **Normalization**: Third Normal Form (3NF)
- **Constraints**: Comprehensive data validation
- **Indexes**: Optimized for analytical queries
- **Relationships**: Proper foreign key relationships with CASCADE options

## Usage

### Setup Database
```bash
# Run table creation script
mysql -u username -p < sql/01_create_tables.sql

# Insert sample data
mysql -u username -p < sql/02_sample_data.sql
```

### Run Analysis Queries
```bash
# Execute analysis queries
mysql -u username -p streaming_platform < queries/analysis_queries.sql
```

```

## Technical Specifications
- **Database**: MySQL/PostgreSQL compatible
- **Normal Form**: Third Normal Form (3NF)
- **Indexes**: 8 strategic indexes for performance
- **Constraints**: 12 CHECK constraints for data integrity
- **Queries**: 6 complex analytical queries with advanced SQL features
