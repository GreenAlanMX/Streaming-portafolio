# Entity-Relationship Diagram (ERD)
## Streaming Platform Database Schema


### Relationship Description

**One-to-Many Relationship**: `users` ← `viewing_sessions`
- **Cardinality**: One user can have many viewing sessions
- **Foreign Key**: `viewing_sessions.user_id` references `users.user_id`
- **Constraint**: CASCADE DELETE and UPDATE for referential integrity

### Entity Descriptions

#### Users Entity
- **Purpose**: Stores user profile information and subscription details
- **Primary Key**: `user_id` - Unique identifier for each user
- **Attributes**:
  - `age`: User's age with validation (0-150)
  - `country`: User's country of residence
  - `subscription_type`: Type of subscription (free, premium, family, student)
  - `registration_date`: When the user registered
  - `total_watch_time_hours`: Cumulative watch time across all sessions

#### Viewing Sessions Entity
- **Purpose**: Records individual viewing sessions with detailed metrics
- **Primary Key**: `session_id` - Unique identifier for each session
- **Foreign Key**: `user_id` - Links to the user who initiated the session
- **Attributes**:
  - `content_id`: Identifier for the content being watched
  - `watch_date`: Date of the viewing session
  - `watch_duration_minutes`: Length of the session in minutes
  - `completion_percentage`: Percentage of content completed (0-100%)
  - `device_type`: Device used for viewing (mobile, tablet, desktop, tv, smart_tv)
  - `quality_level`: Streaming quality (SD, HD, FHD, 4K, 8K)

### Business Rules

1. **User Registration**: Each user must have a unique ID and valid subscription type
2. **Session Tracking**: Every viewing session must be associated with a valid user
3. **Data Validation**: Age, completion percentage, and watch duration have range constraints
4. **Referential Integrity**: Users cannot be deleted if they have active viewing sessions (CASCADE DELETE)
5. **Audit Trail**: Both tables include created_at and updated_at timestamps

### Indexes and Performance

- **Primary Indexes**: Automatically created on primary keys
- **Foreign Key Index**: `idx_user_id` on `viewing_sessions.user_id`
- **Query Optimization Indexes**: On frequently queried columns (date, device_type, quality_level, etc.)
- **Composite Indexes**: For complex analytical queries

This ERD represents a normalized, efficient design for tracking user behavior and viewing patterns in a streaming platform.
