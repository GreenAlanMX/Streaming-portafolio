-- Complex SQL Queries for Analysis
-- Activity 1.1: Relational Model
-- Minimum 5 queries with joins, subqueries, and aggregations

USE streaming_platform;

-- Query 1: Average watch time by subscription type with user demographics
-- Uses JOIN, GROUP BY, and aggregation functions
SELECT 
    u.subscription_type,
    u.country,
    COUNT(DISTINCT u.user_id) as total_users,
    AVG(vs.watch_duration_minutes) as avg_session_duration,
    AVG(vs.completion_percentage) as avg_completion_rate,
    SUM(vs.watch_duration_minutes) as total_watch_time_minutes
FROM users u
JOIN viewing_sessions vs ON u.user_id = vs.user_id
GROUP BY u.subscription_type, u.country
ORDER BY avg_completion_rate DESC;

-- Query 2: Top performing content by device type and quality
-- Uses JOIN, subquery, and multiple aggregations
SELECT 
    vs.content_id,
    vs.device_type,
    vs.quality_level,
    COUNT(*) as total_sessions,
    AVG(vs.completion_percentage) as avg_completion,
    AVG(vs.watch_duration_minutes) as avg_duration
FROM viewing_sessions vs
WHERE vs.content_id IN (
    SELECT content_id 
    FROM viewing_sessions 
    GROUP BY content_id 
    HAVING COUNT(*) >= 2
)
GROUP BY vs.content_id, vs.device_type, vs.quality_level
HAVING avg_completion > 70
ORDER BY avg_completion DESC, total_sessions DESC;

-- Query 3: User engagement analysis with ranking
-- Uses window functions, subqueries, and complex aggregations
SELECT 
    u.user_id,
    u.age,
    u.subscription_type,
    u.total_watch_time_hours,
    COUNT(vs.session_id) as total_sessions,
    AVG(vs.completion_percentage) as avg_completion,
    RANK() OVER (ORDER BY u.total_watch_time_hours DESC) as watch_time_rank,
    RANK() OVER (ORDER BY COUNT(vs.session_id) DESC) as session_count_rank
FROM users u
LEFT JOIN viewing_sessions vs ON u.user_id = vs.user_id
GROUP BY u.user_id, u.age, u.subscription_type, u.total_watch_time_hours
HAVING total_sessions > 0
ORDER BY watch_time_rank;

-- Query 4: Monthly growth analysis with year-over-year comparison
-- Uses date functions, subqueries, and aggregations
SELECT 
    YEAR(vs.watch_date) as watch_year,
    MONTH(vs.watch_date) as watch_month,
    COUNT(DISTINCT vs.user_id) as active_users,
    COUNT(vs.session_id) as total_sessions,
    AVG(vs.watch_duration_minutes) as avg_duration,
    SUM(vs.watch_duration_minutes) as total_watch_time,
    LAG(COUNT(DISTINCT vs.user_id)) OVER (ORDER BY YEAR(vs.watch_date), MONTH(vs.watch_date)) as prev_month_users
FROM viewing_sessions vs
WHERE vs.watch_date >= '2023-01-01'
GROUP BY YEAR(vs.watch_date), MONTH(vs.watch_date)
ORDER BY watch_year, watch_month;

-- Query 5: Device preference analysis with user segmentation
-- Uses CASE statements, subqueries, and complex aggregations
SELECT 
    u.subscription_type,
    u.country,
    vs.device_type,
    COUNT(*) as device_usage_count,
    AVG(vs.completion_percentage) as avg_completion,
    AVG(vs.watch_duration_minutes) as avg_duration,
    CASE 
        WHEN AVG(vs.completion_percentage) >= 90 THEN 'High Engagement'
        WHEN AVG(vs.completion_percentage) >= 70 THEN 'Medium Engagement'
        ELSE 'Low Engagement'
    END as engagement_level
FROM users u
JOIN viewing_sessions vs ON u.user_id = vs.user_id
WHERE u.user_id IN (
    SELECT user_id 
    FROM viewing_sessions 
    GROUP BY user_id 
    HAVING COUNT(*) >= 2
)
GROUP BY u.subscription_type, u.country, vs.device_type
HAVING device_usage_count >= 2
ORDER BY u.subscription_type, avg_completion DESC;

-- Query 6: Content performance with user demographics correlation
-- Uses multiple JOINs, subqueries, and statistical functions
SELECT 
    vs.content_id,
    COUNT(DISTINCT vs.user_id) as unique_viewers,
    AVG(u.age) as avg_viewer_age,
    COUNT(CASE WHEN u.subscription_type = 'premium' THEN 1 END) as premium_viewers,
    COUNT(CASE WHEN u.subscription_type = 'free' THEN 1 END) as free_viewers,
    AVG(vs.completion_percentage) as avg_completion,
    STDDEV(vs.completion_percentage) as completion_stddev,
    AVG(vs.watch_duration_minutes) as avg_duration
FROM viewing_sessions vs
JOIN users u ON vs.user_id = u.user_id
WHERE vs.content_id IN (
    SELECT content_id 
    FROM viewing_sessions 
    GROUP BY content_id 
    HAVING COUNT(DISTINCT user_id) >= 2
)
GROUP BY vs.content_id
HAVING unique_viewers >= 2
ORDER BY avg_completion DESC, unique_viewers DESC;
