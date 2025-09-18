-- Streaming Platform Database Schema
-- Activity 1.1: Relational Model
-- Table Creation Script with Constraints

-- Create database (if not exists)
CREATE DATABASE IF NOT EXISTS streaming_platform;
USE streaming_platform;

-- Drop tables if they exist (for clean recreation)
DROP TABLE IF EXISTS viewing_sessions;
DROP TABLE IF EXISTS users;

-- Create users table
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    age INT NOT NULL CHECK (age >= 0 AND age <= 150),
    country VARCHAR(100) NOT NULL,
    subscription_type VARCHAR(20) NOT NULL CHECK (subscription_type IN ('free', 'premium', 'family', 'student')),
    registration_date DATE NOT NULL,
    total_watch_time_hours DECIMAL(10,2) DEFAULT 0.00 CHECK (total_watch_time_hours >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create viewing_sessions table
CREATE TABLE viewing_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    content_id VARCHAR(50) NOT NULL,
    watch_date DATE NOT NULL,
    watch_duration_minutes INT NOT NULL CHECK (watch_duration_minutes > 0),
    completion_percentage DECIMAL(5,2) NOT NULL CHECK (completion_percentage >= 0 AND completion_percentage <= 100),
    device_type VARCHAR(20) NOT NULL CHECK (device_type IN ('mobile', 'tablet', 'desktop', 'tv', 'smart_tv')),
    quality_level VARCHAR(10) NOT NULL CHECK (quality_level IN ('SD', 'HD', 'FHD', '4K', '8K')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    
    -- Indexes for better performance
    INDEX idx_user_id (user_id),
    INDEX idx_watch_date (watch_date),
    INDEX idx_content_id (content_id),
    INDEX idx_device_type (device_type)
);

-- Create indexes for optimization
CREATE INDEX idx_users_country ON users(country);
CREATE INDEX idx_users_subscription ON users(subscription_type);
CREATE INDEX idx_users_registration ON users(registration_date);
CREATE INDEX idx_sessions_quality ON viewing_sessions(quality_level);
CREATE INDEX idx_sessions_completion ON viewing_sessions(completion_percentage);

-- Add comments for documentation
ALTER TABLE users COMMENT = 'Stores user information and subscription details';
ALTER TABLE viewing_sessions COMMENT = 'Records individual viewing sessions with detailed metrics';
