-- Tabla de usuarios
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

-- Catálogo de contenidos
CREATE TABLE content (
    content_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    genre VARCHAR(50),
    release_year INT CHECK (release_year BETWEEN 1900 AND 2100),
    rating DECIMAL(3,1) CHECK (rating BETWEEN 0 AND 5)
);

-- Sesiones de visualización
CREATE TABLE viewing_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    content_id VARCHAR(50) NOT NULL,
    watch_date DATE NOT NULL,
    watch_duration_minutes INT CHECK (watch_duration_minutes > 0),
    completion_percentage DECIMAL(5,2) CHECK (completion_percentage >= 0 AND completion_percentage <= 100),
    device_type VARCHAR(20) CHECK (device_type IN ('mobile', 'tablet', 'desktop', 'tv', 'smart_tv')),
    quality_level VARCHAR(10) CHECK (quality_level IN ('SD', 'HD', 'FHD', '4K', '8K')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (content_id) REFERENCES content(content_id) ON DELETE CASCADE ON UPDATE CASCADE
);
