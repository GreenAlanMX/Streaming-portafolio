-- Insertar usuarios
INSERT INTO users (user_id, age, country, subscription_type, registration_date, total_watch_time_hours) VALUES
('USR001', 25, 'United States', 'premium', '2023-01-10', 120.5),
('USR002', 32, 'Canada', 'family', '2023-02-05', 95.3),
('USR003', 28, 'Mexico', 'free', '2023-03-12', 45.8),
('USR004', 40, 'Brazil', 'premium', '2023-04-08', 210.7),
('USR005', 22, 'Argentina', 'student', '2023-05-15', 78.2),
('USR006', 35, 'Spain', 'premium', '2023-06-20', 156.4),
('USR007', 27, 'France', 'family', '2023-07-18', 89.5),
('USR008', 30, 'Germany', 'premium', '2023-08-22', 134.9),
('USR009', 45, 'Italy', 'free', '2023-09-11', 67.3),
('USR010', 29, 'United Kingdom', 'premium', '2023-10-03', 312.6);

-- Insertar catálogo de contenidos
INSERT INTO content (content_id, title, genre, release_year, rating) VALUES
('MOV001', 'Avengers: Endgame', 'Action', 2019, 4.8),
('MOV002', 'Inception', 'Sci-Fi', 2010, 4.7),
('SER001', 'Breaking Bad', 'Drama', 2008, 4.9),
('SER002', 'Stranger Things', 'Sci-Fi', 2016, 4.6),
('DOC001', 'Planet Earth', 'Documentary', 2006, 4.7),
('DOC002', 'The Social Dilemma', 'Documentary', 2020, 4.2);

-- Insertar sesiones de visualización
INSERT INTO viewing_sessions (session_id, user_id, content_id, watch_date, watch_duration_minutes, completion_percentage, device_type, quality_level) VALUES
('SES001', 'USR001', 'MOV001', '2023-05-01', 120, 95.5, 'mobile', 'HD'),
('SES002', 'USR002', 'SER001', '2023-05-02', 45, 88.2, 'desktop', 'FHD'),
('SES003', 'USR003', 'DOC001', '2023-05-03', 60, 76.0, 'tv', '4K'),
('SES004', 'USR004', 'MOV002', '2023-05-04', 130, 98.0, 'smart_tv', '4K'),
('SES005', 'USR005', 'SER002', '2023-05-05', 55, 82.3, 'tablet', 'HD'),
('SES006', 'USR006', 'MOV001', '2023-05-06', 125, 91.5, 'mobile', 'HD'),
('SES007', 'USR007', 'DOC002', '2023-05-07', 50, 74.0, 'desktop', 'FHD'),
('SES008', 'USR008', 'SER001', '2023-05-08', 47, 80.2, 'tv', 'FHD'),
('SES009', 'USR009', 'MOV002', '2023-05-09', 110, 89.0, 'smart_tv', '4K'),
('SES010', 'USR010', 'SER002', '2023-05-10', 53, 77.5, 'mobile', 'HD');
