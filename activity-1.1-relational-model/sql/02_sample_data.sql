-- Sample Data for Streaming Platform
-- Activity 1.1: Relational Model

USE streaming_platform;

-- Insert sample users
INSERT INTO users (user_id, age, country, subscription_type, registration_date, total_watch_time_hours) VALUES
('USR001', 25, 'United States', 'premium', '2023-01-15', 245.50),
('USR002', 34, 'Canada', 'family', '2023-02-20', 189.75),
('USR003', 28, 'Mexico', 'free', '2023-03-10', 67.25),
('USR004', 45, 'United Kingdom', 'premium', '2023-01-05', 312.80),
('USR005', 22, 'Germany', 'student', '2023-04-12', 98.40),
('USR006', 31, 'France', 'premium', '2023-02-28', 156.90),
('USR007', 29, 'Spain', 'free', '2023-03-25', 45.60),
('USR008', 38, 'Italy', 'family', '2023-01-30', 278.35),
('USR009', 26, 'Brazil', 'premium', '2023-04-05', 201.70),
('USR010', 33, 'Argentina', 'student', '2023-03-15', 134.85);

-- Insert sample viewing sessions
INSERT INTO viewing_sessions (session_id, user_id, content_id, watch_date, watch_duration_minutes, completion_percentage, device_type, quality_level) VALUES
('SES001', 'USR001', 'MOV001', '2023-05-01', 120, 100.00, 'smart_tv', '4K'),
('SES002', 'USR001', 'SER001', '2023-05-02', 45, 75.00, 'mobile', 'HD'),
('SES003', 'USR002', 'DOC001', '2023-05-01', 90, 100.00, 'desktop', 'FHD'),
('SES004', 'USR002', 'MOV002', '2023-05-03', 95, 85.50, 'tv', 'HD'),
('SES005', 'USR003', 'SER002', '2023-05-02', 30, 50.00, 'mobile', 'SD'),
('SES006', 'USR004', 'MOV003', '2023-05-01', 110, 100.00, 'smart_tv', '4K'),
('SES007', 'USR004', 'DOC002', '2023-05-04', 60, 100.00, 'tablet', 'HD'),
('SES008', 'USR005', 'SER003', '2023-05-03', 25, 40.00, 'mobile', 'SD'),
('SES009', 'USR006', 'MOV004', '2023-05-02', 105, 95.00, 'desktop', 'FHD'),
('SES010', 'USR006', 'SER004', '2023-05-05', 50, 80.00, 'tv', 'HD'),
('SES011', 'USR007', 'DOC003', '2023-05-03', 40, 60.00, 'mobile', 'SD'),
('SES012', 'USR008', 'MOV005', '2023-05-01', 130, 100.00, 'smart_tv', '4K'),
('SES013', 'USR008', 'SER005', '2023-05-04', 35, 70.00, 'tablet', 'HD'),
('SES014', 'USR009', 'DOC004', '2023-05-02', 70, 100.00, 'desktop', 'FHD'),
('SES015', 'USR009', 'MOV006', '2023-05-05', 85, 90.00, 'tv', 'HD'),
('SES016', 'USR010', 'SER006', '2023-05-03', 20, 35.00, 'mobile', 'SD'),
('SES017', 'USR001', 'DOC005', '2023-05-06', 55, 100.00, 'tablet', 'HD'),
('SES018', 'USR002', 'SER007', '2023-05-04', 40, 65.00, 'mobile', 'SD'),
('SES019', 'USR003', 'MOV007', '2023-05-05', 75, 80.00, 'desktop', 'HD'),
('SES020', 'USR004', 'DOC006', '2023-05-06', 65, 100.00, 'smart_tv', '4K');
