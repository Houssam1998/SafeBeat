-- ============================================
-- SafeBeat PostgreSQL + PostGIS Schema
-- Initialize database with geospatial support
-- ============================================

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ============================================
-- Dimension Tables
-- ============================================

-- DIM_GEO: Geographic block groups
CREATE TABLE IF NOT EXISTS dim_geo (
    geo_id VARCHAR(20) PRIMARY KEY,
    latitude_centroid DOUBLE PRECISION,
    longitude_centroid DOUBLE PRECISION,
    area_sq_km DOUBLE PRECISION,
    geom GEOMETRY(POINT, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create spatial index
CREATE INDEX IF NOT EXISTS idx_dim_geo_geom ON dim_geo USING GIST(geom);

-- DIM_VENUE: Foursquare venues
CREATE TABLE IF NOT EXISTS dim_venue (
    venue_id VARCHAR(50) PRIMARY KEY,
    venue_name VARCHAR(500),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    category VARCHAR(200),
    geom GEOMETRY(POINT, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_venue_geom ON dim_venue USING GIST(geom);

-- DIM_WEATHER: Weather data
CREATE TABLE IF NOT EXISTS dim_weather (
    weather_id SERIAL PRIMARY KEY,
    datetime TIMESTAMP,
    date DATE,
    hour INTEGER,
    temperature_f DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    weather_category VARCHAR(50),
    is_raining BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_weather_datetime ON dim_weather(datetime);

-- DIM_EVENT: Festivals and events
CREATE TABLE IF NOT EXISTS dim_event (
    event_id INTEGER PRIMARY KEY,
    event_name VARCHAR(500),
    event_type VARCHAR(100),
    start_date DATE,
    end_date DATE,
    event_lat DOUBLE PRECISION,
    event_lon DOUBLE PRECISION,
    has_alcohol BOOLEAN,
    has_amplified_sound BOOLEAN,
    has_road_closure BOOLEAN,
    expected_attendance INTEGER,
    geom GEOMETRY(POINT, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_event_geom ON dim_event USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_dim_event_dates ON dim_event(start_date, end_date);

-- ============================================
-- Fact Tables
-- ============================================

-- FACT_911_CALLS: All 911 calls
CREATE TABLE IF NOT EXISTS fact_911_calls (
    incident_number VARCHAR(50) PRIMARY KEY,
    response_datetime TIMESTAMP,
    response_date DATE,
    response_hour INTEGER,
    response_day_of_week VARCHAR(10),
    geo_id VARCHAR(20),
    latitude_centroid DOUBLE PRECISION,
    longitude_centroid DOUBLE PRECISION,
    priority_level VARCHAR(20),
    priority_numeric INTEGER,
    initial_problem_category VARCHAR(200),
    final_problem_category VARCHAR(200),
    response_time DOUBLE PRECISION,
    geom GEOMETRY(POINT, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (geo_id) REFERENCES dim_geo(geo_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_911_geom ON fact_911_calls USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_fact_911_datetime ON fact_911_calls(response_datetime);

-- FACT_FESTIVAL_INCIDENTS: Incidents linked to festivals
CREATE TABLE IF NOT EXISTS fact_festival_incidents (
    id SERIAL PRIMARY KEY,
    incident_number VARCHAR(50),
    event_id INTEGER,
    distance_km DOUBLE PRECISION,
    event_name VARCHAR(500),
    event_has_alcohol BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (incident_number) REFERENCES fact_911_calls(incident_number),
    FOREIGN KEY (event_id) REFERENCES dim_event(event_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_festival_event ON fact_festival_incidents(event_id);

-- ============================================
-- Analysis Tables
-- ============================================

-- Risk scores by festival
CREATE TABLE IF NOT EXISTS analysis_risk_scores (
    event_id INTEGER PRIMARY KEY,
    event_name VARCHAR(500),
    incident_count INTEGER,
    avg_priority DOUBLE PRECISION,
    risk_score DOUBLE PRECISION,
    risk_category VARCHAR(20),
    has_alcohol BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES dim_event(event_id)
);

-- Zone risk clusters
CREATE TABLE IF NOT EXISTS analysis_zone_clusters (
    cluster_id SERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    incident_count INTEGER,
    risk_zone VARCHAR(20),
    geom GEOMETRY(POINT, 4326),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_zone_clusters_geom ON analysis_zone_clusters USING GIST(geom);

-- ============================================
-- Views for Dashboard
-- ============================================

-- Real-time risk heatmap view
CREATE OR REPLACE VIEW v_risk_heatmap AS
SELECT 
    latitude_centroid as lat,
    longitude_centroid as lon,
    COUNT(*) as incident_count,
    AVG(priority_numeric) as avg_priority,
    CASE 
        WHEN COUNT(*) > 100 THEN 'HIGH'
        WHEN COUNT(*) > 50 THEN 'MEDIUM'
        ELSE 'LOW'
    END as risk_level
FROM fact_911_calls
WHERE response_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY latitude_centroid, longitude_centroid;

-- Festival incident summary view
CREATE OR REPLACE VIEW v_festival_summary AS
SELECT 
    e.event_id,
    e.event_name,
    e.has_alcohol,
    COUNT(fi.incident_number) as incident_count,
    AVG(c.priority_numeric) as avg_priority
FROM dim_event e
LEFT JOIN fact_festival_incidents fi ON e.event_id = fi.event_id
LEFT JOIN fact_911_calls c ON fi.incident_number = c.incident_number
GROUP BY e.event_id, e.event_name, e.has_alcohol;

-- ============================================
-- Functions
-- ============================================

-- Function to get nearby incidents using PostGIS
CREATE OR REPLACE FUNCTION get_nearby_incidents(
    p_lat DOUBLE PRECISION,
    p_lon DOUBLE PRECISION,
    p_radius_km DOUBLE PRECISION
)
RETURNS TABLE (
    incident_number VARCHAR,
    distance_m DOUBLE PRECISION,
    priority_level VARCHAR,
    response_datetime TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.incident_number,
        ST_Distance(f.geom::geography, ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography) as distance_m,
        f.priority_level,
        f.response_datetime
    FROM fact_911_calls f
    WHERE ST_DWithin(
        f.geom::geography,
        ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography,
        p_radius_km * 1000
    )
    ORDER BY distance_m;
END;
$$ LANGUAGE plpgsql;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO safebeat_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO safebeat_user;
