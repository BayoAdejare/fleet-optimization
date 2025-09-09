-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create tables
CREATE TABLE IF NOT EXISTS vehicle_telemetry (
    time TIMESTAMPTZ NOT NULL,
    vehicle_id TEXT NOT NULL,
    engine_temp DOUBLE PRECISION,
    fuel_level DOUBLE PRECISION,
    tire_pressure DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    speed DOUBLE PRECISION
);

-- Convert to hypertable
SELECT create_hypertable('vehicle_telemetry', 'time', if_not_exists => TRUE);

-- Create index on vehicle_id for faster queries
CREATE INDEX IF NOT EXISTS idx_vehicle_id ON vehicle_telemetry (vehicle_id);

-- Create maintenance predictions table with the new columns
CREATE TABLE IF NOT EXISTS maintenance_data (
    time TIMESTAMPTZ NOT NULL,
    vehicle_id TEXT NOT NULL,
    prediction_score DOUBLE PRECISION,
    predicted_component TEXT,
    confidence DOUBLE PRECISION,
    maintenance_date TIMESTAMPTZ,  -- New column
    maintenance_type TEXT,         -- New column
    maintenance_severity TEXT      -- New column
);

SELECT create_hypertable('maintenance_data', 'time', if_not_exists => TRUE);

-- Create index on vehicle_id for maintenance_data
CREATE INDEX IF NOT EXISTS idx_maintenance_vehicle_id ON maintenance_data (vehicle_id);