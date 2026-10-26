-- Create the tables needed for the local MetroPT-3 pipeline.
-- PostgreSQL runs this script when its data directory is first initialized.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS agg;

CREATE TABLE IF NOT EXISTS raw.sensor_readings (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL,
    tp2 DOUBLE PRECISION,
    tp3 DOUBLE PRECISION,
    h1 DOUBLE PRECISION,
    dv_pressure DOUBLE PRECISION,
    reservoirs DOUBLE PRECISION,
    oil_temperature DOUBLE PRECISION,
    motor_current DOUBLE PRECISION,
    comp DOUBLE PRECISION,
    dv_eletric DOUBLE PRECISION,
    towers DOUBLE PRECISION,
    mpg DOUBLE PRECISION,
    lps DOUBLE PRECISION,
    pressure_switch DOUBLE PRECISION,
    oil_level DOUBLE PRECISION,
    caudal_impulses DOUBLE PRECISION,
    ingestion_batch TEXT
);

CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    month TEXT PRIMARY KEY,
    rows_loaded BIGINT,
    loaded_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agg.fact_sensor_hourly (
    hour_ts TIMESTAMP PRIMARY KEY,
    avg_tp2 DOUBLE PRECISION,
    avg_tp3 DOUBLE PRECISION,
    avg_oil_temperature DOUBLE PRECISION,
    avg_motor_current DOUBLE PRECISION,
    reading_count BIGINT,
    near_failure BOOLEAN
);

CREATE TABLE IF NOT EXISTS agg.dim_failure_window (
    id SERIAL PRIMARY KEY,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    description TEXT
);

INSERT INTO agg.dim_failure_window (start_ts, end_ts, description) VALUES
    ('2020-04-18 00:00:00', '2020-04-18 23:59:00', 'Air leak'),
    ('2020-05-29 23:30:00', '2020-05-30 06:00:00', 'Air leak'),
    ('2020-06-05 10:00:00', '2020-06-07 14:30:00', 'Air leak'),
    ('2020-07-15 14:30:00', '2020-07-15 19:00:00', 'Air leak')
ON CONFLICT DO NOTHING;
