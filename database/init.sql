-- Create the device_telemetry table
CREATE TABLE IF NOT EXISTS device_telemetry (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    factory_id VARCHAR(50) NOT NULL,
    machine_id VARCHAR(50) NOT NULL,
    voltage REAL,
    current REAL,
    power_factor REAL,
    temperature REAL,
    kwh REAL
);

-- Create a composite index to accelerate time-series queries
-- Often we query by machine/device and time range.
CREATE INDEX IF NOT EXISTS idx_telemetry_device_time 
ON device_telemetry (machine_id, timestamp DESC);
