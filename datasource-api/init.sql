-- Initialize database for Data Source API
-- This script runs automatically when the PostgreSQL container starts for the first time

-- The database and user are already created via environment variables in docker-compose

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create sample NYC Taxi data table (optional - for testing)
-- Note: API will create tables dynamically from uploaded CSV files
-- This is just an example of the expected schema

CREATE TABLE IF NOT EXISTS nyc_taxi_sample (
    vendorid INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance NUMERIC(10, 2),
    ratecodeid INTEGER,
    store_and_fwd_flag VARCHAR(1),
    pulocationid INTEGER,
    dolocationid INTEGER,
    payment_type INTEGER,
    fare_amount NUMERIC(10, 2),
    extra NUMERIC(10, 2),
    mta_tax NUMERIC(10, 2),
    tip_amount NUMERIC(10, 2),
    tolls_amount NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    congestion_surcharge NUMERIC(10, 2)
);

-- Insert sample data
INSERT INTO nyc_taxi_sample VALUES
(1, '2020-04-01 00:41:22', '2020-04-01 01:01:53', 1, 1.20, 1, 'N', 41, 24, 2, 5.5, 0.5, 0.5, 0, 0, 0.3, 6.8, 0),
(1, '2020-04-01 00:56:00', '2020-04-01 01:09:25', 1, 3.40, 1, 'N', 95, 197, 1, 12.5, 0.5, 0.5, 2.75, 0, 0.3, 16.55, 0),
(1, '2020-04-01 00:00:26', '2020-04-01 00:09:25', 1, 2.80, 1, 'N', 237, 137, 1, 10, 3, 0.5, 1, 0, 0.3, 14.8, 2.5),
(1, '2020-04-01 00:24:38', '2020-04-01 00:34:38', 0, 2.60, 1, 'N', 68, 142, 1, 10, 3, 0.5, 1, 0, 0.3, 14.8, 2.5),
(2, '2020-04-01 00:13:24', '2020-04-01 00:18:26', 1, 1.44, 1, 'Y', 263, 74, 1, 6.5, 0.5, 0.5, 3, 0, 0.3, 13.3, 2.5),
(2, '2020-04-01 00:24:36', '2020-04-01 00:33:09', 1, 2.93, 1, 'N', 75, 170, 2, 10.5, 0.5, 0.5, 0, 0, 0.3, 14.3, 2.5),
(2, '2020-04-01 00:56:56', '2020-04-01 01:09:13', 1, 6.86, 1, 'N', 141, 243, 1, 20, 0.5, 0.5, 4.76, 0, 0.3, 28.56, 2.5);

-- Create index for common queries
CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON nyc_taxi_sample(tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_dropoff_datetime ON nyc_taxi_sample(tpep_dropoff_datetime);

-- Log initialization
DO $$
BEGIN
  RAISE NOTICE 'Data Source API database initialized successfully';
  RAISE NOTICE 'Sample NYC Taxi data table created with 7 records';
END $$;

