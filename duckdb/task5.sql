-- Task 5 - DuckDB repeated query on the same Parquet file.
-- Run this exact query 5 times within the SAME DuckDB connection
-- (don't reconnect between runs) and record all 5 runtimes.
SELECT
    PULocationID,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare
FROM read_parquet('taxi_1m.parquet')
WHERE PULocationID IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID;
