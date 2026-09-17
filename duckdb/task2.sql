-- Task 2 - DuckDB Parquet filtering and projection
-- Input: taxi_2m.parquet
-- Filter: PULocationID = 161 AND fare_amount > 30.00

-- Query A: Return all columns
SELECT *
FROM read_parquet('taxi_2m.parquet')
WHERE PULocationID = 161
  AND fare_amount > 30.00;


-- Query B: Return only the required four columns
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount
FROM read_parquet('taxi_2m.parquet')
WHERE PULocationID = 161
  AND fare_amount > 30.00;