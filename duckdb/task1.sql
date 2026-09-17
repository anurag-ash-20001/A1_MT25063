-- Task 1 / Task 4 - DuckDB aggregation
-- Swap the file name for each dataset size when running Task 4's scaling sweep.
SELECT
    PULocationID,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare
FROM read_csv_auto('taxi_1m.csv')
WHERE PULocationID IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID;
