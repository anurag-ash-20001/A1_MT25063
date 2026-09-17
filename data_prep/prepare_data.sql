-- Run this from a folder that contains yellow_tripdata_2026-01.parquet,
-- using the DuckDB CLI:  duckdb < prepare_data.sql
-- (or paste into a duckdb Python connection with con.execute(open(...).read()))

COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 100000)
TO 'taxi_100k.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 100000)
TO 'taxi_100k.csv' (FORMAT CSV, HEADER TRUE);

COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 500000)
TO 'taxi_500k.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 500000)
TO 'taxi_500k.csv' (FORMAT CSV, HEADER TRUE);

COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 1000000)
TO 'taxi_1m.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 1000000)
TO 'taxi_1m.csv' (FORMAT CSV, HEADER TRUE);

COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 2000000)
TO 'taxi_2m.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 2000000)
TO 'taxi_2m.csv' (FORMAT CSV, HEADER TRUE);

-- Sanity check row counts before starting any timed experiment:
-- SELECT COUNT(*) FROM read_csv_auto('taxi_100k.csv');   -- expect 100000
-- SELECT COUNT(*) FROM read_csv_auto('taxi_500k.csv');   -- expect 500000
-- SELECT COUNT(*) FROM read_csv_auto('taxi_1m.csv');     -- expect 1000000
-- SELECT COUNT(*) FROM read_csv_auto('taxi_2m.csv');     -- expect 2000000
