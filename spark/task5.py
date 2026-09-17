"""
Task 5 - Spark repeated-query experiment: uncached vs cached.

1. Uncached: load Parquet once and run the Task-1 aggregation 5 times.
2. Cache build: cache the input DataFrame and force materialization with count().
3. Cached: run the same aggregation 5 times using the cached DataFrame.

The cache-build time is reported separately.
"""

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def aggregate(df):
    return (
        df.filter(F.col("PULocationID").isNotNull())
        .filter(
            F.col("fare_amount").isNotNull()
            & (F.col("fare_amount") >= 0)
        )
        .groupBy("PULocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    )


if __name__ == "__main__":
    input_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "taxi_1m.parquet"
    )

    spark = (
        SparkSession.builder
        .appName("BDA-Task5")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # --------------------------------------------------
    # 1. Uncached: 5 runs
    # --------------------------------------------------

    df_uncached = spark.read.parquet(input_path)
    uncached_result = aggregate(df_uncached)

    print("=== Uncached runs ===")

    for i in range(1, 6):
        start = time.perf_counter()

        # Materialize the complete result.
        uncached_result.collect()

        elapsed = time.perf_counter() - start

        print(f"run {i}: {elapsed:.4f}s")

    # --------------------------------------------------
    # 2. Cache build
    # --------------------------------------------------

    df_cached = spark.read.parquet(input_path).cache()

    start = time.perf_counter()
    df_cached.count()
    cache_build_time = time.perf_counter() - start

    print(
        f"\ncache build (not a query run): "
        f"{cache_build_time:.4f}s"
    )

    # Build aggregation after cache is materialized.
    cached_result = aggregate(df_cached)

    # --------------------------------------------------
    # 3. Cached: 5 runs
    # --------------------------------------------------

    print("\n=== Cached runs ===")

    for i in range(1, 6):
        start = time.perf_counter()

        # Materialize the complete result.
        cached_result.collect()

        elapsed = time.perf_counter() - start

        print(f"run {i}: {elapsed:.4f}s")

    spark.stop()