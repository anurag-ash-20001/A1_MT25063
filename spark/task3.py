"""
Task 3 - Spark join with taxi-zone lookup.

Required computation:
- Input: taxi_2m.parquet and taxi_zone_lookup.csv
- Join PULocationID with LocationID
- Keep fare_amount >= 0
- Compute total fare_amount by Borough + Zone
- Return top 5 zones by total fare
- Break ties by Zone ascending
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_task3(spark, taxi_path, lookup_path):
    taxi = spark.read.parquet(taxi_path)

    zones = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(lookup_path)
    )

    filtered_taxi = taxi.filter(
        F.col("fare_amount") >= 0
    )

    joined = filtered_taxi.join(
        zones,
        filtered_taxi.PULocationID == zones.LocationID,
        "inner"
    )

    result = (
        joined
        .groupBy("Borough", "Zone")
        .agg(
            F.sum("fare_amount").alias("total_fare")
        )
        .orderBy(
            F.col("total_fare").desc(),
            F.col("Zone").asc()
        )
        .limit(5)
    )

    return result


if __name__ == "__main__":
    taxi_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "taxi_2m.parquet"
    )

    lookup_path = (
        sys.argv[2]
        if len(sys.argv) > 2
        else "taxi_zone_lookup.csv"
    )

    spark = (
        SparkSession.builder
        .appName("BDA-Task3")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    result_df = run_task3(
        spark,
        taxi_path,
        lookup_path
    )

    result_df.show(truncate=False)

    print(
        "\n=== Physical plan "
        "(check for BroadcastHashJoin / BroadcastExchange) ==="
    )

    result_df.explain(mode="formatted")

    spark.stop()