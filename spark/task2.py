"""
Task 2 - Spark Parquet filtering and projection (Query A vs Query B).

Fixed filter: PULocationID = 161 AND fare_amount > 30.00
  Query A: all columns
  Query B: tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount

Usage:
    python spark/task2.py <input_parquet_path>

Writes spark/task2_explain.txt with df.explain(mode="formatted") for both
queries, as required by the assignment.
"""
import contextlib
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def _filter_cond():
    """Built lazily inside a function - F.col() needs an active
    SparkSession to resolve, so this must NOT run at import time
    (before any SparkSession exists)."""
    return (F.col("PULocationID") == 161) & (F.col("fare_amount") > 30.00)


def query_a(df):
    """All columns for matching rows."""
    return df.filter(_filter_cond())


def query_b(df):
    """Only 4 columns for matching rows."""
    return df.filter(_filter_cond()).select(
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount"
    )

if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "taxi_2m.parquet"

    spark = SparkSession.builder.appName("BDA-Task2").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(input_path)

    a = query_a(df)
    b = query_b(df)

    a_rows = a.collect()
    b_rows = b.collect()

    print("Query A rows:", len(a_rows))
    print("Query B rows:", len(b_rows))

    with open("spark/task2_explain.txt", "w") as f, contextlib.redirect_stdout(f):
        print("=== Query A (all columns) explain ===")
        a.explain(mode="formatted")
        print("\n=== Query B (4 columns) explain ===")
        b.explain(mode="formatted")

    spark.stop()
