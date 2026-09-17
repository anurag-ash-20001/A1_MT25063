"""
Task 1 / Task 4 - Spark DataFrame aggregation.

Usage:
    python spark/task1.py <input_csv_path>
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_task1(spark, input_path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    result = (
        df.filter(F.col("PULocationID").isNotNull())
        .filter(F.col("fare_amount").isNotNull() & (F.col("fare_amount") >= 0))
        .groupBy("PULocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    )
    return result


if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "taxi_1m.csv"

    spark = SparkSession.builder.appName("BDA-Task1").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    result_df = run_task1(spark, input_path)
    result_df.collect()  # materialize the full result
    result_df.show(5)

    spark.stop()
