
import argparse
import csv
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TIMEOUT_SECONDS = 900
RESULTS_CSV = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results", "timings.csv")

CSV_COLUMNS = [
    "student_id", "system", "task", "condition", "dataset_rows", "input_format",
    "input_mb", "run_number", "time_seconds", "status",
    "os", "cpu_model", "cpu_base_ghz", "cpu_physical_cores", "cpu_logical_processors",
    "ram_gb", "storage_size_gb", "storage_type",
    "gpu_model", "gpu_clock_mhz", "gpu_memory_gb",
    "python_version", "tool_version",
]

# ---- FILL THESE IN with your own values (Section 4 of the spec) ----
STUDENT_ID = "MT25063"

MACHINE_CONFIG = {
    "os": "Ubuntu 26.04.1 LTS",
    "cpu_model": "AMD Ryzen 7 7840HS w/ Radeon 780M Graphics",
    "cpu_base_ghz": 3.80,
    "cpu_physical_cores": 8,
    "cpu_logical_processors": 16,
    "ram_gb": 14,
    "storage_size_gb": 192,
    "storage_type": "NVMe SSD",
    "gpu_model": "NVIDIA GeForce RTX 4060 Laptop GPU",
    "gpu_clock_mhz": 3105,
    "gpu_memory_gb": 8,
}

PYTHON_VERSION = "3.11.11"

TOOL_VERSIONS = {
    "mrjob": "0.7.4",
    "pyspark": "4.2.0",
    "duckdb": "1.5.5",
}
# ----------------------------------------------------------------------


def ensure_csv_header():
    os.makedirs(os.path.dirname(RESULTS_CSV), exist_ok=True)
    if not os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, "w", newline="") as f:
            csv.writer(f).writerow(CSV_COLUMNS)


def write_row(system, task, condition, dataset_rows, input_format, input_mb,
              run_number, time_seconds, status):
    ensure_csv_header()
    row = [
        STUDENT_ID, system, task, condition, dataset_rows, input_format,
        round(input_mb, 2), run_number, round(time_seconds, 4), status,
        MACHINE_CONFIG["os"], MACHINE_CONFIG["cpu_model"], MACHINE_CONFIG["cpu_base_ghz"],
        MACHINE_CONFIG["cpu_physical_cores"], MACHINE_CONFIG["cpu_logical_processors"],
        MACHINE_CONFIG["ram_gb"], MACHINE_CONFIG["storage_size_gb"], MACHINE_CONFIG["storage_type"],
        MACHINE_CONFIG["gpu_model"], MACHINE_CONFIG["gpu_clock_mhz"], MACHINE_CONFIG["gpu_memory_gb"],
        PYTHON_VERSION, TOOL_VERSIONS.get(system, "1.5.5"),
    ]
    with open(RESULTS_CSV, "a", newline="") as f:
        csv.writer(f).writerow(row)


def file_size_mb(path):
    return os.path.getsize(path) / (1024 * 1024) if os.path.exists(path) else 0.0


def timed_call(fn):
    """Times one call to fn(). Returns (elapsed_seconds, status)."""
    t0 = time.perf_counter()
    fn()
    elapsed = time.perf_counter() - t0
    if elapsed > TIMEOUT_SECONDS:
        return TIMEOUT_SECONDS, "TIMEOUT_900"
    return elapsed, "OK"


def run_protocol(system, task, condition, dataset_rows, input_format, input_path, fn,
                  warmup_runs=1, recorded_runs=3):
    """
    Runs the standard 1-warmup + N-recorded protocol and writes each
    recorded run to timings.csv. Returns the list of recorded times.
    NOTE: this does NOT preemptively kill a hung process at 900s - if a
    single run is genuinely stuck, interrupt it manually, then call
    write_row(..., status="TIMEOUT_900", time_seconds=900) yourself.
    """
    input_mb = file_size_mb(input_path) if input_path else 0.0

    for _ in range(warmup_runs):
        fn()

    times = []
    for run_number in range(1, recorded_runs + 1):
        elapsed, status = timed_call(fn)
        write_row(system, task, condition, dataset_rows, input_format, input_mb,
                   run_number, elapsed, status)
        times.append(elapsed)
        print(f"  [{system}/{task}/{condition}] run {run_number}: {elapsed:.4f}s ({status})")

    if times:
        print(f"  -> median: {statistics.median(times):.4f}s")
    return times


# ---------------------------------------------------------------------
# Task 1 / Task 4 runners (same aggregation, different dataset sizes)
# ---------------------------------------------------------------------

def make_mapreduce_task1_fn(csv_path):
    from mapreduce.task1 import MRTaxiAggregation

    mrjob_args = [csv_path, "-r", "local", "--quiet"]
    if os.name == "nt":
        # mrjob's local runner shells out via "sh", which isn't on Windows'
        # system PATH by default (only inside a Git Bash session) - point it
        # at Git for Windows' sh.exe explicitly. No-op on Linux/macOS.
        mrjob_args += ["--sh-bin", r'"C:\Program Files\Git\usr\bin\sh.exe"']

    def fn():
        job = MRTaxiAggregation(args=mrjob_args)
        with job.make_runner() as runner:
            runner.run()
            list(job.parse_output(runner.cat_output()))
    return fn


def make_spark_task1_fn(spark, csv_path):
    from spark.task1 import run_task1

    def fn():
        run_task1(spark, csv_path).collect()
    return fn


def make_duckdb_task1_fn(con, csv_path):
    query = f"""
        SELECT PULocationID, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
        FROM read_csv_auto('{csv_path}')
        WHERE PULocationID IS NOT NULL AND fare_amount IS NOT NULL AND fare_amount >= 0
        GROUP BY PULocationID
    """

    def fn():
        con.execute(query).fetchall()
    return fn


def run_task1_or_4(system, dataset_path, dataset_rows, task="task1", spark_session=None, duckdb_con=None):
    input_format = "csv"
    condition = "aggregation"
    if system == "mapreduce":
        fn = make_mapreduce_task1_fn(dataset_path)
    elif system == "spark":
        fn = make_spark_task1_fn(spark_session, dataset_path)
    elif system == "duckdb":
        fn = make_duckdb_task1_fn(duckdb_con, dataset_path)
    else:
        raise ValueError(system)
    run_protocol(system, task, condition, dataset_rows, input_format, dataset_path, fn)


# ---------------------------------------------------------------------
# Task 5 runner (5 runs, no separate warm-up phase; cache build logged once)
# ---------------------------------------------------------------------

def run_task5(parquet_path, dataset_rows):
    from pyspark.sql import SparkSession
    from spark.task5 import aggregate
    import duckdb

    spark = SparkSession.builder.appName("BDA-Task5-Harness").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    input_mb = file_size_mb(parquet_path)

    # Spark uncached: 5 runs, no warm-up excluded (Task 5 wants all 5 raw runs)
    df_uncached = spark.read.parquet(parquet_path)
    for run_number in range(1, 6):
        elapsed, status = timed_call(lambda: aggregate(df_uncached).collect())
        write_row("spark", "task5", "spark_uncached", dataset_rows, "parquet",
                   input_mb, run_number, elapsed, status)

    # Cache build - separate row, run_number=0, not one of the 5 query runs
    df_cached = spark.read.parquet(parquet_path).cache()
    elapsed, status = timed_call(lambda: df_cached.count())
    write_row("spark", "task5", "spark_cache_build", dataset_rows, "parquet",
               input_mb, 0, elapsed, status)

    # Spark cached: 5 runs
    for run_number in range(1, 6):
        elapsed, status = timed_call(lambda: aggregate(df_cached).collect())
        write_row("spark", "task5", "spark_cached", dataset_rows, "parquet",
                   input_mb, run_number, elapsed, status)
    spark.stop()

    # DuckDB: 5 runs, same connection throughout
    con = duckdb.connect()
    query = f"""
        SELECT PULocationID, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
        FROM read_parquet('{parquet_path}')
        WHERE PULocationID IS NOT NULL AND fare_amount IS NOT NULL AND fare_amount >= 0
        GROUP BY PULocationID
    """
    for run_number in range(1, 6):
        elapsed, status = timed_call(lambda: con.execute(query).fetchall())
        write_row("duckdb", "task5", "duckdb_repeated", dataset_rows, "parquet",
                   input_mb, run_number, elapsed, status)
    con.close()


# ---------------------------------------------------------------------
# Task 2 runner (Query A / Query B, Spark + DuckDB, 1 warm-up + 3 runs each)
# ---------------------------------------------------------------------

def run_task2(parquet_path, dataset_rows=2000000):
    from pyspark.sql import SparkSession
    from spark.task2 import query_a, query_b
    import duckdb

    spark = SparkSession.builder.appName("BDA-Task2-Harness").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(parquet_path)

    run_protocol("spark", "task2", "query_a", dataset_rows, "parquet", parquet_path,
                 lambda: query_a(df).collect())
    run_protocol("spark", "task2", "query_b", dataset_rows, "parquet", parquet_path,
                 lambda: query_b(df).collect())
    spark.stop()

    con = duckdb.connect()
    query_a_sql = f"""
        SELECT * FROM read_parquet('{parquet_path}')
        WHERE PULocationID = 161 AND fare_amount > 30.00
    """
    query_b_sql = f"""
        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount
        FROM read_parquet('{parquet_path}')
        WHERE PULocationID = 161 AND fare_amount > 30.00
    """
    run_protocol("duckdb", "task2", "query_a", dataset_rows, "parquet", parquet_path,
                 lambda: con.execute(query_a_sql).fetchall())
    run_protocol("duckdb", "task2", "query_b", dataset_rows, "parquet", parquet_path,
                 lambda: con.execute(query_b_sql).fetchall())
    con.close()


# ---------------------------------------------------------------------
# Task 3 runner (join + top-5, Spark + DuckDB, 1 warm-up + 3 runs each)
# ---------------------------------------------------------------------

def run_task3(taxi_path, lookup_path, dataset_rows=2000000):
    from pyspark.sql import SparkSession
    from spark.task3 import run_task3 as spark_run_task3
    import duckdb

    spark = SparkSession.builder.appName("BDA-Task3-Harness").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    run_protocol("spark", "task3", "join_top5", dataset_rows, "parquet", taxi_path,
                 lambda: spark_run_task3(spark, taxi_path, lookup_path).collect())
    spark.stop()

    con = duckdb.connect()
    query = f"""
        SELECT z.Borough, z.Zone, SUM(t.fare_amount) AS total_fare
        FROM read_parquet('{taxi_path}') t
        JOIN read_csv_auto('{lookup_path}') z ON t.PULocationID = z.LocationID
        WHERE t.fare_amount >= 0
        GROUP BY z.Borough, z.Zone
        ORDER BY total_fare DESC, z.Zone ASC
        LIMIT 5
    """
    run_protocol("duckdb", "task3", "join_top5", dataset_rows, "parquet", taxi_path,
                 lambda: con.execute(query).fetchall())
    con.close()


# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True, choices=["task1", "task2", "task3", "task4", "task5"])
    parser.add_argument("--system", choices=["mapreduce", "spark", "duckdb"])
    parser.add_argument("--dataset", help="path to a single dataset file (task1/task2/task5)")
    parser.add_argument("--lookup", help="path to taxi_zone_lookup.csv (task3)")
    parser.add_argument("--rows", type=int, help="row count label for --dataset")
    args = parser.parse_args()

    if args.task == "task1":
        if not (args.system and args.dataset and args.rows):
            parser.error("task1 requires --system, --dataset, --rows")
        spark_session = None
        duckdb_con = None
        if args.system == "spark":
            from pyspark.sql import SparkSession
            spark_session = SparkSession.builder.appName("BDA-Task1-Harness").master("local[*]").getOrCreate()
            spark_session.sparkContext.setLogLevel("WARN")
        elif args.system == "duckdb":
            import duckdb
            duckdb_con = duckdb.connect()
        run_task1_or_4(args.system, args.dataset, args.rows, task="task1",
                        spark_session=spark_session, duckdb_con=duckdb_con)
        if spark_session:
            spark_session.stop()
        if duckdb_con:
            duckdb_con.close()

    elif args.task == "task4":
        # Full scaling sweep across all 4 sizes x all 3 systems.
        # Adjust file names/paths if your CSVs live elsewhere.
        sizes = [
            ("taxi_100k.csv", 100000),
            ("taxi_500k.csv", 500000),
            ("taxi_1m.csv", 1000000),
            ("taxi_2m.csv", 2000000),
        ]
        from pyspark.sql import SparkSession
        import duckdb
        spark_session = SparkSession.builder.appName("BDA-Task4-Harness").master("local[*]").getOrCreate()
        spark_session.sparkContext.setLogLevel("WARN")
        duckdb_con = duckdb.connect()

        for path, rows in sizes:
            if not os.path.exists(path):
                print(f"skipping {path} (not found)")
                continue
            for system in ["mapreduce", "spark", "duckdb"]:
                run_task1_or_4(system, path, rows, task="task4",
                                spark_session=spark_session, duckdb_con=duckdb_con)

        spark_session.stop()
        duckdb_con.close()

    elif args.task == "task2":
        dataset = args.dataset or "taxi_2m.parquet"
        rows = args.rows or 2000000
        run_task2(dataset, rows)

    elif args.task == "task3":
        taxi_path = args.dataset or "taxi_2m.parquet"
        lookup_path = args.lookup or "taxi_zone_lookup.csv"
        rows = args.rows or 2000000
        run_task3(taxi_path, lookup_path, rows)

    elif args.task == "task5":
        if not (args.dataset and args.rows):
            parser.error("task5 requires --dataset and --rows")
        run_task5(args.dataset, args.rows)


if __name__ == "__main__":
    main()