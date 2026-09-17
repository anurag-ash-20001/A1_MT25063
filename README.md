# Big Data Analytics — Assignment 1: MapReduce vs Spark vs DuckDB

Student ID: <MT25063> (rename this folder and the final ZIP to `A1_<student_id>`)

## Windows Quick Start (tested — copy-paste exactly, no errors)

The rest of this README assumes Linux/Ubuntu (per the spec's machine requirement).
This section is for developing/testing on a **Windows** machine, in **Git Bash**, from
the project root. Every command below was actually run on this machine and verified
to produce output. **Final graded timings must still come from the Ubuntu laptop named
in `MACHINE_CONFIG`, per spec Section 3/5** — treat this section as a dev/functional
loop, not the graded run, unless you update `MACHINE_CONFIG["os"]` to match reality.

### Why this section exists
- The system Python here is 3.14, which breaks `mrjob` (imports the removed stdlib
  modules `pipes`/`distutils`) and doesn't have `pyspark` installed.
- Spark needs a JVM; none was installed by default.
- mrjob's local runner shells out to `sh`, which isn't on Windows' system `PATH`.

### One-time setup
```bash
py install 3.11
winget install --id EclipseAdoptium.Temurin.17.JDK -e \
  --accept-package-agreements --accept-source-agreements

py -3.11 -m venv bda-env
./bda-env/Scripts/python.exe -m pip install --upgrade pip
./bda-env/Scripts/python.exe -m pip install "pandas<3" mrjob pyspark duckdb pyarrow matplotlib
```

### Every terminal session — run this first
```bash
cd "/c/Users/anura/OneDrive/Documents/A1_MT25063/A1_MT25063"
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.20.101-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
PY="./bda-env/Scripts/python.exe"
```
(`JAVA_HOME` above matches JDK 17.0.20.101 as installed by winget on this machine —
adjust the path if your installed build number differs; check with
`ls "/c/Program Files/Eclipse Adoptium"`.)

There is no `duckdb` CLI binary on this machine, only the Python package — every
`duckdb -c "..."` command from the rest of this README is replaced below with the
equivalent `$PY -c "..."` call using `duckdb.connect()`.

### 0. Data prep
```bash
$PY -c "import duckdb; duckdb.connect().execute(open('data_prep/prepare_data.sql').read())"

$PY -c "
import duckdb
con = duckdb.connect()
for f in ['taxi_100k.csv', 'taxi_500k.csv', 'taxi_1m.csv', 'taxi_2m.csv']:
    n = con.execute(f\"SELECT COUNT(*) FROM read_csv_auto('{f}')\").fetchone()[0]
    print(f, n)
"
```

### Task 1 — functional check (one system at a time, not timed)
```bash
$PY mapreduce/task1.py -r local --sh-bin='"C:\Program Files\Git\usr\bin\sh.exe"' taxi_1m.csv > mr_task1_output.txt
$PY spark/task1.py taxi_1m.csv
$PY -c "import duckdb; print(duckdb.connect().execute(open('duckdb/task1.sql').read()).fetchall())"
```

### Task 1 / Task 4 — timed runs (writes `results/timings.csv`)
`utils/run_experiments.py` is already patched to add the same `--sh-bin` fix
automatically on Windows (see `make_mapreduce_task1_fn`), so these run as-is:
```bash
$PY utils/run_experiments.py --task task1 --system mapreduce --dataset taxi_1m.csv --rows 1000000
$PY utils/run_experiments.py --task task1 --system spark     --dataset taxi_1m.csv --rows 1000000
$PY utils/run_experiments.py --task task1 --system duckdb    --dataset taxi_1m.csv --rows 1000000

# full Task 4 scaling sweep (4 sizes x 3 systems):
$PY utils/run_experiments.py --task task4
```

### Task 2
```bash
$PY spark/task2.py taxi_2m.parquet

$PY -c "
import duckdb
con = duckdb.connect()
qa = \"EXPLAIN SELECT * FROM read_parquet('taxi_2m.parquet') WHERE PULocationID = 161 AND fare_amount > 30.00\"
qb = \"EXPLAIN SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount FROM read_parquet('taxi_2m.parquet') WHERE PULocationID = 161 AND fare_amount > 30.00\"
with open('duckdb/task2_explain.txt', 'w') as f:
    for q in (qa, qb):
        f.write(con.execute(q).fetchall()[0][1] + '\n')
"

$PY utils/run_experiments.py --task task2 --dataset taxi_2m.parquet --rows 2000000
```

### Task 3
```bash
$PY spark/task3.py taxi_2m.parquet taxi_zone_lookup.csv
$PY -c "import duckdb; print(duckdb.connect().execute(open('duckdb/task3.sql').read()).fetchall())"
$PY utils/run_experiments.py --task task3 --dataset taxi_2m.parquet --lookup taxi_zone_lookup.csv --rows 2000000
```

### Task 5
```bash
$PY spark/task5.py taxi_1m.parquet
$PY utils/run_experiments.py --task task5 --dataset taxi_1m.parquet --rows 1000000
```

### Checks
```bash
head results/timings.csv
wc -l results/timings.csv
cat results/errors.txt 2>/dev/null
cat spark/task2_explain.txt
cat duckdb/task2_explain.txt
```

### Known gap: plotting
`utils/plot_scaling.py`, referenced later in this README, does not exist in this
repo — only `utils/run_experiments.py` is present. `results/scaling_plot.png`
already exists but there is currently no script here to regenerate it.

## Package versions used
- Python: < — `python --version`>
- mrjob: < — `python -c "import mrjob; print(mrjob.__version__)"`>
- PySpark: < — `python -c "import pyspark; print(pyspark.__version__)"`>
- DuckDB: < — `python -c "import duckdb; print(duckdb.__version__)"`>

Install everything with:
```
python -m pip install mrjob pyspark duckdb pandas pyarrow matplotlib
```

## Student ID and Machine Configuration

Use the following values in `utils/run_experiments.py` before running any timed experiments:

```python
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
```

These values are written into the experiment results by `utils/run_experiments.py`.

## Machine configuration
See `results/timings.csv` (repeated in every row) — filled in from
`utils/run_experiments.py`'s `MACHINE_CONFIG` dict, per Section 4 of the spec.

## Dataset files expected
Place these in the project root before running anything (NOT included in
the submission ZIP per the spec):

rm -rf bda-env
python -m venv bda-env
source bda-env/bin/activate

- `taxi_100k.csv` / `taxi_100k.parquet`
- `taxi_500k.csv` / `taxi_500k.parquet`
- `taxi_1m.csv` / `taxi_1m.parquet`
- `taxi_2m.csv` / `taxi_2m.parquet`
- `taxi_zone_lookup.csv`

Generate them with `data_prep/prepare_data.sql`:
```
duckdb < data_prep/prepare_data.sql
```
Then verify row counts (queries commented at the bottom of that file)
before starting any timed run.

## Exact commands to reproduce results

**Task 1** (one system at a time, functional check — not timed):
```
python mapreduce/task1.py -r local taxi_1m.csv > mr_task1_output.txt
python spark/task1.py taxi_1m.csv
duckdb -c ".read duckdb/task1.sql"
```

**Task 1 / Task 4 timed runs** (writes to `results/timings.csv`):
```
# single system/dataset:
python utils/run_experiments.py --task task1 --system mapreduce --dataset taxi_1m.csv --rows 1000000
python utils/run_experiments.py --task task1 --system spark     --dataset taxi_1m.csv --rows 1000000
python utils/run_experiments.py --task task1 --system duckdb    --dataset taxi_1m.csv --rows 1000000

# full Task 4 scaling sweep (all 4 sizes x all 3 systems):
python utils/run_experiments.py --task task4
```

**Task 2**:
```
python spark/task2.py taxi_2m.parquet
duckdb -c ".read duckdb/task2.sql" > duckdb/task2_explain.txt
```

**Task 3**:
```
python spark/task3.py taxi_2m.parquet taxi_zone_lookup.csv
duckdb -c ".read duckdb/task3.sql"
```

**Task 5**:
```
python spark/task5.py taxi_1m.parquet          # functional check, prints times
python utils/run_experiments.py --task task5 --dataset taxi_1m.parquet --rows 1000000
```

## Non-default settings required to reproduce
- Spark runs with `master("local[*]")` — local mode only, per spec.
- mrjob uses the `-r local` (local) runner, not `-r inline` and not a real cluster.
- `utils/run_experiments.py` needs `MACHINE_CONFIG`, `STUDENT_ID`,
  `PYTHON_VERSION`, and `TOOL_VERSIONS` filled in at the top of the file
  before any timed run — these values get written into every CSV row.

## Notes
- `utils/run_experiments.py` implements the timing protocol (1 warm-up +
  3 recorded runs per system/task/dataset; 5 runs for Task 5) but does
  not preemptively kill a run past 900s — if a run genuinely hangs,
  interrupt it and log `status=TIMEOUT_900, time_seconds=900` manually.
- All timings must come from this same physical laptop, not a cloud VM
  (per spec Section 3/5) — run everything above locally.

---

## Complete Setup and Reproduction Commands

The following commands document the complete setup, data preparation, experiment execution, verification, plotting, report checking, and final ZIP procedure.

### 0. Environment and Data Preparation

```bash
pyenv local 3.11.11
which python

python --version
pip install "pandas<3" mrjob pyspark duckdb pyarrow matplotlib

duckdb < data_prep/prepare_data.sql

duckdb -c "SELECT COUNT(*) FROM read_csv_auto('taxi_100k.csv');"
duckdb -c "SELECT COUNT(*) FROM read_csv_auto('taxi_500k.csv');"
duckdb -c "SELECT COUNT(*) FROM read_csv_auto('taxi_1m.csv');"
duckdb -c "SELECT COUNT(*) FROM read_csv_auto('taxi_2m.csv');"

cd ~/Documents/A1_STUDENT_ID

source bda-env/bin/activate

# ============================================================
# 0. PREPARE DATA
# ============================================================

duckdb < data_prep/prepare_data.sql

duckdb -c "SELECT COUNT(*) FROM 'taxi_100k.csv';"
duckdb -c "SELECT COUNT(*) FROM 'taxi_100k.parquet';"
duckdb -c "SELECT COUNT(*) FROM 'taxi_500k.csv';"
duckdb -c "SELECT COUNT(*) FROM 'taxi_500k.parquet';"
duckdb -c "SELECT COUNT(*) FROM 'taxi_1m.csv';"
duckdb -c "SELECT COUNT(*) FROM 'taxi_1m.parquet';"
duckdb -c "SELECT COUNT(*) FROM 'taxi_2m.csv';"
duckdb -c "SELECT COUNT(*) FROM 'taxi_2m.parquet';"
```

### 1. Task 1 — MapReduce

```bash
python utils/run_experiments.py \
  --task task1 \
  --system mapreduce \
  --dataset taxi_1m.csv \
  --rows 1000000
```

### Task 1 — Spark

```bash
python utils/run_experiments.py \
  --task task1 \
  --system spark \
  --dataset taxi_1m.csv \
  --rows 1000000
```

### Task 1 — DuckDB

```bash
python utils/run_experiments.py \
  --task task1 \
  --system duckdb \
  --dataset taxi_1m.csv \
  --rows 1000000
```

### 2. Task 2 — Generate Spark EXPLAIN

```bash
python spark/task2.py taxi_2m.parquet
```

### Task 2 — DuckDB EXPLAIN

```bash
duckdb -c "EXPLAIN SELECT * FROM read_parquet('taxi_2m.parquet') WHERE PULocationID = 161 AND fare_amount > 30.00;" \
  > duckdb/task2_explain.txt

duckdb -c "EXPLAIN SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount FROM read_parquet('taxi_2m.parquet') WHERE PULocationID = 161 AND fare_amount > 30.00;" \
  >> duckdb/task2_explain.txt
```

### Task 2 — Timing

```bash
python utils/run_experiments.py \
  --task task2 \
  --dataset taxi_2m.parquet \
  --rows 2000000
```

### 3. Task 3 — Join + Top 5

```bash
python spark/task3.py \
  taxi_2m.parquet \
  taxi_zone_lookup.csv
```

```bash
python utils/run_experiments.py \
  --task task3 \
  --dataset taxi_2m.parquet \
  --lookup taxi_zone_lookup.csv \
  --rows 2000000
```

### 4. Task 4 — Scaling Experiment

100k, 500k, 1m, and 2m rows using MapReduce, Spark, and DuckDB.

```bash
python utils/run_experiments.py --task task4
```

### 5. Task 5 — Repeated Query

Spark uncached, Spark cache build, Spark cached, and DuckDB repeated.

```bash
python utils/run_experiments.py \
  --task task5 \
  --dataset taxi_1m.parquet \
  --rows 1000000
```

### 6. Check Generated Timings

```bash
head results/timings.csv

wc -l results/timings.csv

cat results/errors.txt 2>/dev/null || true
```

### 7. Check EXPLAIN Files

```bash
cat spark/task2_explain.txt

cat duckdb/task2_explain.txt
```

### 8. Check Required Files

```bash
find . -maxdepth 3 -type f | sort
```

### 9. Check Package Versions

```bash
python --version

python -c "import mrjob; print('mrjob', mrjob.__version__)"

python -c "import pyspark; print('pyspark', pyspark.__version__)"

python -c "import duckdb; print('duckdb', duckdb.__version__)"
```

### 10. Create Scaling Plot

Run the plotting script used for the assignment. For example:

```bash
python utils/plot_scaling.py
```

It must create:

```text
results/scaling_plot.png
```

### 11. Create / Verify Report

Make sure these files exist:

```text
report/observations.pdf
README.md
ai/ai_usage.md
```

### 12. Final Directory Check

```bash
find . -maxdepth 3 -type f \
  ! -path "./.git/*" \
  ! -name "*.csv" \
  ! -name "*.parquet" \
  | sort
```

### 13. Create Final ZIP

Replace `A1_STUDENT_ID` with the actual project folder name if necessary.

```bash
cd ..

zip -r A1_MT25063.zip A1_STUDENT_ID \
  -x "A1_STUDENT_ID/*.csv" \
     "A1_STUDENT_ID/*.parquet" \
     "A1_STUDENT_ID/**/taxi_*.csv" \
     "A1_STUDENT_ID/**/taxi_*.parquet"
```

> **Note:** Raw taxi CSV and Parquet datasets are excluded from the final submission ZIP. The actual timing results in `results/timings.csv` should be generated from the required experiments on the same physical laptop.
