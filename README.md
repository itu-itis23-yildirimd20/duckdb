# 🦆 DuckDB ETL Demo
### YZV 322E — Applied Data Engineering | Spring 2026

---

## 1. What is this tool?

DuckDB is an open-source, in-process analytical SQL engine designed for OLAP workloads. It runs entirely inside your Python process with no server, no configuration, and no external dependencies — just `pip install duckdb`. It can query CSV, Parquet, and JSON files directly on disk, and integrates seamlessly with pandas DataFrames using zero-copy memory access via Apache Arrow.

---

## 2. Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.9 or higher |
| Docker | 20.10 or higher |
| Docker Compose | v2.0 or higher |
| pip | 22 or higher |
| OS | macOS / Linux / Windows |

> **Note:** Docker is recommended for full reproducibility. The pip path works too if you prefer a local setup.

---

## 3. Installation

### Option A — Docker (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/itu-itis23-yildirimd20/duckdb-etl-demo.git
cd duckdb-etl-demo

# 2. Copy the example env file
cp .env.example .env

# 3. Build and run
docker compose up --build
```

### Option B — pip (Local)

```bash
# 1. Clone the repository
git clone https://github.com/itu-itis23-yildirimd20/duckdb-etl-demo.git
cd duckdb-etl-demo

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the demo
python src/04_etl_pipeline.py
```

---

## 4. Running the Examples

Each script in `src/` demonstrates a different DuckDB feature. Run them in order:

```bash
# Basic queries — query a CSV file directly with SQL
python src/01_basics.py

# Pandas integration — query an existing DataFrame zero-copy
python src/02_pandas_integration.py

# Parquet demo — columnar storage benefits
python src/03_parquet_demo.py

# Full ETL pipeline — Extract → Transform → Load to Parquet + PostgreSQL
python src/04_etl_pipeline.py
```

To run a specific script inside Docker:

```bash
docker compose run app python src/01_basics.py
```

---

## 5. Expected Output

Running `04_etl_pipeline.py` should produce output similar to the following:

```
============================================================
  DuckDB ETL Pipeline Demo — YZV 322E
============================================================

[EXTRACT] Reading sales.csv...
  ✔  Loaded 500,000 rows in 0.31s

[TRANSFORM] Running aggregations with DuckDB SQL...
  ✔  Aggregation complete in 0.08s

  Top 5 Regions by Revenue:
  ┌──────────────┬──────────────┬────────┐
  │ region       │ revenue (₺)  │ orders │
  ├──────────────┼──────────────┼────────┤
  │ Istanbul     │ 2,847,391    │ 98,231 │
  │ Ankara       │ 1,923,847    │ 66,104 │
  │ Izmir        │ 1,456,200    │ 50,892 │
  │ Bursa        │   987,344    │ 34,561 │
  │ Antalya      │   812,903    │ 28,744 │
  └──────────────┴──────────────┴────────┘

[LOAD] Writing results to output/results.parquet...
  ✔  Parquet file written successfully

============================================================
  Pipeline complete. Total time: 0.42s
============================================================
```

> A screenshot of the actual terminal output is available in `docs/expected_output.png`.

---

## 6. Repository Structure

```
duckdb-etl-demo/
├── README.md                    ← You are here
├── data/
│   ├── sales.csv                ← Sample 500K-row e-commerce dataset
│   └── sales.parquet            ← Parquet version of the same dataset
├── src/
│   ├── 01_basics.py             ← Basic DuckDB SQL queries
│   ├── 02_pandas_integration.py ← Zero-copy pandas DataFrame queries
│   ├── 03_parquet_demo.py       ← Columnar storage & Parquet benefits
│   └── 04_etl_pipeline.py       ← Full ETL: Extract → Transform → Load
├── notebooks/
│   └── duckdb_demo.ipynb        ← Interactive Jupyter notebook version
├── output/                      ← Generated output files (gitignored)
├── docs/
│   └── expected_output.png      ← Screenshot of terminal output
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── AI_USAGE.md
```

---

## 7. How It Connects to the Course

| Course Week | Tool | Connection |
|---|---|---|
| Week 9–10 | Python ETL | DuckDB is a Python ETL transformation engine. It implements the Extract → Transform → Load pattern directly. |
| Week 2 | PostgreSQL | DuckDB uses PostgreSQL-compatible SQL. It can also `ATTACH` a live PostgreSQL database and write results into it. |
| Week 8 | Stack Integration | In the full pipeline: NiFi (ingest) → **DuckDB (transform)** → Elasticsearch (index) → Kibana (visualise). |

---

## 8. Key Features Demonstrated

- **Direct file queries** — `SELECT * FROM 'data.csv'` with no loading step
- **Zero-copy pandas integration** — query an existing `df` variable as a SQL table
- **Parquet read/write** — columnar storage for faster I/O
- **PostgreSQL output** — write transformed results to a running Postgres instance
- **In-process execution** — no server, no daemon, no configuration

---

## 9. AI Usage Disclosure

See [`AI_USAGE.md`](AI_USAGE.md) for a full breakdown of which parts of this project used AI assistance and for what purpose.

---

## 10. References

| Source | Link |
|---|---|
| Official Documentation | https://duckdb.org/docs |
| GitHub Repository | https://github.com/duckdb/duckdb |
| Original Paper (SIGMOD 2019) | "DuckDB: an Embeddable Analytical Database" — Raasveldt & Mühleisen |
| SQL on Pandas Benchmark | https://duckdb.org/2021/05/14/sql-on-pandas.html |
| Python API Reference | https://duckdb.org/docs/api/python/overview |

---

*YZV 322E Applied Data Engineering — Spring 2026 — Dr. Mehmet Tunçel — ITU Dept. of AI and Data Engineering*
