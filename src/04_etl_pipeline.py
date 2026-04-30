"""
04_etl_pipeline.py
Tam ETL pipeline: Extract (CSV) -> Transform (DuckDB SQL) -> Load (Parquet)
Bu script assignment'in ana demo dosyasıdır.
"""

import os
import time
import duckdb

BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH       = os.path.join(BASE_DIR, "data", "sales.csv")
OUTPUT_PARQUET = os.path.join(BASE_DIR, "output", "pipeline_results.parquet")

SEPARATOR = "=" * 60

def extract(con, csv_path):
    """Step 1 — Extract: Read raw CSV into a DuckDB table."""
    print("\n[EXTRACT] Reading sales.csv...")
    t0 = time.perf_counter()
    con.execute(f"""
        CREATE TABLE raw_sales AS
        SELECT * FROM '{csv_path}'
    """)
    elapsed = time.perf_counter() - t0
    count = con.sql("SELECT COUNT(*) FROM raw_sales").fetchone()[0]
    print(f"  Loaded {count:,} rows in {elapsed:.2f}s")
    return count

def transform(con):
    """Step 2 — Transform: Clean + aggregate with DuckDB SQL."""
    print("\n[TRANSFORM] Running aggregations...")
    t0 = time.perf_counter()

    con.execute("""
        CREATE TABLE transformed AS
        SELECT
            region,
            category,
            status,
            ROUND(SUM(amount), 2)    AS total_revenue,
            COUNT(*)                 AS order_count,
            ROUND(AVG(amount), 2)    AS avg_order_value,
            ROUND(MAX(amount), 2)    AS max_order_value,
            ROUND(MIN(amount), 2)    AS min_order_value
        FROM raw_sales
        WHERE amount IS NOT NULL
          AND amount > 0
        GROUP BY region, category, status
        ORDER BY total_revenue DESC
    """)

    elapsed = time.perf_counter() - t0
    result_count = con.sql("SELECT COUNT(*) FROM transformed").fetchone()[0]
    print(f"  Aggregated into {result_count:,} result rows in {elapsed:.2f}s")

    print("\n  Top 5 regions by revenue (completed orders):")
    con.sql("""
        SELECT region, ROUND(SUM(total_revenue), 2) AS revenue, SUM(order_count) AS orders
        FROM transformed
        WHERE status = 'completed'
        GROUP BY region
        ORDER BY revenue DESC
        LIMIT 5
    """).show()

def load(con, output_path):
    """Step 3 — Load: Write result to Parquet."""
    print(f"\n[LOAD] Writing to {output_path}...")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (SELECT * FROM transformed)
        TO '{output_path}' (FORMAT PARQUET)
    """)
    elapsed = time.perf_counter() - t0
    size_kb = os.path.getsize(output_path) / 1024
    print(f"  Written {size_kb:.1f} KB in {elapsed:.2f}s")

def main():
    print(SEPARATOR)
    print("  DuckDB ETL Pipeline — YZV 322E")
    print(SEPARATOR)

    pipeline_start = time.perf_counter()
    con = duckdb.connect()

    extract(con, CSV_PATH)
    transform(con)
    load(con, OUTPUT_PARQUET)

    total = time.perf_counter() - pipeline_start
    print(SEPARATOR)
    print(f"  Pipeline complete. Total time: {total:.2f}s")
    print(SEPARATOR)

if __name__ == "__main__":
    main()
