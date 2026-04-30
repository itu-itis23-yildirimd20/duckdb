"""
03_parquet_demo.py
DuckDB ile Parquet dosyası okuma/yazma.
Columnar storage avantajlarını gösterir.
"""

import os
import time
import duckdb

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH      = os.path.join(BASE_DIR, "data", "sales.csv")
PARQUET_PATH  = os.path.join(BASE_DIR, "data", "sales.parquet")
OUTPUT_PATH   = os.path.join(BASE_DIR, "output", "results.parquet")

print("=" * 60)
print("  DuckDB — Parquet Demo")
print("=" * 60)

con = duckdb.connect()

# ── 1. File size comparison ────────────────────────────────────
csv_mb     = os.path.getsize(CSV_PATH)     / 1e6
parquet_mb = os.path.getsize(PARQUET_PATH) / 1e6
print(f"\n[1] File size comparison:")
print(f"    CSV:     {csv_mb:.1f} MB")
print(f"    Parquet: {parquet_mb:.1f} MB")
print(f"    Parquet is {csv_mb / parquet_mb:.1f}x smaller")

# ── 2. Read speed comparison ───────────────────────────────────
print("\n[2] Read speed — same aggregation query:")

QUERY_CSV     = f"SELECT region, ROUND(SUM(amount),2) AS rev FROM '{CSV_PATH}'     WHERE amount IS NOT NULL GROUP BY region ORDER BY rev DESC"
QUERY_PARQUET = f"SELECT region, ROUND(SUM(amount),2) AS rev FROM '{PARQUET_PATH}' GROUP BY region ORDER BY rev DESC"

t0 = time.perf_counter()
con.sql(QUERY_CSV).fetchall()
t_csv = time.perf_counter() - t0

t0 = time.perf_counter()
con.sql(QUERY_PARQUET).fetchall()
t_parquet = time.perf_counter() - t0

print(f"    CSV:     {t_csv:.4f}s")
print(f"    Parquet: {t_parquet:.4f}s")
speedup = t_csv / t_parquet if t_parquet > 0 else 1
print(f"    Parquet is {speedup:.1f}x faster")

# ── 3. Columnar projection — only read what you need ──────────
print("\n[3] Columnar projection (only 'amount' column is read from disk):")
result = con.sql(f"""
    SELECT
        ROUND(AVG(amount), 2) AS avg_amount,
        ROUND(MIN(amount), 2) AS min_amount,
        ROUND(MAX(amount), 2) AS max_amount
    FROM '{PARQUET_PATH}'
""")
result.show()

# ── 4. Write a new Parquet from a query result ─────────────────
print("\n[4] Writing transformed result to a new Parquet file...")
con.sql(f"""
    COPY (
        SELECT
            region,
            category,
            ROUND(SUM(amount), 2)  AS total_revenue,
            COUNT(*)               AS order_count,
            ROUND(AVG(amount), 2)  AS avg_order_value
        FROM '{PARQUET_PATH}'
        GROUP BY region, category
        ORDER BY total_revenue DESC
    ) TO '{OUTPUT_PATH}' (FORMAT PARQUET)
""")
output_mb = os.path.getsize(OUTPUT_PATH) / 1e6
print(f"    Written: {OUTPUT_PATH}  ({output_mb:.3f} MB)")

# ── 5. Read the output back ────────────────────────────────────
print("\n[5] Reading it back — verifying output:")
con.sql(f"SELECT * FROM '{OUTPUT_PATH}' LIMIT 5").show()

print("\nDone.")
