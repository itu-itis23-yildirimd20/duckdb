"""
02_pandas_integration.py
DuckDB zero-copy pandas entegrasyonu.
Mevcut bir pandas DataFrame'i SQL ile sorgula — RAM kopyalanmaz.
"""

import os
import time
import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "sales.csv")

print("=" * 60)
print("  DuckDB — Pandas Integration Demo")
print("=" * 60)

# ── Load with pandas first (simulating an existing DataFrame) ──
print("\n[1] Loading CSV with pandas (simulating existing df)...")
df = pd.read_csv(CSV_PATH)
print(f"    Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

# ── 2. Query the DataFrame directly with DuckDB ────────────────
print("\n[2] Querying the pandas DataFrame with DuckDB (zero-copy):")
print("    — df is used directly as a SQL table name\n")

result = duckdb.sql("""
    SELECT
        region,
        category,
        ROUND(SUM(amount), 2)    AS revenue,
        COUNT(*)                 AS orders,
        ROUND(AVG(amount), 2)    AS avg_value
    FROM df
    WHERE amount IS NOT NULL
      AND status = 'completed'
    GROUP BY region, category
    ORDER BY revenue DESC
    LIMIT 10
""").df()   # <-- .df() converts result back to a pandas DataFrame

print(result.to_string(index=False))

# ── 3. Performance comparison ─────────────────────────────────
print("\n[3] Speed comparison — same aggregation:")

t0 = time.perf_counter()
pandas_result = (
    df.dropna(subset=["amount"])
      .query("status == 'completed'")
      .groupby(["region", "category"])
      .agg(revenue=("amount", "sum"), orders=("amount", "count"), avg_value=("amount", "mean"))
      .reset_index()
      .sort_values("revenue", ascending=False)
      .head(10)
)
t_pandas = time.perf_counter() - t0

t0 = time.perf_counter()
duck_result = duckdb.sql("""
    SELECT region, category,
        ROUND(SUM(amount), 2) AS revenue,
        COUNT(*) AS orders,
        ROUND(AVG(amount), 2) AS avg_value
    FROM df
    WHERE amount IS NOT NULL AND status = 'completed'
    GROUP BY region, category
    ORDER BY revenue DESC
    LIMIT 10
""").df()
t_duck = time.perf_counter() - t0

print(f"    Pandas:  {t_pandas:.4f}s")
print(f"    DuckDB:  {t_duck:.4f}s")
print(f"    Speedup: {t_pandas / t_duck:.1f}x faster")

# ── 4. Write back to pandas ────────────────────────────────────
print("\n[4] Result is a regular pandas DataFrame — works with all pandas tools:")
print(f"    Type: {type(result)}")
print(f"    Columns: {list(result.columns)}")

print("\nDone.")
