"""
01_basics.py
DuckDB temel kullanımı — CSV dosyasını SQL ile direkt sorgulama.
"""

import os
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "sales.csv")

print("=" * 60)
print("  DuckDB Basics Demo")
print("=" * 60)

con = duckdb.connect()  # in-memory, no file needed

# ── 1. Read CSV directly with SQL ─────────────────────────────
print("\n[1] Read CSV directly — no pandas, no loading into RAM:")
result = con.sql(f"SELECT COUNT(*) AS total_rows FROM '{CSV_PATH}'").fetchone()
print(f"    Total rows: {result[0]:,}")

# ── 2. Aggregation ────────────────────────────────────────────
print("\n[2] Top 5 regions by total revenue:")
con.sql(f"""
    SELECT
        region,
        ROUND(SUM(amount), 2)   AS total_revenue,
        COUNT(*)                AS order_count
    FROM '{CSV_PATH}'
    WHERE amount IS NOT NULL
    GROUP BY region
    ORDER BY total_revenue DESC
    LIMIT 5
""").show()

# ── 3. Filter + aggregation ───────────────────────────────────
print("\n[3] Completed orders per category (Electronics only sample):")
con.sql(f"""
    SELECT
        category,
        COUNT(*)            AS orders,
        ROUND(AVG(amount), 2) AS avg_order_value
    FROM '{CSV_PATH}'
    WHERE status = 'completed'
    GROUP BY category
    ORDER BY orders DESC
""").show()

# ── 4. Create a table from CSV ────────────────────────────────
print("\n[4] Loading into DuckDB table for repeated queries:")
con.execute(f"""
    CREATE TABLE sales AS
    SELECT * FROM '{CSV_PATH}'
    WHERE amount IS NOT NULL
""")
row_count = con.sql("SELECT COUNT(*) FROM sales").fetchone()[0]
print(f"    Table 'sales' created with {row_count:,} rows.")

# ── 5. Schema inspection ──────────────────────────────────────
print("\n[5] Schema of the table:")
con.sql("DESCRIBE sales").show()

print("\nDone.")
