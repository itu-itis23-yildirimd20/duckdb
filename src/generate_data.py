"""
generate_data.py
Generates the sample 500K-row sales dataset used in the demos.
Run this once before running the other scripts.
"""

import os
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

N = 500_000

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_path     = os.path.join(BASE_DIR, "data", "sales.csv")
parquet_path = os.path.join(BASE_DIR, "data", "sales.parquet")

regions    = ["Istanbul", "Ankara", "Izmir", "Bursa", "Antalya", "Adana", "Konya", "Mersin"]
categories = ["Electronics", "Clothing", "Food", "Home", "Sports", "Books", "Toys", "Beauty"]
statuses   = ["completed", "completed", "completed", "pending", "refunded"]

start_date = datetime(2024, 1, 1)

print(f"Generating {N:,} rows...")

df = pd.DataFrame({
    "order_id":   range(1, N + 1),
    "region":     np.random.choice(regions, N),
    "category":   np.random.choice(categories, N),
    "amount":     np.round(np.random.exponential(scale=250, size=N), 2),
    "quantity":   np.random.randint(1, 10, size=N),
    "status":     np.random.choice(statuses, N),
    "order_date": [
        (start_date + timedelta(days=int(d))).strftime("%Y-%m-%d")
        for d in np.random.randint(0, 365, size=N)
    ],
})

# Introduce a small number of NULLs to make the cleaning step realistic
null_idx = np.random.choice(df.index, size=500, replace=False)
df.loc[null_idx, "amount"] = None

df.to_csv(csv_path, index=False)
print(f"  CSV written     -> {csv_path}  ({os.path.getsize(csv_path) / 1e6:.1f} MB)")

df.dropna().to_parquet(parquet_path, index=False)
print(f"  Parquet written -> {parquet_path}  ({os.path.getsize(parquet_path) / 1e6:.1f} MB)")
print("Done.")
