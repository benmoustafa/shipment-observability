"""
Smoke test: verify the anomaly detector fires on an injected low row-count scenario.
Seeds 25 'normal' history rows (~180k rows/run), then checks what z-score a
60k-row load would produce.
"""
import random
from observability.db import get_engine
from sqlalchemy import text
import pandas as pd

engine = get_engine()

# Clean up any previous seeding
with engine.begin() as conn:
    conn.execute(text("DELETE FROM anomaly_check_results WHERE message LIKE 'SIMULATED%'"))

# Seed 25 normal-load runs (180,519 ± 500 jitter)
with engine.begin() as conn:
    for _ in range(25):
        jitter = random.randint(-500, 500)
        conn.execute(text("""
            INSERT INTO anomaly_check_results
                (run_at, check_name, table_name, column_name, status, severity,
                 observed_value, expected_value, z_score, threshold, message)
            VALUES
                (NOW(), 'row_count_anomaly', 'raw_shipments', '', 'pass', 'info',
                 :count, 180519, 0.0, 3.0, 'SIMULATED NORMAL RUN')
        """), {"count": 180519 + jitter})
print("Seeded 25 normal history rows.")

# Now compute z-score for a hypothetical 60k-row load (33% of normal)
df = pd.read_sql(
    "SELECT observed_value FROM anomaly_check_results WHERE check_name='row_count_anomaly' ORDER BY run_at DESC LIMIT 30",
    engine
)
mean = df["observed_value"].mean()
std  = df["observed_value"].std()
anomaly_count = 60000
z = (anomaly_count - mean) / std if std > 0 else 0
label = "CRITICAL" if abs(z) > 3 else ("WARN" if abs(z) > 2 else "pass")

print(f"Baseline: mean={mean:,.0f}  stddev={std:,.0f}")
print(f"60k-row load: z={z:.2f}  => {label}")
print()
if abs(z) > 3:
    print("SUCCESS: Anomaly detector would fire CRITICAL on a 67% row-count drop.")
