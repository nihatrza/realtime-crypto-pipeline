import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
PARQUET_PATH = BASE_DIR / "data/parquet/crypto_trades"

con = duckdb.connect(":memory:")

con.execute("SET memory_limit = '1GB'")
con.execute("SET threads = 2")

print("\n========== DUCKDB CONFIGURATION ==========")

print("DuckDB version:", duckdb.__version__)
print("Memory limit:", con.execute(
    "SELECT current_setting('memory_limit')"
).fetchone()[0])

print("Threads:", con.execute(
    "SELECT current_setting('threads')"
).fetchone()[0])

print("\n========== PARQUET DATA ==========")

query = f"""
SELECT
    symbol,
    COUNT(*) AS total_trades,
    ROUND(AVG(TRY_CAST(price AS DOUBLE)), 4) AS average_price,
    ROUND(MIN(TRY_CAST(price AS DOUBLE)), 4) AS minimum_price,
    ROUND(MAX(TRY_CAST(price AS DOUBLE)), 4) AS maximum_price,
    ROUND(
        SUM(
            TRY_CAST(price AS DOUBLE) *
            TRY_CAST(quantity AS DOUBLE)
        ), 2
    ) AS trading_volume
FROM read_parquet('{PARQUET_PATH}/**/*.parquet')
GROUP BY symbol
ORDER BY symbol;
"""

result = con.execute(query).df()

print(result.to_string(index=False))

print("\n========== TOTAL RECORDS ==========")

total = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{PARQUET_PATH}/**/*.parquet')
""").fetchone()[0]

print("Total records:", total)

con.close()
