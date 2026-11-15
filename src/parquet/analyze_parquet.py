
import duckdb

# Connect to DuckDB (in-memory)
con = duckdb.connect(database=":memory:")

PARQUET_PATH = "data/parquet/crypto_trades/**/*.parquet"

# 1. Total records
print("\n========== TOTAL RECORDS ==========")

print(
    con.execute(f"""
        SELECT COUNT(*) AS total_records
        FROM read_parquet('{PARQUET_PATH}')
    """).df()
)

# 2. Trades by cryptocurrency
print("\n========== TRADES BY SYMBOL ==========")

print(
    con.execute(f"""
        SELECT
            symbol,
            COUNT(*) AS total_trades,
            ROUND(AVG(CAST(price AS DOUBLE)), 4) AS avg_price,
            ROUND(MIN(CAST(price AS DOUBLE)), 4) AS min_price,
            ROUND(MAX(CAST(price AS DOUBLE)), 4) AS max_price
        FROM read_parquet('{PARQUET_PATH}')
        GROUP BY symbol
        ORDER BY total_trades DESC
    """).df()
)

# 3. Trading volume
print("\n========== TRADING VOLUME ==========")

print(
    con.execute(f"""
        SELECT
            symbol,
            ROUND(
                SUM(
                    CAST(price AS DOUBLE) *
                    CAST(quantity AS DOUBLE)
                ), 2
            ) AS total_volume_usdt
        FROM read_parquet('{PARQUET_PATH}')
        GROUP BY symbol
    """).df()
)

# 4. Latest trades
print("\n========== LATEST TRADES ==========")

print(
    con.execute(f"""
        SELECT
            symbol,
            trade_time,
            price,
            quantity
        FROM read_parquet('{PARQUET_PATH}')
        ORDER BY trade_time DESC
        LIMIT 10
    """).df()
)

con.close()
