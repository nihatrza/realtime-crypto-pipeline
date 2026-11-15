import duckdb

PARQUET_PATH = "data/parquet/crypto_trades_typed/**/*.parquet"
EXPECTED_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

con = duckdb.connect(database=":memory:")
con.execute("SET memory_limit = '1GB'")
con.execute("SET threads = 1")
con.execute("SET preserve_insertion_order = false")

source = f"read_parquet('{PARQUET_PATH}', union_by_name=true)"

def report(name, passed, details):
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}: {details}")

print("\n" + "=" * 55)
print("       CRYPTO DATA LAKE - QUALITY REPORT")
print("=" * 55)

# 1. General statistics
stats = con.execute(f"""
    SELECT
        COUNT(*) AS total_records,
        COUNT(*) FILTER (WHERE symbol IS NULL) AS null_symbols,
        COUNT(*) FILTER (WHERE trade_id IS NULL) AS null_trade_ids,
        COUNT(*) FILTER (WHERE price IS NULL) AS null_prices,
        COUNT(*) FILTER (WHERE quantity IS NULL) AS null_quantities,
        COUNT(*) FILTER (WHERE trade_time IS NULL) AS null_timestamps,
        COUNT(*) FILTER (WHERE event_time IS NULL) AS null_event_times,
        COUNT(*) FILTER (WHERE source IS NULL) AS null_sources,
        COUNT(*) FILTER (WHERE price <= 0) AS invalid_prices,
        COUNT(*) FILTER (WHERE quantity <= 0) AS invalid_quantities,
        COUNT(*) FILTER (WHERE trade_time > CURRENT_TIMESTAMP)
            AS future_timestamps,
        COUNT(*) FILTER (
            WHERE symbol NOT IN ('BTCUSDT', 'ETHUSDT')
        ) AS unexpected_symbols
    FROM {source}
""").fetchone()

(
    total, null_symbols, null_ids, null_prices, null_qty,
    null_times, null_event_times, null_sources,
    invalid_prices, invalid_qty, future_times, unexpected_symbols
) = stats

print(f"\nTotal records: {total:,}")

print("\n===== NULL CHECKS =====")
report("Symbol NULL", null_symbols == 0, null_symbols)
report("Trade ID NULL", null_ids == 0, null_ids)
report("Price NULL", null_prices == 0, null_prices)
report("Quantity NULL", null_qty == 0, null_qty)
report("Trade timestamp NULL", null_times == 0, null_times)
report("Event timestamp NULL", null_event_times == 0, null_event_times)
report("Source NULL", null_sources == 0, null_sources)

print("\n===== VALUE VALIDATION =====")
report("Positive prices", invalid_prices == 0, invalid_prices)
report("Positive quantities", invalid_qty == 0, invalid_qty)
report("Future timestamps", future_times == 0, future_times)
report("Expected symbols", unexpected_symbols == 0, unexpected_symbols)


# 2. Duplicate trade check
print("\n===== DUPLICATE CHECK =====")

duplicate_data = con.execute(f"""
    SELECT
        symbol,
        COUNT(*) AS total_records,
        COUNT(DISTINCT trade_id) AS unique_trade_ids
    FROM {source}
    GROUP BY symbol
    ORDER BY symbol
""").fetchall()

duplicate_total = 0

for symbol, records, unique_ids in duplicate_data:
    duplicates = records - unique_ids
    duplicate_total += duplicates

    report(
        f"{symbol} duplicate IDs",
        duplicates == 0,
        f"Records: {records:,} | Unique: {unique_ids:,} | Duplicates: {duplicates:,}"
    )

# 3. Expected symbols
print("\n===== SYMBOL DISTRIBUTION =====")

found_symbols = []

for symbol, records, unique_ids in duplicate_data:
    found_symbols.append(symbol)
    print(f"{symbol}: {records:,} records")

missing_symbols = set(EXPECTED_SYMBOLS) - set(found_symbols)

report(
    "All expected symbols present",
    len(missing_symbols) == 0,
    f"Missing: {sorted(missing_symbols)}"
)


# 4. Kafka metadata
print("\n===== KAFKA METADATA =====")

metadata = con.execute(f"""
    SELECT
        COUNT(*) FILTER (WHERE kafka_partition IS NULL),
        COUNT(*) FILTER (WHERE kafka_offset IS NULL)
    FROM {source}
""").fetchone()

print(f"Missing Kafka partitions: {metadata[0]:,}")
print(f"Missing Kafka offsets: {metadata[1]:,}")
print("Informational only - not included in final status.")

# 5. Data timespan
print("\n===== DATA TIMESPAN =====")

time_range = con.execute(f"""
    SELECT MIN(trade_time), MAX(trade_time)
    FROM {source}
""").fetchone()

print(f"First trade: {time_range[0]}")
print(f"Last trade:  {time_range[1]}")

# 6. Final result
critical_checks = [
    null_symbols == 0,
    null_ids == 0,
    null_prices == 0,
    null_qty == 0,
    null_times == 0,
    null_event_times == 0,
    null_sources == 0,
    invalid_prices == 0,
    invalid_qty == 0,
    future_times == 0,
    unexpected_symbols == 0,
    duplicate_total == 0,
    len(missing_symbols) == 0,
]

print("\n" + "=" * 55)

if all(critical_checks):
    print("FINAL RESULT: PASS")
    print("All critical data quality checks passed.")
else:
    print("FINAL RESULT: FAIL")
    print("One or more critical data quality checks failed.")

print("=" * 55)

con.close()

# 7. Save machine-readable quality report
import json
from pathlib import Path
from datetime import datetime, timezone

report_data = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "status": "PASS" if all(critical_checks) else "FAIL",
    "total_records": total,
    "symbols": {
        symbol: {
            "total_records": records,
            "unique_trade_ids": unique_ids,
            "duplicates": records - unique_ids
        }
        for symbol, records, unique_ids in duplicate_data
    },
    "checks": {
        "null_symbols": null_symbols,
        "null_trade_ids": null_ids,
        "null_prices": null_prices,
        "null_quantities": null_qty,
        "null_trade_timestamps": null_times,
        "null_event_timestamps": null_event_times,
        "null_sources": null_sources,
        "invalid_prices": invalid_prices,
        "invalid_quantities": invalid_qty,
        "future_timestamps": future_times,
        "unexpected_symbols": unexpected_symbols,
        "duplicate_records": duplicate_total,
        "missing_symbols": sorted(missing_symbols)
    },
    "kafka_metadata": {
        "null_partitions": metadata[0],
        "null_offsets": metadata[1]
    },
    "data_timespan": {
        "first_trade": str(time_range[0]),
        "last_trade": str(time_range[1])
    }
}

report_path = Path("data/quality/latest_report.json")
report_path.parent.mkdir(parents=True, exist_ok=True)

temp_path = report_path.with_suffix(".tmp")

with open(temp_path, "w") as file:
    json.dump(report_data, file, indent=4)

temp_path.replace(report_path)

print(f"\nJSON report saved: {report_path}")
