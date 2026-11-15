from decimal import Decimal
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer


# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "crypto-trades"
KAFKA_GROUP_ID = "crypto-parquet-consumer"

BATCH_SIZE = 500

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "parquet" / "crypto_trades_typed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PARQUET_SCHEMA = pa.schema([
    ("event_type", pa.string()),
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("symbol", pa.string()),
    ("trade_id", pa.int64()),
    ("price", pa.decimal128(30, 12)),
    ("quantity", pa.decimal128(30, 12)),
    ("trade_time", pa.timestamp("us", tz="UTC")),
    ("buyer_is_market_maker", pa.bool_()),
    ("source", pa.string()),
    ("kafka_partition", pa.int64()),
    ("kafka_offset", pa.int64())
])


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Kafka Consumer
# --------------------------------------------------

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})


# --------------------------------------------------
# Transform Kafka message
# --------------------------------------------------

def transform_message(message):

    trade = json.loads(message.value().decode("utf-8"))

    return {
        "event_type": trade["event_type"],
        "event_time": datetime.fromtimestamp(
            trade["event_time"] / 1000,
            tz=timezone.utc
        ),
        "symbol": trade["symbol"],
        "trade_id": trade["trade_id"],
        "price": Decimal(str(trade["price"])),
        "quantity": Decimal(str(trade["quantity"])),
        "trade_time": datetime.fromtimestamp(
            trade["trade_time"] / 1000,
            tz=timezone.utc
        ),
        "buyer_is_market_maker": trade["buyer_is_market_maker"],
        "source": trade.get("source", "binance"),
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset()
    }


# --------------------------------------------------
# Write batch to Parquet
# --------------------------------------------------

def write_parquet_batch(batch, batch_number):

    if not batch:
        return

    grouped_trades = defaultdict(list)

    for trade in batch:
        grouped_trades[trade["symbol"]].append(trade)

    batch_timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%S%fZ")

    for symbol, records in grouped_trades.items():

        symbol_dir = OUTPUT_DIR / f"symbol={symbol}"
        symbol_dir.mkdir(parents=True, exist_ok=True)

        file_path = (
            symbol_dir
            / f"batch_{batch_timestamp}_{batch_number}.parquet"
        )

        table = pa.Table.from_pylist(
         records,
         schema=PARQUET_SCHEMA
        )

        pq.write_table(
            table,
            file_path,
            compression="snappy"
        )

        logger.info(
            "Parquet saved | Symbol: %s | Rows: %s | File: %s",
            symbol,
            len(records),
            file_path.name
        )

    # Commit only after all Parquet files are written
    consumer.commit(asynchronous=False)

    logger.info(
        "Batch completed | Total records: %s",
        len(batch)
    )


# --------------------------------------------------
# Main execution
# --------------------------------------------------

def main():

    batch = []
    batch_number = 0

    consumer.subscribe([KAFKA_TOPIC])

    logger.info("Parquet Consumer started.")
    logger.info("Listening to topic: %s", KAFKA_TOPIC)
    logger.info("Output directory: %s", OUTPUT_DIR)

    try:

        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                logger.error("Kafka error: %s", message.error())
                continue

            try:
                record = transform_message(message)
                batch.append(record)

            except Exception:
                logger.exception("Message transformation failed.")
                raise

            if len(batch) >= BATCH_SIZE:

                batch_number += 1

                write_parquet_batch(
                    batch,
                    batch_number
                )

                batch.clear()

    except KeyboardInterrupt:
        logger.info("Shutdown requested.")

    finally:

        if batch:
            batch_number += 1

            write_parquet_batch(
                batch,
                batch_number
            )

        consumer.close()

        logger.info("Parquet Consumer stopped.")


if __name__ == "__main__":
    main()
