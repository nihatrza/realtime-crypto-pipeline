
import json
import logging
import os
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, KafkaException


# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "crypto-trades"
KAFKA_GROUP_ID = "crypto-trades-db-consumer"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "crypto_dw",
    "user": "de_user",
    "password": os.environ.get("POSTGRES_PASSWORD")
}

# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Database connection
# --------------------------------------------------

def connect_database():

    if not DB_CONFIG["password"]:
        raise RuntimeError("POSTGRES_PASSWORD is not set.")

    connection = psycopg.connect(**DB_CONFIG)

    logger.info("Connected to TimescaleDB successfully!")

    return connection


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
# Insert trade into database
# --------------------------------------------------

INSERT_QUERY = """
    INSERT INTO crypto_trades (
        trade_time,
        event_time,
        symbol,
        trade_id,
        price,
        quantity,
        buyer_is_market_maker,
        source
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (trade_time, symbol, trade_id)
    DO NOTHING;
"""


def process_message(connection, message):

    trade = json.loads(message.value().decode("utf-8"))

    trade_time = datetime.fromtimestamp(
        trade["trade_time"] / 1000,
        tz=timezone.utc
    )

    event_time = datetime.fromtimestamp(
        trade["event_time"] / 1000,
        tz=timezone.utc
    )

    values = (
        trade_time,
        event_time,
        trade["symbol"],
        trade["trade_id"],
        trade["price"],
        trade["quantity"],
        trade["buyer_is_market_maker"],
        trade.get("source", "binance")
    )

    with connection.cursor() as cursor:
        cursor.execute(INSERT_QUERY, values)

    connection.commit()



# --------------------------------------------------
# Main execution
# --------------------------------------------------

def main():

    connection = connect_database()

    consumer.subscribe([KAFKA_TOPIC])

    logger.info("Kafka Consumer started.")
    logger.info("Listening to topic: %s", KAFKA_TOPIC)

    processed_count = 0

    try:

        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():

                raise KafkaException(message.error())

            try:

                process_message(connection, message)

                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                processed_count += 1

                if processed_count % 5000 == 0:
                    logger.info(
                        "Consumer processed %s messages successfully.",
                        processed_count
                    )

            except Exception:

                connection.rollback()

                logger.exception(
                    "Message processing failed. Offset not committed."
                )

    except KeyboardInterrupt:

        logger.info("Consumer interrupted.")

    finally:

        consumer.close()
        connection.close()

        logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()
