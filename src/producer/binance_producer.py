
import asyncio
import json
import logging
import signal

import websockets
from confluent_kafka import Producer


# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "crypto-trades"

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=btcusdt@trade/ethusdt@trade"
)

# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# --------------------------------------------------
# Kafka Producer
# --------------------------------------------------

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "binance-crypto-producer",
    "acks": "all",
    "enable.idempotence": True,
})


def delivery_report(err, message):
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)


# --------------------------------------------------
# Binance WebSocket
# --------------------------------------------------

running = True


async def consume_binance_stream():

    global running
    trade_count = 0

    while running:

        try:
            logger.info("Connecting to Binance WebSocket...")

            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_queue=1000
            ) as websocket:

                logger.info("Connected to Binance successfully!")

                async for raw_message in websocket:

                    if not running:
                        break

                    message = json.loads(raw_message)

                    # Combined stream structure:
                    # {"stream": "...", "data": {...}}

                    trade = message.get("data")

                    if not trade or trade.get("e") != "trade":
                        continue

                    event = {
                        "event_type": trade["e"],
                        "event_time": trade["E"],
                        "symbol": trade["s"],
                        "trade_id": trade["t"],
                        "price": trade["p"],
                        "quantity": trade["q"],
                        "trade_time": trade["T"],
                        "buyer_is_market_maker": trade["m"],
                        "source": "binance"
                    }

                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=event["symbol"],
                        value=json.dumps(event),
                        callback=delivery_report
                    )

                    producer.poll(0)

                    trade_count += 1

                    if trade_count % 5000 == 0:
                        logger.info(
                            "Producer queued %s trades | Latest: %s",
                            trade_count,
                            event["symbol"]
                        )

        except Exception as error:
            logger.error("WebSocket error: %s", error)

            if running:
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)


# --------------------------------------------------
# Graceful Shutdown
# --------------------------------------------------

def shutdown_handler(signum, frame):
    global running
    running = False
    logger.info("Shutdown signal received...")


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


async def main():

    try:
        await consume_binance_stream()

    finally:
        logger.info("Flushing remaining Kafka messages...")
        producer.flush(timeout=15)
        logger.info("Producer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
