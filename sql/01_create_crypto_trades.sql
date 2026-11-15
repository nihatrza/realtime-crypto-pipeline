
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS crypto_trades (
    trade_time TIMESTAMPTZ NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    trade_id BIGINT NOT NULL,
    price NUMERIC(30, 12) NOT NULL,
    quantity NUMERIC(30, 12) NOT NULL,
    buyer_is_market_maker BOOLEAN NOT NULL,
    source VARCHAR(30) DEFAULT 'binance',
    ingested_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (trade_time, symbol, trade_id)
);

SELECT create_hypertable(
    'crypto_trades',
    'trade_time',
    if_not_exists => TRUE
);
