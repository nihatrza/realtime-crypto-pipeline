
import streamlit as st
import duckdb
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta, timezone

st.set_page_config(
    page_title="Crypto Market Analytics",
    page_icon="📊",
    layout="wide"
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PARQUET_PATH = PROJECT_ROOT / "data/parquet/crypto_trades_typed"
PARQUET_GLOB = str(PARQUET_PATH / "**/*.parquet")


def get_connection():
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit = '1GB'")
    con.execute("SET threads = 1")
    con.execute("SET preserve_insertion_order = false")
    return con


@st.cache_data(ttl=10)
def load_dashboard_data(selected_symbols, hours, bucket):

    con = get_connection()

    placeholders = ",".join(["?"] * len(selected_symbols))

    source = """
        read_parquet(
            ?,
            hive_partitioning=true
        )
    """

    bucket_expression = {
        "1 minute": "minute",
        "5 minutes": "5 minutes",
        "15 minutes": "15 minutes",
        "1 hour": "hour"
    }[bucket]

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    try:
        params = [PARQUET_GLOB] + list(selected_symbols)

        # Latest market price
        latest = con.execute(f"""
            SELECT
                symbol,
                MAX(trade_time) AS trade_time,
                ARG_MAX(
                    CAST(price AS DOUBLE),
                    trade_time
                ) AS price,
                ARG_MAX(
                    CAST(quantity AS DOUBLE),
                    trade_time
                ) AS quantity
            FROM {source}
            WHERE symbol IN ({placeholders})
            GROUP BY symbol
            ORDER BY symbol
        """, params).df()

        # Main KPIs
        kpi = con.execute(f"""
            SELECT
                COUNT(*) AS total_trades,
                SUM(
                    CAST(price AS DOUBLE) *
                    CAST(quantity AS DOUBLE)
                ) AS trading_value,
                COUNT(DISTINCT symbol) AS total_symbols
            FROM {source}
            WHERE symbol IN ({placeholders})
              AND trade_time >= ?
        """, params + [cutoff]).df()

        # Price performance
        price_change = con.execute(f"""
            SELECT
                symbol,
                ARG_MIN(
                    CAST(price AS DOUBLE),
                    trade_time
                ) AS first_price,
                ARG_MAX(
                    CAST(price AS DOUBLE),
                    trade_time
                ) AS last_price
            FROM {source}
            WHERE symbol IN ({placeholders})
              AND trade_time >= ?
            GROUP BY symbol
        """, params + [cutoff]).df()

        # Average price movement
        price_data = con.execute(f"""
            SELECT
                symbol,
                DATE_TRUNC(
                    '{bucket_expression}',
                    trade_time
                ) AS trade_time,
                AVG(CAST(price AS DOUBLE)) AS average_price
            FROM {source}
            WHERE symbol IN ({placeholders})
              AND trade_time >= ?
            GROUP BY
                symbol,
                DATE_TRUNC(
                    '{bucket_expression}',
                    trade_time
                )
            ORDER BY trade_time
        """, params + [cutoff]).df()

        # Trading notional value
        volume_data = con.execute(f"""
            SELECT
                symbol,
                DATE_TRUNC(
                    '{bucket_expression}',
                    trade_time
                ) AS trade_time,
                SUM(
                    CAST(price AS DOUBLE) *
                    CAST(quantity AS DOUBLE)
                ) AS trading_value
            FROM {source}
            WHERE symbol IN ({placeholders})
              AND trade_time >= ?
            GROUP BY
                symbol,
                DATE_TRUNC(
                    '{bucket_expression}',
                    trade_time
                )
            ORDER BY trade_time
        """, params + [cutoff]).df()

        # Transaction count
        activity_data = con.execute(f"""
            SELECT
                symbol,
                DATE_TRUNC(
                    '{bucket_expression}',
                    trade_time
                ) AS trade_time,
                COUNT(*) AS trade_count
            FROM {source}
            WHERE symbol IN ({placeholders})
              AND trade_time >= ?
            GROUP BY
                symbol,
                DATE_TRUNC(
                    '{bucket_expression}',
                    trade_time
                )
            ORDER BY trade_time
        """, params + [cutoff]).df()

        # Latest 20 transactions
        transactions = con.execute(f"""
            SELECT
                symbol,
                trade_id,
                trade_time,
                CAST(price AS DOUBLE) AS price,
                CAST(quantity AS DOUBLE) AS quantity,
                CAST(price AS DOUBLE) *
                    CAST(quantity AS DOUBLE) AS trading_value
            FROM {source}
            WHERE symbol IN ({placeholders})
            ORDER BY trade_time DESC
            LIMIT 20
        """, params).df()

        last_trade = (
            latest["trade_time"].max()
            if not latest.empty
            else None
        )

        return (
            kpi,
            latest,
            price_change,
            price_data,
            volume_data,
            activity_data,
            transactions,
            last_trade
        )

    finally:
        con.close()


# Dashboard Header
st.title("📊 Crypto Market Analytics")

st.caption(
    "Real-Time Cryptocurrency Data Engineering Pipeline | "
    "Binance → Kafka → Parquet → DuckDB → Streamlit"
)

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()


try:

    # Discover available symbols
    con = get_connection()

    available_symbols = con.execute("""
        SELECT DISTINCT symbol
        FROM read_parquet(
            ?,
            hive_partitioning=true
        )
        ORDER BY symbol
    """, [PARQUET_GLOB]).df()["symbol"].tolist()

    con.close()

    if not available_symbols:
        st.warning("No Parquet data found.")
        st.stop()

    # Sidebar filters
    st.sidebar.header("Dashboard Filters")

    selected_symbols = st.sidebar.multiselect(
        "Cryptocurrency",
        available_symbols,
        default=available_symbols
    )

    time_range = st.sidebar.selectbox(
        "Analysis Period",
        [
            "Last 15 minutes",
            "Last 1 hour",
            "Last 3 hours",
            "All available data"
        ],
        index=2
    )

    hours = {
        "Last 15 minutes": 0.25,
        "Last 1 hour": 1,
        "Last 3 hours": 3,
        "All available data": 100000
    }[time_range]

    bucket = st.sidebar.selectbox(
        "Chart Time Interval",
        [
            "1 minute",
            "5 minutes",
            "15 minutes",
            "1 hour"
        ],
        index=0
    )

    if not selected_symbols:
        st.warning("Select at least one cryptocurrency.")
        st.stop()

    # Load data
    (
        kpi,
        latest,
        price_change,
        price_data,
        volume_data,
        activity_data,
        transactions,
        last_trade
    ) = load_dashboard_data(
        tuple(sorted(selected_symbols)),
        hours,
        bucket
    )

    total_trades = int(kpi.iloc[0]["total_trades"])

    trading_value = float(
        kpi.iloc[0]["trading_value"] or 0
    )

    total_symbols = int(kpi.iloc[0]["total_symbols"])

    # Main KPIs
    st.subheader("Market Overview")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Trades",
        f"{total_trades:,}"
    )

    col2.metric(
        "Trading Value (USDT)",
        f"${trading_value:,.2f}"
    )

    col3.metric(
        "Cryptocurrencies",
        str(total_symbols)
    )

    st.divider()

    # Latest market prices
    st.subheader("Latest Market Prices")

    price_cols = st.columns(max(1, len(latest)))

    for col, row in zip(
        price_cols,
        latest.itertuples(index=False)
    ):

        col.metric(
            f"{row.symbol} Latest Price",
            f"${row.price:,.4f}"
        )

    # Price performance
    st.subheader(
        f"Price Performance — {time_range}"
    )

    change_cols = st.columns(
        max(1, len(price_change))
    )

    for col, row in zip(
        change_cols,
        price_change.itertuples(index=False)
    ):

        if row.first_price is not None and row.first_price != 0:

            change_pct = (
                (row.last_price - row.first_price)
                / row.first_price
            ) * 100

            col.metric(
                f"{row.symbol} Price Change",
                f"{change_pct:+.3f}%",
                delta=f"${row.last_price - row.first_price:+,.4f}"
            )

        else:

            col.metric(
                f"{row.symbol} Price Change",
                "Insufficient data"
            )

    st.divider()

    # Separate price charts for each selected cryptocurrency
    st.subheader("Cryptocurrency Price Movement")

    price_columns = st.columns(len(selected_symbols))

    for column, symbol in zip(price_columns, selected_symbols):
        symbol_price_data = price_data[
            price_data["symbol"] == symbol
        ]

        with column:
            if symbol_price_data.empty:
                st.info(f"No price data available for {symbol}.")
                continue

            price_chart = px.line(
                symbol_price_data,
                x="trade_time",
                y="average_price",
                title=f"{symbol.replace('USDT', '')} Average Trade Price — {bucket}",
                labels={
                    "trade_time": "Trade Time (UTC)",
                    "average_price": "Average Price (USDT)"
                }
            )

            price_chart.update_layout(
                hovermode="x unified"
            )

            st.plotly_chart(
                price_chart,
                use_container_width=True
            )

    # Trading value chart
    st.subheader("Trading Activity — USDT")

    volume_chart = px.bar(
        volume_data,
        x="trade_time",
        y="trading_value",
        color="symbol",
        barmode="group",
        title=f"Trading Value — {bucket}",
        labels={
            "trade_time": "Trade Time (UTC)",
            "trading_value": "Trading Value (USDT)",
            "symbol": "Cryptocurrency"
        }
    )

    st.plotly_chart(
        volume_chart,
        use_container_width=True
    )

    # Transaction activity
    st.subheader("Transaction Activity")

    activity_chart = px.line(
        activity_data,
        x="trade_time",
        y="trade_count",
        color="symbol",
        markers=True,
        title=f"Number of Trades — {bucket}",
        labels={
            "trade_time": "Trade Time (UTC)",
            "trade_count": "Number of Trades",
            "symbol": "Cryptocurrency"
        }
    )

    st.plotly_chart(
        activity_chart,
        use_container_width=True
    )

    # Latest transactions
    st.subheader("Latest Transactions")

    st.dataframe(
        transactions,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    st.caption(
        f"Records in selected period: {total_trades:,} | "
        f"Last trade: {last_trade} UTC"
    )

    st.caption(
        "Data source: Typed Parquet Data Lake | "
        "Query engine: DuckDB | "
        "Refresh interval: 10 seconds"
    )


except Exception as error:

    st.error(
        f"Dashboard error: {error}"
    )


# ============================================================

# ============================================================
# PIPELINE HEALTH MONITORING
# ============================================================

import json
import psutil
import subprocess

QUALITY_REPORT_PATH = PROJECT_ROOT / "data/quality/latest_report.json"

SERVICES = {
    "Binance Producer": "crypto-producer",
    "Database Consumer": "crypto-db-consumer",
    "Parquet Consumer": "crypto-parquet-consumer",
    "Streamlit Dashboard": "crypto-dashboard",
}

CONSUMER_GROUPS = {
    "Database Consumer": "crypto-trades-db-consumer",
    "Parquet Consumer": "crypto-parquet-consumer",
}


@st.cache_data(ttl=15)
def get_service_status(service_name):
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service_name],
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


@st.cache_data(ttl=20)
def get_kafka_broker_status():
    try:
        result = subprocess.run(
            [
                "docker", "exec", "kafka",
                "/opt/kafka/bin/kafka-broker-api-versions.sh",
                "--bootstrap-server", "kafka:9092"
            ],
            capture_output=True, text=True, timeout=10
        )
        return result.returncode == 0 and "id:" in result.stdout
    except Exception:
        return False


@st.cache_data(ttl=20)
def get_kafka_topic_status():
    try:
        result = subprocess.run(
            [
                "docker", "exec", "kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "kafka:9092",
                "--describe", "--topic", "crypto-trades"
            ],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            return None
        return result.stdout
    except Exception:
        return None


@st.cache_data(ttl=15)
def get_consumer_lag(group_name):
    try:
        result = subprocess.run(
            [
                "docker", "exec", "kafka",
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server", "kafka:9092",
                "--describe", "--group", group_name
            ],
            capture_output=True, text=True, timeout=10
        )

        if result.returncode != 0:
            return None

        total_lag = 0
        found = False

        for line in result.stdout.splitlines():
            parts = line.split()

            if (
                len(parts) >= 6
                and parts[0] == group_name
                and parts[1] == "crypto-trades"
            ):
                lag = parts[5]
                if lag.isdigit():
                    total_lag += int(lag)
                    found = True

        return total_lag if found else None

    except Exception:
        return None


@st.cache_data(ttl=30)
def get_parquet_stats():
    if not PARQUET_PATH.exists():
        return 0, 0, None

    total_size = 0
    latest_timestamp = None
    file_count = 0

    for file in PARQUET_PATH.rglob("*.parquet"):
        try:
            stat = file.stat()
            file_count += 1
            total_size += stat.st_size

            if latest_timestamp is None or stat.st_mtime > latest_timestamp:
                latest_timestamp = stat.st_mtime
        except OSError:
            continue

    latest_file_time = (
        datetime.fromtimestamp(latest_timestamp, tz=timezone.utc)
        if latest_timestamp is not None else None
    )

    return file_count, total_size, latest_file_time


@st.cache_data(ttl=30)
def load_quality_report():
    if not QUALITY_REPORT_PATH.exists():
        return None

    try:
        with QUALITY_REPORT_PATH.open("r") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return None


def human_bytes(value):
    value = float(value)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024 or unit == "TB":
            return f"{value:.2f} {unit}"
        value /= 1024


def render_monitoring():

    st.divider()
    st.header("Pipeline Health")
    st.caption("Infrastructure, Kafka, pipeline and Data Lake monitoring")

    # --------------------------------------------------------
    # SYSTEM RESOURCES
    # --------------------------------------------------------

    st.subheader("Server Resources")

    cpu = psutil.cpu_percent(interval=0.3)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    network = psutil.net_io_counters()

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("CPU Usage", f"{cpu:.1f}%")
    c2.metric(
        "RAM Usage",
        f"{memory.percent:.1f}%",
        f"{memory.used / 1024**3:.2f} / {memory.total / 1024**3:.2f} GB"
    )
    c3.metric(
        "Disk Usage",
        f"{disk.percent:.1f}%",
        f"{disk.used / 1024**3:.1f} / {disk.total / 1024**3:.1f} GB"
    )
    c4.metric(
        "Network Traffic",
        f"↓ {human_bytes(network.bytes_recv)}",
        f"↑ {human_bytes(network.bytes_sent)}"
    )

    # --------------------------------------------------------
    # PIPELINE SERVICES
    # --------------------------------------------------------

    st.subheader("Pipeline Services")

    service_columns = st.columns(4)
    service_results = {}

    for column, (label, service) in zip(service_columns, SERVICES.items()):
        status = get_service_status(service)
        service_results[label] = status

        with column:
            st.metric(label, status.upper())
            if status == "active":
                st.success("Running")
            else:
                st.error("Service unavailable")

    # --------------------------------------------------------
    # KAFKA
    # --------------------------------------------------------

    st.subheader("Kafka Monitoring")

    broker_ok = get_kafka_broker_status()
    topic_info = get_kafka_topic_status()

    k1, k2 = st.columns(2)

    with k1:
        if broker_ok:
            st.success("Kafka Broker: ONLINE")
        else:
            st.error("Kafka Broker: UNAVAILABLE")

    with k2:
        if topic_info:
            st.success("crypto-trades topic: AVAILABLE")
        else:
            st.error("crypto-trades topic: UNAVAILABLE")

    lag_columns = st.columns(2)
    lag_results = {}

    for column, (label, group) in zip(lag_columns, CONSUMER_GROUPS.items()):
        lag = get_consumer_lag(group)
        lag_results[label] = lag

        with column:
            st.metric(
                f"{label} Lag",
                f"{lag:,} messages" if lag is not None else "Unavailable"
            )

            if lag is None:
                st.error("Could not retrieve consumer lag")
            elif lag > 20000:
                st.error("Critical lag")
            elif lag > 5000:
                st.warning("High lag")
            else:
                st.success("Lag within normal threshold")

    if topic_info:
        with st.expander("Kafka Topic Details"):
            st.code(topic_info)

    # --------------------------------------------------------
    # PARQUET DATA LAKE
    # --------------------------------------------------------

    st.subheader("Parquet Data Lake")

    file_count, total_size, latest_file_time = get_parquet_stats()

    p1, p2, p3 = st.columns(3)

    p1.metric("Parquet Files", f"{file_count:,}")
    p2.metric("Total Data Size", human_bytes(total_size))
    p3.metric(
        "Latest File",
        latest_file_time.strftime("%H:%M:%S UTC")
        if latest_file_time else "No files"
    )

    parquet_age = None

    if latest_file_time:
        parquet_age = (
            datetime.now(timezone.utc) - latest_file_time
        ).total_seconds()

        if parquet_age > 120:
            st.warning(
                f"Latest Parquet file is {parquet_age / 60:.1f} minutes old."
            )
        else:
            st.success("Parquet Data Lake is receiving recent files.")
    else:
        st.error("No Parquet files found.")

    # --------------------------------------------------------
    # DATA QUALITY
    # --------------------------------------------------------

    st.divider()
    st.header("Data Quality Monitoring")

    quality = load_quality_report()

    if quality is None:
        st.error("Quality report unavailable or invalid.")
        quality_age = None
    else:
        quality_status = quality.get("status", "UNKNOWN")
        generated_at = quality.get("generated_at")

        try:
            report_time = datetime.fromisoformat(generated_at)
            if report_time.tzinfo is None:
                report_time = report_time.replace(tzinfo=timezone.utc)

            quality_age = (
                datetime.now(timezone.utc) - report_time.astimezone(timezone.utc)
            ).total_seconds()
        except (ValueError, TypeError):
            quality_age = None

        if quality_status == "PASS":
            st.success("All critical data quality checks passed.")
        elif quality_status == "FAIL":
            st.error("Data quality issues detected!")
        else:
            st.warning(f"Unknown quality status: {quality_status}")

        st.caption(f"Last report: {generated_at or 'N/A'}")

        if quality_age is not None and quality_age > 86400:
            st.warning(f"Quality report is {quality_age / 3600:.1f} hours old.")

        checks = quality.get("checks", {})

        null_total = sum(
            checks.get(key, 0)
            for key in [
                "null_symbols", "null_trade_ids", "null_prices",
                "null_quantities", "null_trade_timestamps",
                "null_event_timestamps", "null_sources"
            ]
        )

        invalid_total = sum(
            checks.get(key, 0)
            for key in [
                "invalid_prices", "invalid_quantities",
                "future_timestamps", "unexpected_symbols"
            ]
        )

        q1, q2, q3, q4 = st.columns(4)

        q1.metric("Total Records", f"{quality.get('total_records', 0):,}")
        q2.metric("Duplicate Records", f"{checks.get('duplicate_records', 0):,}")
        q3.metric("NULL Values", f"{null_total:,}")
        q4.metric("Invalid Values", f"{invalid_total:,}")

        st.subheader("Symbol Data Quality")

        symbols = quality.get("symbols", {})
        symbol_columns = st.columns(2)

        for column, symbol in zip(symbol_columns, ["BTCUSDT", "ETHUSDT"]):
            data = symbols.get(symbol, {})

            with column:
                st.markdown(f"**{symbol}**")
                a, b, c = st.columns(3)
                a.metric("Records", f"{data.get('total_records', 0):,}")
                b.metric("Unique IDs", f"{data.get('unique_trade_ids', 0):,}")
                c.metric("Duplicates", f"{data.get('duplicates', 0):,}")

        with st.expander("Detailed Quality Checks"):
            st.json(checks)

        with st.expander("Data Lake Information"):
            st.write("First trade:", quality.get("data_timespan", {}).get("first_trade"))
            st.write("Last trade:", quality.get("data_timespan", {}).get("last_trade"))
            st.write("Missing Kafka partitions:", quality.get("kafka_metadata", {}).get("null_partitions"))
            st.write("Missing Kafka offsets:", quality.get("kafka_metadata", {}).get("null_offsets"))

    # --------------------------------------------------------
    # ALERT SUMMARY
    # --------------------------------------------------------

    st.divider()
    st.subheader("System Alerts")

    alerts = []

    if cpu > 85:
        alerts.append(f"High CPU usage: {cpu:.1f}%")

    if memory.percent > 85:
        alerts.append(f"High RAM usage: {memory.percent:.1f}%")

    if disk.percent > 80:
        alerts.append(f"High disk usage: {disk.percent:.1f}%")

    for label, status in service_results.items():
        if status != "active":
            alerts.append(f"{label} is {status.upper()}")

    if not broker_ok:
        alerts.append("Kafka broker is unavailable")

    if not topic_info:
        alerts.append("Kafka topic crypto-trades is unavailable")

    for label, lag in lag_results.items():
        if lag is None:
            alerts.append(f"{label} lag could not be retrieved")
        elif lag > 20000:
            alerts.append(f"{label} has critical lag: {lag:,}")
        elif lag > 5000:
            alerts.append(f"{label} has high lag: {lag:,}")

    if parquet_age is None:
        alerts.append("Parquet Data Lake has no files")
    elif parquet_age > 120:
        alerts.append("Parquet Data Lake has not received a recent file")

    if quality is None:
        alerts.append("Data Quality report is unavailable")
    else:
        if quality.get("status") == "FAIL":
            alerts.append("Data Quality status is FAIL")
        if quality_age is not None and quality_age > 86400:
            alerts.append("Data Quality report is older than 24 hours")

    if alerts:
        for alert in alerts:
            st.warning(alert)
    else:
        st.success("No active system alerts.")

    st.caption(
        "Monitoring auto-refreshes every 30 seconds. "
        "Kafka metrics are cached for 15–20 seconds; "
        "Parquet statistics and quality report are cached for 30 seconds."
    )


@st.fragment(run_every="30s")
def monitoring_fragment():
    try:
        render_monitoring()
    except Exception as error:
        st.error(f"Monitoring error: {error}")


monitoring_fragment()
