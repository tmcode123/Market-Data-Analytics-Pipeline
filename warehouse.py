import uuid
import logging
from decimal import Decimal
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

def create_warehouse_objects(cur):
    logger.info("Creating warehouse schemas and tables")

    cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze.alpha_vantage_responses (
            ingestion_id UUID PRIMARY KEY,
            source_system TEXT NOT NULL,
            api_function TEXT NOT NULL,
            symbol TEXT NOT NULL,
            request_params JSONB NOT NULL,
            response_payload JSONB NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.stock_daily_prices (
            symbol TEXT NOT NULL,
            trade_date DATE NOT NULL,
            open_price NUMERIC NOT NULL,
            high_price NUMERIC NOT NULL,
            low_price NUMERIC NOT NULL,
            close_price NUMERIC NOT NULL,
            volume BIGINT NOT NULL,
            ingestion_id UUID NOT NULL REFERENCES bronze.alpha_vantage_responses(ingestion_id),
            transformed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (symbol, trade_date)
        )
    """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS gold.stock_daily_analytics (
                symbol TEXT NOT NULL,
                trade_date DATE NOT NULL,
                open_price NUMERIC NOT NULL,
                high_price NUMERIC NOT NULL,
                low_price NUMERIC NOT NULL,
                close_price NUMERIC NOT NULL,
                volume BIGINT NOT NULL,
                previous_close_price NUMERIC,
                daily_price_change NUMERIC,
                daily_return_pct NUMERIC,
                daily_volatility NUMERIC,
                trend_direction TEXT NOT NULL,
                ingestion_id UUID NOT NULL,
                calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (symbol, trade_date)
            )
        """)


def load_bronze(cur, symbol, request_params, response_payload):
    logger.info("Loading raw API response into Bronze layer")

    ingestion_id = uuid.uuid4()

    cur.execute("""
        INSERT INTO bronze.alpha_vantage_responses (
            ingestion_id,
            source_system,
            api_function,
            symbol,
            request_params,
            response_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        ingestion_id,
        "Alpha Vantage",
        request_params["function"],
        symbol,
        Jsonb(request_params),
        Jsonb(response_payload),
    ))
    logger.info("Bronze load completed with ingestion_id: %s", ingestion_id)
    return ingestion_id


def load_silver(cur, symbol, ingestion_id, response_payload):
    logger.info("Transforming raw market data into Silver layer")

    time_series = response_payload["Time Series (Daily)"]

    for trade_date, values in time_series.items():
        cur.execute("""
            INSERT INTO silver.stock_daily_prices (
                symbol,
                trade_date,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                ingestion_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, trade_date)
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                ingestion_id = EXCLUDED.ingestion_id,
                transformed_at = NOW()
        """, (
            symbol,
            trade_date,
            Decimal(values["1. open"]),
            Decimal(values["2. high"]),
            Decimal(values["3. low"]),
            Decimal(values["4. close"]),
            int(values["5. volume"]),
            ingestion_id,
        ))

    logger.info("Silver load completed with %s daily price records", len(time_series))
    return len(time_series)


def run_data_quality_checks(cur, symbol):
    logger.info("Running data quality checks for symbol: %s", symbol)

    cur.execute("""
        SELECT COUNT(*)
        FROM silver.stock_daily_prices
        WHERE symbol = %s
    """, (symbol,))

    row_count = cur.fetchone()[0]

    if row_count == 0:
        raise RuntimeError("Data quality check failed: no Silver records were loaded.")

    cur.execute("""
        SELECT COUNT(*)
        FROM silver.stock_daily_prices
        WHERE symbol = %s
          AND (
              trade_date IS NULL
              OR open_price IS NULL
              OR high_price IS NULL
              OR low_price IS NULL
              OR close_price IS NULL
              OR volume IS NULL
              OR volume < 0
          )
    """, (symbol,))

    invalid_rows = cur.fetchone()[0]

    if invalid_rows > 0:
        raise RuntimeError(f"Data quality check failed: {invalid_rows} invalid Silver records found.")

    logger.info("Data quality checks passed for %s with %s Silver records", symbol, row_count)


def load_gold_analytics(cur, symbol):
    cur.execute("""
        INSERT INTO gold.stock_daily_analytics (
            symbol,
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            previous_close_price,
            daily_price_change,
            daily_return_pct,
            daily_volatility,
            trend_direction,
            ingestion_id
        )
        SELECT
            symbol,
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            previous_close_price,
            close_price - previous_close_price AS daily_price_change,
            CASE
                WHEN previous_close_price IS NULL OR previous_close_price = 0 THEN NULL
                ELSE ROUND(((close_price - previous_close_price) / previous_close_price) * 100, 4)
            END AS daily_return_pct,
            CASE
                WHEN close_price = 0 THEN NULL
                ELSE ROUND(((high_price - low_price) / close_price) * 100, 4)
            END AS daily_volatility,
            CASE
                WHEN previous_close_price IS NULL THEN 'UNKNOWN'
                WHEN close_price > previous_close_price THEN 'UP'
                WHEN close_price < previous_close_price THEN 'DOWN'
                ELSE 'FLAT'
            END AS trend_direction,
            ingestion_id
        FROM (
            SELECT
                symbol,
                trade_date,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                ingestion_id,
                LAG(close_price) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                ) AS previous_close_price
            FROM silver.stock_daily_prices
            WHERE symbol = %s
        ) prices_with_history
        ON CONFLICT (symbol, trade_date)
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            previous_close_price = EXCLUDED.previous_close_price,
            daily_price_change = EXCLUDED.daily_price_change,
            daily_return_pct = EXCLUDED.daily_return_pct,
            daily_volatility = EXCLUDED.daily_volatility,
            trend_direction = EXCLUDED.trend_direction,
            ingestion_id = EXCLUDED.ingestion_id,
            calculated_at = NOW()
    """, (symbol,))

    logger.info("Gold analytics layer updated for symbol: %s", symbol)
