import logging
import os
import psycopg
from extract import RateLimiter, fetch_daily_prices
from warehouse import (
    create_warehouse_objects,
    load_bronze,
    load_gold_analytics,
    load_silver,
    run_data_quality_checks,
)


SYMBOL = "IBM"

DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
DB_NAME = os.environ.get("POSTGRES_DB", "testDB")
DB_USER = os.environ.get("POSTGRES_USER", "postgres")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def get_required_env(name):
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


def main():
    logger.info("Starting ETL Pipeline")

    api_key = get_required_env("API_KEY")
    postgres_password = get_required_env("POSTGRES_PASSWORD")

    rate_limiter = RateLimiter(max_requests=5, window_seconds=60)

    response_payload, request_params = fetch_daily_prices(
        SYMBOL,
        api_key,
        rate_limiter=rate_limiter,
    )

    with psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=postgres_password,
    ) as conn:
        with conn.cursor() as cur:
            create_warehouse_objects(cur)

            ingestion_id = load_bronze(
                cur,
                SYMBOL,
                request_params,
                response_payload,
            )

            row_count = load_silver(
                cur,
                SYMBOL,
                ingestion_id,
                response_payload,
            )

            run_data_quality_checks(cur, SYMBOL)
            load_gold_analytics(cur, SYMBOL)

    logger.info("Pipeline completed successfully")
    logger.info("Bronze ingestion ID: %s", ingestion_id)
    logger.info("Loaded %s daily prices for %s", row_count, SYMBOL)


if __name__ == "__main__":
    main()