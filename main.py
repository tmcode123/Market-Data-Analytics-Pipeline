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


# The project currently runs the pipeline for one stock symbol.
# If add more symbols later, this could become a list and loop.
SYMBOL = "IBM"

# Database connection values come from Docker Compose when running in containers.
# The defaults make local, non-Docker runs possible if PostgreSQL is available.
# Inside Docker, POSTGRES_HOST is "postgres" because that is the service name.
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
DB_NAME = os.environ.get("POSTGRES_DB", "testDB")
DB_USER = os.environ.get("POSTGRES_USER", "postgres")


logging.basicConfig(
    level=logging.INFO,
    # A consistent log format makes it easier to debug pipeline runs later.
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def get_required_env(name):
    # Fail fast when a required secret/config value is missing.
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


def main():
    logger.info("Starting ETL Pipeline")

    # Secrets are read from environment variables instead of being hard-coded.
    api_key = get_required_env("API_KEY")
    postgres_password = get_required_env("POSTGRES_PASSWORD")

    # Alpha Vantage free API usage is limited, so requests are throttled.
    rate_limiter = RateLimiter(max_requests=5, window_seconds=60)

    # Extract: get raw daily stock prices from the external API.
    response_payload, request_params = fetch_daily_prices(
        SYMBOL,
        api_key,
        rate_limiter=rate_limiter,
    )

    # Load and transform: connect to PostgreSQL and run the medallion workflow.
    # The connection context commits automatically if everything succeeds.
    # If an exception happens, psycopg rolls back the database changes.
    with psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=postgres_password,
    ) as conn:
        with conn.cursor() as cur:
            # Create schemas/tables if this is the first run.
            create_warehouse_objects(cur)

            # Bronze keeps the raw API response for traceability and reprocessing.
            ingestion_id = load_bronze(
                cur,
                SYMBOL,
                request_params,
                response_payload,
            )

            # Silver turns nested JSON into clean relational price rows.
            # row_count is the number of rows in the latest API response, not
            # always the total number of rows already stored in the table.
            row_count = load_silver(
                cur,
                SYMBOL,
                ingestion_id,
                response_payload,
            )

            # Validate Silver data before building reporting metrics.
            run_data_quality_checks(cur, SYMBOL)

            # Gold calculates analytics-friendly metrics for SQL reporting.
            load_gold_analytics(cur, SYMBOL)

    logger.info("Pipeline completed successfully")
    logger.info("Bronze ingestion ID: %s", ingestion_id)
    logger.info("Loaded %s daily prices for %s", row_count, SYMBOL)


if __name__ == "__main__":
    main()
