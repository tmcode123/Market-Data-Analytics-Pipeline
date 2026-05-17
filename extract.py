import random
import time
import logging
from collections import deque
import requests

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


class RateLimiter:
    """Simple sliding-window rate limiter for API requests."""

    def __init__(self, max_requests, window_seconds):
        # max_requests=5 and window_seconds=60 matches the common free-tier limit.
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        # A deque efficiently tracks only the recent request timestamps.
        self.request_times = deque()

    def wait(self):
        # monotonic() is safer for timing than time.time() because it is not
        # affected by system clock changes.
        now = time.monotonic()

        # Remove request timestamps that are outside the active time window.
        while self.request_times and now - self.request_times[0] >= self.window_seconds:
            self.request_times.popleft()

        # If the window is full, pause until another request is allowed.
        if len(self.request_times) >= self.max_requests:
            oldest_request = self.request_times[0]
            wait_seconds = self.window_seconds - (now - oldest_request)
            logger.info("Rate limiter waiting %.1f seconds", wait_seconds)
            time.sleep(wait_seconds)

        self.request_times.append(time.monotonic())


def fetch_daily_prices(symbol, api_key, rate_limiter=None, max_attempts=5):
    """Fetch compact daily stock prices from Alpha Vantage with retries."""

    logger.info("Starting market data extraction for symbol: %s", symbol)

    # These parameters define the Alpha Vantage endpoint and response size.
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        # compact returns the most recent daily records, usually around 100 rows.
        "outputsize": "compact",
        "apikey": api_key,
    }

    # Keep the API key out of logs and Bronze request metadata.
    safe_params = params.copy()
    safe_params.pop("apikey")

    for attempt in range(1, max_attempts + 1):
        if rate_limiter:
            rate_limiter.wait()

        try:
            # timeout prevents the pipeline from hanging forever on a slow request.
            response = requests.get(
                ALPHA_VANTAGE_URL,
                params=params,
                timeout=30,
            )
            # Raises an error for HTTP failures such as 4xx or 5xx responses.
            response.raise_for_status()
            # Convert the HTTP response body into a Python dictionary.
            data = response.json()

        except requests.RequestException as error:
            # Network/server failures are retried with exponential backoff plus jitter.
            wait_seconds = min(2 ** attempt, 60) + random.uniform(0, 1)

            if attempt == max_attempts:
                raise RuntimeError(f"Request failed after {max_attempts} attempts: {error}") from error

            logger.warning("Request error: %s. Retrying in %.1f seconds", error, wait_seconds)
            time.sleep(wait_seconds)
            continue

        except ValueError as error:
            raise RuntimeError("Alpha Vantage returned a non-JSON response.") from error

        if "Error Message" in data:
            # This usually means the symbol or request parameters are invalid.
            raise RuntimeError(data["Error Message"])

        if "Note" in data or "Information" in data:
            # Alpha Vantage returns these fields for rate limits or usage messages.
            wait_seconds = min(2 ** attempt, 60) + random.uniform(0, 1)

            if attempt == max_attempts:
                raise RuntimeError(str(data))

            logger.warning("Alpha Vantage limit/info message received. Retrying in %.1f seconds", wait_seconds)
            time.sleep(wait_seconds)
            continue

        if "Time Series (Daily)" not in data:
            # This protects the transform step from breaking on an unknown payload.
            raise RuntimeError(f"Unexpected Alpha Vantage response: {data}")

        # Return both the raw payload and non-secret request parameters.
        logger.info("Market data extraction completed for symbol: %s", symbol)
        return data, safe_params

    raise RuntimeError("Failed to fetch Alpha Vantage data.")
