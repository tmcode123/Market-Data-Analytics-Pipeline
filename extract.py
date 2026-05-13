import random
import time
import logging
from collections import deque
import requests

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


class RateLimiter:
    def __init__(self, max_requests, window_seconds):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_times = deque()

    def wait(self):
        now = time.monotonic()

        while self.request_times and now - self.request_times[0] >= self.window_seconds:
            self.request_times.popleft()

        if len(self.request_times) >= self.max_requests:
            oldest_request = self.request_times[0]
            wait_seconds = self.window_seconds - (now - oldest_request)
            logger.info("Rate limiter waiting %.1f seconds", wait_seconds)
            time.sleep(wait_seconds)

        self.request_times.append(time.monotonic())


def fetch_daily_prices(symbol, api_key, rate_limiter=None, max_attempts=5):
    logger.info("Starting market data extraction for symbol: %s", symbol)

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }

    safe_params = params.copy()
    safe_params.pop("apikey")

    for attempt in range(1, max_attempts + 1):
        if rate_limiter:
            rate_limiter.wait()

        try:
            response = requests.get(
                ALPHA_VANTAGE_URL,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

        except requests.RequestException as error:
            wait_seconds = min(2 ** attempt, 60) + random.uniform(0, 1)

            if attempt == max_attempts:
                raise RuntimeError(f"Request failed after {max_attempts} attempts: {error}") from error

            logger.warning("Request error: %s. Retrying in %.1f seconds", error, wait_seconds)
            time.sleep(wait_seconds)
            continue

        except ValueError as error:
            raise RuntimeError("Alpha Vantage returned a non-JSON response.") from error

        if "Error Message" in data:
            raise RuntimeError(data["Error Message"])

        if "Note" in data or "Information" in data:
            wait_seconds = min(2 ** attempt, 60) + random.uniform(0, 1)

            if attempt == max_attempts:
                raise RuntimeError(str(data))

            logger.warning("Alpha Vantage limit/info message received. Retrying in %.1f seconds", wait_seconds)
            time.sleep(wait_seconds)
            continue

        if "Time Series (Daily)" not in data:
            raise RuntimeError(f"Unexpected Alpha Vantage response: {data}")

        logger.info("Market data extraction completed for symbol: %s", symbol)
        return data, safe_params

    raise RuntimeError("Failed to fetch Alpha Vantage data.")