-- Market Data Analytics Pipeline
-- Useful SQL queries for validating and exploring the Bronze, Silver, and Gold layers.

-- Check row counts across each layer.
SELECT
    (SELECT COUNT(*) FROM bronze.alpha_vantage_responses) AS bronze_batches,
    (SELECT COUNT(*) FROM silver.stock_daily_prices) AS silver_price_rows,
    (SELECT COUNT(*) FROM gold.stock_daily_analytics) AS gold_analytics_rows;

-- View the latest cleaned stock price records.
SELECT
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
FROM silver.stock_daily_prices
ORDER BY trade_date DESC
LIMIT 20;

-- View business-ready Gold analytics.
SELECT
    symbol,
    trade_date,
    close_price,
    previous_close_price,
    daily_price_change,
    daily_return_pct,
    daily_volatility,
    trend_direction
FROM gold.stock_daily_analytics
ORDER BY trade_date DESC
LIMIT 20;

-- Find the biggest daily gains.
SELECT
    symbol,
    trade_date,
    close_price,
    previous_close_price,
    daily_price_change,
    daily_return_pct
FROM gold.stock_daily_analytics
WHERE daily_return_pct IS NOT NULL
ORDER BY daily_return_pct DESC
LIMIT 5;

-- Find the biggest daily losses.
SELECT
    symbol,
    trade_date,
    close_price,
    previous_close_price,
    daily_price_change,
    daily_return_pct
FROM gold.stock_daily_analytics
WHERE daily_return_pct IS NOT NULL
ORDER BY daily_return_pct ASC
LIMIT 5;

-- Summarize average, highest, and lowest close prices.
SELECT
    symbol,
    ROUND(AVG(close_price), 2) AS average_close_price,
    MAX(close_price) AS highest_close_price,
    MIN(close_price) AS lowest_close_price
FROM silver.stock_daily_prices
GROUP BY symbol;
