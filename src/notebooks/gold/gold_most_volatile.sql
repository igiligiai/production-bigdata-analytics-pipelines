-- Databricks notebook source
-- ====================================================================
-- STAGE 3E: GOLD MOST VOLATILE
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.gold_most_volatile AS
WITH intraday_ranges_ranked AS (
  SELECT
    symbol,
    TO_DATE(extracted_at) AS price_date,
    day_high,
    day_low,
    current_price AS closing_price,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY symbol, TO_DATE(extracted_at) ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_hourly_prices
  WHERE extracted_at >= date_trunc('day', current_date() - INTERVAL 21 DAY)
    AND day_high > 0
    AND day_low > 0
),
intraday_ranges AS (
  SELECT
    symbol,
    price_date,
    day_high,
    day_low,
    closing_price
  FROM intraday_ranges_ranked
  WHERE rn = 1
),
volatility AS (
  SELECT
    symbol,
    COUNT(DISTINCT price_date) AS trading_days,
    ROUND(AVG(day_high - day_low), 2) AS avg_intraday_range,
    ROUND(AVG((day_high - day_low) / day_low * 100), 2) AS avg_intraday_range_pct,
    ROUND(MAX(day_high - day_low), 2) AS max_intraday_range,
    ROUND(MAX((day_high - day_low) / day_low * 100), 2) AS max_intraday_range_pct,
    ROUND(STDDEV(closing_price), 2) AS price_stddev,
    AVG(closing_price) AS avg_closing_price
  FROM intraday_ranges
  GROUP BY symbol
  HAVING COUNT(DISTINCT price_date) >= 2
),
latest_company_ranked AS (
  SELECT
    symbol,
    company_name,
    sector,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_company_info
),
latest_company AS (
  SELECT
    symbol,
    company_name,
    sector
  FROM latest_company_ranked
  WHERE rn = 1
)
SELECT
  c.symbol,
  c.company_name,
  c.sector,
  v.trading_days,
  v.avg_intraday_range,
  v.avg_intraday_range_pct,
  v.max_intraday_range,
  v.max_intraday_range_pct,
  v.price_stddev,
  ROUND(CASE WHEN v.avg_closing_price <> 0 THEN v.price_stddev / v.avg_closing_price * 100 END, 2) AS coefficient_of_variation
FROM volatility v
INNER JOIN latest_company c
  ON v.symbol = c.symbol
ORDER BY avg_intraday_range_pct DESC, max_intraday_range_pct DESC
LIMIT 10;
