-- Databricks notebook source
-- ====================================================================
-- STAGE 3C: GOLD BEST PERFORMING TODAY
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.gold_best_performing_today AS
WITH latest_company_ranked AS (
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
),
today_prices_ranked AS (
  SELECT
    symbol,
    day_high,
    previous_close,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_hourly_prices
  WHERE extracted_at >= date_trunc('DAY', current_timestamp())
    AND extracted_at < date_trunc('DAY', current_timestamp()) + INTERVAL 1 DAY
    AND previous_close > 0
),
today_prices AS (
  SELECT
    symbol,
    day_high,
    previous_close,
    extracted_at
  FROM today_prices_ranked
  WHERE rn = 1
)
SELECT
  c.symbol,
  c.company_name,
  c.sector,
  TO_DATE(p.extracted_at) AS extracted_at_date,
  date_format(p.extracted_at, 'HH:mm:ss') AS extracted_at_time,
  ROUND(p.day_high - p.previous_close, 2) AS daily_gain_price,
  ROUND(
    CASE
      WHEN p.previous_close <> 0 THEN (p.day_high - p.previous_close) / p.previous_close * 100
    END,
    2
  ) AS daily_gain_percentage
FROM today_prices p
INNER JOIN latest_company c
  ON p.symbol = c.symbol
ORDER BY daily_gain_percentage DESC, daily_gain_price DESC
LIMIT 10;
