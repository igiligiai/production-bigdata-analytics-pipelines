-- Databricks notebook source
-- ====================================================================
-- STAGE 3F: GOLD VOLUME ANOMALIES
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.gold_volume_anomalies AS
WITH daily_volume_ranked AS (
  SELECT
    symbol,
    TO_DATE(extracted_at) AS price_date,
    volume,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY symbol, TO_DATE(extracted_at) ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_hourly_prices
  WHERE extracted_at >= date_trunc('day', current_date() - INTERVAL 21 DAY)
    AND volume > 0
),
daily_volume AS (
  SELECT
    symbol,
    price_date,
    volume AS daily_volume
  FROM daily_volume_ranked
  WHERE rn = 1
),
volume_stats AS (
  SELECT
    symbol,
    COUNT(DISTINCT price_date) AS trading_days,
    AVG(daily_volume) AS avg_volume,
    STDDEV(daily_volume) AS volume_stddev
  FROM daily_volume
  WHERE price_date < current_date()
  GROUP BY symbol
  HAVING COUNT(DISTINCT price_date) >= 5
),
today_volume_ranked AS (
  SELECT
    symbol,
    volume,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_hourly_prices
  WHERE extracted_at >= date_trunc('day', current_date())
    AND extracted_at < date_trunc('day', current_date()) + INTERVAL 1 DAY
    AND volume > 0
),
today_volume AS (
  SELECT
    symbol,
    volume AS today_volume
  FROM today_volume_ranked
  WHERE rn = 1
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
  t.today_volume,
  ROUND(s.avg_volume, 0) AS avg_volume_21d,
  ROUND(CASE WHEN s.avg_volume <> 0 THEN t.today_volume / s.avg_volume END, 2) AS volume_ratio,
  CASE
    WHEN s.volume_stddev IS NULL OR s.volume_stddev = 0 THEN 0
    ELSE ROUND((t.today_volume - s.avg_volume) / s.volume_stddev, 2)
  END AS volume_z_score,
  s.trading_days
FROM today_volume t
INNER JOIN volume_stats s
  ON t.symbol = s.symbol
INNER JOIN latest_company c
  ON t.symbol = c.symbol
WHERE t.today_volume > s.avg_volume * 1.5;
