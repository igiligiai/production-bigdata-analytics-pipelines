-- Databricks notebook source
-- ====================================================================
-- STAGE 3G: GOLD 52-WEEK HIGHS/LOWS
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.gold_52_week_highs_lows AS
WITH latest_prices_ranked AS (
  SELECT
    symbol,
    current_price,
    fifty_two_week_high,
    fifty_two_week_low,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_hourly_prices
),
latest_prices AS (
  SELECT
    symbol,
    current_price,
    fifty_two_week_high,
    fifty_two_week_low,
    extracted_at
  FROM latest_prices_ranked
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
  p.symbol,
  c.company_name,
  c.sector,
  ROUND(
    CASE
      WHEN (p.fifty_two_week_high - p.fifty_two_week_low) <> 0
      THEN (p.current_price - p.fifty_two_week_low) / (p.fifty_two_week_high - p.fifty_two_week_low) * 100
    END,
    2
  ) AS range_position_pct,
  CASE
    WHEN p.current_price >= p.fifty_two_week_high * 0.95 THEN 'NEAR_52W_HIGH'
    WHEN p.current_price <= p.fifty_two_week_low * 1.05 THEN 'NEAR_52W_LOW'
    ELSE 'MID_RANGE'
  END AS proximity_signal
FROM latest_prices p
INNER JOIN latest_company c
  ON p.symbol = c.symbol
WHERE p.current_price IS NOT NULL
  AND p.current_price > 0
  AND p.fifty_two_week_high > 0
  AND p.fifty_two_week_low > 0
  AND p.fifty_two_week_high > p.fifty_two_week_low;
