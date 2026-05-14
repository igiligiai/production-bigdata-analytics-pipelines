-- Databricks notebook source
-- ====================================================================
-- STAGE 3H: GOLD PRICE ANALYTICS
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.gold_price_analytics AS
WITH market AS (
  SELECT *
  FROM gold.gold_market_summary
),
filtered_market AS (
  SELECT
    symbol,
    company_name,
    sector,
    industry,
    current_price,
    previous_close,
    fifty_day_average,
    two_hundred_day_average,
    fifty_two_week_low,
    fifty_two_week_high,
    volume,
    market_cap,
    price_extracted_at
  FROM market
  WHERE current_price IS NOT NULL
    AND previous_close IS NOT NULL
    AND previous_close > 0
    AND current_price > 0
    AND fifty_two_week_high > 0
    AND fifty_two_week_low > 0
    AND fifty_two_week_high > fifty_two_week_low
)
SELECT
  symbol,
  company_name,
  sector,
  industry,
  current_price,
  previous_close,
  ROUND(current_price - previous_close, 2) AS price_change,
  ROUND((current_price - previous_close) / previous_close * 100, 2) AS price_change_pct,
  fifty_day_average,
  two_hundred_day_average,
  ROUND(current_price - fifty_day_average, 2) AS vs_50d_avg,
  ROUND(current_price - two_hundred_day_average, 2) AS vs_200d_avg,
  CASE
    WHEN current_price > fifty_day_average AND fifty_day_average > two_hundred_day_average THEN 'BULLISH'
    WHEN current_price < fifty_day_average AND fifty_day_average < two_hundred_day_average THEN 'BEARISH'
    ELSE 'NEUTRAL'
  END AS trend_signal,
  fifty_two_week_low,
  fifty_two_week_high,
  ROUND(
    (current_price - fifty_two_week_low) / (fifty_two_week_high - fifty_two_week_low) * 100,
    2
  ) AS range_52w_pct,
  volume,
  market_cap,
  price_extracted_at
FROM filtered_market;
