-- Databricks notebook source
-- ====================================================================
-- STAGE 3B: GOLD SECTOR PERFORMANCE
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.gold_sector_performance AS
WITH market AS (
  SELECT *
  FROM gold.gold_market_summary
  WHERE sector IS NOT NULL
),
sector_metrics AS (
  SELECT
    sector,
    COUNT(*) AS num_companies,
    SUM(market_cap) AS total_market_cap,
    ROUND(AVG(current_price), 2) AS avg_price,
    ROUND(AVG(trailing_pe), 2) AS avg_trailing_pe,
    ROUND(AVG(forward_pe), 2) AS avg_forward_pe,
    ROUND(AVG(profit_margins), 4) AS avg_profit_margin,
    ROUND(AVG(gross_margins), 4) AS avg_gross_margin,
    ROUND(AVG(dividend_yield), 4) AS avg_dividend_yield,
    SUM(volume) AS total_volume,
    SUM(current_price * volume) AS price_volume_sum
  FROM market
  GROUP BY sector
)
SELECT
  sector,
  num_companies,
  total_market_cap,
  avg_price,
  avg_trailing_pe,
  avg_forward_pe,
  avg_profit_margin,
  avg_gross_margin,
  avg_dividend_yield,
  total_volume,
  ROUND(CASE WHEN total_volume <> 0 THEN price_volume_sum / total_volume END, 2) AS vwap
FROM sector_metrics;
