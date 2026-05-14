-- Databricks notebook source
-- ====================================================================
-- STAGE 3A: GOLD MARKET SUMMARY
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.gold_market_summary AS
WITH latest_prices_ranked AS (
  SELECT
    symbol,
    current_price,
    previous_close,
    day_high,
    day_low,
    volume,
    market_cap,
    fifty_day_average,
    two_hundred_day_average,
    fifty_two_week_high,
    fifty_two_week_low,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_hourly_prices
  WHERE symbol IS NOT NULL
),
latest_prices AS (
  SELECT
    symbol,
    current_price,
    previous_close,
    day_high,
    day_low,
    volume,
    market_cap,
    fifty_day_average,
    two_hundred_day_average,
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
    industry,
    forward_pe,
    trailing_pe,
    profit_margins,
    gross_margins,
    dividend_yield,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_company_info
),
latest_company AS (
  SELECT
    symbol,
    company_name,
    sector,
    industry,
    forward_pe,
    trailing_pe,
    profit_margins,
    gross_margins,
    dividend_yield
  FROM latest_company_ranked
  WHERE rn = 1
)
SELECT
  p.symbol,
  c.company_name,
  c.sector,
  c.industry,
  p.current_price,
  p.previous_close,
  p.day_high,
  p.day_low,
  p.volume,
  p.market_cap,
  p.fifty_day_average,
  p.two_hundred_day_average,
  p.fifty_two_week_high,
  p.fifty_two_week_low,
  c.trailing_pe,
  c.forward_pe,
  c.profit_margins,
  c.gross_margins,
  c.dividend_yield,
  p.extracted_at AS price_extracted_at
FROM latest_prices p
LEFT JOIN latest_company c
  ON p.symbol = c.symbol;
