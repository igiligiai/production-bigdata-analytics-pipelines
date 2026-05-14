-- Databricks notebook source
-- ====================================================================
-- STAGE 3D: GOLD WORST PERFORMING 21D
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.gold_worst_performing_21d AS
WITH daily_prices_ranked AS (
  SELECT
    symbol,
    TO_DATE(extracted_at) AS price_date,
    current_price AS closing_price,
    previous_close,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY symbol, TO_DATE(extracted_at) ORDER BY extracted_at DESC) AS rn
  FROM silver.silver_hourly_prices
  WHERE extracted_at >= date_trunc('day', current_date() - INTERVAL 21 DAY)
),
daily_prices AS (
  SELECT
    symbol,
    price_date,
    closing_price,
    previous_close,
    extracted_at
  FROM daily_prices_ranked
  WHERE rn = 1
),
earliest_prices AS (
  SELECT
    symbol,
    price_date AS period_start,
    closing_price AS earliest_price
  FROM (
    SELECT
      symbol,
      price_date,
      closing_price,
      ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY price_date ASC) AS rn
    FROM daily_prices
  )
  WHERE rn = 1
),
latest_prices AS (
  SELECT
    symbol,
    price_date AS period_end,
    closing_price AS latest_price
  FROM (
    SELECT
      symbol,
      price_date,
      closing_price,
      ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY price_date DESC) AS rn
    FROM daily_prices
  )
  WHERE rn = 1
),
trading_days AS (
  SELECT
    symbol,
    COUNT(DISTINCT price_date) AS trading_days
  FROM daily_prices
  GROUP BY symbol
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
),
price_endpoints AS (
  SELECT
    e.symbol,
    e.period_start,
    l.period_end,
    td.trading_days,
    e.earliest_price,
    l.latest_price
  FROM earliest_prices e
  INNER JOIN latest_prices l
    ON e.symbol = l.symbol
  INNER JOIN trading_days td
    ON e.symbol = td.symbol
  WHERE td.trading_days >= 2
)
SELECT
  c.symbol,
  c.company_name,
  c.sector,
  p.period_start,
  p.period_end,
  p.trading_days,
  p.earliest_price,
  p.latest_price,
  ROUND(p.latest_price - p.earliest_price, 2) AS price_change,
  ROUND(
    CASE
      WHEN p.earliest_price <> 0 THEN (p.latest_price - p.earliest_price) / p.earliest_price * 100
    END,
    2
  ) AS price_change_pct
FROM price_endpoints p
INNER JOIN latest_company c
  ON p.symbol = c.symbol
ORDER BY price_change_pct ASC, price_change ASC
LIMIT 10;
