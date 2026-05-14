# Databricks notebook source
# ====================================================================
# STAGE 3E: GOLD MOST VOLATILE
# ====================================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_most_volatile")
print(f"{'=' * 60}\n")

gold_most_volatile = spark.sql(
    """
    WITH intraday_ranges AS (
      SELECT
        symbol,
        CAST(extracted_at AS DATE) AS price_date,
        MAX(day_high) AS day_high,
        MIN(day_low) AS day_low,
        MAX_BY(current_price, extracted_at) AS closing_price
      FROM silver.silver_hourly_prices
      WHERE extracted_at >= date_trunc('day', current_date() - interval 21 day)
        AND day_high > 0
        AND day_low > 0
      GROUP BY symbol, CAST(extracted_at AS DATE)
    ),
    volatility AS (
      SELECT
        symbol,
        COUNT(DISTINCT price_date) AS trading_days,
        ROUND(AVG(day_high - day_low), 2) AS avg_intraday_range,
        ROUND(AVG((day_high - day_low) / NULLIF(day_low, 0) * 100), 2) AS avg_intraday_range_pct,
        ROUND(MAX(day_high - day_low), 2) AS max_intraday_range,
        ROUND(MAX((day_high - day_low) / NULLIF(day_low, 0) * 100), 2) AS max_intraday_range_pct,
        ROUND(STDDEV(closing_price), 2) AS price_stddev,
        ROUND(
          STDDEV(closing_price) / NULLIF(AVG(closing_price), 0) * 100,
          2
        ) AS coefficient_of_variation
      FROM intraday_ranges
      GROUP BY symbol
      HAVING COUNT(DISTINCT price_date) >= 2
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
      v.coefficient_of_variation
    FROM volatility v
    INNER JOIN silver.silver_company_info c
      ON v.symbol = c.symbol
    ORDER BY v.avg_intraday_range_pct DESC, v.max_intraday_range_pct DESC
    LIMIT 10
    """
)

write_gold_table(gold_most_volatile, "gold.gold_most_volatile")

print(f"✅ {gold_most_volatile.count()} rows written to gold.gold_most_volatile")
