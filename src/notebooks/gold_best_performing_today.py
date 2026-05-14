# Databricks notebook source
# ====================================================================
# STAGE 3C: GOLD BEST PERFORMING TODAY
# ====================================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_best_performing_today")
print(f"{'=' * 60}\n")

gold_best_performing_today = spark.sql(
    """
    WITH latest_company AS (
      SELECT
        symbol,
        company_name,
        sector,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
      FROM silver.silver_company_info
    ),
    today_prices AS (
      SELECT
        symbol,
        day_high,
        previous_close,
        extracted_at,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
      FROM silver.silver_hourly_prices
      WHERE extracted_at >= date_trunc('DAY', current_timestamp())
        AND extracted_at < date_add(date_trunc('DAY', current_timestamp()), 1)
          AND previous_close > 0
      )
    SELECT
      c.symbol,
      c.company_name,
      c.sector,
      CAST(p.extracted_at AS DATE) AS extracted_at_date,
      date_format(p.extracted_at, 'HH:mm:ss') AS extracted_at_time,
      ROUND(p.day_high - p.previous_close, 2) AS daily_gain_price,
      ROUND(
        (p.day_high - p.previous_close) / NULLIF(p.previous_close, 0) * 100,
        2
      ) AS daily_gain_percentage
    FROM today_prices p
    INNER JOIN latest_company c
      ON p.symbol = c.symbol
      AND c.rn = 1
    WHERE p.rn = 1
    ORDER BY daily_gain_percentage DESC, daily_gain_price DESC
    LIMIT 10
    """
)

write_gold_table(gold_best_performing_today, "gold.gold_best_performing_today")

print(f"✅ {gold_best_performing_today.count()} rows written to gold.gold_best_performing_today")
