# Databricks notebook source
# ====================================================================
# STAGE 3D: GOLD WORST PERFORMING 21D
# ====================================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_worst_performing_21d")
print(f"{'=' * 60}\n")

gold_worst_performing_21d = spark.sql(
    """
    WITH daily_prices AS (
      SELECT
        symbol,
        CAST(extracted_at AS DATE) AS price_date,
        MAX_BY(current_price, extracted_at) AS closing_price,
        MAX_BY(previous_close, extracted_at) AS previous_close
      FROM silver.silver_hourly_prices
      WHERE extracted_at >= date_trunc('day', current_date() - interval 21 day)
      GROUP BY symbol, CAST(extracted_at AS DATE)
    ),
    price_endpoints AS (
      SELECT
        symbol,
        MAX_BY(closing_price, price_date) AS latest_price,
        MIN_BY(closing_price, price_date) AS earliest_price,
        MIN(price_date) AS period_start,
        MAX(price_date) AS period_end,
        COUNT(DISTINCT price_date) AS trading_days
      FROM daily_prices
      GROUP BY symbol
      HAVING COUNT(DISTINCT price_date) >= 2
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
        (p.latest_price - p.earliest_price) / NULLIF(p.earliest_price, 0) * 100,
        2
      ) AS price_change_pct
    FROM price_endpoints p
    JOIN silver.silver_company_info c
      ON p.symbol = c.symbol
    ORDER BY price_change_pct ASC, price_change ASC
    LIMIT 10
    """
)

write_gold_table(gold_worst_performing_21d, "gold.gold_worst_performing_21d")

print(f"✅ {gold_worst_performing_21d.count()} rows written to gold.gold_worst_performing_21d")
