# Databricks notebook source
# ====================================================================
# STAGE 3G: GOLD 52-WEEK HIGHS/LOWS
# ====================================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_52_week_highs_lows")
print(f"{'=' * 60}\n")

gold_52_week_highs_lows = spark.sql(
    """
    WITH latest_prices AS (
      SELECT
        symbol,
        current_price,
        fifty_two_week_high,
        fifty_two_week_low,
        extracted_at,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
      FROM silver.silver_hourly_prices
    ),
    latest_company AS (
      SELECT
        symbol,
        company_name,
        sector,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
      FROM silver.silver_company_info
    )
    SELECT
      p.symbol,
      c.company_name,
      c.sector,
      ROUND(
        (p.current_price - p.fifty_two_week_low)
        / NULLIF(p.fifty_two_week_high - p.fifty_two_week_low, 0) * 100,
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
      AND c.rn = 1
    WHERE p.rn = 1
      AND p.current_price IS NOT NULL
      AND p.current_price > 0
      AND p.fifty_two_week_high > 0
      AND p.fifty_two_week_low > 0
      AND p.fifty_two_week_high > p.fifty_two_week_low
    ORDER BY range_position_pct DESC, p.symbol
    """
)

write_gold_table(gold_52_week_highs_lows, "gold.gold_52_week_highs_lows")

print(f"✅ {gold_52_week_highs_lows.count()} rows written to gold.gold_52_week_highs_lows")
