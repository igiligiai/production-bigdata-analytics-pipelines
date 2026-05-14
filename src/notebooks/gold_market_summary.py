# Databricks notebook source
# ====================================================================
# STAGE 3A: GOLD MARKET SUMMARY
# ====================================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_market_summary")
print(f"{'=' * 60}\n")

gold_market_summary = spark.sql(
    """
    WITH latest_prices AS (
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
        dividend_yield,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY extracted_at DESC) AS rn
      FROM silver.silver_company_info
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
      ON p.symbol = c.symbol
      AND c.rn = 1
    WHERE p.rn = 1
      AND p.symbol IS NOT NULL
    ORDER BY p.symbol
    """
)

write_gold_table(gold_market_summary, "gold.gold_market_summary")

print(f"✅ {gold_market_summary.count()} rows written to gold.gold_market_summary")
