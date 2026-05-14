# Databricks notebook source
# ====================================================================
# STAGE 3B: GOLD SECTOR PERFORMANCE
# ====================================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_sector_performance")
print(f"{'=' * 60}\n")

gold_sector_performance = spark.sql(
    """
    WITH market AS (
      SELECT * FROM gold.gold_market_summary
      WHERE sector IS NOT NULL
    )
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
      ROUND(SUM(current_price * volume) / NULLIF(SUM(volume), 0), 2) AS vwap
    FROM market
    GROUP BY sector
    ORDER BY total_market_cap DESC, sector
    """
)

write_gold_table(gold_sector_performance, "gold.gold_sector_performance")

print(f"✅ {gold_sector_performance.count()} rows written to gold.gold_sector_performance")
