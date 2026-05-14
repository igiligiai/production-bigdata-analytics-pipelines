# Databricks notebook source
# ====================================================================
# STAGE 3A: GOLD MARKET SUMMARY
# ====================================================================

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_view(df, view_name: str) -> None:
    staging_table_name = f"{view_name}__staging"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(staging_table_name)
    spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {staging_table_name}")


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_market_summary")
print(f"{'=' * 60}\n")

latest_price_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())
latest_company_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())

latest_prices = (
    spark.table("silver.silver_hourly_prices")
    .select(
        F.col("symbol"),
        F.col("current_price"),
        F.col("previous_close"),
        F.col("day_high"),
        F.col("day_low"),
        F.col("volume"),
        F.col("market_cap"),
        F.col("fifty_day_average"),
        F.col("two_hundred_day_average"),
        F.col("fifty_two_week_high"),
        F.col("fifty_two_week_low"),
        F.col("extracted_at"),
        F.row_number().over(latest_price_window).alias("rn"),
    )
    .where(F.col("symbol").isNotNull())
    .where(F.col("rn") == 1)
)

latest_company = (
    spark.table("silver.silver_company_info")
    .select(
        F.col("symbol"),
        F.col("company_name"),
        F.col("sector"),
        F.col("industry"),
        F.col("forward_pe"),
        F.col("trailing_pe"),
        F.col("profit_margins"),
        F.col("gross_margins"),
        F.col("dividend_yield"),
        F.row_number().over(latest_company_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
)

gold_market_summary = (
    latest_prices.alias("p")
    .join(latest_company.alias("c"), on="symbol", how="left")
    .select(
        F.col("p.symbol"),
        F.col("c.company_name"),
        F.col("c.sector"),
        F.col("c.industry"),
        F.col("p.current_price"),
        F.col("p.previous_close"),
        F.col("p.day_high"),
        F.col("p.day_low"),
        F.col("p.volume"),
        F.col("p.market_cap"),
        F.col("p.fifty_day_average"),
        F.col("p.two_hundred_day_average"),
        F.col("p.fifty_two_week_high"),
        F.col("p.fifty_two_week_low"),
        F.col("c.trailing_pe"),
        F.col("c.forward_pe"),
        F.col("c.profit_margins"),
        F.col("c.gross_margins"),
        F.col("c.dividend_yield"),
        F.col("p.extracted_at").alias("price_extracted_at"),
    )
    .orderBy("symbol")
)

write_gold_view(gold_market_summary, "gold.gold_market_summary")

print(f"✅ {gold_market_summary.count()} rows written to gold.gold_market_summary")
