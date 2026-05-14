# Databricks notebook source
# ====================================================================
# STAGE 3B: GOLD SECTOR PERFORMANCE
# ====================================================================

from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_view(df, view_name: str) -> None:
    temp_view_name = f"_{view_name.replace('.', '_')}_source"
    df.createOrReplaceTempView(temp_view_name)
    spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {temp_view_name}")


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_sector_performance")
print(f"{'=' * 60}\n")

market = spark.table("gold.gold_market_summary").where(F.col("sector").isNotNull())

gold_sector_performance = (
    market.groupBy("sector")
    .agg(
        F.count("*").alias("num_companies"),
        F.sum("market_cap").alias("total_market_cap"),
        F.round(F.avg("current_price"), 2).alias("avg_price"),
        F.round(F.avg("trailing_pe"), 2).alias("avg_trailing_pe"),
        F.round(F.avg("forward_pe"), 2).alias("avg_forward_pe"),
        F.round(F.avg("profit_margins"), 4).alias("avg_profit_margin"),
        F.round(F.avg("gross_margins"), 4).alias("avg_gross_margin"),
        F.round(F.avg("dividend_yield"), 4).alias("avg_dividend_yield"),
        F.sum("volume").alias("total_volume"),
        F.sum(F.col("current_price") * F.col("volume")).alias("price_volume_sum"),
    )
    .select(
        "sector",
        "num_companies",
        "total_market_cap",
        "avg_price",
        "avg_trailing_pe",
        "avg_forward_pe",
        "avg_profit_margin",
        "avg_gross_margin",
        "avg_dividend_yield",
        "total_volume",
        F.round(
            F.when(F.col("total_volume") != 0, F.col("price_volume_sum") / F.col("total_volume")),
            2,
        ).alias("vwap"),
    )
    .orderBy(F.col("total_market_cap").desc(), F.col("sector"))
)

write_gold_view(gold_sector_performance, "gold.gold_sector_performance")

print(f"✅ {gold_sector_performance.count()} rows written to gold.gold_sector_performance")
