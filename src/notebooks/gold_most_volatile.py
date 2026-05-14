# Databricks notebook source
# ====================================================================
# STAGE 3E: GOLD MOST VOLATILE
# ====================================================================

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_most_volatile")
print(f"{'=' * 60}\n")

range_window = Window.partitionBy("symbol", "price_date").orderBy(F.col("extracted_at").desc())
company_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())
start_date = F.expr("date_trunc('day', current_date() - interval 21 day)")

intraday_ranges = (
    spark.table("silver.silver_hourly_prices")
    .withColumn("price_date", F.to_date("extracted_at"))
    .where(F.col("extracted_at") >= start_date)
    .where(F.col("day_high") > 0)
    .where(F.col("day_low") > 0)
    .select(
        F.col("symbol"),
        F.col("price_date"),
        F.col("day_high"),
        F.col("day_low"),
        F.col("current_price").alias("closing_price"),
        F.col("extracted_at"),
        F.row_number().over(range_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
)

daily_ranges = intraday_ranges.select(
    "symbol",
    "price_date",
    "day_high",
    "day_low",
    "closing_price",
)

volatility = (
    daily_ranges.groupBy("symbol")
    .agg(
        F.countDistinct("price_date").alias("trading_days"),
        F.round(F.avg(F.col("day_high") - F.col("day_low")), 2).alias("avg_intraday_range"),
        F.round(
            F.avg((F.col("day_high") - F.col("day_low")) / F.when(F.col("day_low") != 0, F.col("day_low")) * 100),
            2,
        ).alias("avg_intraday_range_pct"),
        F.round(F.max(F.col("day_high") - F.col("day_low")), 2).alias("max_intraday_range"),
        F.round(
            F.max((F.col("day_high") - F.col("day_low")) / F.when(F.col("day_low") != 0, F.col("day_low")) * 100),
            2,
        ).alias("max_intraday_range_pct"),
        F.round(F.stddev("closing_price"), 2).alias("price_stddev"),
        F.avg("closing_price").alias("avg_closing_price"),
    )
    .where(F.col("trading_days") >= 2)
    .withColumn(
        "coefficient_of_variation",
        F.round(
            F.col("price_stddev") / F.when(F.col("avg_closing_price") != 0, F.col("avg_closing_price")) * 100,
            2,
        ),
    )
    .drop("avg_closing_price")
)

latest_company = (
    spark.table("silver.silver_company_info")
    .select(
        F.col("symbol"),
        F.col("company_name"),
        F.col("sector"),
        F.row_number().over(company_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
)

gold_most_volatile = (
    volatility.alias("v")
    .join(latest_company.alias("c"), on="symbol", how="inner")
    .select(
        F.col("c.symbol"),
        F.col("c.company_name"),
        F.col("c.sector"),
        F.col("v.trading_days"),
        F.col("v.avg_intraday_range"),
        F.col("v.avg_intraday_range_pct"),
        F.col("v.max_intraday_range"),
        F.col("v.max_intraday_range_pct"),
        F.col("v.price_stddev"),
        F.col("v.coefficient_of_variation"),
    )
    .orderBy(F.col("avg_intraday_range_pct").desc(), F.col("max_intraday_range_pct").desc())
    .limit(10)
)

write_gold_table(gold_most_volatile, "gold.gold_most_volatile")

print(f"✅ {gold_most_volatile.count()} rows written to gold.gold_most_volatile")
