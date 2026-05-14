# Databricks notebook source
# ====================================================================
# STAGE 3D: GOLD WORST PERFORMING 21D
# ====================================================================

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_worst_performing_21d")
print(f"{'=' * 60}\n")

price_window = Window.partitionBy("symbol", "price_date").orderBy(F.col("extracted_at").desc())
earliest_window = Window.partitionBy("symbol").orderBy(F.col("price_date").asc())
latest_window = Window.partitionBy("symbol").orderBy(F.col("price_date").desc())
company_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())
start_date = F.expr("date_trunc('day', current_date() - interval 21 day)")

daily_prices = (
    spark.table("silver.silver_hourly_prices")
    .withColumn("price_date", F.to_date("extracted_at"))
    .where(F.col("extracted_at") >= start_date)
    .select(
        F.col("symbol"),
        F.col("price_date"),
        F.col("current_price").alias("closing_price"),
        F.col("previous_close"),
        F.col("extracted_at"),
        F.row_number().over(price_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
)

earliest_prices = (
    daily_prices.select(
        "symbol",
        "price_date",
        "closing_price",
        F.row_number().over(earliest_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
    .select(
        "symbol",
        F.col("price_date").alias("period_start"),
        F.col("closing_price").alias("earliest_price"),
    )
)

latest_prices = (
    daily_prices.select(
        "symbol",
        "price_date",
        "closing_price",
        F.row_number().over(latest_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
    .select(
        "symbol",
        F.col("price_date").alias("period_end"),
        F.col("closing_price").alias("latest_price"),
    )
)

trading_days = daily_prices.groupBy("symbol").agg(F.countDistinct("price_date").alias("trading_days"))

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

price_endpoints = (
    earliest_prices.join(latest_prices, on="symbol", how="inner")
    .join(trading_days, on="symbol", how="inner")
    .where(F.col("trading_days") >= 2)
)

gold_worst_performing_21d = (
    price_endpoints.alias("p")
    .join(latest_company.alias("c"), on="symbol", how="inner")
    .select(
        F.col("c.symbol"),
        F.col("c.company_name"),
        F.col("c.sector"),
        F.col("p.period_start"),
        F.col("p.period_end"),
        F.col("p.trading_days"),
        F.col("p.earliest_price"),
        F.col("p.latest_price"),
        F.round(F.col("p.latest_price") - F.col("p.earliest_price"), 2).alias("price_change"),
        F.round(
            (F.col("p.latest_price") - F.col("p.earliest_price"))
            / F.when(F.col("p.earliest_price") != 0, F.col("p.earliest_price"))
            * 100,
            2,
        ).alias("price_change_pct"),
    )
    .orderBy(F.col("price_change_pct").asc(), F.col("price_change").asc())
    .limit(10)
)

write_gold_table(gold_worst_performing_21d, "gold.gold_worst_performing_21d")

print(f"✅ {gold_worst_performing_21d.count()} rows written to gold.gold_worst_performing_21d")
