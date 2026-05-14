# Databricks notebook source
# ====================================================================
# STAGE 3C: GOLD BEST PERFORMING TODAY
# ====================================================================

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_best_performing_today")
print(f"{'=' * 60}\n")

company_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())
price_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())
start_of_day = F.date_trunc("DAY", F.current_timestamp())
end_of_day = F.expr("date_trunc('DAY', current_timestamp()) + interval 1 day")

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

today_prices = (
    spark.table("silver.silver_hourly_prices")
    .where(F.col("extracted_at") >= start_of_day)
    .where(F.col("extracted_at") < end_of_day)
    .where(F.col("previous_close") > 0)
    .select(
       F.col("symbol"),
       F.col("day_high"),
       F.col("previous_close"),
       F.col("extracted_at"),
       F.row_number().over(price_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
)

gold_best_performing_today = (
    today_prices.alias("p")
    .join(latest_company.alias("c"), on="symbol", how="inner")
    .select(
       F.col("c.symbol"),
       F.col("c.company_name"),
       F.col("c.sector"),
       F.to_date(F.col("p.extracted_at")).alias("extracted_at_date"),
       F.date_format(F.col("p.extracted_at"), "HH:mm:ss").alias("extracted_at_time"),
       F.round(F.col("p.day_high") - F.col("p.previous_close"), 2).alias("daily_gain_price"),
       F.round(
           (F.col("p.day_high") - F.col("p.previous_close"))
           / F.when(F.col("p.previous_close") != 0, F.col("p.previous_close"))
           * 100,
           2,
       ).alias("daily_gain_percentage"),
    )
    .orderBy(F.col("daily_gain_percentage").desc(), F.col("daily_gain_price").desc())
    .limit(10)
)

write_gold_table(gold_best_performing_today, "gold.gold_best_performing_today")

print(f"✅ {gold_best_performing_today.count()} rows written to gold.gold_best_performing_today")
