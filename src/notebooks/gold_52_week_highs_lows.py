# Databricks notebook source
# ====================================================================
# STAGE 3G: GOLD 52-WEEK HIGHS/LOWS
# ====================================================================

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_52_week_highs_lows")
print(f"{'=' * 60}\n")

price_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())
company_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())

latest_prices = (
    spark.table("silver.silver_hourly_prices")
    .select(
        F.col("symbol"),
        F.col("current_price"),
        F.col("fifty_two_week_high"),
        F.col("fifty_two_week_low"),
        F.col("extracted_at"),
        F.row_number().over(price_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
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

gold_52_week_highs_lows = (
    latest_prices.alias("p")
    .join(latest_company.alias("c"), on="symbol", how="inner")
    .where(F.col("p.current_price").isNotNull())
    .where(F.col("p.current_price") > 0)
    .where(F.col("p.fifty_two_week_high") > 0)
    .where(F.col("p.fifty_two_week_low") > 0)
    .where(F.col("p.fifty_two_week_high") > F.col("p.fifty_two_week_low"))
    .select(
        F.col("p.symbol"),
        F.col("c.company_name"),
        F.col("c.sector"),
        F.round(
            (F.col("p.current_price") - F.col("p.fifty_two_week_low"))
            / F.when(
                (F.col("p.fifty_two_week_high") - F.col("p.fifty_two_week_low")) != 0,
                F.col("p.fifty_two_week_high") - F.col("p.fifty_two_week_low"),
            )
            * 100,
            2,
        ).alias("range_position_pct"),
        F.when(F.col("p.current_price") >= F.col("p.fifty_two_week_high") * F.lit(0.95), F.lit("NEAR_52W_HIGH"))
        .when(F.col("p.current_price") <= F.col("p.fifty_two_week_low") * F.lit(1.05), F.lit("NEAR_52W_LOW"))
        .otherwise(F.lit("MID_RANGE"))
        .alias("proximity_signal"),
    )
    .orderBy(F.col("range_position_pct").desc(), F.col("p.symbol"))
)

write_gold_table(gold_52_week_highs_lows, "gold.gold_52_week_highs_lows")

print(f"✅ {gold_52_week_highs_lows.count()} rows written to gold.gold_52_week_highs_lows")
