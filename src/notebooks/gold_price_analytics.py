# Databricks notebook source
# ====================================================================
# STAGE 3H: GOLD PRICE ANALYTICS
# ====================================================================

from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_view(df, view_name: str) -> None:
    staging_table_name = f"{view_name}__staging"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(staging_table_name)
    spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {staging_table_name}")


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_price_analytics")
print(f"{'=' * 60}\n")

market = spark.table("gold.gold_market_summary")

gold_price_analytics = (
    market.where(F.col("current_price").isNotNull())
    .where(F.col("previous_close").isNotNull())
    .where(F.col("previous_close") > 0)
    .where(F.col("current_price") > 0)
    .where(F.col("fifty_two_week_high") > 0)
    .where(F.col("fifty_two_week_low") > 0)
    .where(F.col("fifty_two_week_high") > F.col("fifty_two_week_low"))
    .select(
        F.col("symbol"),
        F.col("company_name"),
        F.col("sector"),
        F.col("industry"),
        F.col("current_price"),
        F.col("previous_close"),
        F.round(F.col("current_price") - F.col("previous_close"), 2).alias("price_change"),
        F.round(
            (F.col("current_price") - F.col("previous_close"))
            / F.when(F.col("previous_close") != 0, F.col("previous_close"))
            * 100,
            2,
        ).alias("price_change_pct"),
        F.col("fifty_day_average"),
        F.col("two_hundred_day_average"),
        F.round(F.col("current_price") - F.col("fifty_day_average"), 2).alias("vs_50d_avg"),
        F.round(F.col("current_price") - F.col("two_hundred_day_average"), 2).alias("vs_200d_avg"),
        F.when(
            (F.col("current_price") > F.col("fifty_day_average"))
            & (F.col("fifty_day_average") > F.col("two_hundred_day_average")),
            F.lit("BULLISH"),
        )
        .when(
            (F.col("current_price") < F.col("fifty_day_average"))
            & (F.col("fifty_day_average") < F.col("two_hundred_day_average")),
            F.lit("BEARISH"),
        )
        .otherwise(F.lit("NEUTRAL"))
        .alias("trend_signal"),
        F.col("fifty_two_week_low"),
        F.col("fifty_two_week_high"),
        F.round(
            (F.col("current_price") - F.col("fifty_two_week_low"))
            / F.when((F.col("fifty_two_week_high") - F.col("fifty_two_week_low")) != 0, F.col("fifty_two_week_high") - F.col("fifty_two_week_low"))
            * 100,
            2,
        ).alias("range_52w_pct"),
        F.col("volume"),
        F.col("market_cap"),
        F.col("price_extracted_at"),
    )
    .orderBy("symbol")
)

write_gold_view(gold_price_analytics, "gold.gold_price_analytics")

print(f"✅ {gold_price_analytics.count()} rows written to gold.gold_price_analytics")
