# Databricks notebook source
# ====================================================================
# STAGE 3F: GOLD VOLUME ANOMALIES
# ====================================================================

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_volume_anomalies")
print(f"{'=' * 60}\n")

volume_window = Window.partitionBy("symbol", "price_date").orderBy(F.col("extracted_at").desc())
today_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())
company_window = Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())
start_21d = F.expr("date_trunc('day', current_date() - interval 21 day)")
start_today = F.date_trunc("day", F.current_date())
end_today = F.expr("date_trunc('day', current_date()) + interval 1 day")

daily_volume = (
    spark.table("silver.silver_hourly_prices")
    .withColumn("price_date", F.to_date("extracted_at"))
    .where(F.col("extracted_at") >= start_21d)
    .where(F.col("volume") > 0)
    .select(
        F.col("symbol"),
        F.col("price_date"),
        F.col("volume"),
        F.col("extracted_at"),
        F.row_number().over(volume_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
    .select("symbol", "price_date", F.col("volume").alias("daily_volume"))
)

volume_stats = (
    daily_volume.where(F.col("price_date") < F.current_date())
    .groupBy("symbol")
    .agg(
        F.countDistinct("price_date").alias("trading_days"),
        F.avg("daily_volume").alias("avg_volume"),
        F.stddev("daily_volume").alias("volume_stddev"),
    )
    .where(F.col("trading_days") >= 5)
)

today_volume = (
    spark.table("silver.silver_hourly_prices")
    .where(F.col("extracted_at") >= start_today)
    .where(F.col("extracted_at") < end_today)
    .where(F.col("volume") > 0)
    .select(
        F.col("symbol"),
        F.col("volume"),
        F.col("extracted_at"),
        F.row_number().over(today_window).alias("rn"),
    )
    .where(F.col("rn") == 1)
    .select("symbol", F.col("volume").alias("today_volume"))
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

gold_volume_anomalies = (
    today_volume.alias("t")
    .join(volume_stats.alias("s"), on="symbol", how="inner")
    .join(latest_company.alias("c"), on="symbol", how="inner")
    .where(F.col("t.today_volume") > F.col("s.avg_volume") * F.lit(1.5))
    .select(
        F.col("c.symbol"),
        F.col("c.company_name"),
        F.col("c.sector"),
        F.col("t.today_volume"),
        F.round(F.col("s.avg_volume"), 0).alias("avg_volume_21d"),
        F.round(F.col("t.today_volume") / F.when(F.col("s.avg_volume") != 0, F.col("s.avg_volume")), 2).alias(
            "volume_ratio"
        ),
        F.when(
            F.col("s.volume_stddev").isNull() | (F.col("s.volume_stddev") == 0),
            F.lit(0),
        ).otherwise(
            F.round((F.col("t.today_volume") - F.col("s.avg_volume")) / F.col("s.volume_stddev"), 2)
        ).alias("volume_z_score"),
        F.col("s.trading_days"),
    )
    .orderBy(F.col("volume_ratio").desc(), F.col("today_volume").desc())
)

write_gold_table(gold_volume_anomalies, "gold.gold_volume_anomalies")

print(f"✅ {gold_volume_anomalies.count()} rows written to gold.gold_volume_anomalies")
