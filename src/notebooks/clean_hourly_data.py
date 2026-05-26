# Databricks notebook source
# ====================================================================
# STAGE 2A: HOURLY PRICE CLEANING (Silver Layer)
# ====================================================================

from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

SOURCE_HOURLY_PRICE_CANDIDATES = [
    "bronze.hourly_prices",
    "bronze.stock_data_hourly",
    "bronze.bronze_hourly_prices",
]
TARGET_HOURLY_PRICES_TABLE = "silver.silver_hourly_prices"


def resolve_table(candidates: list[str]) -> str:
    for table_name in candidates:
        if spark.catalog.tableExists(table_name):
            return table_name
    raise ValueError(f"None of these tables exist: {', '.join(candidates)}")


def ensure_source_metadata(df: DataFrame, source_name: str) -> DataFrame:
    if "_source_file" not in df.columns:
        df = df.withColumn("_source_file", F.lit(source_name))
    else:
        df = df.withColumn("_source_file", F.coalesce(F.col("_source_file"), F.lit(source_name)))

    if "_file_modified_at" not in df.columns:
        df = df.withColumn("_file_modified_at", F.current_timestamp())
    else:
        df = df.withColumn("_file_modified_at", F.coalesce(F.col("_file_modified_at"), F.current_timestamp()))

    return df


def clean_hourly_prices(raw_df: DataFrame, source_name: str) -> DataFrame:
    source_df = ensure_source_metadata(raw_df, source_name).withColumn(
        "_extracted_at_ts",
        F.to_timestamp(F.col("extracted_at"), "yyyy-MM-dd HH:mm:ss"),
    )

    typed_df = source_df.select(
        F.col("symbol"),
        F.col("_extracted_at_ts").alias("extracted_at"),
        F.col("current_price").cast("double").alias("current_price"),
        F.col("day_high").cast("double").alias("day_high"),
        F.col("day_low").cast("double").alias("day_low"),
        F.col("previous_close").cast("double").alias("previous_close"),
        F.col("volume").cast("bigint").alias("volume"),
        F.col("market_cap").cast("bigint").alias("market_cap"),
        F.col("fifty_day_average").cast("double").alias("fifty_day_average"),
        F.col("two_hundred_day_average").cast("double").alias("two_hundred_day_average"),
        F.col("fifty_two_week_high").cast("double").alias("fifty_two_week_high"),
        F.col("fifty_two_week_low").cast("double").alias("fifty_two_week_low"),
        F.col("_source_file"),
        F.col("_file_modified_at"),
    ).where(
        F.col("symbol").isNotNull()
        & F.col("current_price").isNotNull()
        & F.col("day_high").isNotNull()
        & F.col("day_low").isNotNull()
    )

    return typed_df.withColumn(
        "_row_num",
        F.row_number().over(
            Window.partitionBy("symbol", "extracted_at").orderBy(F.col("_file_modified_at").desc())
        ),
    ).where(F.col("_row_num") == 1).drop("_row_num")


def merge_incremental(table_name: str, df: DataFrame, key_columns: list[str]) -> None:
    if spark.catalog.tableExists(table_name):
        merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
        DeltaTable.forName(spark, table_name).alias("target").merge(
            df.alias("source"),
            merge_condition,
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        return

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def apply_incremental_lookback(df: DataFrame, table_name: str) -> DataFrame:
    if not spark.catalog.tableExists(table_name):
        return df

    max_extracted_at = spark.table(table_name).agg(F.max("extracted_at").alias("max_extracted_at")).first()["max_extracted_at"]
    threshold = datetime(1900, 1, 1) if max_extracted_at is None else max_extracted_at - timedelta(days=2)
    return df.where(F.col("extracted_at") >= F.lit(threshold))


# COMMAND ----------

print(f"\n{'=' * 60}")
print("CLEANING STAGE 2A: Hourly Prices")
print(f"{'=' * 60}\n")

hourly_prices_source = resolve_table(SOURCE_HOURLY_PRICE_CANDIDATES)
print(f"Cleaning hourly prices from {hourly_prices_source}")

hourly_prices_df = clean_hourly_prices(spark.table(hourly_prices_source), hourly_prices_source)
hourly_prices_df = apply_incremental_lookback(hourly_prices_df, TARGET_HOURLY_PRICES_TABLE)
merge_incremental(TARGET_HOURLY_PRICES_TABLE, hourly_prices_df, ["symbol", "extracted_at"])

print(f"✅ {hourly_prices_df.count()} hourly price rows prepared for {TARGET_HOURLY_PRICES_TABLE}")
print(f"{TARGET_HOURLY_PRICES_TABLE}: {spark.table(TARGET_HOURLY_PRICES_TABLE).count()} rows")
