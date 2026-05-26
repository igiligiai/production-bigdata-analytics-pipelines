# Databricks notebook source
# ====================================================================
# STAGE 1: WEEKLY ANALYSIS (Silver Layer)
# ====================================================================
# Purpose: Perform weekly analysis on the validated silver stock-market
# schema objects and publish weekly summary tables.

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS weekly")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

ANALYSIS_WEEK_WINDOW_DAYS = 35


def resolve_column(df: DataFrame, candidates: list[str], label: str) -> str:
    for column_name in candidates:
        if column_name in df.columns:
            return column_name
    raise ValueError(f"Missing required column for {label}: {', '.join(candidates)}")


def build_optional_column(df: DataFrame, candidates: list[str], dtype: str, alias: str):
    for column_name in candidates:
        if column_name in df.columns:
            return F.col(column_name).cast(dtype).alias(alias)
    return F.lit(None).cast(dtype).alias(alias)


def write_weekly_table(df: DataFrame, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def validate_required_columns(df: DataFrame, table_name: str, columns: list[str]) -> None:
    missing_columns = [column_name for column_name in columns if column_name not in df.columns]
    if missing_columns:
        raise ValueError(f"{table_name} is missing required columns: {', '.join(missing_columns)}")


def validate_unique_keys(df: DataFrame, table_name: str, key_columns: list[str]) -> None:
    total_rows = df.count()
    distinct_rows = df.select(*key_columns).distinct().count()
    if total_rows != distinct_rows:
        raise ValueError(
            f"{table_name} contains duplicate keys for {', '.join(key_columns)} "
            f"({total_rows - distinct_rows} duplicate rows)."
        )


# COMMAND ----------

print(f"\n{'=' * 60}")
print("WEEKLY ANALYSIS: Silver stock-market data")
print(f"{'=' * 60}\n")

hourly_raw = spark.table("silver.silver_hourly_prices")
company_raw = spark.table("silver.silver_company_info")

symbol_col = resolve_column(hourly_raw, ["symbol"], "silver_hourly_prices")
extracted_at_col = resolve_column(hourly_raw, ["extracted_at"], "silver_hourly_prices")
current_price_col = resolve_column(hourly_raw, ["current_price"], "silver_hourly_prices")
day_high_col = resolve_column(hourly_raw, ["day_high"], "silver_hourly_prices")
day_low_col = resolve_column(hourly_raw, ["day_low"], "silver_hourly_prices")
previous_close_col = resolve_column(hourly_raw, ["previous_close"], "silver_hourly_prices")
volume_col = resolve_column(hourly_raw, ["volume"], "silver_hourly_prices")
market_cap_col = resolve_column(hourly_raw, ["market_cap"], "silver_hourly_prices")
fifty_day_average_col = resolve_column(hourly_raw, ["fifty_day_average"], "silver_hourly_prices")
two_hundred_day_average_col = resolve_column(hourly_raw, ["two_hundred_day_average"], "silver_hourly_prices")
fifty_two_week_high_col = resolve_column(hourly_raw, ["fifty_two_week_high"], "silver_hourly_prices")
fifty_two_week_low_col = resolve_column(hourly_raw, ["fifty_two_week_low"], "silver_hourly_prices")

company_symbol_col = resolve_column(company_raw, ["symbol"], "silver_company_info")
company_name_col = resolve_column(company_raw, ["company_name"], "silver_company_info")
sector_col = resolve_column(company_raw, ["sector"], "silver_company_info")
industry_col = resolve_column(company_raw, ["industry"], "silver_company_info")
currency_col = resolve_column(company_raw, ["currency"], "silver_company_info")

hourly_prices = hourly_raw.select(
    F.col(symbol_col).alias("symbol"),
    F.col(extracted_at_col).alias("extracted_at"),
    F.col(current_price_col).cast("double").alias("current_price"),
    F.col(day_high_col).cast("double").alias("day_high"),
    F.col(day_low_col).cast("double").alias("day_low"),
    F.col(previous_close_col).cast("double").alias("previous_close"),
    F.col(volume_col).cast("bigint").alias("volume"),
    F.col(market_cap_col).cast("bigint").alias("market_cap"),
    F.col(fifty_day_average_col).cast("double").alias("fifty_day_average"),
    F.col(two_hundred_day_average_col).cast("double").alias("two_hundred_day_average"),
    F.col(fifty_two_week_high_col).cast("double").alias("fifty_two_week_high"),
    F.col(fifty_two_week_low_col).cast("double").alias("fifty_two_week_low"),
).where(F.col("symbol").isNotNull() & F.col("extracted_at").isNotNull())

company_info = company_raw.select(
    F.col(company_symbol_col).alias("symbol"),
    F.col(company_name_col).alias("company_name"),
    F.col(sector_col).alias("sector"),
    F.col(industry_col).alias("industry"),
    F.col(currency_col).alias("currency"),
).where(F.col("symbol").isNotNull())

latest_prices = (
    hourly_prices.withColumn("_row_num", F.row_number().over(Window.partitionBy("symbol").orderBy(F.col("extracted_at").desc())))
    .where(F.col("_row_num") == 1)
    .drop("_row_num")
)

recent_cutoff = F.date_sub(F.current_date(), ANALYSIS_WEEK_WINDOW_DAYS)

recent_prices = latest_prices.where(F.col("extracted_at") >= recent_cutoff)

market_features = (
    recent_prices.groupBy("symbol")
    .agg(
        F.count("*").alias("observations"),
        F.max("extracted_at").alias("last_observed_at"),
        F.min("extracted_at").alias("first_observed_at"),
        F.avg("current_price").alias("avg_current_price"),
        F.max("current_price").alias("max_current_price"),
        F.min("current_price").alias("min_current_price"),
        F.avg("volume").alias("avg_volume_35d"),
        F.max("volume").alias("max_volume_35d"),
        F.avg("day_high").alias("avg_day_high"),
        F.avg("day_low").alias("avg_day_low"),
        F.avg("previous_close").alias("avg_previous_close"),
        F.avg("market_cap").alias("avg_market_cap"),
        F.avg("fifty_day_average").alias("avg_fifty_day_average"),
        F.avg("two_hundred_day_average").alias("avg_two_hundred_day_average"),
        F.max("fifty_two_week_high").alias("fifty_two_week_high"),
        F.min("fifty_two_week_low").alias("fifty_two_week_low"),
        F.avg(F.col("current_price") - F.col("previous_close")).alias("avg_price_change"),
    )
    .withColumn(
        "price_change_pct",
        F.round(
            F.when(F.col("avg_previous_close") != 0, F.col("avg_price_change") / F.col("avg_previous_close") * 100),
            2,
        ),
    )
    .withColumn("week_over_week_change", F.round(F.col("max_current_price") - F.col("min_current_price"), 2))
    .withColumn(
        "price_position_pct",
        F.round(
            F.when(
                (F.col("fifty_two_week_high") - F.col("fifty_two_week_low")) != 0,
                (F.col("max_current_price") - F.col("fifty_two_week_low"))
                / (F.col("fifty_two_week_high") - F.col("fifty_two_week_low"))
                * 100,
            ),
            2,
        ),
    )
)

symbol_summary = (
    market_features.join(
        latest_prices.select(
            "symbol",
            F.col("current_price").alias("latest_current_price"),
            F.col("day_high").alias("latest_day_high"),
            F.col("day_low").alias("latest_day_low"),
            F.col("previous_close").alias("latest_previous_close"),
            F.col("volume").alias("latest_volume"),
            F.col("market_cap").alias("latest_market_cap"),
            F.col("fifty_day_average").alias("latest_fifty_day_average"),
            F.col("two_hundred_day_average").alias("latest_two_hundred_day_average"),
            F.col("fifty_two_week_high").alias("latest_fifty_two_week_high"),
            F.col("fifty_two_week_low").alias("latest_fifty_two_week_low"),
        ),
        on="symbol",
        how="left",
    ).join(company_info, on="symbol", how="left")
    .withColumn(
        "price_vs_50d_avg_pct",
        F.round(
            F.when(F.col("avg_fifty_day_average") != 0, (F.col("avg_current_price") - F.col("avg_fifty_day_average")) / F.col("avg_fifty_day_average") * 100),
            2,
        ),
    )
    .withColumn(
        "price_vs_200d_avg_pct",
        F.round(
            F.when(
                F.col("avg_two_hundred_day_average") != 0,
                (F.col("avg_current_price") - F.col("avg_two_hundred_day_average")) / F.col("avg_two_hundred_day_average") * 100,
            ),
            2,
        ),
    )
    .withColumn(
        "trend_signal",
        F.when(F.col("price_vs_50d_avg_pct") >= 5, F.lit("BULLISH"))
        .when(F.col("price_vs_50d_avg_pct") <= -5, F.lit("BEARISH"))
        .otherwise(F.lit("NEUTRAL")),
    )
    .withColumn(
        "volatility_band",
        F.when(
            F.when(F.col("avg_current_price") != 0, (F.col("max_current_price") - F.col("min_current_price")) / F.col("avg_current_price")).otherwise(F.lit(None))
            >= 0.15,
            F.lit("HIGH"),
        )
        .when(
            F.when(F.col("avg_current_price") != 0, (F.col("max_current_price") - F.col("min_current_price")) / F.col("avg_current_price")).otherwise(F.lit(None))
            <= 0.05,
            F.lit("LOW"),
        )
        .otherwise(F.lit("MEDIUM")),
    )
    .withColumn("analysis_week_start", F.date_sub(F.current_date(), 7))
    .withColumn("analysis_week_end", F.current_date())
    .withColumn("processed_at", F.current_timestamp())
)

sector_summary = (
    symbol_summary.groupBy("sector")
    .agg(
        F.count("*").alias("company_count"),
        F.countDistinct("symbol").alias("symbol_count"),
        F.sum("avg_market_cap").alias("total_market_cap"),
        F.round(F.avg("avg_current_price"), 2).alias("avg_current_price"),
        F.round(F.avg("price_change_pct"), 2).alias("avg_price_change_pct"),
        F.round(F.avg("avg_volume_35d"), 0).alias("avg_volume_35d"),
    )
    .withColumn("analysis_week_start", F.date_sub(F.current_date(), 7))
    .withColumn("analysis_week_end", F.current_date())
    .withColumn("processed_at", F.current_timestamp())
    .orderBy(F.col("total_market_cap").desc_nulls_last(), F.col("sector"))
)

market_overview = (
    symbol_summary.agg(
        F.count("*").alias("symbol_count"),
        F.countDistinct("sector").alias("sector_count"),
        F.sum("avg_market_cap").alias("total_market_cap"),
        F.round(F.avg("avg_current_price"), 2).alias("avg_current_price"),
        F.round(F.avg("price_change_pct"), 2).alias("avg_price_change_pct"),
        F.round(F.avg("avg_volume_35d"), 0).alias("avg_volume_35d"),
    )
    .withColumn("analysis_week_start", F.date_sub(F.current_date(), 7))
    .withColumn("analysis_week_end", F.current_date())
    .withColumn("processed_at", F.current_timestamp())
)

# COMMAND ----------

write_weekly_table(symbol_summary, "weekly.weekly_symbol_summary")
write_weekly_table(sector_summary, "weekly.weekly_sector_summary")
write_weekly_table(market_overview, "weekly.weekly_market_overview")

validate_required_columns(
    symbol_summary,
    "weekly.weekly_symbol_summary",
    ["symbol", "company_name", "sector", "latest_current_price", "trend_signal", "analysis_week_start", "analysis_week_end"],
)
validate_unique_keys(symbol_summary, "weekly.weekly_symbol_summary", ["symbol"])

validate_required_columns(
    sector_summary,
    "weekly.weekly_sector_summary",
    ["sector", "company_count", "total_market_cap", "avg_current_price"],
)
validate_unique_keys(sector_summary, "weekly.weekly_sector_summary", ["sector"])

validate_required_columns(
    market_overview,
    "weekly.weekly_market_overview",
    ["symbol_count", "sector_count", "total_market_cap", "avg_current_price"],
)

print(f"✅ weekly.weekly_symbol_summary rows: {symbol_summary.count()}")
print(f"✅ weekly.weekly_sector_summary rows: {sector_summary.count()}")
print(f"✅ weekly.weekly_market_overview rows: {market_overview.count()}")
