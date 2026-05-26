# Databricks notebook source
# ====================================================================
# STAGE 2B: COMPANY INFO CLEANING (Silver Layer)
# ====================================================================

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

SOURCE_COMPANY_INFO_CANDIDATES = [
    "bronze.company_info",
    "bronze.bronze_company_info",
]
TARGET_COMPANY_INFO_TABLE = "silver.silver_company_info"


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


def clean_company_info(raw_df: DataFrame, source_name: str) -> DataFrame:
    source_df = ensure_source_metadata(raw_df, source_name)

    typed_df = source_df.select(
        F.col("symbol"),
        F.to_timestamp(F.col("extracted_at"), "yyyy-MM-dd HH:mm:ss").alias("extracted_at"),
        F.col("company_name"),
        F.col("sector"),
        F.col("industry"),
        F.col("quote_type"),
        F.col("long_business_summary"),
        F.col("country"),
        F.col("city"),
        F.col("state"),
        F.col("address"),
        F.col("zip_code"),
        F.col("phone"),
        F.col("website"),
        F.col("ir_website"),
        F.col("full_time_employees").cast("int").alias("full_time_employees"),
        F.col("exchange"),
        F.col("full_exchange_name"),
        F.col("currency"),
        F.coalesce(
            F.to_timestamp(F.col("regular_market_time"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("regular_market_time")),
        ).alias("regular_market_time"),
        F.col("regular_market_open").cast("double").alias("regular_market_open"),
        F.coalesce(
            F.to_timestamp(F.col("first_trade_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("first_trade_date")),
        ).alias("first_trade_date"),
        F.coalesce(
            F.to_timestamp(F.col("last_split_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("last_split_date")),
        ).alias("last_split_date"),
        F.col("last_split_factor"),
        F.col("shares_outstanding").cast("bigint").alias("shares_outstanding"),
        F.col("float_shares").cast("bigint").alias("float_shares"),
        F.col("held_percent_insiders").cast("double").alias("held_percent_insiders"),
        F.col("held_percent_institutions").cast("double").alias("held_percent_institutions"),
        F.col("beta").cast("double").alias("beta"),
        F.col("audit_risk").cast("int").alias("audit_risk"),
        F.col("board_risk").cast("int").alias("board_risk"),
        F.col("compensation_risk").cast("int").alias("compensation_risk"),
        F.col("share_holder_rights_risk").cast("int").alias("share_holder_rights_risk"),
        F.col("overall_risk").cast("int").alias("overall_risk"),
        F.col("dividend_rate").cast("double").alias("dividend_rate"),
        F.col("dividend_yield").cast("double").alias("dividend_yield"),
        F.col("number_of_analyst_opinions").cast("int").alias("number_of_analyst_opinions"),
        F.col("recommendation_key"),
        F.col("total_revenue").cast("bigint").alias("total_revenue"),
        F.col("total_debt").cast("bigint").alias("total_debt"),
        F.col("total_cash").cast("bigint").alias("total_cash"),
        F.col("free_cashflow").cast("bigint").alias("free_cashflow"),
        F.col("book_value").cast("double").alias("book_value"),
        F.col("revenue_growth").cast("double").alias("revenue_growth"),
        F.col("earnings_growth").cast("double").alias("earnings_growth"),
        F.col("profit_margins").cast("double").alias("profit_margins"),
        F.col("gross_margins").cast("double").alias("gross_margins"),
        F.col("operating_margins").cast("double").alias("operating_margins"),
        F.col("return_on_assets").cast("double").alias("return_on_assets"),
        F.col("return_on_equity").cast("double").alias("return_on_equity"),
        F.col("trailing_pe").cast("double").alias("trailing_pe"),
        F.col("forward_pe").cast("double").alias("forward_pe"),
        F.col("peg_ratio").cast("double").alias("peg_ratio"),
        F.col("trailing_eps").cast("double").alias("trailing_eps"),
        F.col("forward_eps").cast("double").alias("forward_eps"),
        F.col("_source_file"),
        F.col("_file_modified_at"),
    ).where(
        F.col("symbol").isNotNull()
        & F.col("currency").isNotNull()
    )

    return typed_df.withColumn(
        "_row_num",
        F.row_number().over(
            Window.partitionBy("symbol", "extracted_at").orderBy(F.col("_file_modified_at").desc())
        ),
    ).where(F.col("_row_num") == 1).drop("_row_num")


# COMMAND ----------

print(f"\n{'=' * 60}")
print("CLEANING STAGE 2B: Company Info")
print(f"{'=' * 60}\n")

company_info_source = resolve_table(SOURCE_COMPANY_INFO_CANDIDATES)
print(f"Cleaning company info from {company_info_source}")

company_info_df = clean_company_info(spark.table(company_info_source), company_info_source)
company_info_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    TARGET_COMPANY_INFO_TABLE
)

print(f"✅ {company_info_df.count()} company info rows written to {TARGET_COMPANY_INFO_TABLE}")
print(f"{TARGET_COMPANY_INFO_TABLE}: {spark.table(TARGET_COMPANY_INFO_TABLE).count()} rows")
