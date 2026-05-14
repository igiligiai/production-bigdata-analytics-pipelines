# Databricks notebook source
# ====================================================================
# STAGE 1: WEEKLY ANALYSIS (Silver Layer)
# ====================================================================
# Purpose: Perform weekly analysis on e-commerce data, including CLV,
# churn prediction, and market segmentation. This stage uses cleaned
# and standardized data from the Silver layer.

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS weekly")
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
print("WEEKLY ANALYSIS: Silver e-commerce data")
print(f"{'=' * 60}\n")

orders_raw = spark.table("silver.orders_cleaned")
products_raw = spark.table("silver.products_cleaned")

order_id_col = resolve_column(orders_raw, ["order_id"], "orders_cleaned")
user_id_col = resolve_column(orders_raw, ["user_id", "userId"], "orders_cleaned")
order_date_col = resolve_column(orders_raw, ["order_date"], "orders_cleaned")
product_ids_col = resolve_column(orders_raw, ["product_ids"], "orders_cleaned")

product_id_col = resolve_column(products_raw, ["product_id"], "products_cleaned")
title_col = resolve_column(products_raw, ["title"], "products_cleaned")
category_col = resolve_column(products_raw, ["category"], "products_cleaned")
price_col = resolve_column(products_raw, ["price"], "products_cleaned")

products = products_raw.select(
    F.col(product_id_col).cast("int").alias("product_id"),
    F.col(title_col).alias("title"),
    F.col(category_col).alias("category"),
    F.col(price_col).cast("double").alias("price"),
    build_optional_column(products_raw, ["rating_score"], "double", "rating_score"),
    build_optional_column(products_raw, ["rating_count"], "bigint", "rating_count"),
)

orders = orders_raw.select(
    F.col(order_id_col).alias("order_id"),
    F.col(user_id_col).alias("user_id"),
    F.to_date(F.col(order_date_col)).alias("order_date"),
    F.col(product_ids_col).alias("product_ids"),
).where(
    F.col("order_id").isNotNull()
    & F.col("user_id").isNotNull()
    & F.col("order_date").isNotNull()
    & F.col("product_ids").isNotNull()
    & (F.trim(F.col("product_ids")) != "")
)

order_items = (
    orders.withColumn(
        "product_id_raw",
        F.explode(F.split(F.regexp_replace(F.col("product_ids"), r"\s+", ""), ",")),
    )
    .select(
        F.col("order_id"),
        F.col("user_id"),
        F.col("order_date"),
        F.trim(F.col("product_id_raw")).cast("int").alias("product_id"),
    )
    .where(F.col("product_id").isNotNull())
    .join(products, on="product_id", how="inner")
    .withColumn("line_revenue", F.coalesce(F.col("price"), F.lit(0.0)))
)

recent_cutoff = F.date_sub(F.current_date(), ANALYSIS_WEEK_WINDOW_DAYS)
current_week_cutoff = F.date_sub(F.current_date(), 7)

recent_items = order_items.where(F.col("order_date") >= recent_cutoff)

customer_lifetime = (
    order_items.groupBy("user_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.count("*").alias("total_items"),
        F.sum("line_revenue").alias("total_revenue"),
        F.avg("line_revenue").alias("avg_item_price"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.countDistinct("category").alias("distinct_categories"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    .withColumn("days_since_last_order", F.datediff(F.current_date(), F.col("last_order_date")))
    .withColumn("customer_lifetime_days", F.greatest(F.datediff(F.col("last_order_date"), F.col("first_order_date")) + 1, F.lit(1)))
    .withColumn("avg_items_per_order", F.round(F.col("total_items") / F.greatest(F.col("total_orders"), F.lit(1)), 2))
    .withColumn("avg_revenue_per_order", F.round(F.col("total_revenue") / F.greatest(F.col("total_orders"), F.lit(1)), 2))
)

recent_activity = (
    recent_items.groupBy("user_id")
    .agg(
        F.sum(F.when(F.col("order_date") >= current_week_cutoff, F.col("line_revenue")).otherwise(F.lit(0.0))).alias("revenue_last_7d"),
        F.sum(F.when(F.col("order_date") < current_week_cutoff, F.col("line_revenue")).otherwise(F.lit(0.0))).alias("revenue_prev_28d"),
        F.countDistinct(F.when(F.col("order_date") >= current_week_cutoff, F.col("order_id"))).alias("orders_last_7d"),
        F.countDistinct(F.when(F.col("order_date") < current_week_cutoff, F.col("order_id"))).alias("orders_prev_28d"),
        F.countDistinct(F.when(F.col("order_date") >= current_week_cutoff, F.col("order_date"))).alias("active_days_last_7d"),
        F.countDistinct(F.when(F.col("order_date") < current_week_cutoff, F.col("order_date"))).alias("active_days_prev_28d"),
    )
    .withColumn(
        "revenue_momentum_7d",
        F.round(F.col("revenue_last_7d") / F.when(F.col("revenue_prev_28d") != 0, F.col("revenue_prev_28d")), 2),
    )
    .withColumn(
        "order_momentum_7d",
        F.round(F.col("orders_last_7d") / F.when(F.col("orders_prev_28d") != 0, F.col("orders_prev_28d")), 2),
    )
)

customer_features = customer_lifetime.join(recent_activity, on="user_id", how="left").fillna(
    {
        "revenue_last_7d": 0.0,
        "revenue_prev_28d": 0.0,
        "orders_last_7d": 0,
        "orders_prev_28d": 0,
        "active_days_last_7d": 0,
        "active_days_prev_28d": 0,
    }
)

customer_features = customer_features.withColumn(
    "revenue_momentum_7d",
    F.coalesce(F.col("revenue_momentum_7d"), F.lit(0.0)),
).withColumn(
    "order_momentum_7d",
    F.coalesce(F.col("order_momentum_7d"), F.lit(0.0)),
)

r_window = Window.orderBy(F.col("days_since_last_order").asc_nulls_last())
f_window = Window.orderBy(F.col("total_orders").asc_nulls_last())
m_window = Window.orderBy(F.col("total_revenue").asc_nulls_last())

customer_scored = (
    customer_features.withColumn("r_score", 6 - F.ntile(5).over(r_window))
    .withColumn("f_score", F.ntile(5).over(f_window))
    .withColumn("m_score", F.ntile(5).over(m_window))
    .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    .withColumn(
        "clv_proxy",
        F.round(F.col("total_revenue") * (1 + F.col("total_orders") / F.greatest(F.col("customer_lifetime_days"), F.lit(1))), 2),
    )
    .withColumn(
        "churn_risk_score",
        F.round(
            ((6 - F.col("r_score")) + (6 - F.col("f_score")) + (6 - F.col("m_score"))) / F.lit(15.0),
            3,
        ),
    )
    .withColumn(
        "churn_risk_label",
        F.when(F.col("days_since_last_order") >= 180, F.lit("HIGH"))
        .when(F.col("churn_risk_score") >= 0.75, F.lit("HIGH"))
        .when(F.col("churn_risk_score") >= 0.5, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )
)

thresholds = customer_scored.agg(
    F.expr("percentile_approx(clv_proxy, 0.75)").alias("clv_p75"),
    F.expr("percentile_approx(total_orders, 0.75)").alias("orders_p75"),
).first()

clv_p75 = float(thresholds["clv_p75"] or 0.0)
orders_p75 = float(thresholds["orders_p75"] or 0.0)

customer_360 = (
    customer_scored.withColumn(
        "customer_segment",
        F.when(F.col("days_since_last_order") >= 180, F.lit("Churned"))
        .when(F.col("churn_risk_score") >= 0.8, F.lit("At Risk"))
        .when(
            (F.col("clv_proxy") >= F.lit(clv_p75))
            & (F.col("total_orders") >= F.lit(orders_p75))
            & (F.col("days_since_last_order") <= 30),
            F.lit("Champions"),
        )
        .when((F.col("total_orders") >= 3) & (F.col("days_since_last_order") <= 60), F.lit("Loyal"))
        .when((F.col("total_orders") == 1) & (F.col("days_since_last_order") <= 30), F.lit("New"))
        .when(F.col("revenue_momentum_7d") >= 1.2, F.lit("Growing"))
        .otherwise(F.lit("Potential Loyalist")),
    )
    .withColumn(
        "engagement_trend",
        F.when(F.col("revenue_momentum_7d") >= 1.2, F.lit("Accelerating"))
        .when(F.col("revenue_momentum_7d") <= 0.8, F.lit("Declining"))
        .otherwise(F.lit("Stable")),
    )
    .withColumn("analysis_week_start", F.date_sub(F.current_date(), 7))
    .withColumn("analysis_week_end", F.current_date())
    .withColumn("processed_at", F.current_timestamp())
    .select(
        "user_id",
        "first_order_date",
        "last_order_date",
        "customer_lifetime_days",
        "days_since_last_order",
        "total_orders",
        "total_items",
        "distinct_products",
        "distinct_categories",
        "total_revenue",
        "avg_item_price",
        "avg_items_per_order",
        "avg_revenue_per_order",
        "revenue_last_7d",
        "revenue_prev_28d",
        "revenue_momentum_7d",
        "orders_last_7d",
        "orders_prev_28d",
        "order_momentum_7d",
        "active_days_last_7d",
        "active_days_prev_28d",
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "clv_proxy",
        "churn_risk_score",
        "churn_risk_label",
        "customer_segment",
        "engagement_trend",
        "analysis_week_start",
        "analysis_week_end",
        "processed_at",
    )
)

segment_summary = (
    customer_360.groupBy("customer_segment")
    .agg(
        F.count("*").alias("customer_count"),
        F.sum("total_orders").alias("total_orders"),
        F.sum("total_revenue").alias("total_revenue"),
        F.round(F.avg("clv_proxy"), 2).alias("avg_clv_proxy"),
        F.round(F.avg("churn_risk_score"), 3).alias("avg_churn_risk_score"),
        F.round(F.avg("days_since_last_order"), 1).alias("avg_days_since_last_order"),
        F.round(F.avg("total_orders"), 2).alias("avg_orders_per_customer"),
    )
    .withColumn("customer_share", F.round(F.col("customer_count") / F.sum("customer_count").over(Window.partitionBy()), 4))
    .withColumn("revenue_share", F.round(F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy()), 4))
    .withColumn("analysis_week_start", F.date_sub(F.current_date(), 7))
    .withColumn("analysis_week_end", F.current_date())
    .withColumn("processed_at", F.current_timestamp())
    .orderBy(F.col("total_revenue").desc(), F.col("customer_segment"))
)

category_insights = (
    recent_items.groupBy("category")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("user_id").alias("active_customers"),
        F.count("*").alias("line_items"),
        F.round(F.sum("line_revenue"), 2).alias("revenue_last_35d"),
        F.countDistinct(F.when(F.col("order_date") >= current_week_cutoff, F.col("order_id"))).alias("orders_last_7d"),
        F.countDistinct(F.when(F.col("order_date") < current_week_cutoff, F.col("order_id"))).alias("orders_prev_28d"),
        F.round(
            F.sum(F.when(F.col("order_date") >= current_week_cutoff, F.col("line_revenue")).otherwise(F.lit(0.0))),
            2,
        ).alias("revenue_last_7d"),
        F.round(
            F.sum(F.when(F.col("order_date") < current_week_cutoff, F.col("line_revenue")).otherwise(F.lit(0.0))),
            2,
        ).alias("revenue_prev_28d"),
        F.round(F.avg("line_revenue"), 2).alias("avg_item_price"),
        F.round(F.avg("rating_score"), 2).alias("avg_rating_score"),
        F.round(F.avg("rating_count"), 0).alias("avg_rating_count"),
    )
    .withColumn(
        "revenue_momentum_7d",
        F.round(F.col("revenue_last_7d") / F.when(F.col("revenue_prev_28d") != 0, F.col("revenue_prev_28d")), 2),
    )
    .withColumn(
        "order_momentum_7d",
        F.round(F.col("orders_last_7d") / F.when(F.col("orders_prev_28d") != 0, F.col("orders_prev_28d")), 2),
    )
    .withColumn("analysis_week_start", F.date_sub(F.current_date(), 7))
    .withColumn("analysis_week_end", F.current_date())
    .withColumn("processed_at", F.current_timestamp())
    .withColumn(
        "revenue_share",
        F.round(F.col("revenue_last_35d") / F.sum("revenue_last_35d").over(Window.partitionBy()), 4),
    )
    .orderBy(F.col("revenue_last_35d").desc(), F.col("category"))
)

weekly_overview = (
    customer_360.agg(
        F.count("*").alias("customer_count"),
        F.sum("total_orders").alias("total_orders"),
        F.sum("total_revenue").alias("total_revenue"),
        F.round(F.avg("clv_proxy"), 2).alias("avg_clv_proxy"),
        F.round(F.avg("churn_risk_score"), 3).alias("avg_churn_risk_score"),
        F.round(F.avg("days_since_last_order"), 1).alias("avg_days_since_last_order"),
    )
    .withColumn("analysis_week_start", F.date_sub(F.current_date(), 7))
    .withColumn("analysis_week_end", F.current_date())
    .withColumn("processed_at", F.current_timestamp())
)

# COMMAND ----------

write_weekly_table(customer_360, "weekly.weekly_customer_360")
write_weekly_table(segment_summary, "weekly.weekly_segment_summary")
write_weekly_table(category_insights, "weekly.weekly_category_insights")
write_weekly_table(weekly_overview, "weekly.weekly_overview")

validate_required_columns(
    customer_360,
    "weekly.weekly_customer_360",
    [
        "user_id",
        "clv_proxy",
        "churn_risk_score",
        "customer_segment",
        "analysis_week_start",
        "analysis_week_end",
    ],
)
validate_unique_keys(customer_360, "weekly.weekly_customer_360", ["user_id"])

validate_required_columns(
    segment_summary,
    "weekly.weekly_segment_summary",
    ["customer_segment", "customer_count", "total_revenue", "avg_clv_proxy"],
)
validate_unique_keys(segment_summary, "weekly.weekly_segment_summary", ["customer_segment"])

validate_required_columns(
    category_insights,
    "weekly.weekly_category_insights",
    ["category", "order_count", "active_customers", "revenue_last_35d"],
)
validate_unique_keys(category_insights, "weekly.weekly_category_insights", ["category"])

validate_required_columns(
    weekly_overview,
    "weekly.weekly_overview",
    ["customer_count", "total_orders", "total_revenue", "avg_clv_proxy"],
)

print(f"✅ weekly.weekly_customer_360 rows: {customer_360.count()}")
print(f"✅ weekly.weekly_segment_summary rows: {segment_summary.count()}")
print(f"✅ weekly.weekly_category_insights rows: {category_insights.count()}")
print(f"✅ weekly.weekly_overview rows: {weekly_overview.count()}")
