# Databricks notebook source
# ====================================================================
# STAGE 3: ADVANCED TRANSFORMATIONS (Gold Layer)
# ====================================================================
# Purpose: Create business-ready analytics tables with complex aggregations,
# RFM analysis, inventory optimization, anomaly detection.

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime, timedelta

# COMMAND ----------

print(f"\n{'='*60}")
print("TRANSFORMATION STAGE: Silver → Gold")
print("Building business analytics layer...")
print(f"{'='*60}\n")

# COMMAND ----------

# ====================================================================
# GOLD TABLE 1: DAILY REVENUE BY CATEGORY
# ====================================================================

print("📊 Creating: daily_revenue_by_category")

daily_revenue = spark.sql("""
  WITH order_details AS (
    -- Explode product_ids from orders and join with products
    SELECT 
      o.order_id,
      o.user_id,
      o.order_date,
      CAST(exploded_product AS INT) as product_id,
      p.title,
      p.price,
      p.category,
      1 as quantity  -- FakeStore doesn't have qty, assume 1 per item
    FROM silver.orders_cleaned o
    LATERAL VIEW EXPLODE(SPLIT(o.product_ids, ',')) AS exploded_product
    INNER JOIN silver.products_cleaned p ON CAST(exploded_product AS INT) = p.product_id
    WHERE o.product_ids IS NOT NULL AND o.product_ids != ''
  ),
  daily_aggregation AS (
    SELECT 
      TO_DATE(order_date) as order_date,
      category,
      COUNT(DISTINCT order_id) as total_orders,
      SUM(quantity) as total_units,
      SUM(price * quantity) as total_revenue,
      AVG(price) as avg_price,
      COUNT(DISTINCT user_id) as unique_customers
    FROM order_details
    GROUP BY TO_DATE(order_date), category
  ),
  with_cumulative AS (
    SELECT *,
      SUM(total_revenue) OVER (
        PARTITION BY category 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) as cumulative_revenue,
      ROW_NUMBER() OVER (
        PARTITION BY category 
        ORDER BY order_date DESC
      ) as recency_rank
    FROM daily_aggregation
  )
  SELECT *,
    CURRENT_TIMESTAMP() as processed_at
  FROM with_cumulative
""")

daily_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.daily_revenue_by_category")

print(f"✅ {daily_revenue.count()} daily revenue records created")
print("Sample:\n")
daily_revenue.limit(5).display()

# COMMAND ----------

# ====================================================================
# GOLD TABLE 2: CUSTOMER 360 (RFM + Churn Risk)
# ====================================================================

print("\n📊 Creating: customer_360_rfm")

customer_360 = spark.sql("""
  WITH customer_orders AS (
    SELECT 
      user_id,
      COUNT(DISTINCT order_id) as purchase_frequency,
      SUM(CAST(total_quantity AS INT)) as total_items_purchased,
      MAX(order_date) as last_purchase_date,
      MIN(order_date) as first_purchase_date,
      COUNT(DISTINCT DATE(order_date)) as days_active
    FROM silver.orders_cleaned
    WHERE user_id IS NOT NULL
    GROUP BY user_id
  ),
  with_recency AS (
    SELECT *,
      DATEDIFF(CURRENT_DATE(), DATE(last_purchase_date)) as days_since_purchase
    FROM customer_orders
  ),
  rfm_calculation AS (
    SELECT 
      user_id,
      purchase_frequency,
      total_items_purchased,
      last_purchase_date,
      first_purchase_date,
      days_active,
      days_since_purchase,
      -- R Score: Recency (lower days = higher score)
      NTILE(5) OVER (ORDER BY days_since_purchase DESC) as r_score,
      -- F Score: Frequency
      NTILE(5) OVER (ORDER BY purchase_frequency ASC) as f_score,
      -- M Score: Monetary (total items as proxy)
      NTILE(5) OVER (ORDER BY total_items_purchased ASC) as m_score
    FROM with_recency
  ),
  with_segments AS (
    SELECT *,
      CONCAT(r_score, f_score, m_score) as rfm_segment,
      -- Calculate churn risk
      CASE 
        WHEN days_since_purchase > 180 THEN 'Churned'
        WHEN days_since_purchase > 90 THEN 'At Risk'
        WHEN days_since_purchase > 30 THEN 'Warm'
        ELSE 'Active'
      END as customer_status,
      -- Customer lifetime segments
      CASE 
        WHEN purchase_frequency >= 5 AND total_items_purchased >= 10 THEN 'High Value'
        WHEN purchase_frequency >= 3 THEN 'Medium Value'
        WHEN purchase_frequency = 1 THEN 'One-time Buyer'
        ELSE 'Low Activity'
      END as customer_segment
    FROM rfm_calculation
  )
  SELECT *,
    CURRENT_TIMESTAMP() as processed_at
  FROM with_segments
  ORDER BY rfm_segment DESC
""")

customer_360.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.customer_360_rfm")

print(f"✅ {customer_360.count()} customer records with RFM created")
print("\nRFM Segment Distribution:\n")
customer_360.groupBy("customer_status").count().display()

# COMMAND ----------

# ====================================================================
# GOLD TABLE 3: PRODUCT PERFORMANCE & ANOMALIES
# ====================================================================

print("\n📊 Creating: product_performance_metrics")

product_performance = spark.sql("""
  WITH product_orders AS (
    SELECT 
      p.product_id,
      p.title,
      p.category,
      p.price,
      p.rating_score,
      p.rating_count,
      o.order_id,
      DATE(o.order_date) as order_date,
      1 as quantity
    FROM silver.orders_cleaned o
    LATERAL VIEW EXPLODE(SPLIT(o.product_ids, ',')) AS exploded_product
    INNER JOIN silver.products_cleaned p ON CAST(exploded_product AS INT) = p.product_id
    WHERE o.product_ids IS NOT NULL
  ),
  daily_metrics AS (
    SELECT 
      product_id,
      title,
      category,
      price,
      rating_score,
      rating_count,
      order_date,
      COUNT(DISTINCT order_id) as daily_orders,
      SUM(quantity) as daily_units,
      SUM(quantity * price) as daily_revenue
    FROM product_orders
    GROUP BY product_id, title, category, price, rating_score, rating_count, order_date
  ),
  aggregated_metrics AS (
    SELECT 
      product_id,
      title,
      category,
      price,
      rating_score,
      rating_count,
      COUNT(DISTINCT order_date) as days_sold,
      COUNT(DISTINCT order_id) as total_orders,
      SUM(daily_units) as total_units_sold,
      SUM(daily_revenue) as total_revenue,
      AVG(daily_orders) as avg_daily_orders,
      STDDEV(daily_orders) as stddev_daily_orders,
      MAX(daily_orders) as peak_daily_orders,
      MIN(CASE WHEN daily_orders > 0 THEN daily_orders END) as min_daily_orders
    FROM daily_metrics
    GROUP BY product_id, title, category, price, rating_score, rating_count
  ),
  with_classification AS (
    SELECT *,
      -- Turnover classification
      CASE 
        WHEN total_orders > 20 THEN 'Fast Mover'
        WHEN total_orders > 5 THEN 'Normal Mover'
        WHEN total_orders = 1 THEN 'Slow Mover'
        ELSE 'No Sales'
      END as movement_category,
      -- Performance score: orders + rating + consistency
      (total_orders * 0.6 + COALESCE(rating_score, 0) * 10 * 0.3 + COALESCE(days_sold, 0) * 0.1) as performance_score
    FROM aggregated_metrics
  )
  SELECT *,
    CURRENT_TIMESTAMP() as processed_at
  FROM with_classification
  ORDER BY performance_score DESC
""")

product_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.product_performance_metrics")

print(f"✅ {product_performance.count()} products analyzed")
print("\nProduct Movement Classification:\n")
product_performance.groupBy("movement_category").agg(F.count("*"), F.avg("total_revenue")).display()

# COMMAND ----------

# ====================================================================
# GOLD TABLE 4: CATEGORY INSIGHTS
# ====================================================================

print("\n📊 Creating: category_insights")

category_insights = spark.sql("""
  WITH category_orders AS (
    SELECT 
      p.category,
      o.order_id,
      p.product_id,
      p.price,
      o.user_id,
      DATE(o.order_date) as order_date
    FROM silver.orders_cleaned o
    LATERAL VIEW EXPLODE(SPLIT(o.product_ids, ',')) AS exploded_product
    INNER JOIN silver.products_cleaned p ON CAST(exploded_product AS INT) = p.product_id
    WHERE p.category IS NOT NULL
  ),
  category_metrics AS (
    SELECT 
      category,
      COUNT(DISTINCT order_id) as total_orders,
      COUNT(DISTINCT user_id) as unique_customers,
      COUNT(DISTINCT product_id) as unique_products,
      SUM(price) as total_revenue,
      AVG(price) as avg_order_value,
      STDDEV(price) as stddev_price,
      COUNT(DISTINCT DATE(order_date)) as days_active,
      MIN(order_date) as first_order_date,
      MAX(order_date) as last_order_date
    FROM category_orders
    GROUP BY category
  ),
  with_growth AS (
    SELECT *,
      DATEDIFF(MAX(last_order_date) OVER(), MIN(first_order_date) OVER()) as days_in_operation,
      total_orders / COUNT(DISTINCT DATE(order_date)) as orders_per_active_day,
      RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
      RANK() OVER (ORDER BY total_orders DESC) as order_rank
    FROM category_metrics
  )
  SELECT *,
    CURRENT_TIMESTAMP() as processed_at
  FROM with_growth
""")

category_insights.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.category_insights")

print(f"✅ {category_insights.count()} categories analyzed")
print("\nCategory Performance:\n")
category_insights.select("category", "total_orders", "total_revenue", "unique_customers", "orders_per_active_day").display()

# COMMAND ----------

# ====================================================================
# GOLD TABLE 5: ANOMALY DETECTION (Price & Volume Spikes)
# ====================================================================

print("\n📊 Creating: daily_anomalies")

anomalies = spark.sql("""
  WITH daily_product_metrics AS (
    SELECT 
      DATE(order_date) as metric_date,
      product_id,
      p.title,
      p.category,
      p.price,
      COUNT(DISTINCT order_id) as daily_orders,
      COUNT(*) as daily_volume
    FROM silver.orders_cleaned o
    LATERAL VIEW EXPLODE(SPLIT(o.product_ids, ',')) AS exploded_product
    INNER JOIN silver.products_cleaned p ON CAST(exploded_product AS INT) = p.product_id
    GROUP BY DATE(order_date), product_id, p.title, p.category, p.price
  ),
  with_rolling_stats AS (
    SELECT *,
      AVG(daily_orders) OVER (
        PARTITION BY product_id 
        ORDER BY metric_date 
        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
      ) as rolling_avg_orders,
      STDDEV(daily_orders) OVER (
        PARTITION BY product_id 
        ORDER BY metric_date 
        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
      ) as rolling_stddev_orders
    FROM daily_product_metrics
  ),
  anomaly_scoring AS (
    SELECT *,
      CASE 
        WHEN rolling_avg_orders IS NULL THEN 'Insufficient History'
        WHEN daily_orders > rolling_avg_orders + (rolling_stddev_orders * 2) 
          THEN 'Volume Spike'
        WHEN daily_orders < rolling_avg_orders - (rolling_stddev_orders * 1.5) 
          THEN 'Volume Drop'
        ELSE 'Normal'
      END as anomaly_type,
      CASE 
        WHEN rolling_avg_orders IS NOT NULL 
          THEN (daily_orders - rolling_avg_orders) / NULLIF(rolling_avg_orders, 0)
        ELSE 0
      END as volume_variance_pct
    FROM with_rolling_stats
  )
  SELECT *,
    CURRENT_TIMESTAMP() as processed_at
  FROM anomaly_scoring
  WHERE anomaly_type IN ('Volume Spike', 'Volume Drop')
  ORDER BY metric_date DESC, ABS(volume_variance_pct) DESC
""")

anomalies.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold.daily_anomalies")

print(f"✅ {anomalies.count()} anomalies detected")
if anomalies.count() > 0:
    print("\nRecent Anomalies:\n")
    anomalies.limit(10).display()

# COMMAND ----------

# ====================================================================
# DATA QUALITY & SUMMARY REPORT
# ====================================================================

print(f"\n{'='*60}")
print("GOLD LAYER TRANSFORMATION SUMMARY")
print(f"{'='*60}\n")

gold_tables = [
    "gold.daily_revenue_by_category",
    "gold.customer_360_rfm",
    "gold.product_performance_metrics",
    "gold.category_insights",
    "gold.daily_anomalies"
]

transformation_summary = {
    "execution_timestamp": datetime.utcnow().isoformat(),
    "tables": {}
}

for table_name in gold_tables:
    try:
        df = spark.table(table_name)
        record_count = df.count()
        col_count = len(df.columns)
        
        transformation_summary["tables"][table_name] = {
            "record_count": record_count,
            "column_count": col_count,
            "status": "success"
        }
        
        print(f"✅ {table_name}")
        print(f"   Records: {record_count:,} | Columns: {col_count}")
    except Exception as e:
        print(f"❌ {table_name}: {str(e)}")

print(f"\n{'='*60}")
print("✅ GOLD LAYER TRANSFORMATION COMPLETE")
print(f"{'='*60}")

# Save summary to logs
import json
log_path = f"/logs/transformation/{datetime.now().date()}_transformation_summary.json"
dbutils.fs.put(log_path, json.dumps(transformation_summary, indent=2))

print(f"\n📝 Summary logged to: {log_path}")
