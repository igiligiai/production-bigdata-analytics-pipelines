# Databricks notebook source
# ====================================================================
# STAGE 2: DATA CLEANING & STANDARDIZATION (Silver Layer)
# ====================================================================
# Purpose: Transform Bronze raw data into clean, typed Silver tables.
# Handles: schema standardization, null values, deduplication, type casting

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime

# COMMAND ----------

def get_clean_products_schema() -> StructType:
    """Define the standardized schema for products table"""
    return StructType([
        StructField("product_id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("rating_score", DoubleType(), True),
        StructField("rating_count", IntegerType(), True),
        StructField("source_api", StringType(), False),
        StructField("processed_at", TimestampType(), False),
        StructField("data_quality_flags", StringType(), True)
    ])

# COMMAND ----------

def clean_products(raw_df) -> DF:
    """
    Transform raw products data:
    - Rename columns for consistency
    - Cast types appropriately
    - Flag data quality issues
    - Remove duplicates
    """
    
    # Standardize column names and extract nested rating
    clean_df = raw_df.select(
        F.col("id").cast(IntegerType()).alias("product_id"),
        F.col("title").cast(StringType()).alias("title"),
        F.col("price").cast(DoubleType()).alias("price"),
        F.col("category").cast(StringType()).alias("category"),
        F.col("description").cast(StringType()).alias("description"),
        F.when(F.col("rating").isNotNull(), 
               F.col("rating.rate")).cast(DoubleType()).alias("rating_score"),
        F.when(F.col("rating").isNotNull(), 
               F.col("rating.count")).cast(IntegerType()).alias("rating_count"),
        F.col("_source_api").alias("source_api"),
        F.current_timestamp().alias("processed_at")
    )
    
    # Add data quality flags
    clean_df = clean_df.withColumn(
        "data_quality_flags",
        F.when(F.col("price").isNull(), "missing_price")
         .when(F.col("price") < 0, "negative_price")
         .when(F.col("title").isNull(), "missing_title")
         .when(F.col("product_id").isNull(), "missing_id")
         .otherwise("ok")
    )
    
    # Deduplicate by product_id and source_api (keep latest)
    clean_df = clean_df.withColumn(
        "row_num",
        F.row_number().over(
            F.Window.partitionBy("product_id", "source_api")
                    .orderBy(F.col("processed_at").desc())
        )
    ).filter(F.col("row_num") == 1).drop("row_num")
    
    # Only keep records with non-null IDs
    clean_df = clean_df.filter(F.col("product_id").isNotNull())
    
    return clean_df

# COMMAND ----------

def get_clean_orders_schema() -> StructType:
    """Define the standardized schema for orders table"""
    return StructType([
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("product_ids", StringType(), True),  # Comma-separated for simplicity
        StructField("total_quantity", IntegerType(), True),
        StructField("source_api", StringType(), False),
        StructField("processed_at", TimestampType(), False),
        StructField("data_quality_flags", StringType(), True)
    ])

# COMMAND ----------

def clean_orders(raw_df) -> DF:
    """
    Transform raw orders data:
    - Parse dates
    - Denormalize products (for simplicity)
    - Calculate order totals
    - Flag data quality issues
    """
    
    clean_df = raw_df.select(
        F.col("id").cast(IntegerType()).alias("order_id"),
        F.col("userId").cast(IntegerType()).alias("user_id"),
        F.from_unixtime(F.col("date")).alias("order_date"),
        # Convert array of product objects to string for now
        F.expr("CONCAT_WS(',', TRANSFORM(products, x -> CAST(x.productId AS STRING)))").alias("product_ids"),
        F.size(F.col("products")).cast(IntegerType()).alias("total_quantity"),
        F.col("_source_api").alias("source_api"),
        F.current_timestamp().alias("processed_at")
    )
    
    # Add data quality flags
    clean_df = clean_df.withColumn(
        "data_quality_flags",
        F.when(F.col("order_id").isNull(), "missing_order_id")
         .when(F.col("user_id").isNull(), "missing_user_id")
         .when(F.col("order_date").isNull(), "missing_date")
         .when(F.col("total_quantity") == 0, "empty_order")
         .otherwise("ok")
    )
    
    # Only keep valid orders
    clean_df = clean_df.filter(
        (F.col("order_id").isNotNull()) & 
        (F.col("user_id").isNotNull())
    )
    
    return clean_df

# COMMAND ----------

def clean_carts(raw_df) -> DF:
    """
    Transform raw carts data (similar to orders but shopping carts)
    """
    
    clean_df = raw_df.select(
        F.col("id").cast(IntegerType()).alias("cart_id"),
        F.col("userId").cast(IntegerType()).alias("user_id"),
        F.from_unixtime(F.col("date")).alias("cart_date"),
        F.expr("CONCAT_WS(',', TRANSFORM(products, x -> CAST(x.productId AS STRING)))").alias("product_ids"),
        F.size(F.col("products")).cast(IntegerType()).alias("total_items"),
        F.col("_source_api").alias("source_api"),
        F.current_timestamp().alias("processed_at")
    )
    
    # Data quality flags
    clean_df = clean_df.withColumn(
        "data_quality_flags",
        F.when(F.col("cart_id").isNull(), "missing_cart_id")
         .when(F.col("user_id").isNull(), "missing_user_id")
         .when(F.col("total_items") == 0, "empty_cart")
         .otherwise("ok")
    )
    
    clean_df = clean_df.filter(F.col("cart_id").isNotNull())
    
    return clean_df

# COMMAND ----------

# MAIN CLEANING LOGIC

print(f"\n{'='*60}")
print("CLEANING STAGE: Bronze → Silver")
print(f"{'='*60}\n")

# Step 1: Clean products
print("🔄 Cleaning products...")
raw_products = spark.table("bronze.raw_fakestore_products")
clean_products_df = clean_products(raw_products)

clean_products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.products_cleaned")

product_count = clean_products_df.count()
print(f"✅ {product_count} products written to silver.products_cleaned")

# Step 2: Clean orders
print("\n🔄 Cleaning orders...")
raw_orders = spark.table("bronze.raw_fakestore_orders")
clean_orders_df = clean_orders(raw_orders)

clean_orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.orders_cleaned")

order_count = clean_orders_df.count()
print(f"✅ {order_count} orders written to silver.orders_cleaned")

# Step 3: Clean carts
print("\n🔄 Cleaning carts...")
raw_carts = spark.table("bronze.raw_fakestore_carts")
clean_carts_df = clean_carts(raw_carts)

clean_carts_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.carts_cleaned")

cart_count = clean_carts_df.count()
print(f"✅ {cart_count} carts written to silver.carts_cleaned")

# COMMAND ----------

# DATA QUALITY SUMMARY

print(f"\n{'='*60}")
print("DATA QUALITY REPORT")
print(f"{'='*60}\n")

quality_summary = {}

for table_name, df in [
    ("products", clean_products_df),
    ("orders", clean_orders_df),
    ("carts", clean_carts_df)
]:
    quality_summary[table_name] = {
        "total_records": df.count(),
        "quality_issues": df.groupBy("data_quality_flags").count().collect()
    }
    
    print(f"📊 {table_name.upper()}")
    df.groupBy("data_quality_flags").count().show()

# COMMAND ----------

# SCHEMA VALIDATION

print(f"\n{'='*60}")
print("SILVER LAYER SCHEMAS")
print(f"{'='*60}\n")

for table_name in ["silver.products_cleaned", "silver.orders_cleaned", "silver.carts_cleaned"]:
    print(f"\n📋 {table_name}")
    spark.table(table_name).printSchema()
    
    # Show sample rows
    print(f"\nSample rows:")
    spark.table(table_name).limit(3).display()

# COMMAND ----------

# Optional: Create test assertions for CI/CD

def assert_data_quality(table_name: str, assertions: dict) -> bool:
    """
    Validate table meets quality thresholds for CI/CD pipelines
    """
    df = spark.table(table_name)
    
    tests_passed = True
    
    # Test: Record count > minimum
    record_count = df.count()
    if record_count < assertions.get("min_records", 0):
        print(f"❌ {table_name}: Record count {record_count} below minimum {assertions['min_records']}")
        tests_passed = False
    
    # Test: No critical null values
    for col_name in assertions.get("non_null_columns", []):
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            print(f"❌ {table_name}: Column '{col_name}' has {null_count} null values")
            tests_passed = False
    
    # Test: Data quality flags mostly "ok"
    ok_count = df.filter(F.col("data_quality_flags") == "ok").count()
    ok_ratio = ok_count / record_count if record_count > 0 else 0
    if ok_ratio < assertions.get("quality_threshold", 0.9):
        print(f"⚠️  {table_name}: Only {ok_ratio:.1%} records passed quality checks (threshold: {assertions['quality_threshold']:.1%})")
    
    return tests_passed

# Run assertions
print(f"\n{'='*60}")
print("QUALITY ASSERTIONS")
print(f"{'='*60}\n")

assertions = {
    "min_records": 1,
    "non_null_columns": ["product_id", "user_id", "order_id"],
    "quality_threshold": 0.85
}

for table_name in ["silver.products_cleaned", "silver.orders_cleaned", "silver.carts_cleaned"]:
    assert_data_quality(table_name, assertions)

print("\n✅ Silver layer transformation complete!")
