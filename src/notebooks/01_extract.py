# Databricks notebook source
# ====================================================================
# STAGE 1: API EXTRACTION (Bronze Layer)
# ====================================================================
# Purpose: Fetch data from multiple e-commerce APIs, validate, and 
# land raw data in DBFS. This is append-only, preserving all historical data.

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# COMMAND ----------

# Configuration
CURRENT_DATE = datetime.utcnow().strftime("%Y-%m-%d")
DBFS_RAW_PATH = f"/dbfs/raw/ecommerce"

# API endpoints
API_CONFIG = {
    "fakestore_products": {
        "url": "https://fakestoreapi.com/products",
        "source": "fakestore",
        "table": "products"
    },
    "fakestore_carts": {
        "url": "https://fakestoreapi.com/carts",
        "source": "fakestore",
        "table": "carts"
    },
    "fakestore_orders": {
        "url": "https://fakestoreapi.com/orders",
        "source": "fakestore",
        "table": "orders"
    }
}

# COMMAND ----------

def extract_from_api(endpoint_config: dict) -> list:
    """
    Extract JSON data from REST API with error handling.
    Returns list of records with metadata.
    """
    url = endpoint_config["url"]
    source = endpoint_config["source"]
    table_name = endpoint_config["table"]
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Normalize to list
        if isinstance(data, dict):
            data = [data]
        
        # Add extraction metadata to each record
        extracted_data = [
            {
                **record,
                "_extracted_at": datetime.utcnow().isoformat(),
                "_source_api": source,
                "_table_name": table_name,
                "_response_code": response.status_code,
                "_extraction_date": CURRENT_DATE
            }
            for record in data
        ]
        
        print(f"✅ Extracted {len(extracted_data)} records from {source}.{table_name}")
        return extracted_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error extracting from {source}.{table_name}: {str(e)}")
        # Return empty list to avoid pipeline failure
        return []

# COMMAND ----------

def write_to_bronze(data: list, source: str, table: str) -> None:
    """
    Write extracted data to Bronze layer (raw, append-only).
    Uses Delta Lake for ACID compliance.
    """
    if not data:
        print(f"⚠️  No data to write for {source}.{table}")
        return
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(data)
    
    # Create table path
    table_name = f"bronze.raw_{source}_{table}"
    
    # Write with merge schema (auto-detect new columns)
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .option("path", f"{DBFS_RAW_PATH}/{source}/{table}/") \
        .saveAsTable(table_name)
    
    print(f"📝 Appended to {table_name}")

# COMMAND ----------

# MAIN EXTRACTION LOGIC

# Step 1: Extract from all configured APIs
all_extractions = {}
for endpoint_key, endpoint_config in API_CONFIG.items():
    print(f"\n{'='*60}")
    print(f"Extracting: {endpoint_key}")
    print(f"URL: {endpoint_config['url']}")
    print(f"{'='*60}")
    
    data = extract_from_api(endpoint_config)
    all_extractions[endpoint_key] = data

# Step 2: Write each extraction to Bronze
for endpoint_key, endpoint_config in API_CONFIG.items():
    data = all_extractions[endpoint_key]
    write_to_bronze(data, endpoint_config["source"], endpoint_config["table"])

# COMMAND ----------

# VALIDATION & LOGGING

extraction_summary = {
    "execution_date": CURRENT_DATE,
    "timestamp": datetime.utcnow().isoformat(),
    "sources": {}
}

for endpoint_key, endpoint_config in API_CONFIG.items():
    source = endpoint_config["source"]
    table = endpoint_config["table"]
    data_count = len(all_extractions[endpoint_key])
    
    extraction_summary["sources"][f"{source}.{table}"] = {
        "record_count": data_count,
        "status": "success" if data_count > 0 else "no_data"
    }

# Log to DBFS for audit trail
log_path = f"/dbfs/logs/extraction/{CURRENT_DATE}_extraction_summary.json"
dbutils.fs.put(log_path, json.dumps(extraction_summary, indent=2))

print(f"\n{'='*60}")
print("EXTRACTION SUMMARY")
print(f"{'='*60}")
print(json.dumps(extraction_summary, indent=2))
print(f"\n✅ Extraction completed. Check logs at: {log_path}")

# COMMAND ----------

# OPTIONAL: Quick validation of Bronze tables

print("\n" + "="*60)
print("BRONZE LAYER VALIDATION")
print("="*60)

bronze_tables = ["bronze.raw_fakestore_products", "bronze.raw_fakestore_orders", "bronze.raw_fakestore_carts"]

for table_name in bronze_tables:
    try:
        df = spark.table(table_name)
        record_count = df.count()
        print(f"\n📊 {table_name}")
        print(f"   Total records: {record_count}")
        print(f"   Schema columns: {len(df.columns)}")
        
        # Show sample
        df.limit(2).display()
    except:
        print(f"⚠️  Table {table_name} not yet created")
