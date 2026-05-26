# Databricks notebook source
# ====================================================================
# STAGE 1A: STOCK DATA EXTRACTION (Bronze Layer)
# ====================================================================
# Purpose: Create the bronze view for hourly stock data and log validation results.

import json
from datetime import datetime

from pyspark.sql import functions as F

# COMMAND ----------

def get_process_datetime() -> datetime:
    process_datetime = dbutils.widgets.getAll().get("ProcessDatetime")

    if process_datetime is None:
        return datetime.utcnow()

    if isinstance(process_datetime, datetime):
        return process_datetime

    try:
        return datetime.fromisoformat(process_datetime.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid ProcessDatetime widget value: {process_datetime}") from exc


CURRENT_DATETIME = get_process_datetime()

# COMMAND ----------

RAW_DIR = "dbfs:/raw/depeap/extract"
current_date = CURRENT_DATETIME.strftime("%Y-%m-%d")
hour = CURRENT_DATETIME.strftime("%H")
hourly_data = f"{hour}_00_00"

print(f"Current date: {current_date}")
print(f"Current hour: {hourly_data}")

# COMMAND ----------

files = dbutils.fs.ls(f"{RAW_DIR}/hourly_data/{current_date}/")
files = [file.name for file in files if file.name.endswith(".json")]
print(f"Available hourly data files for {current_date}: {files}")
if not hourly_data + ".json" in files:
    source_path = f"{RAW_DIR}/hourly_data/{current_date}/{sorted(files)[-1]}"

def stock_data_bronze_view(date: str, source_path: str) -> None:
    """Create or replace the bronze view for the current stock batch."""
    view_name = "bronze.stock_data_hourly"

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {view_name}
        AS SELECT * FROM json.`{source_path}`
        """
    )
    print(f"✅ Created view: {view_name} for date: {date} and hour: {hourly_data}")


def validate_bronze_stock_view(view_name: str, date: str, hourly_data: str) -> dict:
    """Validate the current bronze stock view and persist a lightweight log."""
    df = spark.table(view_name)
    record_count = df.count()
    schema_columns = len(df.columns)
    null_counts = {
        column: df.filter(F.col(column).isNull()).count()
        for column in df.columns
    }
    failed_checks = []

    if record_count == 0:
        failed_checks.append("No records found in the currently processed batch")

    validation_report = {
        "view_name": view_name,
        "process_date": date,
        "process_hour": hourly_data,
        "validated_at": datetime.utcnow().isoformat(),
        "record_count": record_count,
        "schema_columns": schema_columns,
        "columns": df.columns,
        "null_counts": null_counts,
        "status": "PASSED" if not failed_checks else "FAILED",
        "failures": failed_checks,
    }

    log_path = f"dbfs:/logs/depeap/bronze_validation/{date}/{hourly_data}_stock_data_hourly.json"
    dbutils.fs.put(log_path, json.dumps(validation_report, indent=2), overwrite=True)

    print("\n" + "=" * 60)
    print("BRONZE VIEW VALIDATION & LOGGING")
    print("=" * 60)
    print(f"View name: {view_name}")
    print(f"Status: {validation_report['status']}")
    print(f"Records in current batch: {record_count}")
    print(f"Schema columns: {schema_columns}")
    print(f"Validation log written to: {log_path}")

    if failed_checks:
        for failure in failed_checks:
            print(f"❌ {failure}")
    else:
        print("✅ Bronze view validation passed")

    return validation_report


# COMMAND ----------

stock_data_bronze_view(current_date, source_path)

# COMMAND ----------

bronze_validation_report = validate_bronze_stock_view(
    "bronze.stock_data_hourly",
    current_date,
    hourly_data,
)

# COMMAND ----------

print("\n" + "=" * 60)
print("BRONZE LAYER VALIDATION")
print("=" * 60)

bronze_tables = ["bronze.stock_data_hourly"]

for table_name in bronze_tables:
    try:
        df = spark.table(table_name)
        record_count = df.count()
        print(f"\n📊 {table_name}")
        print(f"   Total records: {record_count}")
        print(f"   Schema columns: {len(df.columns)}")
        print(f"   Validation status: {bronze_validation_report['status']}")
        df.limit(2).display()
    except:
        print(f"⚠️  Table {table_name} not yet created")
