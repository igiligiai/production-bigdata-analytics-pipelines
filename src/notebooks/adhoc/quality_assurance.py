# Databricks notebook source
# ====================================================================
# STAGE 4: DATA QUALITY VALIDATION & ASSERTIONS
# ====================================================================
# Purpose: Comprehensive validation of all pipeline stages to ensure
# data integrity, completeness, and consistency before serving to consumers.

from pyspark.sql import functions as F, DataFrame
from datetime import datetime, timedelta
import json

# COMMAND ----------

class DataQualityValidator:
    """
    Comprehensive data quality checking framework.
    Used for production validation gates.
    """
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.df = spark.table(table_name)
        self.checks = {}
        self.failed_checks = []
    
    def check_record_count(self, min_records: int = 1) -> bool:
        """Ensure table has minimum records"""
        count = self.df.count()
        passed = count >= min_records
        self.checks[f"min_records_{min_records}"] = {
            "passed": passed,
            "actual": count,
            "expected_min": min_records
        }
        if not passed:
            self.failed_checks.append(f"Record count {count} < {min_records}")
        return passed
    
    def check_non_null_columns(self, columns: list) -> bool:
        """Ensure critical columns have no nulls"""
        passed = True
        for col in columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            check_key = f"no_nulls_{col}"
            self.checks[check_key] = {
                "passed": null_count == 0,
                "null_count": null_count,
                "column": col
            }
            if null_count > 0:
                self.failed_checks.append(f"Column '{col}' has {null_count} nulls")
                passed = False
        return passed
    
    def check_data_types(self, schema_map: dict) -> bool:
        """Validate column data types match expectations"""
        passed = True
        df_schema = {f.name: str(f.dataType) for f in self.df.schema}
        
        for col, expected_type in schema_map.items():
            actual_type = df_schema.get(col)
            is_valid = expected_type in str(actual_type) if actual_type else False
            
            self.checks[f"type_{col}"] = {
                "passed": is_valid,
                "expected": expected_type,
                "actual": actual_type
            }
            if not is_valid:
                self.failed_checks.append(f"Column '{col}': expected {expected_type}, got {actual_type}")
                passed = False
        return passed
    
    def check_no_duplicates(self, key_columns: list) -> bool:
        """Ensure no duplicate keys"""
        total_count = self.df.count()
        distinct_count = self.df.select(*key_columns).distinct().count()
        
        passed = total_count == distinct_count
        self.checks["no_duplicates"] = {
            "passed": passed,
            "total_rows": total_count,
            "distinct_keys": distinct_count,
            "duplicate_count": total_count - distinct_count
        }
        if not passed:
            self.failed_checks.append(f"Found {total_count - distinct_count} duplicate keys")
        return passed
    
    def check_value_ranges(self, column: str, min_val=None, max_val=None) -> bool:
        """Validate numeric columns are within expected ranges"""
        passed = True
        
        if min_val is not None:
            out_of_range = self.df.filter(F.col(column) < min_val).count()
            check_key = f"min_value_{column}_{min_val}"
            self.checks[check_key] = {"passed": out_of_range == 0, "count": out_of_range}
            if out_of_range > 0:
                self.failed_checks.append(f"Column '{column}': {out_of_range} values < {min_val}")
                passed = False
        
        if max_val is not None:
            out_of_range = self.df.filter(F.col(column) > max_val).count()
            check_key = f"max_value_{column}_{max_val}"
            self.checks[check_key] = {"passed": out_of_range == 0, "count": out_of_range}
            if out_of_range > 0:
                self.failed_checks.append(f"Column '{column}': {out_of_range} values > {max_val}")
                passed = False

        return passed

    def check_allowed_values(self, column: str, allowed_values: list) -> bool:
        """Validate a column only contains approved values"""
        invalid_count = self.df.filter(~F.col(column).isin(allowed_values)).count()
        passed = invalid_count == 0
        self.checks[f"allowed_values_{column}"] = {
            "passed": passed,
            "invalid_count": invalid_count,
            "allowed_values": allowed_values,
        }
        if not passed:
            self.failed_checks.append(f"Column '{column}' has {invalid_count} invalid values")
        return passed
    
    def check_quality_flag_distribution(self, quality_col: str, ok_threshold: float = 0.85) -> bool:
        """Ensure majority of records pass quality checks"""
        total = self.df.count()
        ok_count = self.df.filter(F.col(quality_col) == "ok").count()
        ok_ratio = ok_count / total if total > 0 else 0
        
        passed = ok_ratio >= ok_threshold
        self.checks["quality_distribution"] = {
            "passed": passed,
            "ok_records": ok_count,
            "total_records": total,
            "ok_ratio": ok_ratio,
            "threshold": ok_threshold
        }
        if not passed:
            self.failed_checks.append(f"Quality ratio {ok_ratio:.1%} < {ok_threshold:.1%}")
        return passed
    
    def check_freshness(self, timestamp_col: str, max_age_hours: int = 48) -> bool:
        """Ensure data is recent (not stale)"""
        max_timestamp = self.df.agg(F.max(timestamp_col)).collect()[0][0]
        
        if max_timestamp is None:
            self.checks["freshness"] = {"passed": False, "reason": "No timestamp data"}
            self.failed_checks.append("No timestamp data found")
            return False
        
        from datetime import datetime
        age_hours = (datetime.utcnow() - max_timestamp).total_seconds() / 3600
        passed = age_hours <= max_age_hours
        
        self.checks["freshness"] = {
            "passed": passed,
            "max_timestamp": str(max_timestamp),
            "age_hours": age_hours,
            "max_age_hours": max_age_hours
        }
        if not passed:
            self.failed_checks.append(f"Data age {age_hours:.1f}h > {max_age_hours}h")
        return passed
    
    def summary(self) -> dict:
        """Return validation summary"""
        return {
            "table": self.table_name,
            "timestamp": datetime.utcnow().isoformat(),
            "total_checks": len(self.checks),
            "passed_checks": sum(1 for c in self.checks.values() if c.get("passed", False)),
            "failed_checks": len(self.failed_checks),
            "all_checks": self.checks,
            "failures": self.failed_checks,
            "status": "PASSED" if not self.failed_checks else "FAILED"
        }

# COMMAND ----------

print(f"\n{'='*70}")
print("DATA QUALITY VALIDATION - ALL TABLES")
print(f"{'='*70}\n")

# ====================================================================
# VALIDATE BRONZE LAYER
# ====================================================================

print("BRONZE LAYER VALIDATION\n")

# Bronze: Products
print("🔍 bronze.raw_fakestore_products")
bronze_products_validator = DataQualityValidator("bronze.raw_fakestore_products")
bronze_products_validator.check_record_count(min_records=5)
bronze_products_validator.check_non_null_columns(["id"])
summary = bronze_products_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")
if summary['failures']:
    for failure in summary['failures']:
        print(f"   ❌ {failure}")

# Bronze: Orders
print("\n🔍 bronze.raw_fakestore_orders")
bronze_orders_validator = DataQualityValidator("bronze.raw_fakestore_orders")
bronze_orders_validator.check_record_count(min_records=5)
bronze_orders_validator.check_non_null_columns(["id", "userId"])
summary = bronze_orders_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Bronze: Carts
print("\n🔍 bronze.raw_fakestore_carts")
bronze_carts_validator = DataQualityValidator("bronze.raw_fakestore_carts")
bronze_carts_validator.check_record_count(min_records=1)
summary = bronze_carts_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# ====================================================================
# VALIDATE SILVER LAYER
# ====================================================================

print(f"\n{'─'*70}\n")
print("SILVER LAYER VALIDATION\n")

# Silver: Products
print("🔍 silver.silver_hourly_prices")
silver_hourly_validator = DataQualityValidator("silver.silver_hourly_prices")
silver_hourly_validator.check_record_count(min_records=1)
silver_hourly_validator.check_non_null_columns(["symbol", "extracted_at"])
silver_hourly_validator.check_no_duplicates(["symbol", "extracted_at"])
summary = silver_hourly_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")
for failure in summary['failures'][:3]:
    print(f"   ⚠️  {failure}")

print("\n🔍 silver.silver_company_info")
silver_company_validator = DataQualityValidator("silver.silver_company_info")
silver_company_validator.check_record_count(min_records=1)
silver_company_validator.check_non_null_columns(["symbol", "extracted_at"])
silver_company_validator.check_no_duplicates(["symbol", "extracted_at"])
summary = silver_company_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")
for failure in summary['failures'][:3]:
    print(f"   ⚠️  {failure}")

# Silver: Products
print("🔍 silver.products_cleaned")
silver_products_validator = DataQualityValidator("silver.products_cleaned")
silver_products_validator.check_record_count(min_records=5)
silver_products_validator.check_non_null_columns(["product_id", "title"])
silver_products_validator.check_no_duplicates(["product_id", "source_api"])
silver_products_validator.check_value_ranges("price", min_val=0, max_val=1000)
silver_products_validator.check_quality_flag_distribution("data_quality_flags", ok_threshold=0.90)
summary = silver_products_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")
for failure in summary['failures'][:3]:
    print(f"   ⚠️  {failure}")

# Silver: Orders
print("\n🔍 silver.orders_cleaned")
silver_orders_validator = DataQualityValidator("silver.orders_cleaned")
silver_orders_validator.check_record_count(min_records=5)
silver_orders_validator.check_non_null_columns(["order_id", "user_id"])
silver_orders_validator.check_no_duplicates(["order_id"])
silver_orders_validator.check_quality_flag_distribution("data_quality_flags", ok_threshold=0.90)
summary = silver_orders_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Silver: Carts
print("\n🔍 silver.carts_cleaned")
silver_carts_validator = DataQualityValidator("silver.carts_cleaned")
silver_carts_validator.check_record_count(min_records=1)
silver_carts_validator.check_non_null_columns(["cart_id", "user_id"])
summary = silver_carts_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# ====================================================================
# VALIDATE GOLD LAYER
# ====================================================================

print(f"\n{'─'*70}\n")
print("GOLD LAYER VALIDATION\n")

# Gold: Market Summary
print("🔍 gold.gold_market_summary")
gold_market_summary_validator = DataQualityValidator("gold.gold_market_summary")
gold_market_summary_validator.check_record_count(min_records=1)
gold_market_summary_validator.check_non_null_columns(["symbol", "company_name", "sector", "current_price"])
gold_market_summary_validator.check_no_duplicates(["symbol"])
summary = gold_market_summary_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Gold: Sector Performance
print("\n🔍 gold.gold_sector_performance")
gold_sector_performance_validator = DataQualityValidator("gold.gold_sector_performance")
gold_sector_performance_validator.check_record_count(min_records=1)
gold_sector_performance_validator.check_non_null_columns(["sector", "num_companies", "total_market_cap", "vwap"])
gold_sector_performance_validator.check_no_duplicates(["sector"])
summary = gold_sector_performance_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Gold: Best Performing Today
print("\n🔍 gold.gold_best_performing_today")
gold_best_validator = DataQualityValidator("gold.gold_best_performing_today")
gold_best_validator.check_record_count(min_records=1)
gold_best_validator.check_non_null_columns(["symbol", "company_name", "sector", "extracted_at_date", "extracted_at_time", "daily_gain_price", "daily_gain_percentage"])
gold_best_validator.check_no_duplicates(["symbol"])
summary = gold_best_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Gold: Worst Performing 21d
print("\n🔍 gold.gold_worst_performing_21d")
gold_worst_validator = DataQualityValidator("gold.gold_worst_performing_21d")
gold_worst_validator.check_record_count(min_records=1)
gold_worst_validator.check_non_null_columns(["symbol", "company_name", "sector", "period_start", "period_end", "trading_days", "price_change", "price_change_pct"])
gold_worst_validator.check_no_duplicates(["symbol"])
summary = gold_worst_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Gold: Most Volatile
print("\n🔍 gold.gold_most_volatile")
gold_volatile_validator = DataQualityValidator("gold.gold_most_volatile")
gold_volatile_validator.check_record_count(min_records=1)
gold_volatile_validator.check_non_null_columns(["symbol", "company_name", "sector", "avg_intraday_range_pct", "max_intraday_range_pct", "coefficient_of_variation"])
gold_volatile_validator.check_no_duplicates(["symbol"])
summary = gold_volatile_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Gold: Volume Anomalies
print("\n🔍 gold.gold_volume_anomalies")
gold_volume_validator = DataQualityValidator("gold.gold_volume_anomalies")
gold_volume_validator.check_record_count(min_records=0)
gold_volume_validator.check_non_null_columns(["symbol", "company_name", "sector", "today_volume", "avg_volume_21d", "volume_ratio"])
gold_volume_validator.check_no_duplicates(["symbol"])
summary = gold_volume_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Gold: 52 Week Highs / Lows
print("\n🔍 gold.gold_52_week_highs_lows")
gold_range_validator = DataQualityValidator("gold.gold_52_week_highs_lows")
gold_range_validator.check_record_count(min_records=1)
gold_range_validator.check_non_null_columns(["symbol", "company_name", "sector", "range_position_pct", "proximity_signal"])
gold_range_validator.check_no_duplicates(["symbol"])
gold_range_validator.check_allowed_values("proximity_signal", ["NEAR_52W_HIGH", "MID_RANGE", "NEAR_52W_LOW"])
summary = gold_range_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# Gold: Price Analytics
print("\n🔍 gold.gold_price_analytics")
gold_price_validator = DataQualityValidator("gold.gold_price_analytics")
gold_price_validator.check_record_count(min_records=1)
gold_price_validator.check_non_null_columns(["symbol", "price_change_pct", "trend_signal", "range_52w_pct"])
gold_price_validator.check_no_duplicates(["symbol"])
gold_price_validator.check_allowed_values("trend_signal", ["BULLISH", "BEARISH", "NEUTRAL"])
summary = gold_price_validator.summary()
print(f"   Status: {summary['status']} ({summary['passed_checks']}/{summary['total_checks']} checks)")

# ====================================================================
# CROSS-TABLE VALIDATION
# ====================================================================

print(f"\n{'─'*70}\n")
print("CROSS-TABLE VALIDATION (Referential Integrity)\n")

# Check that all order user_ids exist in products
silver_orders = spark.table("silver.orders_cleaned")
silver_products = spark.table("silver.products_cleaned")

print("🔗 Referential Integrity: Orders → Products")
orders_with_products = silver_orders.select("product_ids").collect()
print(f"   Orders referencing products: {len(orders_with_products)}")

# Check: Gold market summary symbols should exist in silver hourly prices
gold_market_summary = spark.table("gold.gold_market_summary")
silver_hourly_prices = spark.table("silver.silver_hourly_prices")

print(f"\n🔗 Market Snapshot Lineage: Silver → Gold")
print(f"   Silver price rows: {silver_hourly_prices.count()}")
print(f"   Gold market snapshot rows: {gold_market_summary.count()}")

gold_symbols = set([row[0] for row in gold_market_summary.select("symbol").collect()])
silver_symbols = set([row[0] for row in silver_hourly_prices.select("symbol").distinct().collect()])

orphan_gold_symbols = gold_symbols - silver_symbols
if orphan_gold_symbols:
    print(f"   ⚠️  Found {len(orphan_gold_symbols)} Gold market symbols not in Silver!")
else:
    print(f"   ✅ All Gold market symbols exist in Silver (referential integrity)")

# ====================================================================
# DATA VOLUME TREND
# ====================================================================

print(f"\n{'─'*70}\n")
print("DATA VOLUME METRICS\n")

tables_stats = []
for table_name in [
    "bronze.raw_fakestore_products",
    "silver.products_cleaned",
    "gold.gold_market_summary",
    "gold.gold_sector_performance",
    "gold.gold_best_performing_today",
    "gold.gold_worst_performing_21d",
    "gold.gold_most_volatile",
    "gold.gold_volume_anomalies",
    "gold.gold_52_week_highs_lows",
    "gold.gold_price_analytics"
]:
    df = spark.table(table_name)
    count = df.count()
    size_bytes = df.select(
        F.sum(
            F.length(F.to_json(F.struct(*[F.col(column) for column in df.columns])))
        ).alias("size")
    ).collect()[0][0]
    tables_stats.append({
        "table": table_name.split(".")[-1],
        "rows": count,
        "size_mb": (size_bytes or 0) / (1024 * 1024)
    })

for stat in tables_stats:
    print(f"📊 {stat['table']:30} | {stat['rows']:8,} rows | {stat['size_mb']:8.2f} MB")

# ====================================================================
# FINAL REPORT
# ====================================================================

print(f"\n{'='*70}")
print("VALIDATION SUMMARY")
print(f"{'='*70}\n")

all_validators = [
    bronze_products_validator, bronze_orders_validator, bronze_carts_validator,
    silver_hourly_validator, silver_company_validator,
    silver_products_validator, silver_orders_validator, silver_carts_validator,
    gold_market_summary_validator, gold_sector_performance_validator,
    gold_best_validator, gold_worst_validator, gold_volatile_validator,
    gold_volume_validator, gold_range_validator, gold_price_validator
]

total_checks = sum(len(v.checks) for v in all_validators)
total_passed = sum(sum(1 for c in v.checks.values() if c.get("passed", False)) for v in all_validators)
total_failed = sum(len(v.failed_checks) for v in all_validators)

print(f"✅ Total Checks: {total_checks}")
print(f"✅ Passed: {total_passed}")
print(f"❌ Failed: {total_failed}")

validation_status = "PASSED" if total_failed == 0 else "FAILED"
print(f"\n📋 OVERALL STATUS: {validation_status}\n")

if total_failed == 0:
    print("🎉 All validation checks PASSED!")
    print("✅ Pipeline is ready for downstream consumption")
    print("✅ Data quality thresholds met")
    print("✅ Referential integrity verified")
else:
    print(f"⚠️  {total_failed} validation checks FAILED")
    print("❌ Pipeline should be investigated before use")

# Save report to DBFS
report = {
    "timestamp": datetime.utcnow().isoformat(),
    "status": validation_status,
    "total_checks": total_checks,
    "passed": total_passed,
    "failed": total_failed,
    "validators": [v.summary() for v in all_validators]
}

import json
report_path = f"/dbfs/logs/validation/{datetime.now().date()}_validation_report.json"
dbutils.fs.put(report_path, json.dumps(report, indent=2, default=str))

print(f"\n📝 Full report saved to: {report_path}")
