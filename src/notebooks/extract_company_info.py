# Databricks notebook source
# ====================================================================
# STAGE 1B: COMPANY INFO EXTRACTION (Bronze Layer)
# ====================================================================
# Purpose: Create the bronze view for company info.

# COMMAND ----------

RAW_DIR = "dbfs:/raw/depeap/extract"


def company_info_bronze_view() -> None:
    """Create or replace the bronze view for company info."""
    source_path = f"{RAW_DIR}/company_info/company_info.json"
    view_name = "bronze.company_info"

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {view_name}
        AS SELECT * FROM json.`{source_path}`
        """
    )
    print(f"✅ Created view: {view_name} for company info")


# COMMAND ----------

company_info_bronze_view()
