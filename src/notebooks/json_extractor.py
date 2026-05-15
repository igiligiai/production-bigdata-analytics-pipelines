# Databricks notebook source
import json
import os
import sys
sys.path.append(os.path.abspath("../.."))
from src.utils.stock_prices import extract_company_info, extract_hourly_prices

# COMMAND ----------

BASE_DIR = "dbfs:/raw/depeap/extract"

# COMMAND ----------

def write_hourly_data():
    subdir = f"{BASE_DIR}/hourly_data"
    hourly_data = extract_hourly_prices()
    extracted_at = hourly_data[0].get('extracted_at')
    date = extracted_at.split(' ')[0]
    hour = extracted_at.split(' ')[1]
    file_name = f"{hour.replace(':','_')}.json"
    dbutils.fs.mkdirs(f"{subdir}/{date}")
    file_path = f"{subdir}/{date}/{file_name}"
    with open(file_path.replace("dbfs:", "/dbfs/"), 'w') as f:
        json.dump(hourly_data, f)

# COMMAND ----------

def write_company_info():
    subdir = f"{BASE_DIR}/company_info"
    company_info = extract_company_info()
    file_path = f"{subdir}/company_info.json"
    dbutils.fs.mkdirs(subdir)
    with open(file_path.replace("dbfs:", "/dbfs/"), 'w') as f:
        json.dump(company_info, f)

# COMMAND ----------

write_hourly_data()

# COMMAND ----------

write_company_info()
