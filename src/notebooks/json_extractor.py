# Databricks notebook source
import json
import os
import sys
from typing import List, Dict
sys.path.append(os.path.abspath("../.."))
from src.utils.stock_prices import extract_company_info, extract_hourly_prices, fetch_stock_data

# COMMAND ----------

BASE_DIR = "dbfs:/raw/depeap/extract"

# COMMAND ----------

# info: List[Dict] = fetch_stock_data()
info: List[Dict] = [{}]


def write_hourly_data(info: List[Dict]) -> None:
    subdir = f"{BASE_DIR}/hourly_data"
    hourly_data = extract_hourly_prices(info)
    extracted_at = info[0].get('extracted_at')
    date = extracted_at.split(' ')[0]
    hour = extracted_at.split(' ')[1]
    file_name = f"{hour.replace(':','_')}.json"
    dbutils.fs.mkdirs(f"{subdir}/{date}")
    file_path = f"{subdir}/{date}/{file_name}"
    with open(file_path.replace("dbfs:", "/dbfs/"), 'w') as f:
        json.dump(hourly_data, f)

# COMMAND ----------

def write_company_info(info: List[Dict]) -> None:
    subdir = f"{BASE_DIR}/company_info"
    company_info = extract_company_info(info)
    file_path = f"{subdir}/company_info.json"
    dbutils.fs.mkdirs(subdir)
    with open(file_path.replace("dbfs:", "/dbfs/"), 'w') as f:
        json.dump(company_info, f)

# COMMAND ----------

# write_hourly_data(info)

# COMMAND ----------

# write_company_info(info)
