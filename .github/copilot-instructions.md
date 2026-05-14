# Copilot instructions for `production_bigdata_analytics_pipelines`

This repository is a Databricks Asset Bundle for a production-style e-commerce analytics pipeline. Keep changes aligned with the existing Bronze → Silver → Gold flow and the current notebook layout in `src/notebooks/`.

## Project shape

- `src/notebooks/extract_stock_data.py`: extracts hourly stock data into Bronze.
- `src/notebooks/extract_company_info.py`: extracts company info into Bronze.
- `src/notebooks/02_clean.py`: standardizes and deduplicates Bronze data into Silver tables.
- `src/notebooks/03_transform.py`: builds Gold analytics tables with Spark SQL, window functions, and CTEs.
- `src/notebooks/quality_assurance.py`: validates Bronze/Silver/Gold data quality and writes a validation report.
- `databricks.yml`: bundle definition with `dev` and `prod` targets.
- `resources/clusters/` and `resources/jobs/`: bundle resource definitions split by type.

## Data model and table names

Preserve the existing table names and layer prefixes unless the user explicitly asks for a breaking change:

- Bronze: `bronze.raw_fakestore_products`, `bronze.raw_fakestore_orders`, `bronze.raw_fakestore_carts`
- Silver: `silver.products_cleaned`, `silver.orders_cleaned`, `silver.carts_cleaned`
- Gold: `gold.daily_revenue_by_category`, `gold.customer_360_rfm`, `gold.product_performance_metrics`, `gold.category_insights`, `gold.daily_anomalies`

## Working rules

- Keep Bronze append-only and Silver/Gold overwrite semantics as currently implemented.
- Preserve data-quality flags, deduplication logic, and validation thresholds unless the request is specifically about those rules.
- Prefer Spark DataFrame/Spark SQL patterns already used in the repo: explicit casts, `spark.table(...)`, Delta writes, CTEs, and window functions.
- Keep notebook edits consistent with Databricks notebook source format and the existing stage banners.
- Use concise comments only when logic is non-obvious.

## Bundle and validation

- Treat `databricks.yml` as the source of truth for bundle targets and deployment behavior.
- For local validation, use `uv run pytest`.
- For Databricks bundle workflows, use `databricks bundle validate`, `databricks bundle deploy --target dev`, and `databricks bundle deploy --target prod` when appropriate.

## Style expectations

- Match the repository’s naming conventions and keep changes production-oriented.
- Avoid renaming tables, logs, or stage boundaries unless required.
- Keep the pipeline explainable: extraction, cleaning, transformation, and validation should remain easy to trace.
