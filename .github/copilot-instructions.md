# Copilot instructions for `production_bigdata_analytics_pipelines`

This repository is a Databricks Asset Bundle for a production-style e-commerce analytics pipeline. Keep changes aligned with the existing Bronze → Silver → Gold flow and the current notebook layout in `src/notebooks/`.

## Project shape

- `src/notebooks/extract_stock_data.py`: extracts hourly stock data into Bronze.
- `src/notebooks/extract_company_info.py`: extracts company info into Bronze.
- `src/notebooks/clean_hourly_data.py`: cleans hourly stock data into `silver.silver_hourly_prices`.
- `src/notebooks/clean_company_info.py`: cleans company profiles into `silver.silver_company_info`.
- `src/notebooks/gold/gold_market_summary.sql`: latest price snapshot per symbol enriched with company profile data.
- `src/notebooks/gold/gold_sector_performance.sql`: sector-level market aggregation.
- `src/notebooks/gold/gold_best_performing_today.sql`: top daily gainers.
- `src/notebooks/gold/gold_worst_performing_21d.sql`: bottom 21-day performers.
- `src/notebooks/gold/gold_most_volatile.sql`: intraday volatility ranking.
- `src/notebooks/gold/gold_volume_anomalies.sql`: today vs 21-day volume spikes.
- `src/notebooks/gold/gold_52_week_highs_lows.sql`: proximity to 52-week range edges.
- `src/notebooks/gold/gold_price_analytics.sql`: per-symbol technical analytics and trend signals.
- `src/notebooks/adhoc/weekly_silver_analysis.py`: weekly Silver-layer customer and category analysis.
- `src/notebooks/adhoc/quality_assurance.py`: validates Bronze/Silver/Gold data quality and writes a validation report.
- `databricks.yml`: bundle definition with `dev` and `prod` targets.
- `resources/clusters/` and `resources/jobs/`: bundle resource definitions split by type. The daily ETL job and weekly analytics refresh job reference the notebooks above directly.

## Data model and table names

Preserve the existing table names and layer prefixes unless the user explicitly asks for a breaking change:

- Bronze: `bronze.raw_fakestore_products`, `bronze.raw_fakestore_orders`, `bronze.raw_fakestore_carts`
- Silver: `silver.products_cleaned`, `silver.orders_cleaned`, `silver.carts_cleaned`, `silver.silver_hourly_prices`, `silver.silver_company_info`
- Gold views: `gold.gold_market_summary`, `gold.gold_sector_performance`, `gold.gold_best_performing_today`, `gold.gold_worst_performing_21d`, `gold.gold_most_volatile`, `gold.gold_volume_anomalies`, `gold.gold_52_week_highs_lows`, `gold.gold_price_analytics`

## Working rules

- Keep Bronze append-only and Silver overwrite semantics as currently implemented; Gold outputs are permanent views.
- Split unrelated Silver cleaning work into separate notebooks instead of combining hourly prices and company info.
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
