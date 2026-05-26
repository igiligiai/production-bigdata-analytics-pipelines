# production_bigdata_analytics_pipelines

Databricks Asset Bundle for an e-commerce analytics pipeline with Bronze, Silver, Gold, and adhoc analysis jobs.

## Current setup

- `src/notebooks/` contains the pipeline notebooks.
- `src/notebooks/adhoc/` holds the weekly Silver analysis and data-quality validation notebooks used by scheduled jobs.
- `resources/clusters/` defines the shared ETL clusters for dev and prod.
- `resources/jobs/` defines the daily ETL pipeline and weekly analytics refresh jobs.
- `databricks.yml` is the bundle entry point for targets and deployment settings.

## Working with the bundle

Use `databricks bundle validate` to check the bundle, `databricks bundle deploy --target dev` for a development deployment, and `databricks bundle deploy --target prod` for production.

Run the local test suite with `uv run pytest`.
