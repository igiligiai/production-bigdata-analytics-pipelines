# Databricks notebook source
# ====================================================================
# STAGE 3F: GOLD VOLUME ANOMALIES
# ====================================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold_table(df, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


print(f"\n{'=' * 60}")
print("GOLD STAGE: gold_volume_anomalies")
print(f"{'=' * 60}\n")

gold_volume_anomalies = spark.sql(
    """
    WITH daily_volume AS (
      SELECT
        symbol,
        CAST(extracted_at AS DATE) AS price_date,
        MAX_BY(volume, extracted_at) AS daily_volume
      FROM silver.silver_hourly_prices
      WHERE extracted_at >= date_trunc('day', current_date() - interval 21 day)
        AND volume > 0
      GROUP BY symbol, CAST(extracted_at AS DATE)
    ),
    volume_stats AS (
      SELECT
        symbol,
        COUNT(DISTINCT price_date) AS trading_days,
        AVG(daily_volume) AS avg_volume,
        STDDEV(daily_volume) AS volume_stddev
      FROM daily_volume
      WHERE price_date < date_trunc('day', current_date())
      GROUP BY symbol
      HAVING COUNT(DISTINCT price_date) >= 5
    ),
    today_volume AS (
      SELECT
        symbol,
        MAX_BY(volume, extracted_at) AS today_volume
      FROM silver.silver_hourly_prices
      WHERE extracted_at >= date_trunc('day', current_date())
        AND extracted_at < date_trunc('day', current_date() + interval 1 day)
        AND volume > 0
      GROUP BY symbol
    )
    SELECT
      c.symbol,
      c.company_name,
      c.sector,
      t.today_volume,
      ROUND(s.avg_volume, 0) AS avg_volume_21d,
      ROUND(t.today_volume / NULLIF(s.avg_volume, 0), 2) AS volume_ratio,
      CASE
        WHEN s.volume_stddev IS NULL OR s.volume_stddev = 0 THEN 0
        ELSE ROUND((t.today_volume - s.avg_volume) / s.volume_stddev, 2)
      END AS volume_z_score,
      s.trading_days
    FROM today_volume t
    JOIN volume_stats s
      ON t.symbol = s.symbol
    JOIN silver.silver_company_info c
      ON t.symbol = c.symbol
    WHERE t.today_volume > s.avg_volume * 1.5
    ORDER BY volume_ratio DESC, today_volume DESC
    """
)

write_gold_table(gold_volume_anomalies, "gold.gold_volume_anomalies")

print(f"✅ {gold_volume_anomalies.count()} rows written to gold.gold_volume_anomalies")
