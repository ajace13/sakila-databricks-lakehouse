# Databricks notebook source
# MAGIC %md
# MAGIC # Sakila lakehouse on Databricks
# MAGIC
# MAGIC Runs the same layer modules as the local pipeline (`src/lakehouse`), against Unity Catalog on Databricks.
# MAGIC Works on the Free Edition with serverless compute. Never needs a paid account.
# MAGIC
# MAGIC **Setup**
# MAGIC 1. Add this repository as a Git folder (Workspace > Create > Git folder) and open this notebook from it.
# MAGIC 2. Run the next cell. It creates the five layer schemas and a `raw` volume in `sakila_bronze`.
# MAGIC 3. Upload the 11 files in `data/sakila/` to that volume (Catalog > workspace > sakila_bronze > raw > Upload).
# MAGIC 4. Run the cells down to the March 2006 section, then upload the 3 files in `data/march_2006/` and run the rest.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))   # the Git folder's src/, next to this notebook's folder

from lakehouse import bronze, checks, enriched, gold, incremental, platinum, silver

CATALOG = "workspace"
RAW = f"/Volumes/{CATALOG}/sakila_bronze/raw"

spark.sql(f"USE CATALOG {CATALOG}")  # spark and display are provided by Databricks
bronze.create_schemas(spark)
spark.sql("CREATE VOLUME IF NOT EXISTS sakila_bronze.raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze
# MAGIC Every file lands as strings, with `_ingest_ts` and `_source_file`. Append-only, and a file that has already
# MAGIC landed is skipped, so re-running this cell can't double the table.

# COMMAND ----------

for table, rows in bronze.load_sakila(spark, RAW).items():
    print(f"{table:<20} {rows:>7,} rows appended")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver, Silver-Enriched, Gold and Platinum

# COMMAND ----------

silver.build(spark)
enriched.build(spark)
gold.build(spark)
platinum.build(spark)
before = checks.layer_counts(spark)
print(checks.grain_check(spark))
before

# COMMAND ----------

# MAGIC %md
# MAGIC ## A look at Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila_gold.revenue_by_category_month ORDER BY revenue DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## The March 2006 month, applied incrementally
# MAGIC Upload `rental_2006_03.csv`, `payment_2006_03.csv` and `country_region.csv` to the volume first.
# MAGIC The first MERGE is expected to fail with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`;
# MAGIC see `docs/incremental_merge.md`.

# COMMAND ----------

report = incremental.apply_month(spark, RAW)
after = checks.layer_counts(spark)
for key in ("landed", "staged", "merge_without_dedupe", "rental_merge", "payment_merge", "time_travel",
            "rebuild_vs_merge"):
    print(f"{key}: {report[key]}")

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY sakila_silver.rental")
        .select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

for _, table in checks.MAIN_TABLES:
    print(f"{table:<42} {before[table]:>7,} {after[table]:>7,} {after[table] - before[table]:>+7,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Platinum views

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila_platinum.monthly_revenue ORDER BY month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, SUM(revenue) AS revenue
# MAGIC FROM sakila_platinum.revenue_by_region_category
# MAGIC GROUP BY region ORDER BY revenue DESC
