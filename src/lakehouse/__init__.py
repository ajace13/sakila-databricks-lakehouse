"""Sakila medallion lakehouse: Bronze, Silver, Silver-Enriched, Gold and Platinum on Delta Lake.

The same functions run on Databricks (see notebooks/) and on a local Spark session with delta-spark.
"""
SCHEMAS = ["sakila_bronze", "sakila_silver", "sakila_silver_enriched", "sakila_gold", "sakila_platinum"]
