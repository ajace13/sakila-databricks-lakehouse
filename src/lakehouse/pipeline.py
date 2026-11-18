"""The whole pipeline in order: the initial load through every layer, then the incremental month."""
from __future__ import annotations

from pyspark.sql import SparkSession

from . import bronze, checks, enriched, gold, incremental, platinum, silver


def initial_load(spark: SparkSession, sakila_folder: str) -> dict:
    bronze.create_schemas(spark)
    landed = bronze.load_sakila(spark, sakila_folder)
    silver.build(spark)
    enriched.build(spark)
    gold.build(spark)
    platinum.build(spark)
    return {"landed": landed, "grain": checks.grain_check(spark), "counts": checks.layer_counts(spark)}


def run_all(spark: SparkSession, sakila_folder: str, month_folder: str) -> dict:
    report = {"initial": initial_load(spark, sakila_folder)}
    report["incremental"] = incremental.apply_month(spark, month_folder)
    report["after"] = {"grain": checks.grain_check(spark), "counts": checks.layer_counts(spark)}
    return report
