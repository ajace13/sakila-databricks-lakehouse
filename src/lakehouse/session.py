"""A local Spark session with Delta Lake, configured to behave like Databricks for this pipeline."""
from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def local_spark(warehouse: Path) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("sakila-lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.default", "delta")          # tables are Delta by default, as on Databricks
        .config("spark.sql.warehouse.dir", str(warehouse.resolve()))
        .config("spark.sql.shuffle.partitions", "4")           # the data is small
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")   # the failing MERGE is expected; its Java stack trace is noise
    return spark
