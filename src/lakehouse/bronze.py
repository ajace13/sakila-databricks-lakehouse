"""Bronze: keep exactly what arrived.

Every file is read with no schema, so every value stays a string (including the Sakila CSVs' literal \\N for NULL).
Two audit columns record when the row landed and which file it came from. Bronze is append-only: nothing in it is
ever updated or deleted, so any later layer can be rebuilt from it.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

from . import SCHEMAS

SAKILA_TABLES = ["rental", "payment", "customer", "film", "inventory", "category",
                 "film_category", "store", "address", "city", "country"]


def create_schemas(spark: SparkSession) -> None:
    for schema in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def already_landed(spark: SparkSession, table: str, file_name: str) -> bool:
    if not spark.catalog.tableExists(f"sakila_bronze.{table}"):
        return False
    return spark.table(f"sakila_bronze.{table}").where(col("_source_file") == file_name).limit(1).count() > 0


def load_file(spark: SparkSession, path: str, table: str) -> int:
    """Append one CSV file to sakila_bronze.<table> and return the number of rows it added.

    A file that has already landed is skipped, so running the load twice can't double Bronze.
    """
    if already_landed(spark, table, path.rsplit("/", 1)[-1]):
        return 0
    df = (spark.read.option("header", True).option("escape", '"').csv(path)
          .withColumn("_ingest_ts", current_timestamp())
          .withColumn("_source_file", col("_metadata.file_name")))
    df.write.mode("append").saveAsTable(f"sakila_bronze.{table}")
    return df.count()


def load_sakila(spark: SparkSession, folder: str) -> dict[str, int]:
    """Land the initial Sakila extract: one Bronze table per source file."""
    return {f"{t}_raw": load_file(spark, f"{folder}/{t}.csv", f"{t}_raw") for t in SAKILA_TABLES}
