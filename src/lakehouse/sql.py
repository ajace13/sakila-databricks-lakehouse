"""Run a block of SQL statements separated by semicolons."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def run(spark: SparkSession, script: str) -> DataFrame | None:
    """Run each statement in order and return the result of the last one."""
    result = None
    for statement in (s.strip() for s in script.split(";")):
        if statement:
            result = spark.sql(statement)
    return result
