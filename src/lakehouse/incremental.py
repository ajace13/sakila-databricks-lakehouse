"""Apply a later month of data incrementally with Delta MERGE, and show what changed.

1. Bronze appends the new files with the same loader. Nothing about Bronze changes.
2. The new rentals are typed the Silver way but not yet deduped, and a MERGE is tried. It fails, because the source
   sent some rentals twice and two source rows can't both update one target row.
3. The batch is deduped with the same rule Silver uses, and the MERGE updates the rentals Silver already has and
   inserts the new ones. Payments go through the same steps.
4. Delta's history records the MERGE, and time travel compares the table with the version before it.
5. The new reference file lands in Silver, and Silver-Enriched, Gold and Platinum are rebuilt on top.
"""
from __future__ import annotations

import re

from pyspark.sql import SparkSession

from . import bronze, enriched, gold, platinum, silver

MARCH_FILES = {"rental_2006_03.csv": "rental_raw", "payment_2006_03.csv": "payment_raw",
               "country_region.csv": "country_region_raw"}

MERGE_RENTAL = """
    MERGE INTO sakila_silver.rental AS t
    USING {source} AS s
    ON t.rental_id = s.rental_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *"""

MERGE_PAYMENT = """
    MERGE INTO sakila_silver.payment AS t
    USING {source} AS s
    ON t.payment_id = s.payment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *"""


def land(spark: SparkSession, folder: str) -> dict[str, int]:
    return {name: bronze.load_file(spark, f"{folder}/{name}", table) for name, table in MARCH_FILES.items()}


def stage(spark: SparkSession) -> dict[str, int]:
    """Temp views over the March rows in Bronze: typed but raw, and typed and deduped."""
    rental_where = "WHERE _source_file = 'rental_2006_03.csv'"
    payment_where = "WHERE _source_file = 'payment_2006_03.csv'"
    spark.sql(f"CREATE OR REPLACE TEMP VIEW rental_march_raw AS {silver.rental_typed(rental_where)}")
    spark.sql("CREATE OR REPLACE TEMP VIEW rental_march AS "
              + silver.latest_per_key(silver.rental_typed(rental_where), "rental_id", silver.RENTAL_COLUMNS))
    spark.sql("CREATE OR REPLACE TEMP VIEW payment_march AS "
              + silver.latest_per_key(silver.payment_typed(payment_where), "payment_id", silver.PAYMENT_COLUMNS))
    row = spark.sql("""SELECT COUNT(*) AS rows, COUNT(DISTINCT rental_id) AS distinct_rentals,
                              (SELECT COUNT(*) FROM rental_march rm
                               JOIN sakila_silver.rental t ON t.rental_id = rm.rental_id) AS already_in_silver
                       FROM rental_march_raw""").first()
    return row.asDict()


def try_merge_without_dedupe(spark: SparkSession) -> str | None:
    """Run the MERGE straight from the raw batch. Returns Delta's error class, or None if it succeeded."""
    try:
        spark.sql(MERGE_RENTAL.format(source="rental_march_raw"))
    except Exception as e:  # noqa: BLE001 - any failure is reported, the table is left unchanged
        found = re.search(r"\[([A-Z_]+)\]", str(e))
        return found.group(1) if found else type(e).__name__
    return None


def merge(spark: SparkSession, statement: str, source: str) -> dict[str, int]:
    row = spark.sql(statement.format(source=source)).first().asDict()
    return {k: int(v) for k, v in row.items()}


def merge_history(spark: SparkSession, table: str) -> list[dict]:
    """Delta's log of writes to the table, newest first, without the user column."""
    rows = spark.sql(f"DESCRIBE HISTORY {table}").select("version", "operation", "operationMetrics").collect()
    return [{"version": r["version"], "operation": r["operation"], "metrics": dict(r["operationMetrics"] or {})}
            for r in rows]


def time_travel(spark: SparkSession) -> dict[str, int]:
    """Compare Silver rental now with the version just before the first MERGE."""
    history = merge_history(spark, "sakila_silver.rental")
    before = min(h["version"] for h in history if h["operation"] == "MERGE") - 1
    row = spark.sql(f"""
        SELECT {before} AS version_before,
               (SELECT COUNT(*) FROM sakila_silver.rental VERSION AS OF {before}) AS rows_before,
               (SELECT COUNT(*) FROM sakila_silver.rental)                       AS rows_now,
               (SELECT COUNT(*) FROM (SELECT * FROM sakila_silver.rental
                                      EXCEPT
                                      SELECT * FROM sakila_silver.rental VERSION AS OF {before})) AS rows_new_or_changed,
               (SELECT COUNT(*) FROM sakila_silver.rental n
                JOIN sakila_silver.rental VERSION AS OF {before} o ON o.rental_id = n.rental_id
                WHERE o.return_date IS NULL AND n.return_date IS NOT NULL)        AS returned_since
    """).first()
    return row.asDict()


def rebuild_matches_merge(spark: SparkSession) -> dict[str, int]:
    """Rebuilding Silver rental from all of Bronze must give exactly the table the MERGE produced."""
    rebuilt = silver.latest_per_key(silver.rental_typed(), "rental_id", silver.RENTAL_COLUMNS)
    cols = "rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update"
    row = spark.sql(f"""
        SELECT (SELECT COUNT(*) FROM (SELECT {cols} FROM ({rebuilt}) EXCEPT SELECT {cols} FROM sakila_silver.rental))
                   AS only_in_rebuild,
               (SELECT COUNT(*) FROM (SELECT {cols} FROM sakila_silver.rental EXCEPT SELECT {cols} FROM ({rebuilt})))
                   AS only_in_merged""").first()
    return row.asDict()


def apply_month(spark: SparkSession, folder: str) -> dict:
    """Run every step of the incremental load and return what each one reported."""
    report: dict = {"landed": land(spark, folder), "staged": stage(spark)}
    report["merge_without_dedupe"] = try_merge_without_dedupe(spark)
    report["rental_merge"] = merge(spark, MERGE_RENTAL, "rental_march")
    report["payment_merge"] = merge(spark, MERGE_PAYMENT, "payment_march")
    report["history"] = merge_history(spark, "sakila_silver.rental")
    report["time_travel"] = time_travel(spark)
    report["rebuild_vs_merge"] = rebuild_matches_merge(spark)
    silver.build_country_region(spark)
    enriched.build(spark)
    gold.build(spark)
    platinum.build(spark)
    return report
