"""Row counts and reconciliation checks used by the CLI, the notebook and the tests."""
from __future__ import annotations

from pyspark.sql import SparkSession

# The main table at each layer, in the order data flows through them.
MAIN_TABLES = [
    ("1 Bronze", "sakila_bronze.rental_raw"),
    ("1 Bronze", "sakila_bronze.payment_raw"),
    ("2 Silver", "sakila_silver.rental"),
    ("2 Silver", "sakila_silver.payment"),
    ("3 Silver-Enriched", "sakila_silver_enriched.rental_enriched"),
    ("4 Gold", "sakila_gold.fact_rental"),
    ("4 Gold", "sakila_gold.dim_date"),
    ("4 Gold", "sakila_gold.revenue_by_category_month"),
]


def count(spark: SparkSession, table: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS n FROM {table}").first()["n"]


def layer_counts(spark: SparkSession) -> dict[str, int]:
    return {table: count(spark, table) for _, table in MAIN_TABLES}


def grain_check(spark: SparkSession) -> dict:
    """Silver-Enriched must have exactly one row per Silver rental, and its revenue must reconcile with payments."""
    row = spark.sql("""
        SELECT (SELECT COUNT(*) FROM sakila_silver.rental)                                AS silver_rentals,
               (SELECT COUNT(*) FROM sakila_silver_enriched.rental_enriched)              AS enriched_rentals,
               (SELECT SUM(amount_paid) FROM sakila_silver_enriched.rental_enriched)      AS enriched_revenue,
               (SELECT SUM(amount) FROM sakila_silver.payment)                            AS all_payments,
               (SELECT COALESCE(SUM(amount), 0) FROM sakila_silver.payment WHERE rental_id IS NULL) AS no_rental_payments
    """).first()
    return row.asDict()
