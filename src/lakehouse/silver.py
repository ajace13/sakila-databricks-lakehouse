"""Silver: clean, type and dedupe, once.

Each Silver table holds one trusted row per key with real types. Strings become INT, TIMESTAMP and exact DECIMAL,
the source's NULL markers ('' and \\N) become real NULLs, names are conformed, and duplicates are removed by keeping
the most recently updated row for each key.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .sql import run


def nullable(column: str) -> str:
    """A source column where both '' and the literal \\N mean NULL."""
    return rf"NULLIF(NULLIF(TRIM({column}), ''), '\\N')"


RENTAL_COLUMNS = ["rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id",
                  "last_update", "_ingest_ts"]
PAYMENT_COLUMNS = ["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date",
                   "last_update", "_ingest_ts"]


def rental_typed(where: str = "") -> str:
    return f"""
        SELECT CAST(rental_id AS INT)                         AS rental_id,
               CAST(rental_date AS TIMESTAMP)                 AS rental_date,
               CAST(inventory_id AS INT)                      AS inventory_id,
               CAST(customer_id AS INT)                       AS customer_id,
               CAST({nullable('return_date')} AS TIMESTAMP)   AS return_date,
               CAST(staff_id AS INT)                          AS staff_id,
               CAST(last_update AS TIMESTAMP)                 AS last_update,
               _ingest_ts
        FROM sakila_bronze.rental_raw {where}"""


def payment_typed(where: str = "") -> str:
    return f"""
        SELECT CAST(payment_id AS INT)                        AS payment_id,
               CAST(customer_id AS INT)                       AS customer_id,
               CAST(staff_id AS INT)                          AS staff_id,
               CAST({nullable('rental_id')} AS INT)           AS rental_id,
               CAST(amount AS DECIMAL(5,2))                   AS amount,
               CAST(payment_date AS TIMESTAMP)                AS payment_date,
               CAST(last_update AS TIMESTAMP)                 AS last_update,
               _ingest_ts
        FROM sakila_bronze.payment_raw {where}"""


def latest_per_key(typed_sql: str, key: str, columns: list[str]) -> str:
    """Keep one row per key: the most recently updated, then the most recently landed.

    Rows re-sent inside one file share the same _ingest_ts, so last_update is what can tell them apart.
    When it can't either, the rows are exact copies and it doesn't matter which one survives.
    """
    cols = ", ".join(columns)
    return f"""
        SELECT {cols} FROM (
            SELECT t.*, ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY last_update DESC, _ingest_ts DESC) AS _rn
            FROM ({typed_sql}) t
        ) WHERE _rn = 1"""


def build(spark: SparkSession) -> None:
    run(spark, f"""
        CREATE OR REPLACE TABLE sakila_silver.rental AS
        {latest_per_key(rental_typed(), "rental_id", RENTAL_COLUMNS)};

        CREATE OR REPLACE TABLE sakila_silver.payment AS
        {latest_per_key(payment_typed(), "payment_id", PAYMENT_COLUMNS)};

        CREATE OR REPLACE TABLE sakila_silver.customer AS
        SELECT customer_id, store_id, first_name, last_name, email, address_id, active, create_date, _ingest_ts FROM (
            SELECT CAST(customer_id AS INT)       AS customer_id,
                   CAST(store_id AS INT)          AS store_id,
                   INITCAP(TRIM(first_name))      AS first_name,
                   INITCAP(TRIM(last_name))       AS last_name,
                   LOWER(TRIM(email))             AS email,
                   CAST(address_id AS INT)        AS address_id,
                   CAST(active AS INT) = 1        AS active,
                   CAST(create_date AS TIMESTAMP) AS create_date,
                   _ingest_ts,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_update DESC, _ingest_ts DESC) AS _rn
            FROM sakila_bronze.customer_raw
        ) WHERE _rn = 1;

        CREATE OR REPLACE TABLE sakila_silver.film AS
        SELECT film_id, title, release_year, rental_duration, rental_rate, length_minutes, replacement_cost, rating, _ingest_ts
        FROM (
            SELECT CAST(film_id AS INT)                   AS film_id,
                   TRIM(title)                            AS title,
                   CAST(release_year AS INT)              AS release_year,
                   CAST(rental_duration AS INT)           AS rental_duration,
                   CAST(rental_rate AS DECIMAL(4,2))      AS rental_rate,
                   CAST(length AS INT)                    AS length_minutes,
                   CAST(replacement_cost AS DECIMAL(5,2)) AS replacement_cost,
                   TRIM(rating)                           AS rating,
                   _ingest_ts,
                   ROW_NUMBER() OVER (PARTITION BY film_id ORDER BY last_update DESC, _ingest_ts DESC) AS _rn
            FROM sakila_bronze.film_raw
        ) WHERE _rn = 1;

        CREATE OR REPLACE TABLE sakila_silver.inventory AS
        SELECT DISTINCT CAST(inventory_id AS INT) AS inventory_id, CAST(film_id AS INT) AS film_id,
                        CAST(store_id AS INT) AS store_id
        FROM sakila_bronze.inventory_raw;

        CREATE OR REPLACE TABLE sakila_silver.category AS
        SELECT DISTINCT CAST(category_id AS INT) AS category_id, TRIM(name) AS category
        FROM sakila_bronze.category_raw;

        CREATE OR REPLACE TABLE sakila_silver.film_category AS
        SELECT DISTINCT CAST(film_id AS INT) AS film_id, CAST(category_id AS INT) AS category_id
        FROM sakila_bronze.film_category_raw;

        CREATE OR REPLACE TABLE sakila_silver.store AS
        SELECT DISTINCT CAST(store_id AS INT) AS store_id, CAST(address_id AS INT) AS address_id
        FROM sakila_bronze.store_raw;

        CREATE OR REPLACE TABLE sakila_silver.address AS
        SELECT DISTINCT CAST(address_id AS INT) AS address_id, TRIM(address) AS address,
                        {nullable('district')} AS district, CAST(city_id AS INT) AS city_id
        FROM sakila_bronze.address_raw;

        CREATE OR REPLACE TABLE sakila_silver.city AS
        SELECT DISTINCT CAST(city_id AS INT) AS city_id, TRIM(city) AS city, CAST(country_id AS INT) AS country_id
        FROM sakila_bronze.city_raw;

        CREATE OR REPLACE TABLE sakila_silver.country AS
        SELECT DISTINCT CAST(country_id AS INT) AS country_id, TRIM(country) AS country
        FROM sakila_bronze.country_raw;
    """)


def build_country_region(spark: SparkSession) -> None:
    """The reference file that arrives with the March extract."""
    spark.sql("""
        CREATE OR REPLACE TABLE sakila_silver.country_region AS
        SELECT DISTINCT CAST(country_id AS INT) AS country_id, TRIM(country) AS country, TRIM(region) AS region
        FROM sakila_bronze.country_region_raw""")
