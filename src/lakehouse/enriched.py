"""Silver-Enriched: one wide row per rental.

Most questions need a rental together with its film, category, customer, location and payment. The joins happen
once, here. Two rules keep the grain at exactly one row per rental:

* payments are summed per rental before the join, so a rental with several payments can't be repeated
* is_late stays NULL while a rental is still out, because it is neither late nor on time yet

The region column comes from a reference file that arrives later. Until it does, the column exists and is NULL,
so the table's schema doesn't change under the people reading it.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def build(spark: SparkSession) -> None:
    has_region = spark.catalog.tableExists("sakila_silver.country_region")
    region = "cr.region" if has_region else "CAST(NULL AS STRING)"
    region_join = ("LEFT JOIN sakila_silver.country_region cr ON cr.country_id = co.country_id"
                   if has_region else "")
    spark.sql(f"""
        CREATE OR REPLACE TABLE sakila_silver_enriched.rental_enriched AS
        WITH paid AS (
            SELECT rental_id, SUM(amount) AS amount_paid
            FROM sakila_silver.payment
            WHERE rental_id IS NOT NULL
            GROUP BY rental_id
        )
        SELECT r.rental_id, r.rental_date, r.return_date,
               DATE_FORMAT(r.rental_date, 'yyyy-MM')                              AS rental_month,
               r.customer_id, cu.first_name, cu.last_name,
               ci.city, co.country, {region}                                      AS region,
               i.store_id, f.film_id, f.title, cat.category, f.rating, f.rental_duration,
               p.amount_paid,
               DATEDIFF(CAST(r.return_date AS DATE), CAST(r.rental_date AS DATE)) AS rental_days,
               CASE WHEN r.return_date IS NULL THEN NULL
                    ELSE DATEDIFF(CAST(r.return_date AS DATE), CAST(r.rental_date AS DATE)) > f.rental_duration
               END                                                                AS is_late
        FROM sakila_silver.rental r
        JOIN sakila_silver.inventory i      ON i.inventory_id = r.inventory_id
        JOIN sakila_silver.film f           ON f.film_id = i.film_id
        JOIN sakila_silver.film_category fc ON fc.film_id = f.film_id
        JOIN sakila_silver.category cat     ON cat.category_id = fc.category_id
        JOIN sakila_silver.customer cu      ON cu.customer_id = r.customer_id
        JOIN sakila_silver.address a        ON a.address_id = cu.address_id
        JOIN sakila_silver.city ci          ON ci.city_id = a.city_id
        JOIN sakila_silver.country co       ON co.country_id = ci.country_id
        {region_join}
        LEFT JOIN paid p                    ON p.rental_id = r.rental_id""")
