"""Gold: a star schema and one business aggregate.

One fact table of rentals (one row per rental) and four dimensions to slice it by: customer, film, store and day.
dim_date covers every day from the first rental to the last, including days with no rentals, so charts over time
show the gaps instead of hiding them.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .sql import run


def build(spark: SparkSession) -> None:
    has_region = spark.catalog.tableExists("sakila_silver.country_region")
    region = "cr.region" if has_region else "CAST(NULL AS STRING)"
    region_join = ("LEFT JOIN sakila_silver.country_region cr ON cr.country_id = co.country_id"
                   if has_region else "")
    run(spark, f"""
        CREATE OR REPLACE TABLE sakila_gold.fact_rental AS
        SELECT rental_id, customer_id, film_id, store_id,
               CAST(rental_date AS DATE) AS date_key,
               rental_date, return_date, rental_month,
               amount_paid, rental_days, is_late
        FROM sakila_silver_enriched.rental_enriched;

        CREATE OR REPLACE TABLE sakila_gold.dim_customer AS
        SELECT cu.customer_id, cu.first_name, cu.last_name, cu.email, cu.active, cu.store_id,
               ci.city, co.country, {region} AS region
        FROM sakila_silver.customer cu
        JOIN sakila_silver.address a  ON a.address_id = cu.address_id
        JOIN sakila_silver.city ci    ON ci.city_id = a.city_id
        JOIN sakila_silver.country co ON co.country_id = ci.country_id
        {region_join};

        CREATE OR REPLACE TABLE sakila_gold.dim_film AS
        SELECT f.film_id, f.title, cat.category, f.rating, f.rental_rate, f.rental_duration, f.length_minutes
        FROM sakila_silver.film f
        JOIN sakila_silver.film_category fc ON fc.film_id = f.film_id
        JOIN sakila_silver.category cat     ON cat.category_id = fc.category_id;

        CREATE OR REPLACE TABLE sakila_gold.dim_store AS
        SELECT s.store_id, ci.city AS store_city, co.country AS store_country
        FROM sakila_silver.store s
        JOIN sakila_silver.address a  ON a.address_id = s.address_id
        JOIN sakila_silver.city ci    ON ci.city_id = a.city_id
        JOIN sakila_silver.country co ON co.country_id = ci.country_id;

        CREATE OR REPLACE TABLE sakila_gold.dim_date AS
        SELECT d AS date_key, YEAR(d) AS year, MONTH(d) AS month,
               DATE_FORMAT(d, 'yyyy-MM') AS year_month, DATE_FORMAT(d, 'EEEE') AS day_name
        FROM (SELECT EXPLODE(SEQUENCE(MIN(CAST(rental_date AS DATE)), MAX(CAST(rental_date AS DATE)),
                                      INTERVAL 1 DAY)) AS d
              FROM sakila_silver.rental);

        CREATE OR REPLACE TABLE sakila_gold.revenue_by_category_month AS
        SELECT f.rental_month, d.category, COUNT(*) AS rentals, SUM(f.amount_paid) AS revenue
        FROM sakila_gold.fact_rental f
        JOIN sakila_gold.dim_film d ON d.film_id = f.film_id
        GROUP BY f.rental_month, d.category;
    """)
