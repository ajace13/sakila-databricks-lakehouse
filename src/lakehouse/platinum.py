"""Platinum: documented views that a dashboard reads directly.

A dashboard should read one simple view with its business rules written down, not rebuild joins itself.
One row of each view is one point on a chart.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .sql import run


def build(spark: SparkSession) -> None:
    run(spark, """
        CREATE OR REPLACE VIEW sakila_platinum.monthly_revenue (
            month    COMMENT 'Rental month, yyyy-MM',
            rentals  COMMENT 'Rentals that started in the month',
            revenue  COMMENT 'Payments for those rentals, counted in the month the rental started',
            returned COMMENT 'Rentals from the month that have come back, the base of late_pct',
            late_pct COMMENT 'Late rentals as a percentage of returned rentals (rentals still out are excluded)'
        ) COMMENT 'One row per rental month. Reads sakila_gold.fact_rental.'
        AS SELECT rental_month AS month, COUNT(*) AS rentals, SUM(amount_paid) AS revenue,
                  COUNT(return_date) AS returned,
                  ROUND(100 * COUNT_IF(is_late) / NULLIF(COUNT(return_date), 0), 1) AS late_pct
           FROM sakila_gold.fact_rental
           GROUP BY rental_month;

        CREATE OR REPLACE VIEW sakila_platinum.revenue_by_region_category (
            region   COMMENT 'Customer region, from the country_region reference file',
            category COMMENT 'Film category',
            rentals  COMMENT 'Rentals by customers in the region, of films in the category',
            revenue  COMMENT 'Payments for those rentals'
        ) COMMENT 'One row per region and category. Reads sakila_gold.fact_rental, dim_customer and dim_film.'
        AS SELECT c.region, d.category, COUNT(*) AS rentals, SUM(f.amount_paid) AS revenue
           FROM sakila_gold.fact_rental f
           JOIN sakila_gold.dim_customer c ON c.customer_id = f.customer_id
           JOIN sakila_gold.dim_film d     ON d.film_id = f.film_id
           GROUP BY c.region, d.category;
    """)
