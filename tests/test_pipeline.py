"""Run the whole lakehouse once on local Spark + Delta Lake and check every layer."""
import csv
import shutil
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SAKILA = (ROOT / "data" / "sakila").as_posix()
MARCH = (ROOT / "data" / "march_2006").as_posix()

if shutil.which("java") is None:
    pytest.skip("Spark needs Java", allow_module_level=True)

from lakehouse import bronze, checks  # noqa: E402
from lakehouse.pipeline import run_all  # noqa: E402
from lakehouse.session import local_spark  # noqa: E402


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    spark = local_spark(tmp_path_factory.mktemp("warehouse"))
    report = run_all(spark, SAKILA, MARCH)
    yield spark, report
    spark.stop()


def scalar(spark, sql):
    return spark.sql(sql).first()[0]


def march_payment_total() -> Decimal:
    with open(ROOT / "data" / "march_2006" / "payment_2006_03.csv", newline="") as f:
        distinct = {tuple(r.values()) for r in csv.DictReader(f)}
    return sum(Decimal(p[4]) for p in distinct)


def test_bronze_keeps_every_row_as_text(run):
    spark, report = run
    assert report["initial"]["landed"]["rental_raw"] == 16044
    assert report["incremental"]["landed"] == {"rental_2006_03.csv": 2575, "payment_2006_03.csv": 2415,
                                               "country_region.csv": 109}
    types = dict(spark.table("sakila_bronze.rental_raw").dtypes)
    assert types["rental_id"] == types["return_date"] == "string"
    # the source's NULL marker is kept as it arrived
    assert scalar(spark, r"SELECT COUNT(*) FROM sakila_bronze.rental_raw WHERE return_date = '\\N'") == 183 + 559


def test_landing_a_file_twice_is_skipped(run):
    spark, _ = run
    before = checks.count(spark, "sakila_bronze.rental_raw")
    assert bronze.load_file(spark, f"{SAKILA}/rental.csv", "rental_raw") == 0
    assert checks.count(spark, "sakila_bronze.rental_raw") == before


def test_silver_enriched_keeps_one_row_per_rental(run):
    _, report = run
    for grain in (report["initial"]["grain"], report["after"]["grain"]):
        assert grain["silver_rentals"] == grain["enriched_rentals"]
        assert grain["enriched_revenue"] == grain["all_payments"] - grain["no_rental_payments"]
    assert report["initial"]["grain"]["enriched_revenue"] == Decimal("67406.56")


def test_merge_without_dedupe_fails_and_changes_nothing(run):
    _, report = run
    inc = report["incremental"]
    assert inc["merge_without_dedupe"] == "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"
    assert inc["staged"] == {"rows": 2575, "distinct_rentals": 2550, "already_in_silver": 150}
    # the failed MERGE left no version behind: CREATE, then the one successful MERGE
    assert [h["operation"] for h in reversed(inc["history"])] == ["CREATE OR REPLACE TABLE AS SELECT", "MERGE"]


def test_deduped_merge_updates_and_inserts(run):
    _, report = run
    inc = report["incremental"]
    assert inc["rental_merge"] == {"num_affected_rows": 2550, "num_updated_rows": 150,
                                   "num_deleted_rows": 0, "num_inserted_rows": 2400}
    assert inc["payment_merge"] == {"num_affected_rows": 2400, "num_updated_rows": 0,
                                    "num_deleted_rows": 0, "num_inserted_rows": 2400}
    metrics = inc["history"][0]["metrics"]
    assert metrics["numTargetRowsInserted"] == "2400" and metrics["numTargetRowsUpdated"] == "150"


def test_time_travel_shows_what_the_merge_changed(run):
    _, report = run
    assert report["incremental"]["time_travel"] == {"version_before": 0, "rows_before": 16044, "rows_now": 18444,
                                                    "rows_new_or_changed": 2550, "returned_since": 150}


def test_rebuilding_silver_from_bronze_matches_the_merge(run):
    _, report = run
    assert report["incremental"]["rebuild_vs_merge"] == {"only_in_rebuild": 0, "only_in_merged": 0}


def test_row_counts_by_layer(run):
    _, report = run
    before, after = report["initial"]["counts"], report["after"]["counts"]
    assert before == {
        "sakila_bronze.rental_raw": 16044, "sakila_bronze.payment_raw": 16044,
        "sakila_silver.rental": 16044, "sakila_silver.payment": 16044,
        "sakila_silver_enriched.rental_enriched": 16044, "sakila_gold.fact_rental": 16044,
        "sakila_gold.dim_date": 267, "sakila_gold.revenue_by_category_month": 80,
    }
    assert after == {
        "sakila_bronze.rental_raw": 18619, "sakila_bronze.payment_raw": 18459,
        "sakila_silver.rental": 18444, "sakila_silver.payment": 18444,
        "sakila_silver_enriched.rental_enriched": 18444, "sakila_gold.fact_rental": 18444,
        "sakila_gold.dim_date": 312, "sakila_gold.revenue_by_category_month": 96,
    }


def test_gold_revenue_reconciles_after_the_month(run):
    spark, _ = run
    assert scalar(spark, "SELECT SUM(amount_paid) FROM sakila_gold.fact_rental") == \
        Decimal("67406.56") + march_payment_total()
    assert scalar(spark, "SELECT COUNT(*) FROM sakila_gold.dim_film") == 1000
    assert scalar(spark, "SELECT COUNT(*) FROM sakila_gold.dim_customer WHERE region IS NULL") == 0


def test_platinum_monthly_revenue(run):
    spark, _ = run
    rows = {r["month"]: r for r in spark.sql("SELECT * FROM sakila_platinum.monthly_revenue").collect()}
    # the 2005 months are untouched by the new data and match the Databricks run to the cent
    assert {m: str(rows[m]["revenue"]) for m in ("2005-05", "2005-06", "2005-07", "2005-08")} == \
        {"2005-05": "4823.44", "2005-06": "9629.89", "2005-07": "28368.91", "2005-08": "24070.14"}
    assert rows["2006-03"]["rentals"] == 2400 and rows["2006-03"]["revenue"] == march_payment_total()
    # the only February rentals ever returned came back weeks later, so every one of them is late
    assert rows["2006-02"]["late_pct"] == 100.0 and rows["2006-02"]["returned"] == 150


def test_platinum_revenue_by_region_covers_all_revenue(run):
    spark, _ = run
    total = scalar(spark, "SELECT SUM(revenue) FROM sakila_platinum.revenue_by_region_category")
    assert total == scalar(spark, "SELECT SUM(amount_paid) FROM sakila_gold.fact_rental")
    regions = [r[0] for r in spark.sql("""SELECT region FROM sakila_platinum.revenue_by_region_category
                                          GROUP BY region ORDER BY SUM(revenue) DESC""").collect()]
    assert regions == ["Asia", "Americas", "Europe", "Africa", "Oceania"]
