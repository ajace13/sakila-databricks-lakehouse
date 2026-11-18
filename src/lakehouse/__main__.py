"""Run the whole lakehouse locally and print what each layer did.

    python -m lakehouse                       # uses data/sakila and data/march_2006, writes to ./spark-warehouse
    python -m lakehouse --warehouse /tmp/wh
    python -m lakehouse --chart docs/images/local_run.png
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from . import checks
from .pipeline import run_all
from .session import local_spark

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Sakila lakehouse locally on Spark + Delta Lake.")
    parser.add_argument("--sakila", type=Path, default=ROOT / "data" / "sakila")
    parser.add_argument("--month", type=Path, default=ROOT / "data" / "march_2006")
    parser.add_argument("--warehouse", type=Path, default=ROOT / "spark-warehouse",
                        help="emptied first, so every run starts from nothing")
    parser.add_argument("--chart", type=Path, help="also save the three summary charts as a PNG here (needs matplotlib)")
    args = parser.parse_args()

    shutil.rmtree(args.warehouse, ignore_errors=True)
    spark = local_spark(args.warehouse)
    report = run_all(spark, args.sakila.resolve().as_posix(), args.month.resolve().as_posix())
    inc = report["incremental"]

    print("\nBronze: rows landed per file")
    for table, n in {**report["initial"]["landed"], **inc["landed"]}.items():
        print(f"  {table:<26} {n:>7,}")

    g = report["initial"]["grain"]
    print(f"\nSilver-Enriched grain: {g['silver_rentals']:,} Silver rentals -> {g['enriched_rentals']:,} enriched rows; "
          f"revenue {g['enriched_revenue']:,} of {g['all_payments']:,} paid")

    s = inc["staged"]
    print(f"\nMarch batch: {s['rows']:,} rental rows, {s['distinct_rentals']:,} distinct, "
          f"{s['already_in_silver']:,} already in Silver")
    print(f"MERGE without dedupe: {inc['merge_without_dedupe'] or 'succeeded (unexpected)'}")
    for name in ("rental_merge", "payment_merge"):
        m = inc[name]
        print(f"{name.replace('_', ' ').capitalize()} after dedupe: {m['num_affected_rows']:,} affected, "
              f"{m['num_updated_rows']:,} updated, {m['num_inserted_rows']:,} inserted")

    t = inc["time_travel"]
    print(f"\nTime travel on sakila_silver.rental: version {t['version_before']} had {t['rows_before']:,} rows, "
          f"now {t['rows_now']:,}; {t['rows_new_or_changed']:,} new or changed; "
          f"{t['returned_since']:,} rentals returned since")
    r = inc["rebuild_vs_merge"]
    print(f"Rebuild from Bronze vs MERGE result: {r['only_in_rebuild']} rows only in the rebuild, "
          f"{r['only_in_merged']} only in the merged table")

    print("\nMain table at each layer, before and after the month")
    before, after = report["initial"]["counts"], report["after"]["counts"]
    for layer, table in checks.MAIN_TABLES:
        print(f"  {layer:<18} {table:<42} {before[table]:>7,} {after[table]:>7,} {after[table] - before[table]:>+7,}")

    print("\nsakila_platinum.monthly_revenue")
    for row in spark.sql("SELECT * FROM sakila_platinum.monthly_revenue ORDER BY month").collect():
        print(f"  {row['month']}  {row['rentals']:>6,} rentals  {row['revenue']:>10,} revenue  "
              f"{row['late_pct']:>5}% of {row['returned']:,} returned were late")
    print("\nRevenue by region")
    for row in spark.sql("""SELECT region, SUM(revenue) AS revenue FROM sakila_platinum.revenue_by_region_category
                            GROUP BY region ORDER BY revenue DESC""").collect():
        print(f"  {row['region']:<10} {row['revenue']:>10,}")

    if args.chart:
        from .charts import save
        save(spark, before, after, args.chart, new_month="2006-03")
        print(f"\nCharts saved to {args.chart}")
    spark.stop()


if __name__ == "__main__":
    main()
