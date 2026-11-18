"""Three charts from the finished tables: rows by layer, revenue by month, revenue by region."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

LAYERS = [("sakila_bronze.rental_raw", "Bronze\nrental_raw"), ("sakila_silver.rental", "Silver\nrental"),
          ("sakila_silver_enriched.rental_enriched", "Silver-Enriched\nrental_enriched"),
          ("sakila_gold.fact_rental", "Gold\nfact_rental")]


def save(spark: SparkSession, before: dict[str, int], after: dict[str, int], path: Path, new_month: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    monthly = spark.sql("SELECT month, revenue FROM sakila_platinum.monthly_revenue ORDER BY month").collect()
    region = spark.sql("""SELECT region, SUM(revenue) AS revenue FROM sakila_platinum.revenue_by_region_category
                          GROUP BY region ORDER BY revenue""").collect()

    fig, ax = plt.subplots(1, 3, figsize=(18, 5))
    x = range(len(LAYERS))
    b = [before[t] for t, _ in LAYERS]
    a = [after[t] for t, _ in LAYERS]
    ax[0].bar([i - 0.2 for i in x], b, 0.4, label=f"before {new_month}", color="#9e9e9e")
    ax[0].bar([i + 0.2 for i in x], a, 0.4, label=f"after {new_month}", color="#800000")
    for i, (bi, ai) in enumerate(zip(b, a)):
        ax[0].text(i + 0.2, ai, f"+{ai - bi:,}", ha="center", va="bottom", fontsize=9)
    ax[0].set_ylim(0, max(a) * 1.15)
    ax[0].set_xticks(list(x))
    ax[0].set_xticklabels([label for _, label in LAYERS], fontsize=8)
    ax[0].set_title("Rental rows by layer")
    ax[0].legend(loc="lower right")

    months = [r["month"] for r in monthly]
    ax[1].bar(months, [float(r["revenue"]) for r in monthly],
              color=["#800000" if m == new_month else "#c9a3a3" for m in months])
    ax[1].set_title(f"Revenue by rental month ({new_month} = the new data)")
    ax[1].tick_params(axis="x", labelsize=8)

    ax[2].barh([r["region"] for r in region], [float(r["revenue"]) for r in region], color="#800000")
    ax[2].set_title("Revenue by customer region")
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=100)
    plt.close(fig)
