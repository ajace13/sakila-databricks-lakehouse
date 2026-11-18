"""Generate a synthetic month of Sakila data (March 2006) to apply incrementally.

The month is built to exercise every case an incremental load has to handle:

* 2,400 new rentals in March 2006, each with one payment
* 150 February 2006 rentals that were still out and are now returned (updates to rows Silver already has)
* 25 rental rows and 15 payment rows that the "source" sends twice, as exact copies
  (10 of the duplicated rentals are updates, so two source rows match one existing target row)
* a new reference file that maps each country to a region

Everything is seeded, so the same files come out on every run:

    python -m lakehouse.march_generator                 # writes data/march_2006/
    python -m lakehouse.march_generator --out some/dir
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SAKILA_DIR = ROOT / "data" / "sakila"
OUT_DIR = ROOT / "data" / "march_2006"

SEED = 20060301
NEW_RENTALS = 2400
RETURNED_FEBRUARY = 150
DUPLICATE_UPDATES = 10      # re-sent copies of the February returns
DUPLICATE_NEW = 15          # re-sent copies of new rentals
DUPLICATE_PAYMENTS = 15
OPEN_SHARE = 0.06           # share of new rentals still out when the month is extracted
LATE_FEE_PER_DAY = Decimal("1.00")

MONTH_START = datetime(2006, 3, 1)
EXTRACTED_AT = datetime(2006, 4, 1, 3, 0, 0)   # the source system's export time, used as last_update
TS = "%Y-%m-%d %H:%M:%S"
NULL = r"\N"                                  # how the Sakila CSVs write NULL

RENTAL_COLUMNS = ["rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"]
PAYMENT_COLUMNS = ["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date", "last_update"]

# Continents, following the UN geoscheme. Every Sakila country must be listed exactly once.
REGIONS = {
    "Africa": ["Algeria", "Angola", "Cameroon", "Chad", "Congo, The Democratic Republic of the", "Egypt", "Ethiopia",
               "Gambia", "Kenya", "Madagascar", "Malawi", "Morocco", "Mozambique", "Nigeria", "Réunion", "Senegal",
               "South Africa", "Sudan", "Tanzania", "Tunisia", "Zambia"],
    "Americas": ["Anguilla", "Argentina", "Bolivia", "Brazil", "Canada", "Chile", "Colombia", "Dominican Republic",
                 "Ecuador", "French Guiana", "Greenland", "Mexico", "Paraguay", "Peru", "Puerto Rico",
                 "Saint Vincent and the Grenadines", "United States", "Venezuela", "Virgin Islands, U.S."],
    "Asia": ["Afghanistan", "Armenia", "Azerbaijan", "Bahrain", "Bangladesh", "Brunei", "Cambodia", "China", "Hong Kong",
             "India", "Indonesia", "Iran", "Iraq", "Israel", "Japan", "Kazakstan", "Kuwait", "Malaysia", "Myanmar",
             "Nepal", "North Korea", "Oman", "Pakistan", "Philippines", "Saudi Arabia", "South Korea", "Sri Lanka",
             "Taiwan", "Thailand", "Turkey", "Turkmenistan", "United Arab Emirates", "Vietnam", "Yemen"],
    "Europe": ["Austria", "Belarus", "Bulgaria", "Czech Republic", "Estonia", "Faroe Islands", "Finland", "France",
               "Germany", "Greece", "Holy See (Vatican City State)", "Hungary", "Italy", "Latvia", "Liechtenstein",
               "Lithuania", "Moldova", "Netherlands", "Poland", "Romania", "Russian Federation", "Slovakia", "Spain",
               "Sweden", "Switzerland", "Ukraine", "United Kingdom", "Yugoslavia"],
    "Oceania": ["American Samoa", "Australia", "French Polynesia", "Nauru", "New Zealand", "Tonga", "Tuvalu"],
}


def _read(name: str, sakila_dir: Path) -> list[dict[str, str]]:
    with open(sakila_dir / f"{name}.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _random_time(rng: random.Random, start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=rng.randrange(int((end - start).total_seconds())))


def generate(sakila_dir: Path = SAKILA_DIR, out_dir: Path = OUT_DIR) -> dict[str, int]:
    rng = random.Random(SEED)
    rentals = _read("rental", sakila_dir)
    payments = _read("payment", sakila_dir)
    inventory = _read("inventory", sakila_dir)
    films = {f["film_id"]: f for f in _read("film", sakila_dir)}
    customers = [c["customer_id"] for c in _read("customer", sakila_dir)]
    countries = _read("country", sakila_dir)

    # 1. February rentals that are still out get returned in March.
    still_out = [r for r in rentals if r["return_date"] == NULL and r["rental_date"].startswith("2006-02")]
    returned = []
    for r in sorted(rng.sample(still_out, RETURNED_FEBRUARY), key=lambda r: int(r["rental_id"])):
        back = _random_time(rng, MONTH_START, datetime(2006, 3, 21))
        returned.append({**r, "return_date": back.strftime(TS), "last_update": EXTRACTED_AT.strftime(TS)})

    # 2. New March rentals, numbered after the highest existing id, in time order.
    next_rental = max(int(r["rental_id"]) for r in rentals) + 1
    next_payment = max(int(p["payment_id"]) for p in payments) + 1
    starts = sorted(_random_time(rng, MONTH_START, datetime(2006, 4, 1)) for _ in range(NEW_RENTALS))
    new_rentals, new_payments = [], []
    for i, start in enumerate(starts):
        item = rng.choice(inventory)
        film = films[item["film_id"]]
        customer = rng.choice(customers)
        staff = item["store_id"]                      # one staff member per store in Sakila
        days_out = rng.randint(1, 10)
        back = start + timedelta(days=days_out, seconds=rng.randrange(86400))
        is_open = rng.random() < OPEN_SHARE or back >= EXTRACTED_AT
        rental_id = str(next_rental + i)
        new_rentals.append({"rental_id": rental_id, "rental_date": start.strftime(TS), "inventory_id": item["inventory_id"],
                            "customer_id": customer, "return_date": NULL if is_open else back.strftime(TS),
                            "staff_id": staff, "last_update": EXTRACTED_AT.strftime(TS)})
        # The rental rate covers rental_duration days; each extra day costs a late fee.
        amount = Decimal(film["rental_rate"])
        if not is_open:
            overdue = (back.date() - start.date()).days - int(film["rental_duration"])
            amount += LATE_FEE_PER_DAY * max(overdue, 0)
        new_payments.append({"payment_id": str(next_payment + i), "customer_id": customer, "staff_id": staff,
                             "rental_id": rental_id, "amount": f"{amount:.2f}", "payment_date": start.strftime(TS),
                             "last_update": EXTRACTED_AT.strftime(TS)})

    # 3. The source re-sends some rows. They are exact copies.
    dup_rentals = rng.sample(returned, DUPLICATE_UPDATES) + rng.sample(new_rentals, DUPLICATE_NEW)
    dup_payments = rng.sample(new_payments, DUPLICATE_PAYMENTS)
    rental_rows = sorted(returned + new_rentals + dup_rentals, key=lambda r: int(r["rental_id"]))
    payment_rows = sorted(new_payments + dup_payments, key=lambda p: int(p["payment_id"]))

    # 4. Reference data: country -> region.
    region_of = {country: region for region, names in REGIONS.items() for country in names}
    names = [c["country"] for c in countries]
    missing, extra = set(names) - set(region_of), set(region_of) - set(names)
    if missing or extra:
        raise ValueError(f"region map out of sync: missing {sorted(missing)}, unknown {sorted(extra)}")
    region_rows = [{"country_id": c["country_id"], "country": c["country"], "region": region_of[c["country"]]}
                   for c in countries]

    out_dir.mkdir(parents=True, exist_ok=True)
    _write(out_dir / "rental_2006_03.csv", RENTAL_COLUMNS, rental_rows)
    _write(out_dir / "payment_2006_03.csv", PAYMENT_COLUMNS, payment_rows)
    _write(out_dir / "country_region.csv", ["country_id", "country", "region"], region_rows)
    return {"rental_2006_03.csv": len(rental_rows), "payment_2006_03.csv": len(payment_rows),
            "country_region.csv": len(region_rows)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sakila", type=Path, default=SAKILA_DIR, help="folder with the Sakila CSVs")
    parser.add_argument("--out", type=Path, default=OUT_DIR, help="where to write the March files")
    args = parser.parse_args()
    for name, n in generate(args.sakila, args.out).items():
        print(f"{name:<22} {n:>6,} rows")


if __name__ == "__main__":
    main()
