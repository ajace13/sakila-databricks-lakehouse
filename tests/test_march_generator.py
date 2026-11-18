"""The synthetic March 2006 files: shape, determinism, and the cases the incremental load must handle."""
import csv
from collections import Counter
from decimal import Decimal
from pathlib import Path

from lakehouse import march_generator as gen

ROOT = Path(__file__).resolve().parents[1]
MARCH = ROOT / "data" / "march_2006"
SAKILA = ROOT / "data" / "sakila"


def read(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_regenerating_gives_the_committed_files(tmp_path):
    counts = gen.generate(SAKILA, tmp_path)
    assert counts == {"rental_2006_03.csv": 2575, "payment_2006_03.csv": 2415, "country_region.csv": 109}
    for name in counts:
        assert (tmp_path / name).read_bytes() == (MARCH / name).read_bytes(), name


def test_rental_file_has_new_rows_updates_and_exact_duplicates():
    existing = {r["rental_id"]: r for r in read(SAKILA / "rental.csv")}
    rows = read(MARCH / "rental_2006_03.csv")
    copies = Counter(tuple(r.values()) for r in rows)
    ids = Counter(r["rental_id"] for r in rows)
    assert len(ids) == 2550
    # every repeated id is an exact copy of the same row
    assert sum(n - 1 for n in copies.values()) == 25 == sum(n - 1 for n in ids.values())
    updates = {i for i in ids if i in existing}
    new = {i for i in ids if i not in existing}
    assert len(updates) == 150 and len(new) == 2400
    assert min(int(i) for i in new) == max(int(i) for i in existing) + 1
    # 10 of the duplicated ids are updates: two source rows for one target row
    assert sum(1 for i, n in ids.items() if n > 1 and i in updates) == 10
    for r in rows:
        if r["rental_id"] in updates:
            old = existing[r["rental_id"]]
            assert old["return_date"] == r"\N" and old["rental_date"].startswith("2006-02")
            assert r["return_date"].startswith("2006-03")
            assert {k: v for k, v in r.items() if k not in ("return_date", "last_update")} == \
                   {k: v for k, v in old.items() if k not in ("return_date", "last_update")}
        else:
            assert r["rental_date"].startswith("2006-03")


def test_payment_file_has_one_payment_per_new_rental_plus_copies():
    rentals = {r["rental_id"] for r in read(MARCH / "rental_2006_03.csv")}
    existing = {r["rental_id"] for r in read(SAKILA / "rental.csv")}
    rows = read(MARCH / "payment_2006_03.csv")
    distinct = {tuple(r.values()) for r in rows}
    assert len(rows) == 2415 and len(distinct) == 2400
    assert {p[3] for p in distinct} == rentals - existing
    assert all(Decimal(p[4]) > 0 for p in distinct)


def test_every_country_has_exactly_one_region():
    countries = read(SAKILA / "country.csv")
    regions = read(MARCH / "country_region.csv")
    assert [r["country_id"] for r in regions] == [c["country_id"] for c in countries]
    assert {r["region"] for r in regions} == {"Africa", "Americas", "Asia", "Europe", "Oceania"}
    assert sum(len(v) for v in gen.REGIONS.values()) == len(countries)
