# Applying a new month with MERGE

A month after the initial load, the source system sends more data: new March 2006 rentals and payments, February rentals that have since come back, and a new reference file. This page walks through how that month reaches every layer, why the first MERGE fails, and how Delta's history and time travel prove what changed.

The numbers are from the local run (`python -m lakehouse`) on the synthetic month in [data/march_2006/](../data/march_2006/). The Databricks run on the original files gave the same rental counts and MERGE results at every step. Its payment counts are 5 higher because the original extract has 5 payments with no rental; see [databricks_run.md](databricks_run.md).

## What arrives

| File | Rows | What's in it |
|---|---:|---|
| `rental_2006_03.csv` | 2,575 | 2,400 new March rentals, 150 February rentals now returned, 25 rows sent twice |
| `payment_2006_03.csv` | 2,415 | one payment for each new rental, 15 rows sent twice |
| `country_region.csv` | 109 | a new reference file: each country's region |

The files come from [src/lakehouse/march_generator.py](../src/lakehouse/march_generator.py). It's seeded, so the files are identical on every run, and a test regenerates them and compares byte for byte. Ten of the 25 re-sent rentals are February returns. That means two source rows point at one rental Silver already has, which is what breaks the first MERGE.

## Step 1: Bronze appends, and changes nothing else

The new files go through the same loader as the initial extract. Bronze is append-only, and `_source_file` tells the batches apart:

| `_source_file` | rows in `sakila_bronze.rental_raw` |
|---|---:|
| rental.csv | 16,044 |
| rental_2006_03.csv | 2,575 |

The loader skips a file name that has already landed. In the original notebook, running the Bronze cell twice silently doubled every table. The trade-off: a corrected file re-sent under the same name is also skipped, so a correction has to arrive under a new name. For a batch feed, I'd rather have that than silent doubling.

## Step 2: MERGE without deduping fails

The March rows are typed the Silver way (the same `rental_typed()` SQL that builds Silver) but not deduped, and merged into `sakila_silver.rental` on `rental_id`:

```sql
MERGE INTO sakila_silver.rental AS t
USING rental_march_raw AS s
ON t.rental_id = s.rental_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Delta refuses:

```
[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows
matched and attempted to modify the same target row in the Delta table in possibly conflicting ways.
```

The batch has 2,575 rows but only 2,550 distinct rentals. For the 10 duplicated February returns, two source rows match the same target row. Delta can't know which one should win, so it fails the whole statement instead of guessing. Inserts don't raise this error, which is why the 15 duplicated *new* rentals aren't what triggers it. They would have been inserted twice if the matched rows hadn't stopped the statement.

The failure is atomic. `DESCRIBE HISTORY` shows no version for it, and the table is exactly as it was. The test suite checks this too.

## Step 3: dedupe with Silver's rule, then MERGE

The fix is the dedupe Silver already uses: keep the latest row per key.

```sql
ROW_NUMBER() OVER (PARTITION BY rental_id ORDER BY last_update DESC, _ingest_ts DESC) = 1
```

| | affected | updated | inserted |
|---|---:|---:|---:|
| rental MERGE | 2,550 | 150 | 2,400 |
| payment MERGE | 2,400 | 0 | 2,400 |

Silver rental goes from 16,044 to 18,444, and Silver payment from 16,044 to 18,444.

**The tie-breaker matters.** The original notebook ordered by `_ingest_ts` alone. Every row in one file lands with the same `_ingest_ts`, so within a batch that ordering picks a row arbitrarily. It only gave the right answer because the re-sent rows were exact copies. This version orders by the source's `last_update` first, so if a re-sent row carries a newer change, the newer change wins. `_ingest_ts` then breaks ties between batches.

## Step 4: history and time travel

Every Delta write is a version. `DESCRIBE HISTORY sakila_silver.rental` after the month:

| version | operation | key metrics |
|---:|---|---|
| 1 | MERGE | numSourceRows 2,550 · numTargetRowsInserted 2,400 · numTargetRowsUpdated 150 |
| 0 | CREATE OR REPLACE TABLE AS SELECT | numOutputRows 16,044 |

Time travel reads the version before the MERGE and compares it with now:

```sql
SELECT (SELECT COUNT(*) FROM sakila_silver.rental VERSION AS OF 0) AS rows_before,   -- 16,044
       (SELECT COUNT(*) FROM sakila_silver.rental)                 AS rows_now,      -- 18,444
       (SELECT COUNT(*) FROM (SELECT * FROM sakila_silver.rental
                              EXCEPT
                              SELECT * FROM sakila_silver.rental VERSION AS OF 0)) AS rows_new_or_changed  -- 2,550
```

A join of the two versions finds the 150 rentals that had no `return_date` before and have one now.

## Step 5: is the MERGE result right? Rebuild from Bronze and compare

Because Bronze holds every row that ever arrived, Silver can also be rebuilt from scratch with the same dedupe. The pipeline does that as a query (without replacing the table) and compares it with the merged table using `EXCEPT` in both directions. The result is 0 rows only in the rebuild and 0 rows only in the merged table. The incremental path and the full rebuild agree, so the MERGE didn't lose or invent anything.

## Step 6: the layers above

`country_region` lands in Silver, and Silver-Enriched, Gold and Platinum are rebuilt with `CREATE OR REPLACE`. The `region` column has been in Silver-Enriched and `dim_customer` from the start, NULL until the reference file arrives, so their schema doesn't change under the people who read them. After the month, no rental is missing a region.

| layer | table | before | after | change |
|---|---|---:|---:|---:|
| Bronze | rental_raw | 16,044 | 18,619 | +2,575 |
| Bronze | payment_raw | 16,044 | 18,459 | +2,415 |
| Silver | rental | 16,044 | 18,444 | +2,400 |
| Silver | payment | 16,044 | 18,444 | +2,400 |
| Silver-Enriched | rental_enriched | 16,044 | 18,444 | +2,400 |
| Gold | fact_rental | 16,044 | 18,444 | +2,400 |
| Gold | dim_date | 267 | 312 | +45 |
| Gold | revenue_by_category_month | 80 | 96 | +16 |

Bronze grows by everything that arrived, duplicates included. Every layer above grows by the 2,400 new rentals only, because the 150 returns are updates and the 25 copies are gone. `dim_date` gains the 45 days from 15 February to 31 March, and the category aggregate gains one row for each of the 16 categories in the new month.

## A number that looks wrong but isn't

After the month, `sakila_platinum.monthly_revenue` says 100% of February 2006 rentals were late. That's correct. Those 182 rentals all started on 14 February, and the only ones that have come back are the 150 returned in March, weeks past any film's rental duration. The 32 still out aren't counted, because they're neither late nor on time yet. A percentage alone hides that, so the view also has a `returned` column (150 here) to show what the percentage is based on.
