# Sakila lakehouse on Databricks *(work in progress)*

A batch medallion pipeline on Databricks: Sakila's CSVs land as Bronze Delta tables, get cleaned and typed into Silver, joined into Silver-Enriched, and aggregated into Gold. A later month of data is applied incrementally with `MERGE`, and Delta's history and time travel show exactly what changed.

> Scaffold only. It gets filled in once the Databricks work is finished and its outputs are exported. Plan: [docs/plan.md](docs/plan.md).

## Planned layout

```
notebooks/   one notebook per layer, written as my own code (not the course notebook)
src/         the same pipeline as PySpark + delta-spark, runnable locally
tests/       row counts by layer on a small sample, run in CI
docs/        incremental_merge.md (why the first MERGE failed, the fix, the time-travel diff), findings, screenshots
```

## Credits

Built for ADSP 31011 Cloud-Native Data Engineering at the University of Chicago, Autumn 2026. The Sakila sample database is © Oracle Corporation, New BSD license.
