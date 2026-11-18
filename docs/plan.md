# Build plan

1. Finish the Databricks work; export the notebook as HTML with outputs, plus screenshots of the charts and the Delta history.
2. Rewrite the pipeline in my own words, one notebook or script per layer (Bronze, Silver, Silver-Enriched, Gold).
3. Replace the provided March 2006 files with a small script that generates a synthetic month of rentals and payments, so every published file is mine. The Sakila CSVs themselves are fine to include.
4. Add a local PySpark + delta-spark version and a GitHub Actions job that runs it on a small sample and checks row counts per layer.
5. Write docs/incremental_merge.md: why the first MERGE failed, the fix, and the time-travel diff, plus findings (for example, where the missing rental rows went).
6. Before publishing: no workspace URLs, tokens or personal IDs anywhere (the starter notebook had a student ID cell).
