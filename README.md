# Senior DE Assignment

This repository implements Task 1, Task 2, and Task 3 with a shared ingestion core where appropriate. Task 1 is a full basic load. Task 2 is a Spark SQL transformation layer on top of the validated bronze tables. Task 3 reuses the same validation and write path but adds watermark-based incremental filtering.

## Layout

- `ingestion/task1_ingest.py` - Task 1 entrypoint for full basic ingestion.
- `ingestion/task3_incremental.py` - Task 3 entrypoint for incremental ingestion.
- `entrypoint.py` - Unified Databricks entrypoint with `--pipeline basic|incremental|task2`.
- `sql/task2_aggregation.py` - Task 2 entrypoint for the gold aggregation model.
- `ingestion/common.py` - Shared normalization, validation, duplicate detection, quarantine routing, watermark handling, and Delta merge logic.
- `ingestion/cli.py` - Shared argument definitions for all ingestion entrypoints.
- `ddl/bronze_tables.sql` - Unity Catalog DDL for the precreated raw, quarantine, watermark, and control tables.
- `ddl/daily_account_summary.sql` - Spark SQL template for the Task 2 gold model.
- `outputs/` - Submission artifacts such as quarantine samples and watermark snapshots.

## Implementation Summary

- The code assumes the target tables already exist in Unity Catalog under `workspace.bronze`.
- The pipeline does not create schemas or tables at runtime.
- The shared implementation keeps Task 1 and Task 3 behavior aligned, so the only real difference is whether watermark filtering is enabled.
- A successful basic load also advances the watermark, so a later incremental run starts from the latest processed transaction date.
- The bronze transaction and quarantine tables are partitioned by `country_code` to support pruning on a common business dimension.
- Each successful run appends metadata to a run-log table so the submission has an auditable control trail.
- All managed tables are prefixed with `gibson_eletrolux_` to keep the namespace isolated and unambiguous.
- REST ingestion is modeled with `dlt`/dlthub so the source definition stays declarative: auth headers, pagination, retries, and incremental query parameters live in one place while the rest of the pipeline stays source-agnostic.
- `entrypoint.py` defaults to Task 1/basic ingestion when `--pipeline` is omitted.
- Task 2 is implemented as a single Spark SQL model rather than dbt because the assignment allows any transformation framework and the SQL file is easier to inspect and run directly in Databricks.

## Error Handling

The implementation addresses the assignment's error-handling expectations as follows:

- Offset-based pagination is handled by the dlt REST source configuration, and the fetch loop continues until Supabase returns an empty page.
- The REST request session includes authentication headers on every call and uses dlt's retry-capable HTTP client, which retries transient 429/5xx responses and connection/timeout failures with exponential backoff and `Retry-After` support.
- Each source record is validated against the schema and domain checks before persistence.
- Invalid records are routed to the quarantine table with a detailed `error_reason`; they are not silently dropped.
- Duplicate rows are detected by comparing the natural key and are marked with `is_duplicate = true` instead of being discarded.
- Valid records are persisted to bronze with the source fields preserved, plus UTC `ingestion_timestamp` and the duplicate metadata required by the assignment.

## Task 1: Basic Ingestion

Task 1 is the full ingest path. It reads every record from the source, normalizes each record to the final schema, validates the payload, quarantines invalid rows with an `error_reason`, and flags duplicates using a natural-key hash. Valid rows are written to `workspace.bronze.gibson_eletrolux_transactions_test`.

Why this design:

- It keeps the basic load deterministic and easy to rerun.
- Invalid records remain visible in quarantine instead of failing the whole batch.
- Duplicate rows are flagged rather than removed so the output preserves lineage.
- Duplicate detection is performed in Spark with a batch-order window and a lookup against the already ingested bronze table, which avoids driver-side duplicate state.
- The write path matches the final target schema, which lets Task 3 reuse the same code.
- The pipeline assumes the bronze tables are precreated, which avoids runtime schema creation and Hive Metastore fallback issues in Databricks serverless.

Validation approach:

- Each record is validated against the supplied JSON schema in `transactions_schema.json`.
- The pipeline also applies explicit domain checks that are easier to express in code than in JSON schema, such as ISO datetime parsing for `transaction_date` and country code membership checks.
- `amount` is normalized as a `Decimal` before validation, and the schema is loaded with decimal-aware numeric literals, so the schema's `multipleOf: 0.01` rule is evaluated exactly instead of through floating-point rounding.
- Validation failures do not stop the batch. Instead, the row is routed to quarantine with a combined `error_reason` so the dataset stays auditable.

Duplicate-handling strategy:

- Duplicates are identified with a natural key built from the business columns that define a transaction.
- The natural key is hashed to make comparisons stable and efficient.
- The code checks both the current batch and the already ingested bronze table so reruns and cross-batch duplicates are both caught.
- Duplicate rows are still written with `is_duplicate = true` rather than being dropped, because the assignment asks for traceability rather than loss of records.

Run it with the Task 1 script:

```bash
python3 ingestion/task1_ingest.py --source-type file --source-file transactions.csv
```

Or use the unified entrypoint:

```bash
python3 entrypoint.py --source-type file --source-file transactions.csv
```

## Task 2: Daily Aggregations

Task 2 is implemented as a Spark SQL gold model that reads the validated bronze tables and materializes `workspace.gold.daily_account_summary`.

Why this design:

- It satisfies the assignment without introducing dbt-specific project structure or conventions that are unnecessary for a single transformation.
- The logic stays in one SQL file, which makes the transformation easy to inspect, test, and rerun in Databricks.
- The model is idempotent because it uses `CREATE OR REPLACE TABLE` and computes `updated_at` from source data instead of `CURRENT_TIMESTAMP()`.
- Partitioning by `transaction_date` matches the primary access pattern for daily summaries and keeps date-range reads efficient.
- The query depends only on the bronze transaction table and the quarantine table, so it stays isolated from ingestion concerns.

Transformation rules:

- Only records with `status = 'completed'` are included.
- Quarantined records are excluded with a `LEFT ANTI JOIN` on `transaction_id`.
- Duplicate bronze rows are excluded with `COALESCE(is_duplicate, false) = false` so the summary does not double count the same transaction.
- `transaction_date` is truncated to a UTC calendar date.
- `total_debit_amount` and `total_credit_amount` are aggregated separately.
- `net_amount = total_credit_amount - total_debit_amount`, which matches the assignment text.
- `transaction_count` counts included transactions.
- `distinct_merchants` counts distinct `merchant_name` values.
- `top_category` is the `merchant_category` with the highest total debit spend for the day, with alphabetical tie-breaking for deterministic output. If there is no debit spend that day, the field stays null.
- `currencies` is stored as a sorted comma-separated list of distinct currencies.
- `updated_at` is the latest `ingestion_timestamp` among the contributing rows, which keeps reruns stable.

Run the model with:

```bash
python3 sql/task2_aggregation.py
```

Or use the unified entrypoint:

```bash
python3 entrypoint.py --pipeline task2
```

To produce the assignment extract for January through March 2024, query the gold table after the model finishes:

```sql
SELECT *
FROM workspace.gold.daily_account_summary
WHERE transaction_date BETWEEN DATE'2024-01-01' AND DATE'2024-03-31'
ORDER BY account_id, transaction_date;
```

If you need a file artifact for submission, export that result to `outputs/daily_summary_output.csv`.

## Task 3: Incremental Ingestion

Task 3 extends the same core pipeline with watermark-based filtering. Before ingesting, it reads the last successful transaction date from `workspace.bronze.gibson_eletrolux_ingestion_watermark_test`. The default `--lookback-hours` is `0`, so the incremental run only processes records newer than the saved watermark. For REST sources, the lower bound is pushed down to the API. For file sources, the filter is applied locally. After a successful load, the watermark is advanced to the latest processed transaction date.

Why this design:

- It lets the basic load and the incremental load share the same validation, quarantine, and write path.
- A zero-hour lookback makes the common "run Task 1, then run Task 3" flow safe and predictable.
- Increasing `--lookback-hours` remains available if late-arriving data needs a replay window.
- Using the same watermark table for both file and REST sources keeps the behavior consistent across source types.
- The REST path uses `dlt`/dlthub so the API integration stays declarative. Pagination, headers, and the `transaction_date` filter live in `supabase_transactions_source`, which keeps source-specific concerns isolated from validation, duplicate handling, and Delta writes. That makes the REST source easier to extend or replace later without changing the pipeline core.
- Task 3 inherits the same validation and duplicate logic as Task 1, so incremental behavior only changes which records are selected, not how each record is judged.

Run it with the Task 3 script:

```bash
python3 ingestion/task3_incremental.py --source-type file --source-file transactions.csv
```

Or use the unified entrypoint:

```bash
python3 entrypoint.py --pipeline incremental --source-type file --source-file transactions.csv
```

## Algorithm

### Task 1

```text
1. Read all source rows.
2. Normalize each row to the final output shape.
3. Validate the row against the JSON schema and extra domain checks.
4. If validation fails, send the row to quarantine with an error reason.
5. Build a natural key and hash for duplicate detection.
6. Compare against rows already in bronze and rows seen in the current batch.
7. Mark duplicates with is_duplicate = true.
8. Write valid rows to workspace.bronze.gibson_eletrolux_transactions_test.
9. Write invalid rows to workspace.bronze.gibson_eletrolux_quarantine_test.
10. Advance the watermark to the latest successful transaction date.
11. Append run metadata to workspace.bronze.gibson_eletrolux_ingestion_run_log_test.
```

### Task 3

```text
1. Read the saved watermark from workspace.bronze.gibson_eletrolux_ingestion_watermark_test.
2. Compute the start boundary using --lookback-hours (default 0).
3. Filter the source so only rows newer than the boundary are processed.
4. Run the same normalization, validation, quarantine, and duplicate steps as Task 1.
5. Write valid and quarantine rows to the bronze tables.
6. Advance the watermark after a successful load.
7. Append run metadata to workspace.bronze.gibson_eletrolux_ingestion_run_log_test.
```

## DDL

If you need to recreate the tables, use `ddl/bronze_tables.sql`. It creates the four Delta tables under `workspace.bronze` with the `gibson_eletrolux_` prefix. The ingestion code assumes those tables already exist and does not create them at runtime.
If you already created the old unpartitioned tables, drop and recreate them to apply the new partitioning and the run-log table.

## Notes

- Duplicate handling is implemented as flagging, not deduplication.
- The ingestion timestamp and watermark timestamps are written as UTC timestamps.
- Default target tables are fully qualified as `workspace.bronze.<table>` unless you override `--catalog` or `--dataset`.
- The run-log table captures batch metadata such as counts, watermark value, and status.
- Default `--source-file` and `--schema-file` values point at the repo root; relative overrides are also resolved against the repo root if needed.
- `ddl/bronze_tables.sql` is for table setup or recreation, not for normal pipeline runs.
- `ddl/daily_account_summary.sql` is the Spark SQL model used by Task 2.
- The Task 2 output table lives in `workspace.gold.daily_account_summary` by default.
- `sql/` contains only the Task 2 runner script.

## Unit Tests

The repository includes a small pytest suite that checks the parts of the implementation most likely to regress:

- CLI defaults for Task 1, Task 3, and the unified entrypoint
- schema loading and record normalization/validation
- file source CSV/Excel reading and repo-root resolution
- watermark lookback logic in hours
- duplicate-detection pipeline structure
- REST source configuration and incremental boundary pushdown
- Delta merge and control-table write side effects
- Task 2 SQL rendering and Spark SQL execution path

Run the suite locally with:

```bash
pytest -q
```
