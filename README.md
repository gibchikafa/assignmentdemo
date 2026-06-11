# Senior DE Assignment

This repository implements Task 1 and Task 3 with a shared ingestion core. Task 1 is a full basic load. Task 3 reuses the same validation and write path but adds watermark-based incremental filtering.

## Layout

- `ingestion/task1_ingest.py` - Task 1 entrypoint for full basic ingestion.
- `ingestion/task3_incremental.py` - Task 3 entrypoint for incremental ingestion.
- `entrypoint.py` - Unified Databricks entrypoint with `--pipeline basic|incremental`.
- `ingestion/common.py` - Shared normalization, validation, duplicate detection, quarantine routing, watermark handling, and Delta merge logic.
- `ingestion/cli.py` - Shared argument definitions for all ingestion entrypoints.
- `sql/bronze_tables.sql` - Unity Catalog DDL for the precreated raw, quarantine, watermark, and control tables.
- `outputs/` - Submission artifacts such as quarantine samples and watermark snapshots.

## Implementation Summary

- The code assumes the target tables already exist in Unity Catalog under `workspace.bronze`.
- The pipeline does not create schemas or tables at runtime.
- The shared implementation keeps Task 1 and Task 3 behavior aligned, so the only real difference is whether watermark filtering is enabled.
- A successful basic load also advances the watermark, so a later incremental run starts from the latest processed transaction date.
- The bronze transaction and quarantine tables are partitioned by `country_code` to support pruning on a common business dimension.
- Each successful run appends metadata to a run-log table so the submission has an auditable control trail.
- All managed tables are prefixed with `gibson_eletrolux_` to keep the namespace isolated and unambiguous.
- `entrypoint.py` defaults to Task 1/basic ingestion when `--pipeline` is omitted.

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

## Task 3: Incremental Ingestion

Task 3 extends the same core pipeline with watermark-based filtering. Before ingesting, it reads the last successful transaction date from `workspace.bronze.gibson_eletrolux_ingestion_watermark_test`. The default `--lookback-hours` is `0`, so the incremental run only processes records newer than the saved watermark. For REST sources, the lower bound is pushed down to the API. For file sources, the filter is applied locally. After a successful load, the watermark is advanced to the latest processed transaction date.

Why this design:

- It lets the basic load and the incremental load share the same validation, quarantine, and write path.
- A zero-hour lookback makes the common "run Task 1, then run Task 3" flow safe and predictable.
- Increasing `--lookback-hours` remains available if late-arriving data needs a replay window.
- Using the same watermark table for both file and REST sources keeps the behavior consistent across source types.
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

If you need to recreate the tables, use `sql/bronze_tables.sql`. It creates the four Delta tables under `workspace.bronze` with the `gibson_eletrolux_` prefix. The ingestion code assumes those tables already exist and does not create them at runtime.
If you already created the old unpartitioned tables, drop and recreate them to apply the new partitioning and the run-log table.

## Notes

- Duplicate handling is implemented as flagging, not deduplication.
- The ingestion timestamp and watermark timestamps are written as UTC timestamps.
- Default target tables are fully qualified as `workspace.bronze.<table>` unless you override `--catalog` or `--dataset`.
- The run-log table captures batch metadata such as counts, watermark value, and status.
- Default `--source-file` and `--schema-file` values point at the repo root; relative overrides are also resolved against the repo root if needed.
- `sql/bronze_tables.sql` is for table setup or recreation, not for normal pipeline runs.
