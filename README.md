# Senior DE Assignment

This repository implements Task 1 and Task 3 with a shared ingestion core. Task 1 is a full basic load. Task 3 reuses the same validation and write path but adds watermark-based incremental filtering.

## Layout

- `ingestion/task1_ingest.py` - Task 1 entrypoint for full basic ingestion.
- `ingestion/task3_incremental.py` - Task 3 entrypoint for incremental ingestion.
- `entrypoint.py` - Unified Databricks entrypoint with `--pipeline basic|incremental`.
- `ingestion/common.py` - Shared normalization, validation, duplicate detection, quarantine routing, watermark handling, and Delta merge logic.
- `ingestion/cli.py` - Shared argument definitions for all ingestion entrypoints.
- `sql/bronze_tables.sql` - Unity Catalog DDL for the precreated raw, quarantine, and watermark tables.
- `outputs/` - Submission artifacts such as quarantine samples and watermark snapshots.

## Implementation Summary

- The code assumes the target tables already exist in Unity Catalog under `workspace.bronze`.
- The pipeline does not create schemas or tables at runtime.
- The shared implementation keeps Task 1 and Task 3 behavior aligned, so the only real difference is whether watermark filtering is enabled.
- A successful basic load also advances the watermark, so a later incremental run starts from the latest processed transaction date.
- `entrypoint.py` defaults to Task 1/basic ingestion when `--pipeline` is omitted.

## Task 1: Basic Ingestion

Task 1 is the full ingest path. It reads every record from the source, normalizes each record to the final schema, validates the payload, quarantines invalid rows with an `error_reason`, and flags duplicates using a natural-key hash. Valid rows are written to the bronze transactions table.

Why this design:

- It keeps the basic load deterministic and easy to rerun.
- Invalid records remain visible in quarantine instead of failing the whole batch.
- Duplicate rows are flagged rather than removed so the output preserves lineage.
- The write path matches the final target schema, which lets Task 3 reuse the same code.
- The pipeline assumes the bronze tables are precreated, which avoids runtime schema creation and Hive Metastore fallback issues in Databricks serverless.

Run it with the Task 1 script:

```bash
python3 ingestion/task1_ingest.py --source-type file --source-file transactions.csv
```

Or use the unified entrypoint:

```bash
python3 entrypoint.py --source-type file --source-file transactions.csv
```

## Task 3: Incremental Ingestion

Task 3 extends the same core pipeline with watermark-based filtering. Before ingesting, it reads the last successful transaction date from `workspace.bronze.ingestion_watermark_test`. The default `--lookback-days` is `0`, so the incremental run only processes records newer than the saved watermark. For REST sources, the lower bound is pushed down to the API. For file sources, the filter is applied locally. After a successful load, the watermark is advanced to the latest processed transaction date.

Why this design:

- It lets the basic load and the incremental load share the same validation, quarantine, and write path.
- A zero-day lookback makes the common "run Task 1, then run Task 3" flow safe and predictable.
- Increasing `--lookback-days` remains available if late-arriving data needs a replay window.
- Using the same watermark table for both file and REST sources keeps the behavior consistent across source types.

Run it with the Task 3 script:

```bash
python3 ingestion/task3_incremental.py --source-type file --source-file transactions.csv
```

Or use the unified entrypoint:

```bash
python3 entrypoint.py --pipeline incremental --source-type file --source-file transactions.csv
```

## DDL

If you need to recreate the tables, use `sql/bronze_tables.sql`. It creates the three Delta tables under `workspace.bronze`. The ingestion code assumes those tables already exist and does not create them at runtime.

## Notes

- Duplicate handling is implemented as flagging, not deduplication.
- The ingestion timestamp and watermark timestamps are written as UTC timestamps.
- Default target tables are fully qualified as `workspace.bronze.<table>` unless you override `--catalog` or `--dataset`.
- Default `--source-file` and `--schema-file` values point at the repo root; relative overrides are also resolved against the repo root if needed.
- `sql/bronze_tables.sql` is for table setup or recreation, not for normal pipeline runs.
