# Senior DE Assignment

This repository is organized to match the assignment hand-in structure and now separates Task 1 basic ingestion from Task 3 incremental ingestion.

## Layout

- `ingestion/task1_ingest.py` - Task 1, full raw ingestion with validation and quarantine handling.
- `ingestion/task3_incremental.py` - Task 3, incremental ingestion with watermark support.
- `entrypoint.py` - Unified wrapper for Databricks jobs with `--pipeline basic|incremental`.
- `ingestion/common.py` - Shared ingestion, validation, duplicate detection, and Delta write helpers.
- `ingestion/cli.py` - Shared argument definitions for all ingestion entrypoints.
- `sql/bronze_tables.sql` - Unity Catalog DDL for the precreated raw, quarantine, and watermark tables.
- `outputs/` - Submission artifacts such as quarantine samples and watermark snapshots.

## Task 1

Task 1 is the basic ingestion path. It:

- fetches all records from the chosen source
- validates each record against the transaction schema
- routes invalid records to quarantine with `error_reason`
- flags duplicates with `is_duplicate`
- writes valid raw records to the bronze table

Run it with the Task 1 script:

```bash
python3 ingestion/task1_ingest.py --source-type file --source-file transactions.csv
```

Or use the unified entrypoint:

```bash
python3 entrypoint.py --pipeline basic --source-type file --source-file transactions.csv
```

## Task 3

Task 3 is the incremental path. It extends Task 1 with:

- watermark lookup from the precreated watermark table
- late-data lookback handling
- API-side filtering for new records only

Run it with the Task 3 script:

```bash
python3 ingestion/task3_incremental.py --source-type file --source-file transactions.csv
```

Or use the unified entrypoint:

```bash
python3 entrypoint.py --pipeline incremental --source-type file --source-file transactions.csv
```

## DDL

Run `sql/bronze_tables.sql` before the ingestion scripts to pre-create the Unity Catalog tables under `workspace.bronze`.
The ingestion code assumes the target tables already exist and does not create schemas or tables at runtime.

## Notes

- Duplicate handling is implemented as flagging, not deduplication.
- The ingestion timestamp and watermark timestamps are written as UTC timestamps.
- `entrypoint.py` accepts the union of Task 1 and Task 3 arguments and switches behavior with `--pipeline basic|incremental`.
- Default `--source-file` and `--schema-file` values point at the repo root; relative overrides are also resolved against the repo root if needed.
- Default target tables are fully qualified as `workspace.bronze.<table>` unless you override `--catalog` or `--dataset`.
