# ingestion/ingest_transactions_databricks.py

import argparse
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dlt
import pandas as pd
import pycountry
from dateutil.parser import isoparse
from dlt.sources.rest_api import rest_api_source
from jsonschema import Draft7Validator


API_BASE_URL = "https://fgbjekjqnbmtkmeewexb.supabase.co/rest/v1/"

NATURAL_KEY_COLUMNS = [
    "account_id",
    "transaction_date",
    "amount",
    "currency",
    "transaction_type",
    "merchant_name",
    "merchant_category",
    "status",
    "country_code",
]

VALID_COUNTRY_CODES = {country.alpha_2 for country in pycountry.countries}


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--source-type", choices=["file", "rest"], default="file")
    parser.add_argument("--source-file", default="transactions.csv")
    parser.add_argument("--schema-file", default="transactions_schema.json")
    parser.add_argument("--api-key", default=os.getenv("SUPABASE_API_KEY"))

    parser.add_argument("--dataset", default="bronze")
    parser.add_argument("--transactions-table", default="transactions_test")
    parser.add_argument("--quarantine-table", default="quarantine_test")
    parser.add_argument("--watermark-table", default="ingestion_watermark_test")

    parser.add_argument("--source-name", default="transactions")
    parser.add_argument("--lookback-days", type=int, default=2)
    parser.add_argument("--use-watermark", action="store_true")

    return parser.parse_args()


def load_validator(schema_file: str) -> Draft7Validator:
    with open(schema_file, "r") as f:
        return Draft7Validator(json.load(f))


def normalize_record(record: dict) -> dict:
    record = dict(record)
    record.pop("id", None)

    tx_date = record.get("transaction_date")
    if tx_date is not None:
        tx_date = str(tx_date).strip()

        if tx_date.endswith("+00:00"):
            tx_date = tx_date.replace("+00:00", "Z")

        if " " in tx_date and "T" not in tx_date:
            tx_date = tx_date.replace(" ", "T")

        if not tx_date.endswith("Z"):
            tx_date = tx_date + "Z"

        record["transaction_date"] = tx_date

    amount = record.get("amount")
    if amount not in (None, ""):
        try:
            record["amount"] = float(amount)
        except Exception:
            pass

    return record


def validate_record(record: dict, validator: Draft7Validator) -> list[str]:
    errors = []

    for error in validator.iter_errors(record):
        field = ".".join(str(x) for x in error.path) or "record"
        errors.append(f"{field}: {error.message}")

    tx_date = record.get("transaction_date")
    if tx_date:
        try:
            isoparse(tx_date)
        except Exception:
            errors.append(f"transaction_date: invalid calendar datetime {tx_date}")

    country_code = record.get("country_code")
    if country_code and country_code not in VALID_COUNTRY_CODES:
        errors.append(f"country_code: invalid assigned ISO code {country_code}")

    return errors


def natural_key(record: dict) -> str:
    return "|".join(str(record.get(col)) for col in NATURAL_KEY_COLUMNS)


def natural_key_hash(record: dict) -> str:
    return hashlib.sha256(natural_key(record).encode("utf-8")).hexdigest()


def table_exists(full_table_name: str) -> bool:
    try:
        spark.table(full_table_name)
        return True
    except Exception:
        return False


def load_existing_natural_keys(transactions_table: str) -> dict[str, set[str]]:
    """
    Returns:
      {
        natural_key_hash: {transaction_id_1, transaction_id_2, ...}
      }

    This lets us detect duplicates across multiple job runs.
    If the same transaction_id appears again, that is not considered a duplicate;
    it is an idempotent reprocess handled by MERGE.
    """
    if not table_exists(transactions_table):
        return {}

    rows = spark.sql(f"""
        SELECT transaction_id, natural_key_hash
        FROM {transactions_table}
        WHERE natural_key_hash IS NOT NULL
    """).collect()

    existing = {}

    for row in rows:
        existing.setdefault(row["natural_key_hash"], set()).add(row["transaction_id"])

    return existing


def get_watermark(args):
    full_table = f"{args.dataset}.{args.watermark_table}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            source_name STRING,
            last_successful_transaction_date TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)

    rows = spark.sql(f"""
        SELECT last_successful_transaction_date
        FROM {full_table}
        WHERE source_name = '{args.source_name}'
        ORDER BY updated_at DESC
        LIMIT 1
    """).collect()

    if not rows:
        return None

    watermark = rows[0]["last_successful_transaction_date"]

    if watermark is not None and watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)

    return watermark


def watermark_start_value(args):
    if not args.use_watermark:
        return None

    watermark = get_watermark(args)

    if watermark is None:
        return None

    start = watermark - timedelta(days=args.lookback_days)
    return start.isoformat().replace("+00:00", "Z")


def update_watermark(args, valid_records: list[dict]):
    if not args.use_watermark:
        return

    if not valid_records:
        print("No valid records, watermark not updated")
        return

    full_table = f"{args.dataset}.{args.watermark_table}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            source_name STRING,
            last_successful_transaction_date TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)

    max_tx_date = max(isoparse(r["transaction_date"]) for r in valid_records)

    update_df = spark.createDataFrame([
        {
            "source_name": args.source_name,
            "last_successful_transaction_date": max_tx_date,
            "updated_at": datetime.now(timezone.utc),
        }
    ])

    update_df.createOrReplaceTempView("watermark_update_view")

    spark.sql(f"""
        MERGE INTO {full_table} target
        USING watermark_update_view source
        ON target.source_name = source.source_name
        WHEN MATCHED THEN UPDATE SET
            target.last_successful_transaction_date = source.last_successful_transaction_date,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)


def supabase_transactions_source(api_key: str, start_value: str | None):
    if not api_key:
        raise ValueError("Pass --api-key or set SUPABASE_API_KEY")

    params = {"order": "transaction_date.asc"}

    if start_value:
        params["transaction_date"] = f"gte.{start_value}"

    return rest_api_source(
        {
            "client": {
                "base_url": API_BASE_URL,
                "headers": {
                    "apikey": api_key,
                    "Authorization": f"Bearer {api_key}",
                },
            },
            "resources": [
                {
                    "name": "transactions",
                    "endpoint": {
                        "path": "transactions",
                        "params": params,
                        "paginator": {
                            "type": "offset",
                            "limit": 100,
                            "offset": 0,
                            "limit_param": "limit",
                            "offset_param": "offset",
                            "stop_after_empty_page": True,
                        },
                    },
                }
            ],
        },
        name="supabase_transactions",
    )


def iter_file_records(source_file: str):
    path = Path(source_file)

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str)
    elif path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path, dtype=str)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")

    df = df.where(pd.notnull(df), None)

    for record in df.to_dict(orient="records"):
        yield record


def iter_source_records(args):
    start_value = watermark_start_value(args)

    if args.source_type == "file":
        for record in iter_file_records(args.source_file):
            record = normalize_record(record)

            if start_value:
                tx_date = isoparse(record["transaction_date"])
                if tx_date < isoparse(start_value):
                    continue

            yield record

    elif args.source_type == "rest":
        source = supabase_transactions_source(args.api_key, start_value)
        yield from source.resources["transactions"]


def split_records(args, validator):
    transactions_table = f"{args.dataset}.{args.transactions_table}"
    existing_natural_keys = load_existing_natural_keys(transactions_table)

    valid_records = []
    quarantine_records = []

    seen_in_current_batch = {}
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    for raw_record in iter_source_records(args):
        record = normalize_record(raw_record)

        errors = validate_record(record, validator)

        tx_id = record.get("transaction_id")
        nk = natural_key(record)
        nk_hash = natural_key_hash(record)

        existing_tx_ids = existing_natural_keys.get(nk_hash, set())
        current_tx_ids = seen_in_current_batch.get(nk_hash, set())

        is_duplicate_existing = bool(existing_tx_ids and tx_id not in existing_tx_ids)
        is_duplicate_current = bool(current_tx_ids and tx_id not in current_tx_ids)

        is_duplicate = is_duplicate_existing or is_duplicate_current

        seen_in_current_batch.setdefault(nk_hash, set()).add(tx_id)

        record["ingestion_timestamp"] = ingestion_timestamp
        record["natural_key"] = nk
        record["natural_key_hash"] = nk_hash
        record["is_duplicate"] = is_duplicate

        if errors:
            record["error_reason"] = "; ".join(errors)
            quarantine_records.append(record)
        else:
            # Duplicates are kept in bronze, flagged with is_duplicate=true.
            # dbt staging can exclude them later with WHERE is_duplicate = false.
            valid_records.append(record)

    return valid_records, quarantine_records


def write_delta_merge(records: list[dict], full_table_name: str, merge_key: str):
    if not records:
        print(f"No records to write to {full_table_name}")
        return

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    df = spark.createDataFrame(records)
    temp_view = full_table_name.replace(".", "_") + "_view"
    df.createOrReplaceTempView(temp_view)

    if not table_exists(full_table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
    else:
        spark.sql(f"""
            MERGE INTO {full_table_name} target
            USING {temp_view} source
            ON target.{merge_key} = source.{merge_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    print(f"Wrote/merged {len(records)} records into {full_table_name}")


def main():
    args = parse_args()
    validator = load_validator(args.schema_file)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.dataset}")

    valid_records, quarantine_records = split_records(args, validator)

    transactions_table = f"{args.dataset}.{args.transactions_table}"
    quarantine_table = f"{args.dataset}.{args.quarantine_table}"

    write_delta_merge(valid_records, transactions_table, "transaction_id")
    write_delta_merge(quarantine_records, quarantine_table, "transaction_id")

    update_watermark(args, valid_records)

    duplicate_count = sum(r["is_duplicate"] for r in valid_records)

    print("=========================================")
    print("Transaction Ingestion Summary")
    print("=========================================")
    print(f"Source type        : {args.source_type}")
    print(f"Input records      : {len(valid_records) + len(quarantine_records)}")
    print(f"Valid records      : {len(valid_records)}")
    print(f"Quarantined records: {len(quarantine_records)}")
    print(f"Duplicates flagged : {duplicate_count}")
    print("")
    print("Destination:")
    print(f"  Valid      : {transactions_table}")
    print(f"  Quarantine : {quarantine_table}")
    print(f"  Watermark  : {args.dataset}.{args.watermark_table}")
    print("=========================================")


if __name__ == "__main__":
    main()
