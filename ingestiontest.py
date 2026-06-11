# ingestion/ingest_transactions_databricks.py

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dateutil.parser import isoparse
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

VALID_COUNTRY_CODES = {
    "US", "GB", "DE", "FR", "SE", "CH", "JP", "AU", "CA",
    "NL", "ES", "IT", "NO", "DK", "FI", "IE", "BE", "AT",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-type", choices=["file"], default="file")
    parser.add_argument("--source-file", required=True)
    parser.add_argument("--schema-file", required=True)

    parser.add_argument("--dataset", default="bronze")
    parser.add_argument("--transactions-table", default="transactions_test")
    parser.add_argument("--quarantine-table", default="quarantine_test")

    return parser.parse_args()


def load_validator(schema_file: str) -> Draft7Validator:
    with open(schema_file, "r") as f:
        schema = json.load(f)
    return Draft7Validator(schema)


def normalize_record(record: dict) -> dict:
    record = dict(record)
    record.pop("id", None)

    tx_date = record.get("transaction_date")

    if tx_date is not None:
        tx_date = str(tx_date).strip()

        if tx_date.endswith("+00:00"):
            tx_date = tx_date.replace("+00:00", "Z")

        # Handles pandas datetime-like values such as:
        # 2024-01-01 12:30:00
        if " " in tx_date and "T" not in tx_date:
            tx_date = tx_date.replace(" ", "T")

        # If timezone missing, assume UTC for fallback file.
        if not tx_date.endswith("Z"):
            tx_date = tx_date + "Z"

        record["transaction_date"] = tx_date

    amount = record.get("amount")
    if amount is not None and amount != "":
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


def natural_key_hash(record: dict) -> str:
    raw_key = "|".join(str(record.get(col)) for col in NATURAL_KEY_COLUMNS)
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


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


def fetch_and_split_records(args, validator: Draft7Validator):
    valid_records = []
    quarantine_records = []

    seen_natural_keys = set()
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    for raw_record in iter_file_records(args.source_file):
        record = normalize_record(raw_record)

        record["ingestion_timestamp"] = ingestion_timestamp

        nk_hash = natural_key_hash(record)
        record["natural_key_hash"] = nk_hash

        record["is_duplicate"] = nk_hash in seen_natural_keys
        seen_natural_keys.add(nk_hash)

        errors = validate_record(record, validator)

        if errors:
            record["error_reason"] = "; ".join(errors)
            quarantine_records.append(record)
        else:
            valid_records.append(record)

    return valid_records, quarantine_records


def write_records_to_delta(records: list[dict], full_table_name: str):
    if not records:
        print(f"No records to write to {full_table_name}")
        return

    df = spark.createDataFrame(records)

    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(full_table_name)

    print(f"Wrote {len(records)} records to {full_table_name}")


def main():
    args = parse_args()
    validator = load_validator(args.schema_file)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.dataset}")

    valid_records, quarantine_records = fetch_and_split_records(args, validator)

    print(f"Valid records: {len(valid_records)}")
    print(f"Quarantine records: {len(quarantine_records)}")

    if quarantine_records:
        print("Sample quarantine record:")
        print(quarantine_records[0])

    write_records_to_delta(
        valid_records,
        f"{args.dataset}.{args.transactions_table}",
    )

    write_records_to_delta(
        quarantine_records,
        f"{args.dataset}.{args.quarantine_table}",
    )


if __name__ == "__main__":
    main()
