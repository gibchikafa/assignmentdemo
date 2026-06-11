import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd
import pycountry
from dateutil.parser import isoparse
from dlt.sources.rest_api import rest_api_source
from jsonschema import Draft7Validator
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


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
AMOUNT_SCALE = Decimal("0.01")
REPO_ROOT = Path(__file__).resolve().parents[1]


def transactions_table_schema(include_error_reason: bool = False) -> StructType:
    fields = [
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("status", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("natural_key", StringType(), True),
        StructField("natural_key_hash", StringType(), True),
        StructField("is_duplicate", BooleanType(), True),
    ]

    if include_error_reason:
        fields.append(StructField("error_reason", StringType(), True))

    return StructType(fields)


def watermark_table_schema() -> StructType:
    return StructType(
        [
            StructField("source_name", StringType(), True),
            StructField("last_successful_transaction_date", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    )


def resolve_repo_file(file_path: str) -> Path:
    path = Path(file_path)

    if path.is_file():
        return path

    repo_path = REPO_ROOT / file_path
    if repo_path.is_file():
        return repo_path

    raise FileNotFoundError(
        f"Could not find file '{file_path}'. Checked '{path}' and '{repo_path}'."
    )


def load_validator(schema_file: str) -> Draft7Validator:
    with open(resolve_repo_file(schema_file), "r") as f:
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


def parse_output_timestamp(value):
    if value in (None, ""):
        return None

    try:
        if isinstance(value, datetime):
            dt = value
        else:
            dt = isoparse(str(value))
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def parse_output_amount(value):
    if value in (None, ""):
        return None

    try:
        return Decimal(str(value)).quantize(AMOUNT_SCALE, rounding=ROUND_HALF_UP)
    except Exception:
        return None


def prepare_output_record(record: dict) -> dict:
    prepared = dict(record)
    prepared["transaction_date"] = parse_output_timestamp(prepared.get("transaction_date"))
    prepared["amount"] = parse_output_amount(prepared.get("amount"))
    prepared["ingestion_timestamp"] = parse_output_timestamp(
        prepared.get("ingestion_timestamp")
    )
    return prepared


def table_exists(full_table_name: str) -> bool:
    parts = full_table_name.split(".")

    if len(parts) == 1:
        return spark.catalog.tableExists(parts[0])

    if len(parts) == 2:
        schema_name, table_name = parts
        return spark.catalog.tableExists(table_name, schema_name)

    if len(parts) == 3:
        catalog_name, schema_name, table_name = parts
        rows = spark.sql(
            f"SHOW TABLES IN `{catalog_name}`.`{schema_name}` LIKE '{table_name}'"
        ).collect()
        return any(row["tableName"] == table_name for row in rows)

    raise ValueError(f"Unsupported table name: {full_table_name}")


def load_existing_natural_keys(transactions_table: str) -> dict[str, set[str]]:
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
    full_table = f"{args.dataset}.{getattr(args, 'watermark_table', 'ingestion_watermark_test')}"

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
        WHERE source_name = '{getattr(args, 'source_name', 'transactions')}'
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
    watermark = get_watermark(args)

    if watermark is None:
        return None

    lookback_days = getattr(args, "lookback_days", 2)
    start = watermark - timedelta(days=lookback_days)
    return start.isoformat().replace("+00:00", "Z")


def update_watermark(args, valid_records: list[dict]):
    if not valid_records:
        print("No valid records, watermark not updated")
        return

    full_table = f"{args.dataset}.{getattr(args, 'watermark_table', 'ingestion_watermark_test')}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            source_name STRING,
            last_successful_transaction_date TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)

    max_tx_date = max(isoparse(r["transaction_date"]) for r in valid_records)
    if max_tx_date.tzinfo is None:
        max_tx_date = max_tx_date.replace(tzinfo=timezone.utc)

    update_df = spark.createDataFrame(
        [
            {
                "source_name": getattr(args, "source_name", "transactions"),
                "last_successful_transaction_date": max_tx_date.astimezone(
                    timezone.utc
                ).replace(tzinfo=None),
                "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        ],
        schema=watermark_table_schema(),
    )

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
    path = resolve_repo_file(source_file)

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str)
    elif path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path, dtype=str)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")

    df = df.where(pd.notnull(df), None)

    for record in df.to_dict(orient="records"):
        yield record


def iter_source_records(args, use_watermark: bool):
    start_value = watermark_start_value(args) if use_watermark else None

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


def split_records(args, validator, use_watermark: bool):
    transactions_table = f"{args.dataset}.{args.transactions_table}"
    existing_natural_keys = load_existing_natural_keys(transactions_table)

    valid_records = []
    quarantine_records = []

    seen_in_current_batch = {}
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    for raw_record in iter_source_records(args, use_watermark=use_watermark):
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
            valid_records.append(record)

    return valid_records, quarantine_records


def write_delta_merge(
    records: list[dict],
    full_table_name: str,
    merge_key: str,
    include_error_reason: bool = False,
):
    if not records:
        print(f"No records to write to {full_table_name}")
        return

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    prepared_records = [prepare_output_record(record) for record in records]
    df = spark.createDataFrame(
        prepared_records,
        schema=transactions_table_schema(include_error_reason=include_error_reason),
    )
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


def run_pipeline(args, use_watermark: bool):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.dataset}")

    validator = load_validator(args.schema_file)
    valid_records, quarantine_records = split_records(
        args, validator, use_watermark=use_watermark
    )

    transactions_table = f"{args.dataset}.{args.transactions_table}"
    quarantine_table = f"{args.dataset}.{args.quarantine_table}"

    write_delta_merge(valid_records, transactions_table, "transaction_id")
    write_delta_merge(
        quarantine_records,
        quarantine_table,
        "transaction_id",
        include_error_reason=True,
    )

    if use_watermark:
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

    if use_watermark:
        print(f"  Watermark  : {args.dataset}.{getattr(args, 'watermark_table', 'ingestion_watermark_test')}")

    print("=========================================")
