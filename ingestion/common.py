import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd
import pycountry
from dateutil.parser import isoparse
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.requests.retry import Client as RetryClient
from jsonschema import Draft7Validator
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


API_BASE_URL = "https://fgbjekjqnbmtkmeewexb.supabase.co/rest/v1/"
REST_REQUEST_MAX_ATTEMPTS = 5
REST_REQUEST_BACKOFF_FACTOR = 1
REST_REQUEST_MAX_RETRY_DELAY = 300
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

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


def quote_ident(identifier: str) -> str:
    return f"`{identifier}`"


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def table_fqn(args, table_name: str) -> str:
    catalog = getattr(args, "catalog", "main")
    dataset = getattr(args, "dataset", "bronze")
    return ".".join(
        [quote_ident(catalog), quote_ident(dataset), quote_ident(table_name)]
    )


def watermark_table_fqn(args) -> str:
    return table_fqn(
        args,
        getattr(
            args,
            "watermark_table",
            "gibson_eletrolux_ingestion_watermark_test",
        ),
    )


def temp_view_name(full_table_name: str) -> str:
    return full_table_name.replace("`", "").replace(".", "_") + "_view"


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


def run_log_schema() -> StructType:
    return StructType(
        [
            StructField("run_id", StringType(), True),
            StructField("pipeline", StringType(), True),
            StructField("source_type", StringType(), True),
            StructField("source_name", StringType(), True),
            StructField("catalog_name", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("started_at", TimestampType(), True),
            StructField("completed_at", TimestampType(), True),
            StructField("input_records", LongType(), True),
            StructField("valid_records", LongType(), True),
            StructField("quarantine_records", LongType(), True),
            StructField("duplicate_records", LongType(), True),
            StructField("lookback_hours", IntegerType(), True),
            StructField("watermark_value", TimestampType(), True),
            StructField("status", StringType(), True),
        ]
    )


def ingestion_stage_schema(include_error_reason: bool = True) -> StructType:
    fields = list(transactions_table_schema(include_error_reason=include_error_reason).fields)
    fields.append(StructField("batch_seq", LongType(), True))
    return StructType(fields)


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
        return Draft7Validator(json.load(f, parse_float=Decimal))


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
            record["amount"] = Decimal(str(amount))
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
    parts = []
    for col in NATURAL_KEY_COLUMNS:
        value = record.get(col)
        if col == "amount" and value not in (None, ""):
            value = str(float(value))
        else:
            value = str(value)
        parts.append(value)
    return "|".join(parts)


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


def build_staged_records(args, validator, use_watermark: bool) -> list[dict]:
    staged_records = []
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    for batch_seq, raw_record in enumerate(
        iter_source_records(args, use_watermark=use_watermark), start=1
    ):
        record = normalize_record(raw_record)
        errors = validate_record(record, validator)
        prepared = prepare_output_record(record)

        prepared["batch_seq"] = batch_seq
        prepared["natural_key"] = natural_key(record)
        prepared["natural_key_hash"] = natural_key_hash(record)
        prepared["ingestion_timestamp"] = parse_output_timestamp(ingestion_timestamp)
        prepared["is_duplicate"] = False
        prepared["error_reason"] = "; ".join(errors) if errors else None

        staged_records.append(prepared)

    return staged_records


def load_existing_duplicate_reference(transactions_table: str):
    return spark.sql(
        f"""
        SELECT
            natural_key_hash,
            collect_set(transaction_id) AS existing_transaction_ids
        FROM {transactions_table}
        WHERE natural_key_hash IS NOT NULL
        GROUP BY natural_key_hash
        """
    )


def flag_duplicates(staged_df, transactions_table: str):
    empty_string_array = F.expr("CAST(array() AS ARRAY<STRING>)")
    previous_transactions_window = (
        Window.partitionBy("natural_key_hash")
        .orderBy("batch_seq")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    existing_reference_df = load_existing_duplicate_reference(transactions_table)

    return (
        staged_df.withColumn(
            "previous_batch_transaction_ids",
            F.coalesce(
                F.collect_set("transaction_id").over(previous_transactions_window),
                empty_string_array,
            ),
        )
        .join(existing_reference_df, on="natural_key_hash", how="left")
        .withColumn(
            "is_duplicate_existing",
            F.when(
                F.col("existing_transaction_ids").isNotNull()
                & (
                    ~F.array_contains(
                        F.col("existing_transaction_ids"), F.col("transaction_id")
                    )
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "is_duplicate_current",
            F.when(
                (F.size(F.col("previous_batch_transaction_ids")) > 0)
                & (
                    ~F.array_contains(
                        F.col("previous_batch_transaction_ids"), F.col("transaction_id")
                    )
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "is_duplicate",
            F.col("is_duplicate_existing") | F.col("is_duplicate_current"),
        )
    )


def get_watermark(args):
    full_table = watermark_table_fqn(args)

    rows = spark.sql(
        f"""
        SELECT last_successful_transaction_date
        FROM {full_table}
        WHERE source_name = {sql_string_literal(getattr(args, 'source_name', 'transactions'))}
        ORDER BY updated_at DESC
        LIMIT 1
        """
    ).collect()

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

    lookback_hours = getattr(args, "lookback_hours", 0)
    start = watermark - timedelta(hours=lookback_hours)
    return start.isoformat().replace("+00:00", "Z")


def watermark_includes_boundary(args) -> bool:
    return getattr(args, "lookback_hours", 0) > 0


def update_watermark(args, valid_df):
    full_table = watermark_table_fqn(args)

    max_row = valid_df.select(F.max("transaction_date").alias("max_transaction_date")).collect()[
        0
    ]
    max_tx_date = max_row["max_transaction_date"]

    if max_tx_date is None:
        print("No valid records, watermark not updated")
        return

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


def append_run_log(args, run_summary: dict):
    run_log_df = spark.createDataFrame([run_summary], schema=run_log_schema())
    run_log_table = ".".join(
        [
            getattr(args, "catalog", "main"),
            getattr(args, "dataset", "bronze"),
            getattr(
                args,
                "control_table",
                "gibson_eletrolux_ingestion_run_log_test",
            ),
        ]
    )
    run_log_df.write.format("delta").mode("append").saveAsTable(run_log_table)


def supabase_transactions_source(
    api_key: str, start_value: str | None, include_boundary: bool = True
):
    if not api_key:
        raise ValueError("Pass --api-key or set SUPABASE_API_KEY")

    params = {"order": "transaction_date.asc"}

    if start_value:
        op = "gte" if include_boundary else "gt"
        params["transaction_date"] = f"{op}.{start_value}"

    retry_session = RetryClient(
        raise_for_status=False,
        request_max_attempts=REST_REQUEST_MAX_ATTEMPTS,
        request_backoff_factor=REST_REQUEST_BACKOFF_FACTOR,
        request_max_retry_delay=REST_REQUEST_MAX_RETRY_DELAY,
    ).session

    return rest_api_source(
        {
            "client": {
                "base_url": API_BASE_URL,
                "headers": {
                    "apikey": api_key,
                    "Authorization": f"Bearer {api_key}",
                },
                "session": retry_session,
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
    include_boundary = watermark_includes_boundary(args) if use_watermark else True

    if args.source_type == "file":
        for record in iter_file_records(args.source_file):
            record = normalize_record(record)

            if start_value:
                tx_date = isoparse(record["transaction_date"])
                boundary = isoparse(start_value)
                if include_boundary:
                    if tx_date < boundary:
                        continue
                else:
                    if tx_date <= boundary:
                        continue

            yield record

    elif args.source_type == "rest":
        source = supabase_transactions_source(
            args.api_key, start_value, include_boundary=include_boundary
        )
        yield from source.resources["transactions"]


def split_records(args, validator, use_watermark: bool):
    staged_records = build_staged_records(args, validator, use_watermark=use_watermark)

    if not staged_records:
        return spark.createDataFrame([], schema=ingestion_stage_schema())

    transactions_table = table_fqn(args, args.transactions_table)
    staged_df = spark.createDataFrame(staged_records, schema=ingestion_stage_schema())

    return flag_duplicates(staged_df, transactions_table)


def write_delta_merge(
    df,
    full_table_name: str,
    merge_key: str,
    include_error_reason: bool = False,
):
    if not df.take(1):
        print(f"No records to write to {full_table_name}")
        return

    output_columns = [
        field.name
        for field in transactions_table_schema(
            include_error_reason=include_error_reason
        ).fields
    ]
    temp_view = temp_view_name(full_table_name)
    df.select(*output_columns).createOrReplaceTempView(temp_view)

    spark.sql(
        f"""
        MERGE INTO {full_table_name} target
        USING {temp_view} source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    print(f"Wrote/merged records into {full_table_name}")


def run_pipeline(args, use_watermark: bool):
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    validator = load_validator(args.schema_file)
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    staged_df = split_records(args, validator, use_watermark=use_watermark)

    summary_row = staged_df.agg(
        F.count(F.lit(1)).alias("input_records"),
        F.coalesce(
            F.sum(F.when(F.col("error_reason").isNull(), F.lit(1)).otherwise(F.lit(0))),
            F.lit(0),
        ).alias("valid_records"),
        F.coalesce(
            F.sum(
                F.when(F.col("error_reason").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            ),
            F.lit(0),
        ).alias("quarantine_records"),
        F.coalesce(
            F.sum(F.col("is_duplicate").cast("int")),
            F.lit(0),
        ).alias("duplicate_records"),
    ).collect()[0]

    valid_df = staged_df.filter(F.col("error_reason").isNull())
    quarantine_df = staged_df.filter(F.col("error_reason").isNotNull())

    transactions_table = table_fqn(args, args.transactions_table)
    quarantine_table = table_fqn(args, args.quarantine_table)

    write_delta_merge(valid_df, transactions_table, "transaction_id")
    write_delta_merge(
        quarantine_df,
        quarantine_table,
        "transaction_id",
        include_error_reason=True,
    )

    # Advance the watermark after every successful load so a later incremental
    # run can continue from the latest processed transaction date.
    update_watermark(args, valid_df)

    watermark_value = (
        valid_df.select(F.max("transaction_date").alias("max_transaction_date"))
        .collect()[0]["max_transaction_date"]
    )

    completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    append_run_log(
        args,
        {
            "run_id": run_id,
            "pipeline": "incremental" if use_watermark else "basic",
            "source_type": args.source_type,
            "source_name": getattr(args, "source_name", "transactions"),
            "catalog_name": getattr(args, "catalog", "workspace"),
            "schema_name": getattr(args, "dataset", "bronze"),
            "started_at": started_at,
            "completed_at": completed_at,
            "input_records": int(summary_row["input_records"] or 0),
            "valid_records": int(summary_row["valid_records"] or 0),
            "quarantine_records": int(summary_row["quarantine_records"] or 0),
            "duplicate_records": int(summary_row["duplicate_records"] or 0),
            "lookback_hours": int(getattr(args, "lookback_hours", 0) or 0),
            "watermark_value": watermark_value,
            "status": "success",
        },
    )

    duplicate_count = int(summary_row["duplicate_records"] or 0)

    print("=========================================")
    print("Transaction Ingestion Summary")
    print("=========================================")
    print(f"Source type        : {args.source_type}")
    print(f"Input records      : {int(summary_row['input_records'] or 0)}")
    print(f"Valid records      : {int(summary_row['valid_records'] or 0)}")
    print(f"Quarantined records: {int(summary_row['quarantine_records'] or 0)}")
    print(f"Duplicates flagged : {duplicate_count}")
    print("")
    print("Destination:")
    print(f"  Valid      : {transactions_table}")
    print(f"  Quarantine : {quarantine_table}")

    if use_watermark:
        print(f"  Watermark  : {watermark_table_fqn(args)}")

    print("=========================================")
