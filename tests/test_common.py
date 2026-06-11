from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from ingestion import common


def make_args(**overrides):
    base = {
        "catalog": "workspace",
        "dataset": "bronze",
        "transactions_table": "gibson_eletrolux_transactions_test",
        "quarantine_table": "gibson_eletrolux_quarantine_test",
        "control_table": "gibson_eletrolux_ingestion_run_log_test",
        "watermark_table": "gibson_eletrolux_ingestion_watermark_test",
        "source_type": "file",
        "source_file": "transactions.csv",
        "schema_file": "transactions_schema.json",
        "source_name": "transactions",
        "lookback_hours": 0,
        "api_key": "test-key",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def make_raw_record(
    transaction_id: str,
    transaction_date: str,
    country_code: str = "SE",
    amount: str = "10.50",
):
    return {
        "id": transaction_id,
        "transaction_id": transaction_id,
        "account_id": "acct-1",
        "transaction_date": transaction_date,
        "amount": amount,
        "currency": "USD",
        "transaction_type": "sale",
        "merchant_name": "Example Shop",
        "merchant_category": "Retail",
        "status": "open",
        "country_code": country_code,
    }


def test_table_helpers_use_prefixed_names():
    args = make_args()

    assert (
        common.table_fqn(args, args.transactions_table)
        == "`workspace`.`bronze`.`gibson_eletrolux_transactions_test`"
    )
    assert (
        common.watermark_table_fqn(args)
        == "`workspace`.`bronze`.`gibson_eletrolux_ingestion_watermark_test`"
    )


def test_load_validator_reads_schema_from_repo_root(tmp_path, monkeypatch):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(
        '{"type": "object", "properties": {"amount": {"type": "number", "multipleOf": 0.01}}}'
    )

    monkeypatch.setattr(common, "REPO_ROOT", tmp_path)

    validator = common.load_validator("schema.json")

    assert validator.schema["properties"]["amount"]["multipleOf"] == Decimal("0.01")


def test_normalize_record_and_validate_record():
    record = make_raw_record(
        "tx-1",
        "2026-01-01 10:00:00+00:00",
        country_code="XX",
        amount="75.74",
    )

    normalized = common.normalize_record(record)

    assert "id" not in normalized
    assert normalized["transaction_date"] == "2026-01-01T10:00:00Z"
    assert normalized["amount"] == Decimal("75.74")
    assert isinstance(normalized["amount"], Decimal)

    errors = common.validate_record(normalized, common.Draft7Validator({}))

    assert any("country_code: invalid assigned ISO code XX" in err for err in errors)

    broken = dict(normalized, transaction_date="not-a-dateZ")
    broken_errors = common.validate_record(broken, common.Draft7Validator({}))

    assert any(
        "transaction_date: invalid calendar datetime not-a-dateZ" in err
        for err in broken_errors
    )


def test_build_staged_records_adds_batch_metadata(monkeypatch):
    args = make_args()
    validator = common.Draft7Validator({})

    source_rows = [
        make_raw_record(
            "tx-1",
            "2026-01-01 10:00:00+00:00",
            country_code="SE",
            amount="10.50",
        ),
        make_raw_record(
            "tx-2",
            "not-a-date",
            country_code="XX",
            amount="7.25",
        ),
    ]

    monkeypatch.setattr(
        common,
        "iter_source_records",
        lambda _args, use_watermark: iter(source_rows),
    )

    staged_records = common.build_staged_records(args, validator, use_watermark=False)

    assert len(staged_records) == 2

    first = staged_records[0]
    assert first["batch_seq"] == 1
    assert first["transaction_id"] == "tx-1"
    assert first["transaction_date"] == datetime(2026, 1, 1, 10, 0)
    assert first["amount"] == Decimal("10.50")
    assert first["error_reason"] is None
    assert first["is_duplicate"] is False
    assert len(first["natural_key_hash"]) == 64
    assert first["ingestion_timestamp"].tzinfo is None

    second = staged_records[1]
    assert second["batch_seq"] == 2
    assert second["transaction_id"] == "tx-2"
    assert second["error_reason"] is not None
    assert "transaction_date: invalid calendar datetime" in second["error_reason"]
    assert "country_code: invalid assigned ISO code XX" in second["error_reason"]


def test_watermark_helpers_use_hour_lookback(monkeypatch):
    args = make_args(lookback_hours=3)
    watermark = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(common, "get_watermark", lambda _args: watermark)

    assert common.watermark_start_value(args) == "2026-01-01T09:00:00Z"
    assert common.watermark_includes_boundary(args) is True

    zero_lookback_args = make_args(lookback_hours=0)
    assert common.watermark_start_value(zero_lookback_args) == "2026-01-01T12:00:00Z"
    assert common.watermark_includes_boundary(zero_lookback_args) is False


def test_iter_source_records_filters_boundary_by_lookback_hours(monkeypatch):
    boundary = "2026-01-01T00:00:00Z"
    source_rows = [
        make_raw_record("before", "2025-12-31T23:59:59Z"),
        make_raw_record("boundary", "2026-01-01T00:00:00Z"),
        make_raw_record("after", "2026-01-01T00:00:01Z"),
    ]

    monkeypatch.setattr(
        common,
        "iter_file_records",
        lambda _source_file: iter(source_rows),
    )
    monkeypatch.setattr(common, "watermark_start_value", lambda _args: boundary)

    zero_lookback = make_args(lookback_hours=0)
    inclusive_lookback = make_args(lookback_hours=2)

    zero_result = list(common.iter_source_records(zero_lookback, use_watermark=True))
    inclusive_result = list(
        common.iter_source_records(inclusive_lookback, use_watermark=True)
    )

    assert [row["transaction_id"] for row in zero_result] == ["after"]
    assert [row["transaction_id"] for row in inclusive_result] == [
        "boundary",
        "after",
    ]


def test_iter_file_records_reads_csv_from_repo_root(monkeypatch, tmp_path):
    csv_file = tmp_path / "nested" / "transactions.csv"
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    csv_file.write_text(
        "transaction_id,account_id,transaction_date,amount,currency,transaction_type,"
        "merchant_name,merchant_category,status,country_code\n"
        "file-1,acct-1,2026-01-01T00:00:00Z,10.50,USD,sale,,Retail,open,SE\n"
        "file-2,acct-2,2026-01-02T00:00:00Z,11.00,USD,sale,Shop,Retail,open,US\n"
    )

    monkeypatch.setattr(common, "REPO_ROOT", tmp_path)

    records = list(common.iter_file_records("nested/transactions.csv"))

    assert [row["transaction_id"] for row in records] == ["file-1", "file-2"]
    assert records[0]["merchant_name"] is None
    assert records[1]["merchant_name"] == "Shop"


def test_iter_file_records_reads_excel_with_null_normalization(monkeypatch, tmp_path):
    excel_path = tmp_path / "transactions.xlsx"
    captured = {}

    def fake_read_excel(path, dtype=None):
        captured["path"] = Path(path)
        captured["dtype"] = dtype
        return common.pd.DataFrame(
            [
                {
                    "transaction_id": "xl-1",
                    "account_id": "acct-9",
                    "transaction_date": "2026-01-03T00:00:00Z",
                    "amount": "12.00",
                    "currency": "USD",
                    "transaction_type": "sale",
                    "merchant_name": None,
                    "merchant_category": "Retail",
                    "status": "open",
                    "country_code": "SE",
                }
            ]
        )

    monkeypatch.setattr(common, "resolve_repo_file", lambda _source_file: excel_path)
    monkeypatch.setattr(common.pd, "read_excel", fake_read_excel)

    records = list(common.iter_file_records("transactions.xlsx"))

    assert captured["path"] == excel_path
    assert captured["dtype"] == str
    assert records == [
        {
            "transaction_id": "xl-1",
            "account_id": "acct-9",
            "transaction_date": "2026-01-03T00:00:00Z",
            "amount": "12.00",
            "currency": "USD",
            "transaction_type": "sale",
            "merchant_name": None,
            "merchant_category": "Retail",
            "status": "open",
            "country_code": "SE",
        }
    ]


def test_iter_file_records_rejects_unsupported_suffix(monkeypatch, tmp_path):
    monkeypatch.setattr(common, "resolve_repo_file", lambda _source_file: tmp_path / "transactions.json")

    try:
        list(common.iter_file_records("transactions.json"))
    except ValueError as exc:
        assert "Unsupported file type: .json" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unsupported file suffix")


def test_supabase_transactions_source_builds_dlt_config(monkeypatch):
    captured = {}
    sentinel_source = object()
    sentinel_session = object()

    class FakeRetryClient:
        def __init__(self, **kwargs):
            captured["retry_kwargs"] = kwargs
            self.session = sentinel_session

    def fake_rest_api_source(config, name):
        captured["config"] = config
        captured["name"] = name
        return sentinel_source

    monkeypatch.setattr(common, "RetryClient", FakeRetryClient)
    monkeypatch.setattr(common, "rest_api_source", fake_rest_api_source)

    result = common.supabase_transactions_source(
        "api-key-123",
        "2026-01-01T00:00:00Z",
        include_boundary=False,
    )

    assert result is sentinel_source
    assert captured["name"] == "supabase_transactions"
    assert captured["config"]["client"]["base_url"] == common.API_BASE_URL
    assert captured["config"]["client"]["headers"] == {
        "apikey": "api-key-123",
        "Authorization": "Bearer api-key-123",
    }
    assert captured["config"]["client"]["session"] is sentinel_session
    assert captured["retry_kwargs"] == {
        "raise_for_status": False,
        "request_max_attempts": common.REST_REQUEST_MAX_ATTEMPTS,
        "request_backoff_factor": common.REST_REQUEST_BACKOFF_FACTOR,
        "request_max_retry_delay": common.REST_REQUEST_MAX_RETRY_DELAY,
    }
    assert captured["config"]["resources"][0]["name"] == "transactions"
    assert captured["config"]["resources"][0]["endpoint"]["path"] == "transactions"
    assert captured["config"]["resources"][0]["endpoint"]["params"] == {
        "order": "transaction_date.asc",
        "transaction_date": "gt.2026-01-01T00:00:00Z",
    }


def test_supabase_transactions_source_uses_inclusive_boundary(monkeypatch):
    captured = {}

    def fake_rest_api_source(config, name):
        captured["config"] = config
        captured["name"] = name
        return "sentinel"

    monkeypatch.setattr(common, "rest_api_source", fake_rest_api_source)

    result = common.supabase_transactions_source(
        "api-key-123",
        "2026-01-01T00:00:00Z",
        include_boundary=True,
    )

    assert result == "sentinel"
    assert captured["config"]["resources"][0]["endpoint"]["params"][
        "transaction_date"
    ] == "gte.2026-01-01T00:00:00Z"


def test_supabase_transactions_source_omits_filter_when_start_value_missing(monkeypatch):
    captured = {}

    def fake_rest_api_source(config, name):
        captured["config"] = config
        captured["name"] = name
        return "sentinel"

    monkeypatch.setattr(common, "rest_api_source", fake_rest_api_source)

    result = common.supabase_transactions_source(
        "api-key-123",
        None,
        include_boundary=True,
    )

    assert result == "sentinel"
    assert captured["config"]["resources"][0]["endpoint"]["params"] == {
        "order": "transaction_date.asc"
    }


def test_supabase_transactions_source_requires_api_key():
    try:
        common.supabase_transactions_source("", None)
    except ValueError as exc:
        assert "Pass --api-key or set SUPABASE_API_KEY" in str(exc)
    else:
        raise AssertionError("Expected ValueError when API key is missing")


def test_iter_source_records_reads_from_rest_source(monkeypatch):
    rest_rows = [
        make_raw_record("rest-1", "2026-01-01T00:00:01Z"),
        make_raw_record("rest-2", "2026-01-01T00:00:02Z"),
    ]
    source = SimpleNamespace(resources={"transactions": rest_rows})

    monkeypatch.setattr(
        common,
        "supabase_transactions_source",
        lambda api_key, start_value, include_boundary=True: source,
    )
    monkeypatch.setattr(common, "watermark_start_value", lambda _args: "2026-01-01T00:00:00Z")
    monkeypatch.setattr(common, "watermark_includes_boundary", lambda _args: False)

    args = make_args(source_type="rest", api_key="api-key-123", lookback_hours=0)

    result = list(common.iter_source_records(args, use_watermark=True))

    assert [row["transaction_id"] for row in result] == ["rest-1", "rest-2"]
    assert result[0]["transaction_date"] == "2026-01-01T00:00:01Z"
    assert result[1]["transaction_date"] == "2026-01-01T00:00:02Z"


def test_iter_source_records_passes_rest_boundary_mode(monkeypatch):
    captured = {}
    source = SimpleNamespace(resources={"transactions": []})

    def fake_supabase_transactions_source(api_key, start_value, include_boundary=True):
        captured["api_key"] = api_key
        captured["start_value"] = start_value
        captured["include_boundary"] = include_boundary
        return source

    monkeypatch.setattr(common, "supabase_transactions_source", fake_supabase_transactions_source)
    monkeypatch.setattr(common, "watermark_start_value", lambda _args: "2026-01-01T00:00:00Z")
    monkeypatch.setattr(common, "watermark_includes_boundary", lambda _args: True)

    args = make_args(source_type="rest", api_key="api-key-123", lookback_hours=4)

    result = list(common.iter_source_records(args, use_watermark=True))

    assert result == []
    assert captured == {
        "api_key": "api-key-123",
        "start_value": "2026-01-01T00:00:00Z",
        "include_boundary": True,
    }


def test_load_existing_duplicate_reference_uses_collect_set(fake_spark_session, monkeypatch):
    args = make_args()
    expected_table = common.table_fqn(args, args.transactions_table)
    result_df = fake_spark_session.createDataFrame(
        [{"natural_key_hash": "hash-1", "existing_transaction_ids": ["tx-1"]}]
    )
    fake_spark_session.sql_results = [result_df]

    monkeypatch.setattr(common, "spark", fake_spark_session)

    returned_df = common.load_existing_duplicate_reference(expected_table)

    assert returned_df is result_df
    query = fake_spark_session.sql_queries[0]
    assert "collect_set(transaction_id) AS existing_transaction_ids" in query
    assert "GROUP BY natural_key_hash" in query
    assert expected_table in query


def test_flag_duplicates_builds_expected_spark_pipeline(fake_spark_session, monkeypatch):
    staged_df = fake_spark_session.createDataFrame(
        [
            {
                "natural_key_hash": "hash-1",
                "transaction_id": "tx-1",
                "batch_seq": 1,
            }
        ]
    )
    reference_df = fake_spark_session.createDataFrame(
        [
            {
                "natural_key_hash": "hash-1",
                "existing_transaction_ids": ["tx-0"],
            }
        ]
    )

    monkeypatch.setattr(common, "spark", fake_spark_session)
    monkeypatch.setattr(
        common,
        "load_existing_duplicate_reference",
        lambda _transactions_table: reference_df,
    )

    result = common.flag_duplicates(
        staged_df,
        "`workspace`.`bronze`.`gibson_eletrolux_transactions_test`",
    )

    assert result is staged_df
    assert [name for name, _ in staged_df.with_columns] == [
        "previous_batch_transaction_ids",
        "is_duplicate_existing",
        "is_duplicate_current",
        "is_duplicate",
    ]
    assert staged_df.join_calls[0][0] is reference_df
    assert staged_df.join_calls[0][1] == "natural_key_hash"
    assert staged_df.join_calls[0][2] == "left"


def test_write_delta_merge_creates_temp_view_and_merges(fake_spark_session, monkeypatch):
    args = make_args()
    full_table = common.table_fqn(args, args.transactions_table)
    row = {
        "transaction_id": "tx-1",
        "account_id": "acct-1",
        "transaction_date": datetime(2026, 1, 1, 10, 0),
        "amount": Decimal("10.50"),
        "currency": "USD",
        "transaction_type": "sale",
        "merchant_name": "Example Shop",
        "merchant_category": "Retail",
        "status": "open",
        "country_code": "SE",
        "ingestion_timestamp": datetime(2026, 1, 1, 11, 0),
        "natural_key": "acct-1|2026-01-01T10:00:00Z|10.5|USD|sale|Example Shop|Retail|open|SE",
        "natural_key_hash": "hash-1",
        "is_duplicate": False,
    }
    df = fake_spark_session.createDataFrame([row], schema=common.transactions_table_schema())

    monkeypatch.setattr(common, "spark", fake_spark_session)

    common.write_delta_merge(df, full_table, "transaction_id")

    assert df.selected_columns == tuple(
        field.name for field in common.transactions_table_schema().fields
    )
    assert df.temp_view_name == "workspace_bronze_gibson_eletrolux_transactions_test_view"
    assert fake_spark_session.sql_queries[-1].strip().startswith("MERGE INTO")
    assert full_table in fake_spark_session.sql_queries[-1]


def test_append_run_log_uses_control_table(fake_spark_session, monkeypatch):
    args = make_args()
    run_summary = {
        "run_id": "run-1",
        "pipeline": "basic",
        "source_type": "file",
        "source_name": "transactions",
        "catalog_name": "workspace",
        "schema_name": "bronze",
        "started_at": datetime(2026, 1, 1, 10, 0),
        "completed_at": datetime(2026, 1, 1, 10, 1),
        "input_records": 2,
        "valid_records": 1,
        "quarantine_records": 1,
        "duplicate_records": 0,
        "lookback_hours": 0,
        "watermark_value": datetime(2026, 1, 1, 10, 0),
        "status": "success",
    }

    monkeypatch.setattr(common, "spark", fake_spark_session)

    common.append_run_log(args, run_summary)

    written_df = fake_spark_session.created_dataframes[-1]
    assert written_df.data == [run_summary]
    assert written_df.saved_table == (
        "workspace.bronze.gibson_eletrolux_ingestion_run_log_test"
    )
    assert written_df.write.format_name == "delta"
    assert written_df.write.mode_name == "append"


def test_update_watermark_merges_latest_transaction_date(fake_spark_session, monkeypatch):
    args = make_args()
    valid_df = fake_spark_session.createDataFrame(
        [
            {
                "max_transaction_date": datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)
            }
        ]
    )

    monkeypatch.setattr(common, "spark", fake_spark_session)

    common.update_watermark(args, valid_df)

    update_df = fake_spark_session.created_dataframes[-1]
    assert [field.name for field in update_df.schema.fields] == [
        field.name for field in common.watermark_table_schema().fields
    ]
    assert update_df.temp_view_name == "watermark_update_view"
    assert update_df.data[0]["source_name"] == "transactions"
    assert update_df.data[0]["last_successful_transaction_date"] == datetime(
        2026, 1, 1, 12, 30
    )
    assert fake_spark_session.sql_queries[-1].strip().startswith("MERGE INTO")
    assert (
        "`workspace`.`bronze`.`gibson_eletrolux_ingestion_watermark_test`"
        in fake_spark_session.sql_queries[-1]
    )
