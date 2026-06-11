import sys

from ingestion.cli import build_parser
import entrypoint


def test_common_cli_defaults():
    args = build_parser(include_incremental=False).parse_args([])

    assert args.source_type == "file"
    assert args.source_file.endswith("transactions.csv")
    assert args.schema_file.endswith("transactions_schema.json")
    assert args.catalog == "workspace"
    assert args.dataset == "bronze"
    assert args.transactions_table == "gibson_eletrolux_transactions_test"
    assert args.quarantine_table == "gibson_eletrolux_quarantine_test"
    assert args.control_table == "gibson_eletrolux_ingestion_run_log_test"


def test_incremental_cli_defaults_and_lookback_alias():
    args = build_parser(include_incremental=True).parse_args(["--lookback-days", "4"])

    assert args.watermark_table == "gibson_eletrolux_ingestion_watermark_test"
    assert args.source_name == "transactions"
    assert args.lookback_hours == 4


def test_entrypoint_defaults_to_basic_pipeline(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["entrypoint.py"])

    args = entrypoint.parse_args()

    assert args.pipeline == "basic"
    assert args.lookback_hours == 0


def test_entrypoint_accepts_incremental_pipeline(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["entrypoint.py", "--pipeline", "incremental", "--lookback-hours", "2"],
    )

    args = entrypoint.parse_args()

    assert args.pipeline == "incremental"
    assert args.lookback_hours == 2
