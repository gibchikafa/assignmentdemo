import sys

from ingestion.cli import build_parser
import entrypoint
import entrypoint_incremental
import entrypoint_summaries


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


def test_basic_entrypoint_defaults(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["entrypoint.py"])

    args = entrypoint.parse_args()

    assert args.source_type == "file"
    assert args.catalog == "workspace"
    assert args.dataset == "bronze"
    assert args.transactions_table == "gibson_eletrolux_transactions_test"
    assert args.quarantine_table == "gibson_eletrolux_quarantine_test"


def test_incremental_entrypoint_defaults(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["entrypoint_incremental.py"],
    )

    args = entrypoint_incremental.parse_args()

    assert args.source_type == "file"
    assert args.lookback_hours == 0
    assert args.watermark_table == "gibson_eletrolux_ingestion_watermark_test"


def test_summaries_entrypoint_defaults(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["entrypoint_summaries.py"],
    )

    args = entrypoint_summaries.parse_args()

    assert args.source_type == "file"
    assert args.catalog == "workspace"
    assert args.dataset == "bronze"
    assert args.output_dataset == "gold"
    assert args.output_table == "daily_account_summary"
