import argparse
import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-type", choices=["file", "rest"], default="file")
    parser.add_argument("--source-file", default=str(REPO_ROOT / "transactions.csv"))
    parser.add_argument(
        "--schema-file", default=str(REPO_ROOT / "transactions_schema.json")
    )
    parser.add_argument("--api-key", default=os.getenv("SUPABASE_API_KEY"))

    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--dataset", default="bronze")
    parser.add_argument("--transactions-table", default="gibson_eletrolux_transactions_test")
    parser.add_argument("--quarantine-table", default="gibson_eletrolux_quarantine_test")
    parser.add_argument("--control-table", default="gibson_eletrolux_ingestion_run_log_test")


def add_incremental_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--watermark-table", default="gibson_eletrolux_ingestion_watermark_test"
    )
    parser.add_argument("--source-name", default="transactions")
    parser.add_argument(
        "--lookback-hours",
        "--lookback-days",
        dest="lookback_hours",
        type=int,
        default=0,
    )


def build_parser(include_incremental: bool) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    add_common_args(parser)

    if include_incremental:
        add_incremental_args(parser)

    return parser
