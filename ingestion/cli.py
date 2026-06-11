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

    parser.add_argument("--catalog", default="main")
    parser.add_argument("--dataset", default="bronze")
    parser.add_argument("--transactions-table", default="transactions_test")
    parser.add_argument("--quarantine-table", default="quarantine_test")


def add_incremental_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--watermark-table", default="ingestion_watermark_test")
    parser.add_argument("--source-name", default="transactions")
    parser.add_argument("--lookback-days", type=int, default=2)


def build_parser(include_incremental: bool) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    add_common_args(parser)

    if include_incremental:
        add_incremental_args(parser)

    return parser
