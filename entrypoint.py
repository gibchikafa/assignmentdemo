import argparse
import sys
sys.dont_write_bytecode = True
from pathlib import Path


def add_repo_root_to_path() -> None:
    """Make the repo importable when executed via Databricks notebook exec."""
    candidates = []
    cwd = Path.cwd().resolve()
    candidates.extend([cwd, *cwd.parents])

    for entry in sys.path:
        if not entry:
            continue
        path = Path(entry).resolve()
        candidates.extend([path, *path.parents])

    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)

        if (candidate / "ingestion").is_dir() and (candidate / "transactions.csv").is_file():
            sys.path.insert(0, str(candidate))
            return


add_repo_root_to_path()

from ingestion.cli import add_common_args, add_incremental_args
from ingestion.common import run_pipeline
from sql import task2_aggregation as task2_runner


def build_parser():
    parser = argparse.ArgumentParser()
    add_common_args(parser)
    add_incremental_args(parser)
    task2_runner.add_task2_args(parser)
    parser.add_argument(
        "--pipeline",
        choices=["basic", "incremental", "task2"],
        default="basic",
        help="Choose basic Task 1 ingestion, incremental Task 3 ingestion, or Task 2 aggregation.",
    )
    return parser


def parse_args():
    return build_parser().parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.pipeline == "task2":
        task2_runner.run_task2(args)
    else:
        run_pipeline(args, use_watermark=args.pipeline == "incremental")
