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

from ingestion.cli import build_parser
from ingestion.common import run_pipeline


def parse_args():
    parser = build_parser(include_incremental=True)
    parser.add_argument(
        "--pipeline",
        choices=["basic", "incremental"],
        default="incremental",
        help="Choose basic Task 1 ingestion or incremental Task 3 ingestion.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args, use_watermark=args.pipeline == "incremental")
