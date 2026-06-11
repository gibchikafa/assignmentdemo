from pathlib import Path
import sys


def add_repo_root_to_path() -> None:
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


def main():
    args = build_parser(include_incremental=True).parse_args()
    run_pipeline(args, use_watermark=True)


if __name__ == "__main__":
    main()
