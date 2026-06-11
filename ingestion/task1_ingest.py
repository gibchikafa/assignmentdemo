from pathlib import Path

if __package__ is None or __package__ == "":
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))

from ingestion.cli import build_parser
from ingestion.common import run_pipeline


def main():
    args = build_parser(include_incremental=False).parse_args()
    run_pipeline(args, use_watermark=False)


if __name__ == "__main__":
    main()
