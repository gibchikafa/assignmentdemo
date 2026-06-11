from pathlib import Path

if __package__ is None or __package__ == "":
    import sys

    sys.path.append(str(Path(__file__).resolve().parent))

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
