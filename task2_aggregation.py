import argparse
import sys
from pathlib import Path
from types import SimpleNamespace

sys.dont_write_bytecode = True


def find_repo_root() -> Path:
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
            return candidate

    return cwd


def add_repo_root_to_path() -> None:
    repo_root = find_repo_root()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


add_repo_root_to_path()

import ingestion.common as common


REPO_ROOT = find_repo_root()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the daily_account_summary gold table with Spark SQL."
    )
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--source-dataset", default="bronze")
    parser.add_argument(
        "--source-table", default="gibson_eletrolux_transactions_test"
    )
    parser.add_argument(
        "--quarantine-dataset", default="bronze"
    )
    parser.add_argument(
        "--quarantine-table", default="gibson_eletrolux_quarantine_test"
    )
    parser.add_argument("--output-dataset", default="gold")
    parser.add_argument("--output-table", default="daily_account_summary")
    parser.add_argument(
        "--sql-template",
        default=str(REPO_ROOT / "sql" / "daily_account_summary.sql"),
    )
    return parser


def _fqn_args(catalog: str, dataset: str) -> SimpleNamespace:
    return SimpleNamespace(catalog=catalog, dataset=dataset)


def render_daily_account_summary_sql(args) -> str:
    template_path = common.resolve_repo_file(args.sql_template)
    template = template_path.read_text()

    source_table_fqn = common.table_fqn(
        _fqn_args(args.catalog, args.source_dataset),
        args.source_table,
    )
    quarantine_table_fqn = common.table_fqn(
        _fqn_args(args.catalog, args.quarantine_dataset),
        args.quarantine_table,
    )
    output_table_fqn = common.table_fqn(
        _fqn_args(args.catalog, args.output_dataset),
        args.output_table,
    )

    return template.format(
        source_table_fqn=source_table_fqn,
        quarantine_table_fqn=quarantine_table_fqn,
        output_table_fqn=output_table_fqn,
    )


def run_task2(args) -> None:
    common.spark.conf.set("spark.sql.session.timeZone", "UTC")

    output_schema = ".".join(
        [common.quote_ident(args.catalog), common.quote_ident(args.output_dataset)]
    )
    common.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")
    common.spark.sql(render_daily_account_summary_sql(args))

    output_table_fqn = common.table_fqn(
        _fqn_args(args.catalog, args.output_dataset),
        args.output_table,
    )

    print("=========================================")
    print("Task 2 Daily Summary")
    print("=========================================")
    print(
        "Source table  : "
        f"{common.table_fqn(_fqn_args(args.catalog, args.source_dataset), args.source_table)}"
    )
    print(
        "Quarantine    : "
        f"{common.table_fqn(_fqn_args(args.catalog, args.quarantine_dataset), args.quarantine_table)}"
    )
    print(f"Output table  : {output_table_fqn}")
    print("Mode          : Spark SQL full refresh")
    print("=========================================")


def main() -> None:
    args = build_parser().parse_args()
    run_task2(args)


if __name__ == "__main__":
    main()
