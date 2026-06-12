import argparse
import sys
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

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
from ingestion.cli import add_common_args


REPO_ROOT = find_repo_root()


def add_summary_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-dataset", default="gold")
    parser.add_argument("--output-table", default="daily_account_summary")
    parser.add_argument(
        "--summary-watermark-table",
        default="gibson_eletrolux_daily_summary_watermark_test",
    )
    parser.add_argument(
        "--sql-template",
        default=str(REPO_ROOT / "sql" / "daily_account_summary.sql"),
    )


def _fqn_args(catalog: str, dataset: str) -> SimpleNamespace:
    return SimpleNamespace(catalog=catalog, dataset=dataset)


def output_table_fqn(args) -> str:
    return common.table_fqn(_fqn_args(args.catalog, args.output_dataset), args.output_table)


def summary_watermark_table_fqn(args) -> str:
    return common.table_fqn(
        _fqn_args(args.catalog, args.output_dataset),
        args.summary_watermark_table,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the daily_account_summary gold table with Spark SQL."
    )
    add_common_args(parser)
    add_summary_args(parser)
    return parser


def render_daily_account_summary_sql(
    args,
    incremental: bool = False,
    start_value: str | None = None,
    include_boundary: bool = True,
) -> str:
    template_path = common.resolve_repo_file(args.sql_template)
    template = template_path.read_text()

    source_table_fqn = common.table_fqn(args, args.transactions_table)
    quarantine_table_fqn = common.table_fqn(args, args.quarantine_table)
    output_fqn = output_table_fqn(args)

    statement_prefix: str
    statement_suffix: str
    changed_keys_cte: str
    filtered_transactions_source_join: str
    source_date_clause: str

    if incremental:
        if start_value is None:
            raise ValueError("start_value is required for incremental Task 2 rendering")

        comparison_operator = ">=" if include_boundary else ">"
        statement_prefix = f"MERGE INTO {output_fqn} AS target\nUSING ("
        statement_suffix = (
            ") summary_source\n"
            "ON target.account_id = summary_source.account_id\n"
            "AND target.transaction_date = summary_source.transaction_date\n"
            "WHEN MATCHED THEN UPDATE SET *\n"
            "WHEN NOT MATCHED THEN INSERT *"
        )
        changed_keys_cte = (
            "changed_keys AS (\n"
            "    SELECT DISTINCT\n"
            "        source.account_id,\n"
            "        CAST(source.transaction_date AS DATE) AS transaction_date\n"
            f"    FROM {source_table_fqn} source\n"
            f"    LEFT ANTI JOIN {quarantine_table_fqn} quarantine\n"
            "        ON source.transaction_id = quarantine.transaction_id\n"
            "    WHERE source.status = 'completed'\n"
            "      AND COALESCE(source.is_duplicate, false) = false\n"
            f"      AND source.transaction_date {comparison_operator} to_timestamp('{start_value}')\n"
            "),\n    "
        )
        filtered_transactions_source_join = (
            "    INNER JOIN changed_keys keys\n"
            "        ON source.account_id = keys.account_id\n"
            "       AND CAST(source.transaction_date AS DATE) = keys.transaction_date"
        )
        source_date_clause = ""
    else:
        statement_prefix = (
            f"CREATE OR REPLACE TABLE {output_fqn}\n"
            "USING DELTA\n"
            "PARTITIONED BY (transaction_date)\n"
            "AS"
        )
        statement_suffix = ""
        changed_keys_cte = ""
        filtered_transactions_source_join = ""
        source_date_clause = ""

    return template.format(
        statement_prefix=statement_prefix,
        statement_suffix=statement_suffix,
        changed_keys_cte=changed_keys_cte,
        filtered_transactions_source_join=filtered_transactions_source_join,
        source_table_fqn=source_table_fqn,
        quarantine_table_fqn=quarantine_table_fqn,
        source_date_clause=source_date_clause,
    )


def read_summary_watermark(args):
    try:
        rows = common.spark.sql(
            f"""
            SELECT MAX(last_successful_transaction_date) AS last_successful_transaction_date
            FROM {summary_watermark_table_fqn(args)}
            """
        ).collect()
    except Exception:
        return None

    if not rows:
        return None

    row = rows[0]
    if isinstance(row, dict):
        watermark = row.get("last_successful_transaction_date") or row.get(
            "max_transaction_date"
        )
    else:
        watermark = getattr(row, "last_successful_transaction_date", None) or getattr(
            row, "max_transaction_date", None
        )

    if watermark is not None and watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)

    return watermark


def write_summary_watermark(args, watermark_value) -> None:
    if watermark_value is None:
        return

    if watermark_value.tzinfo is None:
        watermark_value = watermark_value.replace(tzinfo=timezone.utc)

    watermark_df = common.spark.createDataFrame(
        [
            {
                "last_successful_transaction_date": watermark_value.astimezone(
                    timezone.utc
                ).replace(tzinfo=None),
                "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
        ]
    )

    watermark_df.write.format("delta").mode("overwrite").saveAsTable(
        summary_watermark_table_fqn(args)
    )


def latest_included_source_watermark(
    args,
    start_value: str | None = None,
    include_boundary: bool = True,
):
    source_table_fqn = common.table_fqn(args, args.transactions_table)
    quarantine_table_fqn = common.table_fqn(args, args.quarantine_table)
    comparison_operator = ">=" if include_boundary else ">"

    where_clauses = [
        "source.status = 'completed'",
        "COALESCE(source.is_duplicate, false) = false",
    ]
    if start_value is not None:
        where_clauses.append(
            f"source.transaction_date {comparison_operator} to_timestamp('{start_value}')"
        )

    rows = common.spark.sql(
        f"""
        SELECT MAX(source.transaction_date) AS max_transaction_date
        FROM {source_table_fqn} source
        LEFT ANTI JOIN {quarantine_table_fqn} quarantine
            ON source.transaction_id = quarantine.transaction_id
        WHERE {" AND ".join(where_clauses)}
        """
    ).collect()

    if not rows:
        return None

    row = rows[0]
    if isinstance(row, dict):
        watermark = row.get("max_transaction_date") or row.get(
            "last_successful_transaction_date"
        )
    else:
        watermark = getattr(row, "max_transaction_date", None) or getattr(
            row, "last_successful_transaction_date", None
        )

    if watermark is not None and watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)

    return watermark


def summary_start_value(watermark, lookback_hours: int) -> str:
    start = watermark - timedelta(hours=lookback_hours)
    return start.isoformat().replace("+00:00", "Z")


def run_task2(args) -> None:
    common.spark.conf.set("spark.sql.session.timeZone", "UTC")

    output_schema = ".".join(
        [common.quote_ident(args.catalog), common.quote_ident(args.output_dataset)]
    )
    common.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")

    watermark = read_summary_watermark(args)
    if watermark is None:
        common.spark.sql(render_daily_account_summary_sql(args))
        watermark = latest_included_source_watermark(args)
        mode = "Spark SQL bootstrap full refresh"
    else:
        start_value = summary_start_value(
            watermark, int(getattr(args, "lookback_hours", 0) or 0)
        )
        include_boundary = common.watermark_includes_boundary(args)
        common.spark.sql(
            render_daily_account_summary_sql(
                args,
                incremental=True,
                start_value=start_value,
                include_boundary=include_boundary,
            )
        )
        watermark = latest_included_source_watermark(
            args,
            start_value=start_value,
            include_boundary=include_boundary,
        )
        mode = "Spark SQL incremental merge"

    if watermark is not None:
        write_summary_watermark(args, watermark)

    print("=========================================")
    print("Task 2 Daily Summary")
    print("=========================================")
    print(
        "Source table  : "
        f"{common.table_fqn(args, args.transactions_table)}"
    )
    print(
        "Quarantine    : "
        f"{common.table_fqn(args, args.quarantine_table)}"
    )
    print(f"Output table  : {output_table_fqn(args)}")
    print(f"Watermark     : {summary_watermark_table_fqn(args)}")
    print(f"Mode          : {mode}")
    print("=========================================")


def parse_args():
    return build_parser().parse_args()


def main() -> None:
    args = parse_args()
    run_task2(args)


if __name__ == "__main__":
    main()
