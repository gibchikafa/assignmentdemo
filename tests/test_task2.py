from types import SimpleNamespace

from ingestion import common
import entrypoint_summaries


def make_args(**overrides):
    base = {
        "catalog": "workspace",
        "dataset": "bronze",
        "transactions_table": "gibson_eletrolux_transactions_test",
        "quarantine_table": "gibson_eletrolux_quarantine_test",
        "output_dataset": "gold",
        "output_table": "daily_account_summary",
        "sql_template": "sql/daily_account_summary.sql",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def squash_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_task2_parser_defaults():
    args = entrypoint_summaries.build_parser().parse_args([])

    assert args.source_type == "file"
    assert args.catalog == "workspace"
    assert args.dataset == "bronze"
    assert args.transactions_table == "gibson_eletrolux_transactions_test"
    assert args.quarantine_table == "gibson_eletrolux_quarantine_test"
    assert args.output_dataset == "gold"
    assert args.output_table == "daily_account_summary"
    assert args.sql_template.endswith("sql/daily_account_summary.sql")


def test_render_daily_account_summary_sql_includes_assignment_rules():
    sql = entrypoint_summaries.render_daily_account_summary_sql(make_args())
    rendered = squash_sql(sql).upper()

    assert "CREATE OR REPLACE TABLE" in rendered
    assert "`WORKSPACE`.`BRONZE`.`GIBSON_ELETROLUX_TRANSACTIONS_TEST`" in rendered
    assert "`WORKSPACE`.`BRONZE`.`GIBSON_ELETROLUX_QUARANTINE_TEST`" in rendered
    assert "`WORKSPACE`.`GOLD`.`DAILY_ACCOUNT_SUMMARY`" in rendered
    assert "PARTITIONED BY (TRANSACTION_DATE)" in rendered
    assert "STATUS = 'COMPLETED'" in rendered
    assert "COALESCE(SOURCE.IS_DUPLICATE, FALSE) = FALSE" in rendered
    assert "LEFT ANTI JOIN" in rendered
    assert "ROW_NUMBER() OVER" in rendered
    assert "ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(CURRENCY)), ',')" in rendered
    assert "MAX(INGESTION_TIMESTAMP)" in rendered
    assert "TOTALS.TOTAL_CREDIT_AMOUNT - TOTALS.TOTAL_DEBIT_AMOUNT" in rendered
    assert "CURRENT_TIMESTAMP" not in rendered


def test_run_task2_creates_schema_and_executes_sql():
    common.spark.sql_queries[:] = []

    entrypoint_summaries.run_task2(make_args())

    assert common.spark.sql_queries[0] == "CREATE SCHEMA IF NOT EXISTS `workspace`.`gold`"
    assert "CREATE OR REPLACE TABLE `workspace`.`gold`.`daily_account_summary`" in common.spark.sql_queries[1]
