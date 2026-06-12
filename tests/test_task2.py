from types import SimpleNamespace
from datetime import datetime, timezone

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
        "summary_watermark_table": "gibson_eletrolux_daily_summary_watermark_test",
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
    assert args.summary_watermark_table == "gibson_eletrolux_daily_summary_watermark_test"
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


def test_render_daily_account_summary_sql_incremental_targets_only_changed_keys():
    sql = entrypoint_summaries.render_daily_account_summary_sql(
        make_args(),
        incremental=True,
        start_value="2026-01-01T12:00:00Z",
        include_boundary=False,
    )
    rendered = squash_sql(sql).upper()

    assert "MERGE INTO" in rendered
    assert "CHANGED_KEYS" in rendered
    assert "INNER JOIN CHANGED_KEYS KEYS" in rendered
    assert "TO_TIMESTAMP('2026-01-01T12:00:00Z')" in rendered
    assert "SOURCE.TRANSACTION_DATE > TO_TIMESTAMP('2026-01-01T12:00:00Z')" in rendered
    assert "ON TARGET.ACCOUNT_ID = SUMMARY_SOURCE.ACCOUNT_ID" in rendered
    assert "AND TARGET.TRANSACTION_DATE = SUMMARY_SOURCE.TRANSACTION_DATE" in rendered
    assert "CREATE OR REPLACE TABLE" not in rendered
    assert "`WORKSPACE`.`GOLD`.`DAILY_ACCOUNT_SUMMARY`" in rendered


def test_run_task2_bootstraps_when_summary_watermark_is_missing():
    common.spark.sql_queries[:] = []
    create_schema_result = common.spark.createDataFrame([])
    watermark_lookup_result = common.spark.createDataFrame([])
    summary_watermark_result = common.spark.createDataFrame(
        [{"max_transaction_date": datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)}]
    )
    row_count_result = common.spark.createDataFrame([{"computed_rows": 7}])
    common.spark.created_dataframes[:] = []
    common.spark.sql_results[:] = [
        create_schema_result,
        watermark_lookup_result,
        common.spark.createDataFrame([]),
        summary_watermark_result,
        row_count_result,
    ]

    entrypoint_summaries.run_task2(make_args())

    assert common.spark.sql_queries[0] == "CREATE SCHEMA IF NOT EXISTS `workspace`.`gold`"
    assert (
        "SELECT MAX(LAST_SUCCESSFUL_TRANSACTION_DATE) AS LAST_SUCCESSFUL_TRANSACTION_DATE"
        in common.spark.sql_queries[1].upper()
    )
    assert "CREATE OR REPLACE TABLE `workspace`.`gold`.`daily_account_summary`" in common.spark.sql_queries[2]
    assert (
        "SELECT MAX(SOURCE.TRANSACTION_DATE) AS MAX_TRANSACTION_DATE"
        in common.spark.sql_queries[3].upper()
    )
    assert common.spark.created_dataframes[-1].saved_table == (
        "`workspace`.`gold`.`gibson_eletrolux_daily_summary_watermark_test`"
    )


def test_run_task2_uses_incremental_merge_when_summary_watermark_exists():
    common.spark.sql_queries[:] = []
    create_schema_result = common.spark.createDataFrame([])
    watermark_lookup_result = common.spark.createDataFrame(
        [
            {
                "last_successful_transaction_date": datetime(
                    2026, 1, 1, 12, 0, tzinfo=timezone.utc
                )
            }
        ]
    )
    summary_watermark_result = common.spark.createDataFrame(
        [{"max_transaction_date": datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)}]
    )
    row_count_result = common.spark.createDataFrame([{"computed_rows": 3}])
    common.spark.created_dataframes[:] = []
    common.spark.sql_results[:] = [
        create_schema_result,
        watermark_lookup_result,
        common.spark.createDataFrame([]),
        summary_watermark_result,
        row_count_result,
    ]

    entrypoint_summaries.run_task2(make_args())

    assert common.spark.sql_queries[0] == "CREATE SCHEMA IF NOT EXISTS `workspace`.`gold`"
    assert (
        "SELECT MAX(LAST_SUCCESSFUL_TRANSACTION_DATE) AS LAST_SUCCESSFUL_TRANSACTION_DATE"
        in common.spark.sql_queries[1].upper()
    )
    assert "MERGE INTO `workspace`.`gold`.`daily_account_summary`" in common.spark.sql_queries[2]
    assert (
        "SELECT MAX(SOURCE.TRANSACTION_DATE) AS MAX_TRANSACTION_DATE"
        in common.spark.sql_queries[3].upper()
    )
    assert "COMPUTED_ROWS" in common.spark.sql_queries[4].upper()


def test_run_task2_incremental_with_no_new_data_keeps_existing_summary_state(capsys):
    common.spark.sql_queries[:] = []
    create_schema_result = common.spark.createDataFrame([])
    watermark_lookup_result = common.spark.createDataFrame(
        [
            {
                "last_successful_transaction_date": datetime(
                    2026, 1, 1, 12, 0, tzinfo=timezone.utc
                )
            }
        ]
    )
    summary_watermark_result = common.spark.createDataFrame([])
    row_count_result = common.spark.createDataFrame([{"computed_rows": 0}])
    common.spark.created_dataframes[:] = []
    common.spark.sql_results[:] = [
        create_schema_result,
        watermark_lookup_result,
        common.spark.createDataFrame([]),
        summary_watermark_result,
        row_count_result,
    ]

    entrypoint_summaries.run_task2(make_args())

    captured = capsys.readouterr().out

    assert common.spark.sql_queries[0] == "CREATE SCHEMA IF NOT EXISTS `workspace`.`gold`"
    assert (
        "SELECT MAX(LAST_SUCCESSFUL_TRANSACTION_DATE) AS LAST_SUCCESSFUL_TRANSACTION_DATE"
        in common.spark.sql_queries[1].upper()
    )
    assert "MERGE INTO `workspace`.`gold`.`daily_account_summary`" in common.spark.sql_queries[2]
    assert "CHANGED_KEYS" in common.spark.sql_queries[2].upper()
    assert common.spark.sql_queries[3].strip().upper().startswith(
        "SELECT MAX(SOURCE.TRANSACTION_DATE) AS MAX_TRANSACTION_DATE"
    )
    assert "COMPUTED_ROWS" in common.spark.sql_queries[4].upper()
    assert "Rows computed : 0" in captured
    assert all(df.saved_table is None for df in common.spark.created_dataframes)
