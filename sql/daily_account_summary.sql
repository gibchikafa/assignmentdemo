{statement_prefix}
WITH {changed_keys_cte}filtered_transactions AS (
    SELECT
        source.account_id,
        CAST(source.transaction_date AS DATE) AS transaction_date,
        source.amount,
        source.currency,
        source.transaction_type,
        source.merchant_name,
        source.merchant_category,
        source.ingestion_timestamp
    FROM {source_table_fqn} source
{filtered_transactions_source_join}
    LEFT ANTI JOIN {quarantine_table_fqn} quarantine
        ON source.transaction_id = quarantine.transaction_id
    WHERE source.status = 'completed'
      AND COALESCE(source.is_duplicate, false) = false
{source_date_clause}
),
daily_account_totals AS (
    SELECT
        account_id,
        transaction_date,
        CAST(
            SUM(
                CASE
                    WHEN transaction_type = 'debit' THEN amount
                    ELSE CAST(0 AS DECIMAL(18, 2))
                END
            ) AS DECIMAL(18, 2)
        ) AS total_debit_amount,
        CAST(
            SUM(
                CASE
                    WHEN transaction_type = 'credit' THEN amount
                    ELSE CAST(0 AS DECIMAL(18, 2))
                END
            ) AS DECIMAL(18, 2)
        ) AS total_credit_amount,
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT merchant_name) AS distinct_merchants,
        ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(currency)), ',') AS currencies,
        MAX(ingestion_timestamp) AS updated_at
    FROM filtered_transactions
    GROUP BY account_id, transaction_date
),
category_spend AS (
    SELECT
        account_id,
        transaction_date,
        merchant_category,
        CAST(
            SUM(
                CASE
                    WHEN transaction_type = 'debit' THEN amount
                    ELSE CAST(0 AS DECIMAL(18, 2))
                END
            ) AS DECIMAL(18, 2)
        ) AS category_debit_amount
    FROM filtered_transactions
    GROUP BY account_id, transaction_date, merchant_category
),
ranked_categories AS (
    SELECT
        account_id,
        transaction_date,
        merchant_category,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, transaction_date
            ORDER BY category_debit_amount DESC, merchant_category ASC
        ) AS category_rank
    FROM category_spend
    WHERE category_debit_amount > CAST(0 AS DECIMAL(18, 2))
)
SELECT
    totals.account_id,
    totals.transaction_date,
    totals.total_debit_amount,
    totals.total_credit_amount,
    CAST(
        totals.total_credit_amount - totals.total_debit_amount
        AS DECIMAL(18, 2)
    ) AS net_amount,
    totals.transaction_count,
    totals.distinct_merchants,
    ranked_categories.merchant_category AS top_category,
    totals.currencies,
    totals.updated_at
FROM daily_account_totals totals
LEFT JOIN ranked_categories
    ON totals.account_id = ranked_categories.account_id
   AND totals.transaction_date = ranked_categories.transaction_date
   AND ranked_categories.category_rank = 1
{statement_suffix}
