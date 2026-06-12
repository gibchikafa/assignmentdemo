CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.gold;

CREATE TABLE IF NOT EXISTS workspace.bronze.gibson_eletrolux_transactions_test (
  transaction_id STRING,
  account_id STRING,
  transaction_date TIMESTAMP,
  amount DECIMAL(18,2),
  currency STRING,
  transaction_type STRING,
  merchant_name STRING,
  merchant_category STRING,
  status STRING,
  country_code STRING,
  ingestion_timestamp TIMESTAMP,
  natural_key STRING,
  natural_key_hash STRING,
  is_duplicate BOOLEAN
)
USING DELTA
PARTITIONED BY (country_code);

CREATE TABLE IF NOT EXISTS workspace.bronze.gibson_eletrolux_quarantine_test (
  transaction_id STRING,
  account_id STRING,
  transaction_date TIMESTAMP,
  amount DECIMAL(18,2),
  currency STRING,
  transaction_type STRING,
  merchant_name STRING,
  merchant_category STRING,
  status STRING,
  country_code STRING,
  ingestion_timestamp TIMESTAMP,
  natural_key STRING,
  natural_key_hash STRING,
  is_duplicate BOOLEAN,
  error_reason STRING
)
USING DELTA
PARTITIONED BY (country_code);

CREATE TABLE IF NOT EXISTS workspace.bronze.gibson_eletrolux_ingestion_watermark_test (
  source_name STRING,
  last_successful_transaction_date TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.bronze.gibson_eletrolux_ingestion_run_log_test (
  run_id STRING,
  pipeline STRING,
  source_type STRING,
  source_name STRING,
  catalog_name STRING,
  schema_name STRING,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  input_records BIGINT,
  valid_records BIGINT,
  quarantine_records BIGINT,
  duplicate_records BIGINT,
  lookback_hours INT,
  watermark_value TIMESTAMP,
  status STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.gibson_eletrolux_daily_summary_watermark_test (
  last_successful_transaction_date TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA;
