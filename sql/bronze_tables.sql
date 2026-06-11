CREATE SCHEMA IF NOT EXISTS main.bronze;

CREATE TABLE IF NOT EXISTS main.bronze.transactions_test (
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
USING DELTA;

CREATE TABLE IF NOT EXISTS main.bronze.quarantine_test (
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
USING DELTA;

CREATE TABLE IF NOT EXISTS main.bronze.ingestion_watermark_test (
  source_name STRING,
  last_successful_transaction_date TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA;
