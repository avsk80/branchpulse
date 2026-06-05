-- Run in Athena after creating the Glue database.
-- Replace the S3 bucket name before running.

CREATE DATABASE IF NOT EXISTS banking_iceberg_db;

CREATE TABLE IF NOT EXISTS banking_iceberg_db.fact_transactions (
  transaction_id string,
  account_id string,
  customer_id string,
  merchant_id string,
  transaction_ts timestamp,
  transaction_date date,
  transaction_type string,
  channel string,
  category string,
  amount double,
  currency string,
  status string,
  is_suspected_fraud boolean,
  transaction_risk_band string,
  batch_date string
)
PARTITIONED BY (transaction_date)
LOCATION 's3://replace-with-your-banking-data-lake-bucket/banking/curated/warehouse/fact_transactions/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS banking_iceberg_db.dim_customers (
  customer_id string,
  customer_name string,
  city string,
  state string,
  kyc_status string,
  customer_segment string,
  home_branch_id string,
  created_at string,
  batch_date string,
  customer_risk_band string
)
LOCATION 's3://replace-with-your-banking-data-lake-bucket/banking/curated/warehouse/dim_customers/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS banking_iceberg_db.dim_accounts (
  account_id string,
  customer_id string,
  branch_id string,
  account_type string,
  account_status string,
  open_date string,
  current_balance double,
  batch_date string,
  balance_band string
)
LOCATION 's3://replace-with-your-banking-data-lake-bucket/banking/curated/warehouse/dim_accounts/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS banking_iceberg_db.dim_merchants (
  merchant_id string,
  merchant_name string,
  merchant_category string,
  risk_level string,
  batch_date string,
  validated_at timestamp
)
LOCATION 's3://replace-with-your-banking-data-lake-bucket/banking/curated/warehouse/dim_merchants/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS banking_iceberg_db.dim_branches (
  branch_id string,
  branch_name string,
  city string,
  state string,
  region string,
  batch_date string,
  validated_at timestamp
)
LOCATION 's3://replace-with-your-banking-data-lake-bucket/banking/curated/warehouse/dim_branches/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'PARQUET'
);
