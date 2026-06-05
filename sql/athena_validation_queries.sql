-- Basic checks after the Airflow DAG completes.

SELECT COUNT(*) AS transaction_count
FROM banking_iceberg_db.fact_transactions;

SELECT transaction_date, COUNT(*) AS transaction_count, SUM(amount) AS total_amount
FROM banking_iceberg_db.fact_transactions
GROUP BY transaction_date
ORDER BY transaction_date DESC;

SELECT transaction_risk_band, COUNT(*) AS transaction_count
FROM banking_iceberg_db.fact_transactions
GROUP BY transaction_risk_band;

SELECT COUNT(*) AS high_risk_success_txns
FROM banking_iceberg_db.fact_transactions
WHERE transaction_risk_band = 'HIGH'
  AND status = 'SUCCESS';

SELECT COUNT(*) AS customer_count
FROM banking_iceberg_db.dim_customers;

SELECT COUNT(*) AS account_count
FROM banking_iceberg_db.dim_accounts;
