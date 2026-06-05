-- Business-facing Athena queries for demos and interviews.

-- 1. Daily transaction volume by banking channel.
SELECT
  transaction_date,
  channel,
  COUNT(*) AS transaction_count,
  SUM(amount) AS total_amount
FROM banking_iceberg_db.fact_transactions
GROUP BY transaction_date, channel
ORDER BY transaction_date DESC, total_amount DESC;

-- 2. Highest-risk merchant categories.
SELECT
  m.merchant_category,
  m.risk_level,
  COUNT(f.transaction_id) AS transaction_count,
  SUM(f.amount) AS total_amount
FROM banking_iceberg_db.fact_transactions f
JOIN banking_iceberg_db.dim_merchants m
  ON f.merchant_id = m.merchant_id
WHERE f.transaction_risk_band = 'HIGH'
GROUP BY m.merchant_category, m.risk_level
ORDER BY total_amount DESC;

-- 3. Branch regions with large transaction value.
SELECT
  b.region,
  b.state,
  b.city,
  COUNT(*) AS transaction_count,
  SUM(f.amount) AS total_amount
FROM banking_iceberg_db.fact_transactions f
JOIN banking_iceberg_db.dim_accounts a
  ON f.account_id = a.account_id
JOIN banking_iceberg_db.dim_branches b
  ON a.branch_id = b.branch_id
GROUP BY b.region, b.state, b.city
ORDER BY total_amount DESC
LIMIT 20;

-- 4. Customer risk by segment.
SELECT
  customer_segment,
  customer_risk_band,
  customer_count,
  transaction_count,
  total_transaction_amount
FROM banking_iceberg_db.customer_risk_summary
ORDER BY total_transaction_amount DESC;

-- 5. Iceberg time travel example.
-- Replace the timestamp with a real point in time after multiple pipeline runs.
SELECT COUNT(*)
FROM banking_iceberg_db.fact_transactions
FOR TIMESTAMP AS OF TIMESTAMP '2026-04-25 10:00:00';
