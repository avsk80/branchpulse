"""
Build simple analytics Iceberg tables from the curated fact and dimensions.

Pass --summary-name to build one table at a time. Airflow uses that option to
run multiple analytics jobs in parallel.
"""

import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument(
        "--summary-name",
        choices=["daily_branch", "customer_risk", "merchant_spend", "account_balance", "all"],
        default="all",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("build-banking-analytics").getOrCreate()
    db = f"glue_catalog.{args.database}"

    if args.summary_name in ("daily_branch", "all"):
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {db}.daily_branch_summary
            USING iceberg
            AS
            SELECT
                f.transaction_date,
                b.region,
                b.state,
                b.city,
                COUNT(*) AS transaction_count,
                SUM(f.amount) AS total_amount,
                SUM(CASE WHEN f.transaction_risk_band = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_count
            FROM {db}.fact_transactions f
            JOIN {db}.dim_accounts a ON f.account_id = a.account_id
            JOIN {db}.dim_branches b ON a.branch_id = b.branch_id
            GROUP BY f.transaction_date, b.region, b.state, b.city
            """
        )

    if args.summary_name in ("customer_risk", "all"):
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {db}.customer_risk_summary
            USING iceberg
            AS
            SELECT
                c.customer_segment,
                c.customer_risk_band,
                COUNT(DISTINCT c.customer_id) AS customer_count,
                COUNT(f.transaction_id) AS transaction_count,
                SUM(f.amount) AS total_transaction_amount
            FROM {db}.dim_customers c
            LEFT JOIN {db}.fact_transactions f ON c.customer_id = f.customer_id
            GROUP BY c.customer_segment, c.customer_risk_band
            """
        )

    if args.summary_name in ("merchant_spend", "all"):
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {db}.merchant_spend_summary
            USING iceberg
            AS
            SELECT
                m.merchant_category,
                m.risk_level,
                COUNT(f.transaction_id) AS transaction_count,
                SUM(f.amount) AS total_amount
            FROM {db}.dim_merchants m
            LEFT JOIN {db}.fact_transactions f ON m.merchant_id = f.merchant_id
            GROUP BY m.merchant_category, m.risk_level
            """
        )

    if args.summary_name in ("account_balance", "all"):
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {db}.account_balance_snapshot
            USING iceberg
            AS
            SELECT
                account_type,
                account_status,
                balance_band,
                COUNT(*) AS account_count,
                SUM(current_balance) AS total_balance,
                AVG(current_balance) AS average_balance
            FROM {db}.dim_accounts
            GROUP BY account_type, account_status, balance_band
            """
        )

    spark.stop()


if __name__ == "__main__":
    main()
