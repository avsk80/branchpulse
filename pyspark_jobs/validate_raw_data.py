"""
Validate one raw banking CSV file and write clean Parquet output.

This job is reused by Airflow for customers, accounts, transactions, and other
datasets. Keep it simple: read CSV, remove bad rows, drop duplicates, write.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


REQUIRED_COLUMNS = {
    "customers": ["customer_id", "kyc_status", "home_branch_id"],
    "accounts": ["account_id", "customer_id", "branch_id"],
    "transactions": ["transaction_id", "account_id", "customer_id", "amount"],
    "merchants": ["merchant_id", "merchant_category", "risk_level"],
    "branches": ["branch_id", "city", "region"],
    "cards": ["card_id", "account_id", "card_status"],
    "loan_applications": ["application_id", "customer_id", "requested_amount"],
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--batch-date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"validate-{args.dataset}").getOrCreate()

    df = spark.read.option("header", True).option("inferSchema", True).csv(args.input_path)

    # Reject rows where mandatory business keys are missing.
    for column_name in REQUIRED_COLUMNS[args.dataset]:
        df = df.filter(col(column_name).isNotNull())

    # The first required column is the natural unique key for each source file.
    unique_key = REQUIRED_COLUMNS[args.dataset][0]
    clean_df = (
        df.dropDuplicates([unique_key])
        .withColumn("batch_date", lit(args.batch_date))
        .withColumn("validated_at", current_timestamp())
    )

    clean_df.write.mode("overwrite").parquet(args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
