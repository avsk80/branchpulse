"""
Merge daily transaction updates into an Iceberg fact table.

Iceberg MERGE gives the table ACID upsert behavior on S3, which is the main
engineering concept this project is designed to demonstrate.
"""

import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--transactions-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("merge-transactions-iceberg").getOrCreate()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{args.database}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{args.database}.fact_transactions (
            transaction_id STRING,
            account_id STRING,
            customer_id STRING,
            merchant_id STRING,
            transaction_ts TIMESTAMP,
            transaction_date DATE,
            transaction_type STRING,
            channel STRING,
            category STRING,
            amount DOUBLE,
            currency STRING,
            status STRING,
            is_suspected_fraud BOOLEAN,
            transaction_risk_band STRING,
            batch_date STRING
        )
        USING iceberg
        PARTITIONED BY (transaction_date)
        """
    )

    spark.read.parquet(args.transactions_path).createOrReplaceTempView("transaction_updates")

    spark.sql(
        f"""
        MERGE INTO glue_catalog.{args.database}.fact_transactions target
        USING transaction_updates source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN UPDATE SET
            account_id = source.account_id,
            customer_id = source.customer_id,
            merchant_id = source.merchant_id,
            transaction_ts = source.transaction_ts,
            transaction_date = source.transaction_date,
            transaction_type = source.transaction_type,
            channel = source.channel,
            category = source.category,
            amount = source.amount,
            currency = source.currency,
            status = source.status,
            is_suspected_fraud = source.is_suspected_fraud,
            transaction_risk_band = source.transaction_risk_band,
            batch_date = source.batch_date
        WHEN NOT MATCHED THEN INSERT (
            transaction_id,
            account_id,
            customer_id,
            merchant_id,
            transaction_ts,
            transaction_date,
            transaction_type,
            channel,
            category,
            amount,
            currency,
            status,
            is_suspected_fraud,
            transaction_risk_band,
            batch_date
        )
        VALUES (
            source.transaction_id,
            source.account_id,
            source.customer_id,
            source.merchant_id,
            source.transaction_ts,
            source.transaction_date,
            source.transaction_type,
            source.channel,
            source.category,
            source.amount,
            source.currency,
            source.status,
            source.is_suspected_fraud,
            source.transaction_risk_band,
            source.batch_date
        )
        """
    )

    spark.stop()


if __name__ == "__main__":
    main()
