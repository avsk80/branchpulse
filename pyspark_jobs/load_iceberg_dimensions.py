"""
Load dimension datasets into Glue Catalog Iceberg tables.

This job refreshes small dimension tables. The fact table is handled separately
with MERGE INTO so the project can demonstrate Iceberg upserts.
"""

import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--customers-path", required=True)
    parser.add_argument("--accounts-path", required=True)
    parser.add_argument("--merchants-path", required=True)
    parser.add_argument("--branches-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load-iceberg-dimensions").getOrCreate()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{args.database}")

    spark.read.parquet(args.customers_path).createOrReplaceTempView("customer_profile")
    spark.read.parquet(args.accounts_path).createOrReplaceTempView("account_master")
    spark.read.parquet(args.merchants_path).createOrReplaceTempView("merchant_master")
    spark.read.parquet(args.branches_path).createOrReplaceTempView("branch_master")

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE glue_catalog.{args.database}.dim_customers
        USING iceberg
        AS SELECT * FROM customer_profile
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE glue_catalog.{args.database}.dim_accounts
        USING iceberg
        AS SELECT * FROM account_master
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE glue_catalog.{args.database}.dim_merchants
        USING iceberg
        AS SELECT * FROM merchant_master
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE glue_catalog.{args.database}.dim_branches
        USING iceberg
        AS SELECT * FROM branch_master
        """
    )

    spark.stop()


if __name__ == "__main__":
    main()
