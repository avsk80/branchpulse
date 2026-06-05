"""
Create a curated account master dataset from validated accounts.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("transform-accounts").getOrCreate()

    accounts = spark.read.parquet(args.input_path)

    result = (
        accounts.select(
            "account_id",
            "customer_id",
            "branch_id",
            "account_type",
            "account_status",
            "open_date",
            col("current_balance").cast("double").alias("current_balance"),
            "batch_date",
        )
        .withColumn(
            "balance_band",
            when(col("current_balance") >= 1000000, "HIGH")
            .when(col("current_balance") >= 100000, "MEDIUM")
            .otherwise("LOW"),
        )
    )

    result.write.mode("overwrite").parquet(args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
