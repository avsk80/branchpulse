"""
Create clean transaction events and add simple risk flags.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, when


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transactions-path", required=True)
    parser.add_argument("--merchants-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("transform-transactions").getOrCreate()

    transactions = spark.read.parquet(args.transactions_path)
    merchants = spark.read.parquet(args.merchants_path).select("merchant_id", "risk_level")

    result = (
        transactions.join(merchants, "merchant_id", "left")
        .withColumn("transaction_ts", to_timestamp("transaction_ts"))
        .withColumn("transaction_date", to_date("transaction_ts"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn(
            "transaction_risk_band",
            when(col("is_suspected_fraud") == True, "HIGH")
            .when(col("risk_level") == "HIGH", "HIGH")
            .when(col("amount") >= 100000, "MEDIUM")
            .otherwise("LOW"),
        )
        .select(
            "transaction_id",
            "account_id",
            "customer_id",
            "merchant_id",
            "transaction_ts",
            "transaction_date",
            "transaction_type",
            "channel",
            "category",
            "amount",
            "currency",
            "status",
            "is_suspected_fraud",
            "transaction_risk_band",
            "batch_date",
        )
    )

    result.write.mode("overwrite").parquet(args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
