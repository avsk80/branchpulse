"""
Create a curated customer profile dataset from validated raw customers.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, when


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("transform-customers").getOrCreate()

    customers = spark.read.parquet(args.input_path)

    result = (
        customers.select(
            "customer_id",
            concat_ws(" ", col("first_name"), col("last_name")).alias("customer_name"),
            "city",
            "state",
            "kyc_status",
            "customer_segment",
            "home_branch_id",
            "created_at",
            "batch_date",
        )
        .withColumn(
            "customer_risk_band",
            when(col("kyc_status") == "REJECTED", "HIGH")
            .when(col("kyc_status") == "PENDING", "MEDIUM")
            .otherwise("LOW"),
        )
    )

    result.write.mode("overwrite").parquet(args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
