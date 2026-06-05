#!/usr/bin/env bash
set -euo pipefail

APPLICATION_ID="${APPLICATION_ID:-replace-with-emr-serverless-application-id}"
JOB_ROLE_ARN="${JOB_ROLE_ARN:-arn:aws:iam::<account-id>:role/EMRServerlessBankingJobRole}"
BUCKET_NAME="${BUCKET_NAME:-replace-with-your-banking-data-lake-bucket}"
BATCH_DATE="${BATCH_DATE:-2026-04-25}"
DATABASE_NAME="${DATABASE_NAME:-banking_iceberg_db}"

SPARK_PARAMS="--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.glue_catalog.warehouse=s3://${BUCKET_NAME}/banking/curated/warehouse \
--conf spark.sql.defaultCatalog=glue_catalog"

aws emr-serverless start-job-run \
  --application-id "${APPLICATION_ID}" \
  --execution-role-arn "${JOB_ROLE_ARN}" \
  --name "validate-customers-${BATCH_DATE}" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${BUCKET_NAME}/banking/scripts/pyspark_jobs/validate_raw_data.py\",
      \"entryPointArguments\": [
        \"--dataset\", \"customers\",
        \"--input-path\", \"s3://${BUCKET_NAME}/banking/raw/batch_date=${BATCH_DATE}/customers.csv\",
        \"--output-path\", \"s3://${BUCKET_NAME}/banking/validated/customers/batch_date=${BATCH_DATE}\",
        \"--batch-date\", \"${BATCH_DATE}\"
      ],
      \"sparkSubmitParameters\": \"${SPARK_PARAMS}\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {
        \"logUri\": \"s3://${BUCKET_NAME}/banking/logs/\"
      }
    }
  }"

aws emr-serverless start-job-run \
  --application-id "${APPLICATION_ID}" \
  --execution-role-arn "${JOB_ROLE_ARN}" \
  --name "merge-transactions-${BATCH_DATE}" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${BUCKET_NAME}/banking/scripts/pyspark_jobs/merge_transactions_iceberg.py\",
      \"entryPointArguments\": [
        \"--database\", \"${DATABASE_NAME}\",
        \"--transactions-path\", \"s3://${BUCKET_NAME}/banking/staging/transaction_events/batch_date=${BATCH_DATE}\"
      ],
      \"sparkSubmitParameters\": \"${SPARK_PARAMS}\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {
        \"logUri\": \"s3://${BUCKET_NAME}/banking/logs/\"
      }
    }
  }"
