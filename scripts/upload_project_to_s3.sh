#!/usr/bin/env bash
set -euo pipefail

BUCKET_NAME="${1:-replace-with-your-banking-data-lake-bucket}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Uploading PySpark jobs to s3://${BUCKET_NAME}/banking/scripts/pyspark_jobs/"
aws s3 sync "${PROJECT_ROOT}/pyspark_jobs/" "s3://${BUCKET_NAME}/banking/scripts/pyspark_jobs/" --delete

echo "Uploading Airflow DAG backup to s3://${BUCKET_NAME}/banking/scripts/airflow/"
aws s3 sync "${PROJECT_ROOT}/airflow/dags/" "s3://${BUCKET_NAME}/banking/scripts/airflow/" --delete

echo "Uploading SQL files to s3://${BUCKET_NAME}/banking/scripts/sql/"
aws s3 sync "${PROJECT_ROOT}/sql/" "s3://${BUCKET_NAME}/banking/scripts/sql/" --delete

echo "Upload complete."
