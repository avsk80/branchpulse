"""
Airflow DAG for the banking batch Iceberg pipeline.

The DAG intentionally has both sequential and parallel sections so it works
well as a portfolio/demo project.
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


AWS_CONN_ID = Variable.get("BANKING_AWS_CONN_ID", default_var="aws_default")
AWS_REGION = Variable.get("BANKING_AWS_REGION", default_var="us-east-1")
BUCKET = Variable.get("BANKING_BUCKET")
GLUE_DB = Variable.get("BANKING_GLUE_DB", default_var="banking_iceberg_db")
EMR_JOB_ROLE_ARN = Variable.get("BANKING_EMR_JOB_ROLE_ARN")
EMR_APPLICATION_ID = Variable.get(
    "BANKING_EMR_APPLICATION_ID",
    default_var="replace-with-emr-serverless-application-id",
)

RAW_PREFIX = "banking/raw"
VALIDATED_PREFIX = "banking/validated"
STAGING_PREFIX = "banking/staging"
SCRIPTS_PREFIX = "banking/scripts/pyspark_jobs"
LOGS_PREFIX = "banking/logs"
ATHENA_RESULTS = f"s3://{BUCKET}/banking/athena-results/"

# Manual DAG runs can pass {"batch_date": "YYYY-MM-DD"}.
# If not passed, the pipeline falls back to Airflow's logical date.
BATCH_DATE = "{{ dag_run.conf.get('batch_date', ds) }}"

SPARK_SUBMIT_PARAMETERS = (
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
    f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{BUCKET}/banking/curated/warehouse "
    "--conf spark.sql.defaultCatalog=glue_catalog "
    "--conf spark.executor.cores=2 "
    "--conf spark.executor.memory=4g "
    "--conf spark.driver.cores=2 "
    "--conf spark.driver.memory=4g"
)


def raw_file_sensor(dataset_name):
    return S3KeySensor(
        task_id=f"check_{dataset_name}_raw_file",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=BUCKET,
        bucket_key=f"{RAW_PREFIX}/batch_date={BATCH_DATE}/{dataset_name}.csv",
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
    )


def emr_spark_job(task_id, script_name, arguments):
    return EmrServerlessStartJobOperator(
        task_id=task_id,
        aws_conn_id=AWS_CONN_ID,
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{BUCKET}/{SCRIPTS_PREFIX}/{script_name}",
                "entryPointArguments": arguments,
                "sparkSubmitParameters": SPARK_SUBMIT_PARAMETERS,
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{BUCKET}/{LOGS_PREFIX}/"
                }
            }
        },
        wait_for_completion=True,
    )


with DAG(
    dag_id="banking_batch_iceberg_pipeline",
    description="Daily banking batch pipeline using S3, EMR Serverless, PySpark, Iceberg, Athena, and Airflow.",
    start_date=datetime(2026, 4, 24),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=16,
    tags=["banking", "batch", "emr-serverless", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    check_customers = raw_file_sensor("customers")
    check_accounts = raw_file_sensor("accounts")
    check_transactions = raw_file_sensor("transactions")
    check_merchants = raw_file_sensor("merchants")
    check_branches = raw_file_sensor("branches")
    check_cards = raw_file_sensor("cards")
    check_loans = raw_file_sensor("loan_applications")

    use_existing_application = EmptyOperator(task_id="use_existing_emr_serverless_application")

    validate_customers = emr_spark_job(
        "validate_customers",
        "validate_raw_data.py",
        [
            "--dataset", "customers",
            "--input-path", f"s3://{BUCKET}/{RAW_PREFIX}/batch_date={BATCH_DATE}/customers.csv",
            "--output-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/customers/batch_date={BATCH_DATE}",
            "--batch-date", BATCH_DATE,
        ],
    )

    validate_accounts = emr_spark_job(
        "validate_accounts",
        "validate_raw_data.py",
        [
            "--dataset", "accounts",
            "--input-path", f"s3://{BUCKET}/{RAW_PREFIX}/batch_date={BATCH_DATE}/accounts.csv",
            "--output-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/accounts/batch_date={BATCH_DATE}",
            "--batch-date", BATCH_DATE,
        ],
    )

    validate_transactions = emr_spark_job(
        "validate_transactions",
        "validate_raw_data.py",
        [
            "--dataset", "transactions",
            "--input-path", f"s3://{BUCKET}/{RAW_PREFIX}/batch_date={BATCH_DATE}/transactions.csv",
            "--output-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/transactions/batch_date={BATCH_DATE}",
            "--batch-date", BATCH_DATE,
        ],
    )

    validate_merchants = emr_spark_job(
        "validate_merchants",
        "validate_raw_data.py",
        [
            "--dataset", "merchants",
            "--input-path", f"s3://{BUCKET}/{RAW_PREFIX}/batch_date={BATCH_DATE}/merchants.csv",
            "--output-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/merchants/batch_date={BATCH_DATE}",
            "--batch-date", BATCH_DATE,
        ],
    )

    validate_branches = emr_spark_job(
        "validate_branches",
        "validate_raw_data.py",
        [
            "--dataset", "branches",
            "--input-path", f"s3://{BUCKET}/{RAW_PREFIX}/batch_date={BATCH_DATE}/branches.csv",
            "--output-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/branches/batch_date={BATCH_DATE}",
            "--batch-date", BATCH_DATE,
        ],
    )

    validate_cards = emr_spark_job(
        "validate_cards",
        "validate_raw_data.py",
        [
            "--dataset", "cards",
            "--input-path", f"s3://{BUCKET}/{RAW_PREFIX}/batch_date={BATCH_DATE}/cards.csv",
            "--output-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/cards/batch_date={BATCH_DATE}",
            "--batch-date", BATCH_DATE,
        ],
    )

    validate_loans = emr_spark_job(
        "validate_loan_applications",
        "validate_raw_data.py",
        [
            "--dataset", "loan_applications",
            "--input-path", f"s3://{BUCKET}/{RAW_PREFIX}/batch_date={BATCH_DATE}/loan_applications.csv",
            "--output-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/loan_applications/batch_date={BATCH_DATE}",
            "--batch-date", BATCH_DATE,
        ],
    )

    transform_customers = emr_spark_job(
        "transform_customers",
        "transform_customers.py",
        [
            "--input-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/customers/batch_date={BATCH_DATE}",
            "--output-path", f"s3://{BUCKET}/{STAGING_PREFIX}/customer_profile/batch_date={BATCH_DATE}",
        ],
    )

    transform_accounts = emr_spark_job(
        "transform_accounts",
        "transform_accounts.py",
        [
            "--input-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/accounts/batch_date={BATCH_DATE}",
            "--output-path", f"s3://{BUCKET}/{STAGING_PREFIX}/account_master/batch_date={BATCH_DATE}",
        ],
    )

    transform_transactions = emr_spark_job(
        "transform_transactions",
        "transform_transactions.py",
        [
            "--transactions-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/transactions/batch_date={BATCH_DATE}",
            "--merchants-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/merchants/batch_date={BATCH_DATE}",
            "--output-path", f"s3://{BUCKET}/{STAGING_PREFIX}/transaction_events/batch_date={BATCH_DATE}",
        ],
    )

    load_dimensions = emr_spark_job(
        "load_iceberg_dimensions",
        "load_iceberg_dimensions.py",
        [
            "--database", GLUE_DB,
            "--customers-path", f"s3://{BUCKET}/{STAGING_PREFIX}/customer_profile/batch_date={BATCH_DATE}",
            "--accounts-path", f"s3://{BUCKET}/{STAGING_PREFIX}/account_master/batch_date={BATCH_DATE}",
            "--merchants-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/merchants/batch_date={BATCH_DATE}",
            "--branches-path", f"s3://{BUCKET}/{VALIDATED_PREFIX}/branches/batch_date={BATCH_DATE}",
        ],
    )

    merge_transactions = emr_spark_job(
        "merge_transactions_into_iceberg",
        "merge_transactions_iceberg.py",
        [
            "--database", GLUE_DB,
            "--transactions-path", f"s3://{BUCKET}/{STAGING_PREFIX}/transaction_events/batch_date={BATCH_DATE}",
        ],
    )

    build_daily_branch_summary = emr_spark_job(
        "build_daily_branch_summary",
        "build_analytics_tables.py",
        ["--database", GLUE_DB, "--summary-name", "daily_branch"],
    )

    build_customer_risk_summary = emr_spark_job(
        "build_customer_risk_summary",
        "build_analytics_tables.py",
        ["--database", GLUE_DB, "--summary-name", "customer_risk"],
    )

    build_merchant_spend_summary = emr_spark_job(
        "build_merchant_spend_summary",
        "build_analytics_tables.py",
        ["--database", GLUE_DB, "--summary-name", "merchant_spend"],
    )

    build_account_balance_snapshot = emr_spark_job(
        "build_account_balance_snapshot",
        "build_analytics_tables.py",
        ["--database", GLUE_DB, "--summary-name", "account_balance"],
    )

    check_fact_table = AthenaOperator(
        task_id="athena_check_fact_transactions",
        aws_conn_id=AWS_CONN_ID,
        database=GLUE_DB,
        output_location=ATHENA_RESULTS,
        query="SELECT COUNT(*) AS transaction_count FROM fact_transactions",
    )

    check_high_risk_transactions = AthenaOperator(
        task_id="athena_check_high_risk_transactions",
        aws_conn_id=AWS_CONN_ID,
        database=GLUE_DB,
        output_location=ATHENA_RESULTS,
        query="""
        SELECT transaction_risk_band, COUNT(*) AS row_count
        FROM fact_transactions
        GROUP BY transaction_risk_band
        """,
    )

    emr_auto_stop_note = EmptyOperator(task_id="emr_application_auto_stops_after_idle_timeout")

    raw_checks = [
        check_customers,
        check_accounts,
        check_transactions,
        check_merchants,
        check_branches,
        check_cards,
        check_loans,
    ]

    validation_jobs = [
        validate_customers,
        validate_accounts,
        validate_transactions,
        validate_merchants,
        validate_branches,
        validate_cards,
        validate_loans,
    ]

    analytics_jobs = [
        build_daily_branch_summary,
        build_customer_risk_summary,
        build_merchant_spend_summary,
        build_account_balance_snapshot,
    ]

    start >> raw_checks

    for raw_check in raw_checks:
        raw_check >> use_existing_application

    use_existing_application >> validation_jobs

    validate_customers >> transform_customers
    validate_accounts >> transform_accounts
    validate_transactions >> transform_transactions
    validate_merchants >> transform_transactions

    transform_customers >> load_dimensions
    transform_accounts >> load_dimensions
    validate_merchants >> load_dimensions
    validate_branches >> load_dimensions

    load_dimensions >> merge_transactions
    transform_transactions >> merge_transactions

    merge_transactions >> analytics_jobs

    for analytics_job in analytics_jobs:
        analytics_job >> check_fact_table
        analytics_job >> check_high_risk_transactions

    check_fact_table >> emr_auto_stop_note
    check_high_risk_transactions >> emr_auto_stop_note
    emr_auto_stop_note >> end
