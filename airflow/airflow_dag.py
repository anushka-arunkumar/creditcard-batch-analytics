import re
import logging
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
GCP_PROJECT_ID = "project-5b09c9f0-c89b-4689-9bb"
GCS_BUCKET = "creditcard-batch-analytics-gcs"
GCP_CONN_ID = "google_cloud_default"
DATAPROC_REGION = "us-central1"
GCS_SOURCE_PREFIX = "transactions/"
GCS_FILE_PREFIX = "transactions/transactions_"

DATAPROC_SERVICE_ACCOUNT = (
    "credit-card-airflow-sa@project-5b09c9f0-c89b-4689-9bb.iam.gserviceaccount.com"
)

DATAPROC_CONTAINER_IMAGE = "us-central1-docker.pkg.dev/project-5b09c9f0-c89b-4689-9bb/dataproc-images/creditcard-dataproc:1.0.1"

# Expected filename: transactions_<16-character-hex-file_id>_<YYYYMMDD>.json
FILE_NAME_PATTERN = re.compile(r"^transactions_([0-9a-fA-F]{16})_(\d{8})\.json$")


# ---------------------------------------------------------
# Default arguments
# ---------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------
# Task 1: List and validate incoming transaction files
# ---------------------------------------------------------
def list_incoming_transaction_files():

    logging.info("starting gcs transaction file discovery")

    source_files = []

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    object_names = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=GCS_FILE_PREFIX)

    for object_name in object_names or []:

        source_file_uri = f"gs://{GCS_BUCKET}/{object_name}"

        source_files.append(source_file_uri)

    if not source_files:
        logging.info("no valid files for processing")
        return []

    # returned value is automatically pushed to XCom
    return source_files


# ---------------------------------------------------------
# Task 2: Check whether valid files are available
# ---------------------------------------------------------
def check_valid_files(**context):

    source_files = context["ti"].xcom_pull(task_ids="list_incoming_transaction_files")

    if source_files:
        logging.info(
            "%d valid transaction file(s) found. Processing with Dataproc execution",
            len(source_files),
        )
        return True

    logging.info("no valid files for processing, skipping dataproc execution")
    return False


# ---------------------------------------------------------
# Dataproc batch configuration
# ---------------------------------------------------------
batch_details = {
    "pyspark_batch": {
        "main_python_file_uri": "gs://creditcard-batch-analytics-gcs/spark_jobs/main.py",
        "args": [
            "--source-files",
            "({{ ti.xcom_pull(task_ids='list_incoming_transaction_files') | tojson }})",
        ],
        "python_file_uris": [
            "gs://creditcard-batch-analytics-gcs/spark_jobs/spark_job.py",
            "gs://creditcard-batch-analytics-gcs/spark_jobs/config.py",
            "gs://creditcard-batch-analytics-gcs/spark_jobs/utils.py",
            "gs://creditcard-batch-analytics-gcs/spark_jobs/file_processing_audit.py",
        ],
    },
    "runtime_config": {"version": "3.0", "container_image": DATAPROC_CONTAINER_IMAGE},
    "environment_config": {
        "execution_config": {"service_account": DATAPROC_SERVICE_ACCOUNT}
    },
}


# ---------------------------------------------------------
# DAG definition
# ---------------------------------------------------------
with DAG(
    dag_id="credit_card_transactions_dag",
    default_args=default_args,
    schedule_interval="40 4 * * *",  # daily execution at 8 PM UTC
    start_date=pendulum.datetime(2026, 11, 13, tz="UTC"),
    catchup=False,
    max_active_runs=1,
) as dag:

    # task 1: discover and validate incoming files
    list_files = PythonOperator(
        task_id="list_incoming_transaction_files",
        python_callable=list_incoming_transaction_files,
    )

    # task 2: continue only when valid files exist
    check_files = ShortCircuitOperator(
        task_id="check_valid_transaction_files", python_callable=check_valid_files
    )

    # task 3: submit pyspark batch to dataproc serverless
    pyspark_task = DataprocCreateBatchOperator(
        task_id="process_cc_transactions",
        batch=batch_details,
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        gcp_conn_id=GCP_CONN_ID,
    )


# -----------------------------------------------------
# Task dependencies
# -----------------------------------------------------
list_files >> check_files >> pyspark_task