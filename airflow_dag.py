import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

GCP_PROJECT_ID = "project-5b09c9f0-c89b-4689-9bb"
GCS_BUCKET = "creditcard-batch-analytics-gcs"
FILE_PATTERN = "transactions/transactions_"
SOURCE_PREFIX = "transactions/"
ARCHIVE_PREFIX = "archive/"
batch_id = f"credit-card-batch-{str(uuid.uuid4())}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 10, 22)
}

with DAG(
    dag_id = "credit_card_transactions_dag",
    default_args = default_args,
    schedule_interval = None,
    catchup = False
) as dag:
    
    file_sensor = GCSObjectsWithPrefixExistenceSensor (

        task_id = "check_json_file_arrival",
        bucket = GCS_BUCKET,
        prefix = FILE_PATTERN,
        timeout = 600, # timeout is the total maximum waiting time
        poke_interval = 30,
        mode = "poke"
    )

    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": (
                "gs://creditcard-batch-analytics-gcs/spark_jobs/main.py"
            ),
            "python_file_uris": [
                "gs://creditcard-batch-analytics-gcs/spark_jobs/spark_job.py",
                "gs://creditcard-batch-analytics-gcs/spark_jobs/config.py"
            ]
        },
        "runtime_config": {
            "version": "3.0"
        },
        "environment_config": {
            "execution_config": {
                "service_account": "credit-card-airflow-sa@project-5b09c9f0-c89b-4689-9bb.iam.gserviceaccount.com"
            }
        }
    }

    # create/submit a batch in dataproc serverless
    pyspark_task = DataprocCreateBatchOperator (

        task_id = "process_cc_transactions",
        batch = batch_details,
        batch_id = batch_id,
        project_id = GCP_PROJECT_ID,
        region = "us-central1",
        gcp_conn_id = "google_cloud_default"
    )

    move_files_to_archive = GCSToGCSOperator (

        task_id = "move_files_to_archive",
        source_bucket = GCS_BUCKET,
        source_object = SOURCE_PREFIX,
        destination_bucket = GCS_BUCKET,
        destination_object = ARCHIVE_PREFIX,
        move_object = True
    )

    file_sensor >> pyspark_task >> move_files_to_archive