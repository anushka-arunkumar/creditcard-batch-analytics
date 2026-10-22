from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

GCP_PROJECT_ID = "project-5b09c9f0-c89b-4689-9bb"
GCS_BUCKET = "creditcard-batch-analytics-gcs"
FILE_PATTERN = "transactions/transactions_"



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 10, 19)
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
            "main_python_file_uri": ""
        },
        "runtime_config": {

        },
        "environment_config": {
            "execution_config": {
                "service_account": ""
            }
        }
    }

    # create/submit a batch in dataproc serverless
    pyspark_task = DataprocCreateBatchOperator (
        task_id = "process_cc_transactions",
        batch = batch_details,
        batch_id = 
        project_id = GCP_PROJECT_ID,
        region = ""
    )





 move_files_to_archive = GCSToGCSOperator(
        task_id="move_files_to_archive",
        source_bucket=gcs_bucket,
        source_object=source_prefix,
        destination_bucket=gcs_bucket,
        destination_object=archive_prefix,
        move_object=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


    file_sensor >> pyspark_task >> move_files_to_archive