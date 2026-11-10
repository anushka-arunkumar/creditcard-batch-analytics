GCP_PROJECT_ID = "project-5b09c9f0-c89b-4689-9bb"
GCP_PROJECT_NUMBER = "870802335159"

GCS_BUCKET = "creditcard-batch-analytics-gcs"

GCS_TRANSACTIONS_FOLDER = "transactions/"
GCS_ARCHIVED_FOLDER = "archived/"
GCS_FAILED_FOLDER = "failed/"
GCS_REJECTED_FOLDER = "rejected/"
GCS_CUSTOMERS_FOLDER = "customers/"
GCS_SPARK_JOBS_FOLDER = "spark_jobs/"

TRANSACTION_FILE_REGEX = r"^transactions_([a-fA-F0-9]{16})_(\d{8})\.json$"

BQ_DATASET = "creditcard_batch_analytics_bq"

BQ_CARDHOLDERS_TABLE = "cardholders"
BQ_CARDHOLDERS_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CARDHOLDERS_TABLE}"
BQ_TRANSACTIONS_TABLE = "transactions"
BQ_TRANSACTIONS_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TRANSACTIONS_TABLE}"

STARTED_STATUS = "STARTED"
SUCCESS_STATUS = "SUCCESS"
FAILED_STATUS = "FAILED"

# Cloud SQL PostgreSQL configuration
CLOUD_SQL_INSTANCE_CONNECTION_NAME = (
    "project-5b09c9f0-c89b-4689-9bb:us-central1:creditcard-pg-instance"
)
CLOUD_SQL_DB_USER = "app_user"
CLOUD_SQL_DB_NAME = "creditcard_db"
SECRET_ID = "DB_PASSWORD"