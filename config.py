GCP_PROJECT_ID = "project-5b09c9f0-c89b-4689-9bb"

GCS_BUCKET = "creditcard-batch-analytics-gcs"
GCS_TRXN_FILE_PATH = (
    "gs://creditcard-batch-analytics-gcs/"
    "transactions/transactions_*.json"
)

BQ_DATASET = "creditcard_batch_analytics_bq"

BQ_CARDHOLDERS_TABLE = "cardholders"
BQ_CARDHOLDERS_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_CARDHOLDERS_TABLE}"
BQ_TRANSACTIONS_TABLE = "transactions"
BQ_TRANSACTIONS_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TRANSACTIONS_TABLE}"