import re
import logging
from google.cloud import storage, bigquery
from datetime import datetime
from config import (
    GCP_PROJECT_ID,
    BQ_TRANSACTIONS_TABLE_ID,
    GCS_BUCKET,
    GCS_ARCHIVED_FOLDER,
    GCS_FAILED_FOLDER,
    GCS_REJECTED_FOLDER,
    GCS_DUPLICATES_FOLDER,
    TRANSACTION_FILE_REGEX,
    SUCCESS_STATUS,
    STARTED_STATUS,
    FAILED_STATUS,
)
from file_processing_audit import (
    get_audit_id_and_processing_status,
    update_status_by_audit_id,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_file_id(json_file_path):

    filename = json_file_path.rsplit("/", 1)[-1]
    match = re.match(TRANSACTION_FILE_REGEX, filename)
    return match.group(1)


def is_valid_transaction_filename(json_file_path):

    filename = json_file_path.rsplit("/", 1)[-1]

    match = re.fullmatch(TRANSACTION_FILE_REGEX, filename)

    if not match:
        return False

    try:
        datetime.strptime(match.group(2), "%Y%m%d")
    except ValueError:
        return False

    return True


def move_gcs_file(gcs_file_path, destination_folder):

    excepted_prefix = f"gs://{GCS_BUCKET}/"

    if not gcs_file_path.startswith(excepted_prefix):
        raise ValueError(f"Expected a GCS path in bucket {GCS_BUCKET}: {gcs_file_path}")

    source_blob_name = gcs_file_path[len(excepted_prefix) :]

    filename = source_blob_name.rsplit("/", 1)[-1]

    destination_blob_name = f"{destination_folder}{filename}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    source_blob = bucket.blob(source_blob_name)
    destination_blob = bucket.blob(destination_blob_name)

    if not source_blob.exists():
        raise FileNotFoundError(f"Source GCS object does not exist: {gcs_file_path}")

    if destination_blob.exists():
        raise FileExistsError(
            f"Destination already exists; refusing to overwrite: "
            f"gs://{GCS_BUCKET}/{destination_blob_name}"
        )

    bucket.copy_blob(source_blob, bucket, destination_blob_name)

    source_blob.delete()

    destination_path = f"gs://{GCS_BUCKET}/{destination_blob_name}"

    logger.info("Moved GCS object from %s to %s", gcs_file_path, destination_path)

    return destination_path


def archive_transaction_file(gcs_file_path):
    return move_gcs_file(gcs_file_path, GCS_ARCHIVED_FOLDER)


def move_failed_transaction_file(gcs_file_path):
    return move_gcs_file(gcs_file_path, GCS_FAILED_FOLDER)


def reject_transaction_file(gcs_file_path):
    return move_gcs_file(gcs_file_path, GCS_REJECTED_FOLDER)


def move_duplicate_file(gcs_file_path):
    return move_gcs_file(gcs_file_path, GCS_DUPLICATES_FOLDER)


def check_file_id_exists_in_bq(file_id):

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
        SELECT 1
        FROM `{BQ_TRANSACTIONS_TABLE_ID}`
        WHERE file_id = @file_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("file_id", "STRING", file_id)]
    )

    results = bq_client.query(query, job_config=job_config).result()

    return any(results)


def process_audit_precheck(json_file_path):

    file_id = get_file_id(json_file_path)

    audit_id, processing_status = get_audit_id_and_processing_status(file_id)

    # Case 1: Brand new file (no audit history yet)
    if processing_status is None:
        return True

    # Case 2: Previously succeeded -> Duplicate file
    elif processing_status == SUCCESS_STATUS:
        return reconcile_success_audit_record(json_file_path)

    # Case 3: Previously failed -> Retry processing
    elif processing_status == FAILED_STATUS:
        return reconcile_failed_audit_record()

    # Case 4: Stuck in STARTED -> Self-heal or process
    elif processing_status == STARTED_STATUS:
        return reconcile_started_audit_record(json_file_path, audit_id)


def reconcile_success_audit_record(json_file_path):

    logger.warning(
        "duplicate file detected based on SUCCESS audit status, archiving: %s",
        json_file_path,
    )
    try:
        move_duplicate_file(json_file_path)
    except Exception:
        logger.exception("failed to archive duplicate file: %s", json_file_path)
    return False    # do not add to valid_files


def reconcile_failed_audit_record():

    logger.info("previous run failed, retrying file processing")
    return True     # proceed with normal processing


def reconcile_started_audit_record(json_file_path, audit_id):

    file_id = get_file_id(json_file_path)

    if check_file_id_exists_in_bq(file_id):

        logger.info(
            "File_id %s found in BigQuery with STARTED status. Self-healing audit record to SUCCESS.",
            file_id,
        )
        update_status_by_audit_id(audit_id, SUCCESS_STATUS)

        try:
            archive_transaction_file(json_file_path)
        except Exception:
            logger.exception("Failed to archive self-healed file: %s", json_file_path)

        return False  # Skip reprocessing

    else:
        logger.warning(
            "File_id %s is marked STARTED in audit table but NOT found in BigQuery. ",
            file_id,
        )
        return True  # Proceed with normal processing
