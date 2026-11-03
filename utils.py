import re
import logging
from google.cloud import storage
from datetime import datetime
from config import (
    GCS_BUCKET,
    GCS_TRANSACTIONS_FOLDER,
    GCS_ARCHIVED_FOLDER,
    GCS_FAILED_FOLDER,
    GCS_REJECTED_FOLDER,
    TRANSACTION_FILE_REGEX,
)

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def list_transaction_files():

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix = GCS_TRANSACTIONS_FOLDER)

    matching_objects = []

    for blob in blobs:

        # ignore files inside nested subfolders
        relative_name = blob.name[len(GCS_TRANSACTIONS_FOLDER):]

        if not relative_name or "/" in relative_name or blob.name.endswith("/"):
            continue

        matching_objects.append(f"gs://{GCS_BUCKET}/{blob.name}")

    matching_objects.sort()

    logger.info(
        "Found %d object(s) in gs://%s/%s",
        len(matching_objects),
        GCS_BUCKET,
        GCS_TRANSACTIONS_FOLDER
    )

    return matching_objects


def is_valid_transaction_filename(gcs_file_path):

    filename = gcs_file_path.rsplit("/", 1)[-1]

    match = re.fullmatch(TRANSACTION_FILE_REGEX, filename)

    if not match:
        return False

    try:
        datetime.strptime(match.group(1), "%Y%m%d")
    except ValueError:
        return False
    
    return True


def move_gcs_file(gcs_file_path, destination_folder):

    excepted_prefix = f"gs://{GCS_BUCKET}/"

    if not gcs_file_path.startswith(excepted_prefix):
        raise ValueError(
            f"Expected a GCS path in bucket {GCS_BUCKET}: {gcs_file_path}"
        )

    source_blob_name = gcs_file_path[len(excepted_prefix):]

    filename = source_blob_name.rsplit("/", 1)[-1]

    destination_blob_name = f"{destination_folder}{filename}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    source_blob = bucket.blob(source_blob_name)
    destination_blob = bucket.blob(destination_blob_name)

    if not source_blob.exists():
        raise FileNotFoundError(
           f"Source GCS object does not exist: {gcs_file_path}"  
        )

    if destination_blob.exists():
        raise FileExistsError(
            f"Destination already exists; refusing to overwrite: "
            f"gs://{GCS_BUCKET}/{destination_blob_name}"
        )

    bucket.copy_blob(source_blob, bucket, destination_blob_name)

    source_blob.delete()

    destination_path = f"gs://{GCS_BUCKET}/{destination_blob_name}"

    logger.info(
        "Moved GCS object from %s to %s",
        gcs_file_path,
        destination_path
    )

    return destination_path


def archive_transaction_file(gcs_file_path):
    return move_gcs_file(gcs_file_path, GCS_ARCHIVED_FOLDER)


def move_failed_transaction_file(gcs_file_path):
    return move_gcs_file(gcs_file_path, GCS_FAILED_FOLDER)


def reject_transaction_file(gcs_file_path):
    return move_gcs_file(gcs_file_path, GCS_REJECTED_FOLDER)