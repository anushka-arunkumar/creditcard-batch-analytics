import json
import logging
from google.cloud import storage
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TRANSACTION_BATCH_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "transaction_id": {
                "type": "string",
                "pattern": "^[a-fA-F0-9]{16}-TXN-\\d+$",
            },
            "cardholder_id": {"type": "string", "pattern": "^CH\\d+$"},
            "merchant_id": {"type": "string", "pattern": "^M\\d+$"},
            "merchant_name": {"type": "string"},
            "merchant_category": {"type": "string"},
            "transaction_amount": {"type": "number", "minimum": 0.01},
            "transaction_currency": {"type": "string", "minLength": 3, "maxLength": 3},
            "transaction_timestamp": {"type": "string"},
            "transaction_status": {
                "type": "string",
                "enum": ["SUCCESS", "PENDING", "FAILED"],
            },
            "fraud_flag": {"type": "boolean"},
            "device_type": {"type": "string"},
            "merchant_location": {"type": "string"},
            "file_id": {"type": "string", "pattern": "^[a-fA-F0-9]{16}$"},
        },
        "required": [
            "transaction_id",
            "cardholder_id",
            "merchant_id",
            "merchant_name",
            "merchant_category",
            "transaction_amount",
            "transaction_currency",
            "transaction_timestamp",
            "transaction_status",
            "fraud_flag",
            "device_type",
            "merchant_location",
            "file_id",
        ],
    },
}


def is_schema_valid(json_file_path):

    try:
        if not json_file_path.startswith("gs://"):
            error_msg = f"Invalid GCS path format: {json_file_path}"
            logger.error(error_msg)
            return False, error_msg

        parts = json_file_path[5:].split("/", 1)
        bucket_name = parts[0]
        blob_name = parts[1]

        gcs_client = storage.Client()
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists() or blob.size == 0:
            error_msg = f"File is missing or empty in GCS: {json_file_path}"
            logger.warning(error_msg)
            return False, error_msg

        file_content = blob.download_as_text()
        data = json.loads(file_content)

        validate(instance=data, schema=TRANSACTION_BATCH_SCHEMA)

        return True, None

    # malformed JSON file
    except json.JSONDecodeError as e:
        error_msg = f"Invalid json syntax in file {json_file_path}: {str(e)}"
        logger.warning(error_msg)
        return False, error_msg

    # JSON is syntactically correct but violates schema rules
    except ValidationError as e:
        error_msg = f"JSON schema validation failed for file {json_file_path}: {str(e)}"
        logger.warning(error_msg)
        return False, error_msg

    except Exception as e:
        error_msg = f"Unexpected error while validating file {json_file_path}: {str(e)}"
        logger.exception(error_msg)
        return False, error_msg
