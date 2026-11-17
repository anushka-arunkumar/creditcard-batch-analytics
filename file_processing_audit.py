import logging
from datetime import datetime, timezone
from contextlib import contextmanager
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes
from config import (
    CLOUD_SQL_INSTANCE_CONNECTION_NAME,
    CLOUD_SQL_DB_USER,
    CLOUD_SQL_DB_NAME,
    STARTED_STATUS,
    SUCCESS_STATUS,
    FAILED_STATUS,
    GCP_PROJECT_NUMBER,
    SECRET_ID,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_db_password():

    client = secretmanager.SecretManagerServiceClient()

    secret_version_name = (
        f"projects/{GCP_PROJECT_NUMBER}/secrets/{SECRET_ID}/versions/latest"
    )
    response = client.access_secret_version(request={"name": secret_version_name})
    return response.payload.data.decode("UTF-8")


@contextmanager
def get_db_connection():

    db_password = get_db_password()

    with Connector() as connector:

        connection = connector.connect(
            CLOUD_SQL_INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=CLOUD_SQL_DB_USER,
            password=db_password,
            db=CLOUD_SQL_DB_NAME,
            ip_type=IPTypes.PRIVATE,
        )

        try:
            logger.info("connected to cloud sql successfully")
            yield connection

        finally:
            connection.close()
            logger.info("cloud sql connection closed")


def insert_file_audit(file_id, file_name):

    insert_query = """
        INSERT INTO file_processing_audit (
            file_id,
            file_name,
            processing_status,
            start_time  
        )
        VALUES (%s, %s, %s, %s)
        RETURNING audit_id
    """
    cursor = None
    start_time = datetime.now(timezone.utc)
    audit_id = None

    try:
        with get_db_connection() as connection:

            cursor = connection.cursor()
            cursor.execute(
                insert_query, (file_id, file_name, STARTED_STATUS, start_time)
            )

            # fetch the returned audit_id
            tup = cursor.fetchone()
            if tup:
                audit_id = tup[0]

            connection.commit()
            logger.info("audit record inserted for file_id: %s", file_id)

            return audit_id

    except Exception:
        logger.exception("failed to insert audit record for file_id: %s", file_id)
        raise

    finally:
        if cursor:
            cursor.close()


def update_file_audit_success(
    audit_id, file_id, records_read, records_processed, records_rejected
):
    update_query = """
        UPDATE file_processing_audit
        SET
            processing_status = %s,
            end_time = %s,
            records_read = %s,
            records_processed = %s,
            records_rejected = %s,
            error_message = %s
        WHERE 
            audit_id = %s
    """
    cursor = None
    end_time = datetime.now(timezone.utc)

    try:

        with get_db_connection() as connection:

            cursor = connection.cursor()
            cursor.execute(
                update_query,
                (
                    SUCCESS_STATUS,
                    end_time,
                    records_read,
                    records_processed,
                    records_rejected,
                    None,
                    audit_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    f"Expected one audit record for file_id {file_id}, "
                    f"but updated {cursor.rowcount} records"
                )

            connection.commit()
            logger.info("audit record marked SUCCESS for file_id: %s", file_id)

    except Exception:
        logger.exception("Failed to update SUCCESS audit for file_id: %s", file_id)
        raise

    finally:
        if cursor:
            cursor.close()


def update_file_audit_failed(
    audit_id,
    file_id,
    error_message,
    records_read=None,
    records_processed=None,
    records_rejected=None,
):
    update_query = """
        UPDATE file_processing_audit
        SET
            processing_status = %s,
            end_time = %s,
            records_read = %s,
            records_processed = %s,
            records_rejected = %s,
            error_message = %s
        WHERE 
            audit_id = %s
    """
    cursor = None
    end_time = datetime.now(timezone.utc)

    try:
        with get_db_connection() as connection:

            cursor = connection.cursor()
            cursor.execute(
                update_query,
                (
                    FAILED_STATUS,
                    end_time,
                    records_read,
                    records_processed,
                    records_rejected,
                    error_message,
                    audit_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    f"Expected one audit record for file_id {file_id}, "
                    f"but updated {cursor.rowcount} records"
                )

            connection.commit()
            logger.info("Audit record marked FAILED for file_id: %s", file_id)

    except Exception:
        logger.exception("Failed to update FAILED audit for file_id: %s", file_id)
        raise

    finally:
        if cursor:
            cursor.close()


def get_audit_id_and_processing_status(file_id):

    select_query = """
        SELECT audit_id, processing_status
        FROM file_processing_audit
        WHERE file_id = %s
        ORDER BY start_time DESC
        LIMIT 1
    """
    cursor = None

    try:
        with get_db_connection() as connection:

            cursor = connection.cursor()
            cursor.execute(select_query, (file_id,))
            result = cursor.fetchone()
            return result if result else (None, None)

    except:
        logger.exception(
            "Failed to retrieve audit_id & processing_status for file_id: %s", file_id
        )
        raise

    finally:
        if cursor:
            cursor.close()


def update_status_by_audit_id(audit_id, processing_status):

    update_query = """
        UPDATE file_processing_audit
        SET
            processing_status = %s,
            end_time = NOW()
        WHERE 
            audit_id = %s
    """
    cursor = None

    try:
        with get_db_connection() as connection:

            cursor = connection.cursor()
            cursor.execute(update_query, (processing_status, audit_id))
            connection.commit()
            logger.info(
                "Audit record marked %s for audit_id: %s",
                processing_status,
                audit_id,
            )

    except Exception:
        logger.exception(
            "Failed to update %s audit for audit_id: %s", processing_status, audit_id
        )
        raise

    finally:
        if cursor:
            cursor.close()
