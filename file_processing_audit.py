import os
import logging
from datetime import datetime, timezone
from contextlib import contextmanager
from google.cloud.sql.connector import Connector, IPTypes
from config import (
    CLOUD_SQL_INSTANCE_CONNECTION_NAME,
    CLOUD_SQL_DB_USER,
    CLOUD_SQL_DB_NAME,
    STARTED_STATUS,
    SUCCESS_STATUS,
    FAILED_STATUS
)

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection():

    db_password = os.environ.get("DB_PASSWORD")

    if not db_password:
        raise ValueError("DB_PASSWORD environment variable is not set")

    with Connector() as connector:

        connection = connector.connect(
            CLOUD_SQL_INSTANCE_CONNECTION_NAME,
            "pg8000",
            user = CLOUD_SQL_DB_USER,
            password = db_password,
            db = CLOUD_SQL_DB_NAME,
            ip_type = IPTypes.PUBLIC
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
    """

    start_time = datetime.now(timezone.utc)

    with get_db_connection() as connection:

        cursor = None

        try:

            cursor = connection.cursor()

            cursor.execute(
                insert_query,
                (
                    file_id,
                    file_name,
                    STARTED_STATUS,
                    start_time
                )
            )
            connection.commit()
            logger.info("audit record inserted for file_id: %s", file_id)

        except Exception:

            connection.rollback()
            logger.exception("failed to insert audit record for file_id: %s", file_id)
            raise

        finally:

            if cursor:
                cursor.close()
          

def update_file_audit_success(
    file_id,
    records_read,
    records_processed,
    records_rejected
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
        WHERE file_id = %s
    """
    end_time = datetime.now(timezone.utc)    

    with get_db_connection() as connection:

        cursor = None

        try:

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
                    file_id
                )
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    f"Expected one audit record for file_id {file_id}, "
                    f"but updated {cursor.rowcount} records"
                )

            connection.commit()
            logger.info("audit record marked SUCCESS for file_id: %s", file_id)

        except Exception:

            connection.rollback()
            logger.exception("Failed to update SUCCESS audit for file_id: %s", file_id)
            raise

        finally:

            if cursor:
                cursor.close()


def update_file_audit_failed(
    file_id, 
    error_message,
    records_read = None,
    records_processed = None,
    records_rejected = None 
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
        WHERE file_id = %s
    """ 

    end_time = datetime.now(timezone.utc)

    with get_db_connection() as connection:
        
        cursor = None

        try:

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
                    file_id
                )
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    f"Expected one audit record for file_id {file_id}, "
                    f"but updated {cursor.rowcount} records"
                )

            connection.commit()
            logger.info("Audit record marked FAILED for file_id: %s", file_id)

        except Exception:

            connection.rollback()
            logger.exception("Failed to update FAILED audit for file_id: %s", file_id)    
            raise

        finally:

            if cursor:
                cursor.close()