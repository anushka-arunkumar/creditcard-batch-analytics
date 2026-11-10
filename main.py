import re
import logging
from config import (
    TRANSACTION_FILE_REGEX
)
from spark_job import (
    create_sparksession, 
    read_transactions_from_gcs, 
    read_cardholders_from_bq, 
    process_transactions, 
    load_bq_transactions_table
)
from utils import (
    list_transaction_files,
    is_valid_transaction_filename,
    reject_transaction_file,
    move_failed_transaction_file,
    archive_transaction_file
)
from file_processing_audit import (
    insert_file_audit,
    update_file_audit_success,
    update_file_audit_failed
)

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_single_file(spark, cardholders_df, json_file_path):

    file_name = json_file_path.rsplit("/", 1)[-1]

    # Extract file_id from the filename
    match = re.fullmatch(TRANSACTION_FILE_REGEX, file_name)
    file_id = match.group(1)

    audit_started = False
    bq_load_completed = False
    audit_success = False

    records_read = None
    records_processed = None
    records_rejected = None

    try:

        logger.info("starting processing for file: %s", file_name)

        # Step 1: Insert STARTED audit record
        insert_file_audit(file_id, file_name)
        audit_started = True

        # Step 2: Read transactions from GCS
        transactions_df = read_transactions_from_gcs(spark, json_file_path)

        # Step 3: Count records read
        records_read = transactions_df.count()
        logger.info("records read from file %s: %d", file_name, records_read)

        # Step 4: Validate file_id inside transaction records
        file_ids = [
            row["file_id"]
            for row in (
                transactions_df
                .select("file_id")
                .distinct()
                .limit(2)
                .collect()
            )
        ]
        if len(file_ids) != 1 or file_ids[0] != file_id:
            raise ValueError(
                f"file id mismatch in {file_name}"
                f"expected: {file_id}, found: {file_ids}"
            )

        # Step 5: Transform transactions
        logger.info("processing transactions for file: %s", file_name)
        enriched_df = process_transactions(cardholders_df, transactions_df)

        # Step 6: Count processed and rejected records
        records_processed = enriched_df.count()
        records_rejected = records_read - records_processed

        logger.info(
            "File: %s | Read: %d | Processed: %d | Rejected: %d",
            file_name,
            records_read,
            records_processed,
            records_rejected
        )

        # Step 7: Load processed records into BigQuery
        load_bq_transactions_table(enriched_df)
        bq_load_completed = True

        # Step 8: Update audit status to SUCCESS
        update_file_audit_success(file_id, records_read, records_processed, records_rejected)
        audit_success = True

        logger.info("successfully processed file: %s", file_name)

    except Exception as e:

        logger.exception("processing failed for file: %s", file_name)

        # Mark audit record FAILED only if BigQuery loading has not completed
        if audit_started and not bq_load_completed:

            try:
                update_file_audit_failed(file_id, str(e), records_read, records_processed, records_rejected)
            except Exception:
                logger.exception("could not update FAILED audit for file_id: %s", file_id)

        elif bq_load_completed and not audit_success:

            logger.error(
                "BigQuery load completed, but SUCCESS audit update failed "
                "for file_id: %s. Manual reconciliation may be required.",
                file_id
            )

        # Move to failed folder only if BigQuery loading did not complete
        if not bq_load_completed:

            try:
                move_failed_transaction_file(json_file_path)
            except Exception:
                logger.exception("Could not move file to failed folder: %s", file_name)

        return
     
    # Step 9: Archive only after SUCCESS audit update
    if audit_success:

        try:
            archive_transaction_file(json_file_path)
        except Exception:
            logger.exception("Processing succeeded, but archiving failed for file: %s", file_name)


def main():

    spark = None

    try:

        logger.info("starting credit card batch processing")

        # Step 1: List files in incoming GCS folder
        transaction_files = list_transaction_files()

        if not transaction_files:
            logger.info("no transaction files found")
            return

        # Step 2: Validate filenames
        valid_files = []

        for json_file_path in transaction_files:

            if is_valid_transaction_filename(json_file_path):
                valid_files.append(json_file_path)

            else:
                logger.warning("invalid transaction file name: %s", json_file_path)

                try:
                    reject_transaction_file(json_file_path)
                except Exception:
                    logger.exception("Could not move invalid file to rejected folder: %s", json_file_path)

        if not valid_files:
            logger.info("no valid transaction files available for processing")
            return

        # Step 3: Create one SparkSession
        spark = create_sparksession()

        # Step 4: Read cardholders from BigQuery once
        cardholders_df = read_cardholders_from_bq(spark)

        # Step 5: Process valid files one at a time
        for json_file_path in valid_files:

            process_single_file(spark, cardholders_df, json_file_path)

        logger.info("Batch processing completed")

    except Exception:

        logger.exception("Batch processing job failed")

        # Make the Dataproc job fail instead of appearing successful
        raise

    finally:

        if spark:
            logger.info("Stopping SparkSession")
            spark.stop()      


if __name__ == "__main__":
    main()