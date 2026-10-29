import sys
import logging
from config import GCS_TRXN_FILE_PATH
from spark_job import (
    create_sparksession, 
    read_transactions_from_gcs, 
    read_cardholders_from_bq, 
    process_transactions, 
    load_bq_transactions_table
)

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main():

    spark = None

    try:
        logger.info("starting spark job....")
        
        json_file_path = sys.argv[1] if len(sys.argv) > 1 else GCS_TRXN_FILE_PATH
        logger.info("Transaction input path: %s", json_file_path)
    
        spark = create_sparksession()
    
        transactions_df = read_transactions_from_gcs(spark, json_file_path)
    
        cardholders_df = read_cardholders_from_bq(spark)
    
        logger.info("processing transactions....")
    
        enriched_df = process_transactions(cardholders_df, transactions_df)
    
        logger.info("transaction processing completed")
    
        load_bq_transactions_table(enriched_df)
    
        logger.info("processing completed")
    
    except Exception:

        logger.exception("spark job failed")
        
        # make the Dataproc job fail, instead of appearing successful
        raise

    finally:

        if spark:
            logger.info("stopping SparkSession")
            spark.stop()


if __name__ == "__main__":
    main()