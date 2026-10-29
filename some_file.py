from google.cloud import bigquery

def load_batch_audit(row):

    bq_client = bigquery.Client()
    table_id = "project-5b09c9f0-c89b-4689-9bb.creditcard_batch_analytics_bq.batch_audit"

    errors = bq_client.insert_rows_json(table_id, row)

    if not errors:
        print("row inserted successfully")
    else:
        print(f"error occured during row insertion: {errors}")

# row = [{
#     "batch_id": "test_batch_id"	
#     "airflow_dag_run_id"		STRING	
#     "dataproc_batch_id"		STRING	
#     "batch_start_time"		TIMESTAMP	
#     "batch_end_time"		TIMESTAMP	
#     "status"	REQUIRED	STRING	
#     "total_files_discovered"		INTEGER	
#     "total_files_processed"		INTEGER	
#     "total_files_failed"		INTEGER	
#     "records_read"		INTEGER	
#     "records_processed"		INTEGER	
#     "records_rejected"		INTEGER	
#     "suspicious_records"		INTEGER	
#     "error_message"		STRING	
#     "created_at"	REQUIRED	TIMESTAMP	
#     "updated_at"	REQUIRED	TIMESTAMP	
# }]

# def load_batch_audit(row):

#     bq_client = bigquery.Client()
#     table_id = "project-5b09c9f0-c89b-4689-9bb.creditcard_batch_analytics_bq.batch_audit"

#     errors = bq_client.insert_rows_json(table_id, row)

#     if not errors:
#         print("row inserted successfully")
#     else:
#         print(f"error occured during row insertion: {errors}")