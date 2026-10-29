CREATE TABLE IF NOT EXISTS
  `project-5b09c9f0-c89b-4689-9bb.creditcard_batch_analytics_bq.batch_audit`
(
  batch_id STRING NOT NULL,
  airflow_dag_run_id STRING,
  dataproc_batch_id STRING,

  batch_start_time TIMESTAMP,
  batch_end_time TIMESTAMP,

  status STRING NOT NULL,

  total_files_discovered INT64,
  total_files_processed INT64,
  total_files_failed INT64,

  records_read INT64,
  records_processed INT64,
  records_rejected INT64,
  suspicious_records INT64,

  error_message STRING,

  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY status;



