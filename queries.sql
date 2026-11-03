CREATE TABLE file_processing_audit
(
    file_id VARCHAR(100) NOT NULL,
    file_name VARCHAR(255),
    processing_status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_read BIGINT,
    records_processed BIGINT,
    records_rejected BIGINT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,

    PRIMARY KEY (file_id)
);