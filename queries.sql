CREATE TABLE file_processing_audit
(
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id VARCHAR(100) NOT NULL,
    file_name VARCHAR(255),
    processing_status VARCHAR(50) NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    records_read BIGINT,
    records_processed BIGINT,
    records_rejected BIGINT,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);


CREATE OR REPLACE FUNCTION public.update_modified_column()
RETURNS TRIGGER 
LANGUAGE plpgsql 
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;
CREATE TRIGGER update_file_processing_audit_mod_time
    BEFORE INSERT OR UPDATE ON file_processing_audit
    FOR EACH ROW
    EXECUTE FUNCTION public.update_modified_column();


SELECT pg_get_functiondef('update_modified_column'::regproc);