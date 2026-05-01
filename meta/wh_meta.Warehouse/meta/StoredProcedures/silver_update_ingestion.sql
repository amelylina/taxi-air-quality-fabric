CREATE PROC meta.silver_update_ingestion
    @source_name VARCHAR(255)
AS
BEGIN
    MERGE INTO meta.ingestion_control AS target
    USING meta.silver_staging AS source
    ON target.partition_key = source.partition_key
    AND target.source_name = @source_name
    WHEN MATCHED THEN
    UPDATE SET 
        target.silver_status = source.silver_status,
        target.silver_ended_at = CURRENT_TIMESTAMP,
        target.silver_rows_written = source.silver_rows_written,
        target.silver_error_message = source.silver_error_message
    ;
END;