CREATE   PROCEDURE meta.update_partition_status
    @source_name VARCHAR(100),
    @partition_key VARCHAR(255),
    @layer VARCHAR(10),
    @new_status VARCHAR(20),
    @rows_written BIGINT = NULL,
    @error_message VARCHAR(MAX) = NULL,
    @cascade_silver_pending BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    IF @layer = 'bronze'
    BEGIN
        UPDATE meta.ingestion_control
        SET status = @new_status,
            started_at = CASE
                WHEN @new_status = 'running'
                THEN CURRENT_TIMESTAMP
                ELSE started_at
            END,
            ended_at = CASE
                WHEN @new_status IN ('succeeded', 'failed')
                THEN CURRENT_TIMESTAMP
                ELSE ended_at
            END,
            bronze_rows_written = @rows_written,
            error_message = @error_message,
            silver_status = CASE 
                WHEN @cascade_silver_pending = 1 AND @new_status = 'succeeded' 
                THEN 'pending' 
                ELSE silver_status 
            END
        WHERE source_name = @source_name AND partition_key = @partition_key;
    END
    ELSE IF @layer = 'silver'
    BEGIN
        UPDATE meta.ingestion_control
        SET silver_status = @new_status,
            silver_ended_at = CURRENT_TIMESTAMP,
            silver_rows_written = @rows_written,
            silver_error_message = @error_message
        WHERE source_name = @source_name AND partition_key = @partition_key;
    END
    ELSE
        THROW 50000, 'Invalid layer value.', 1;
END;