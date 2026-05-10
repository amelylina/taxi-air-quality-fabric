CREATE PROCEDURE meta.bulk_reset_running
    @source_name VARCHAR(100),
    @layer VARCHAR(10),
    @new_status VARCHAR(20),
    @error_message VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @layer = 'bronze'
        UPDATE meta.ingestion_control
        SET status = @new_status,
            ended_at = CURRENT_TIMESTAMP,
            error_message = @error_message
        WHERE source_name = @source_name AND status = 'running';
    ELSE IF @layer = 'silver'
        UPDATE meta.ingestion_control
        SET silver_status = @new_status,
            silver_ended_at = CURRENT_TIMESTAMP,
            silver_error_message = @error_message
        WHERE source_name = @source_name AND silver_status = 'running';
    ELSE
        THROW 50000, 'Invalid layer value.', 1;
END;