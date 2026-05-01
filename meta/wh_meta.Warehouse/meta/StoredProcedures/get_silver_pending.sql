CREATE PROCEDURE meta.get_silver_pending
    @source_name VARCHAR(100),
    @limit INT = NULL
AS
BEGIN
    UPDATE t 
    SET silver_status='running', silver_started_at=CURRENT_TIMESTAMP
    FROM (
        SELECT TOP (ISNULL(@limit, 2147483647)) *
        FROM meta.ingestion_control
        WHERE source_name=@source_name
        AND status='succeeded'
        AND silver_status='pending'
        ORDER BY partition_key
    )t;
END;