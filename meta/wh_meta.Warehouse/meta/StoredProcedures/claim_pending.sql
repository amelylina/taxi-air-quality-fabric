CREATE   PROCEDURE meta.claim_pending
    @source_name VARCHAR(100),
    @layer VARCHAR(10),
    @limit INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @top INT = ISNULL(@limit, 2147483647);

    IF @layer = 'bronze'
    BEGIN
        UPDATE t
        SET status = 'running',
            started_at = CURRENT_TIMESTAMP,
            ended_at = NULL,
            error_message = NULL
        FROM (
            SELECT TOP (@top) *
            FROM meta.ingestion_control
            WHERE source_name = @source_name
              AND status = 'pending'
            ORDER BY partition_key
        ) t;
    END
    ELSE IF @layer = 'silver'
    BEGIN
        UPDATE t
        SET silver_status = 'running',
            silver_started_at = CURRENT_TIMESTAMP,
            silver_ended_at = NULL,
            silver_error_message = NULL
        FROM (
            SELECT TOP (@top) *
            FROM meta.ingestion_control
            WHERE source_name = @source_name
              AND status = 'succeeded'
              AND silver_status = 'pending'
            ORDER BY partition_key
        ) t;
    END
    ELSE
        THROW 50000, 'Invalid layer. Must be bronze or silver.', 1;
END;