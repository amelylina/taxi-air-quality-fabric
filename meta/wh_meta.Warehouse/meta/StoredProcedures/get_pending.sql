CREATE   PROCEDURE meta.get_pending
    @source_name VARCHAR(100),
    @layer VARCHAR(10),
    @limit INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @top INT = ISNULL(@limit, 2147483647);

    IF @layer = 'bronze'
    BEGIN
        SELECT TOP (@top)
            source_name, partition_key, source_url
        FROM meta.ingestion_control
        WHERE source_name = @source_name
          AND status = 'pending'
        ORDER BY partition_key;
    END
    ELSE IF @layer = 'silver'
    BEGIN
        SELECT TOP (@top)
            source_name, partition_key, source_url
        FROM meta.ingestion_control
        WHERE source_name = @source_name
          AND status = 'succeeded'
          AND silver_status = 'pending'
        ORDER BY partition_key;
    END
    ELSE
        THROW 50000, 'Invalid layer. Must be bronze or silver.', 1;
END;