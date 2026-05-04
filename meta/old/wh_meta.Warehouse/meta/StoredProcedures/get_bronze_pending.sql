CREATE PROCEDURE meta.get_bronze_pending
    @source_name VARCHAR(100),
    @limit INT = 12
AS
BEGIN
    SELECT TOP (@limit)
        source_name,
        partition_key,
        source_url
    FROM meta.ingestion_control
    WHERE source_name = @source_name
      AND status = 'pending'
    ORDER BY partition_key;
END;