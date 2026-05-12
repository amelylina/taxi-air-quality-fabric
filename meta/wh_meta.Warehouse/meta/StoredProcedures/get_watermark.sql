CREATE   PROCEDURE meta.get_watermark
    @target_table VARCHAR(255)
AS
BEGIN
    SELECT COALESCE(MAX(last_processed_ts), '1900-01-01') AS last_processed_ts
    FROM meta.watermark
    WHERE target_table = @target_table;
END;