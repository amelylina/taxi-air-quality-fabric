CREATE PROCEDURE meta.get_watermark
    @target_table VARCHAR(255)
AS
BEGIN
    SELECT last_processed_ts
    FROM meta.watermark
    WHERE target_table = @target_table;
END;