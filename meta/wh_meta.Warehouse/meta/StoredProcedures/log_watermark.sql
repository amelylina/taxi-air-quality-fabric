CREATE   PROCEDURE meta.log_watermark
    @target_table VARCHAR(255),
    @source_table VARCHAR(255) = NULL,
    @ts DATETIME2(6),
    @rows_merged BIGINT,
    @run_id VARCHAR(255)
AS
BEGIN
    UPDATE meta.watermark
    SET last_processed_ts = @ts,
        last_run_at = CURRENT_TIMESTAMP,
        rows_merged = @rows_merged,
        pipeline_run_id = @run_id
    WHERE target_table = @target_table;

    IF @@ROWCOUNT = 0
        INSERT INTO meta.watermark (target_table, source_table, last_processed_ts, last_run_at, rows_merged, pipeline_run_id)
        VALUES (@target_table,@source_table, @ts, CURRENT_TIMESTAMP, @rows_merged, @run_id);
END;