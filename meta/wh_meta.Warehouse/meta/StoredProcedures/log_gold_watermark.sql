CREATE PROCEDURE meta.log_gold_watermark
    @table_name VARCHAR(100),
    @ts DATETIME2(6),
    @rows_merged BIGINT,
    @run_id VARCHAR(255)
AS
BEGIN
    UPDATE meta.gold_watermark
    SET last_processed_silver_ts = @ts,
    last_run_at = CURRENT_TIMESTAMP,
    rows_merged = @rows_merged,
    pipeline_run_id = @run_id
    WHERE gold_table_name = @table_name;
END;