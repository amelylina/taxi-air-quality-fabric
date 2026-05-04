CREATE PROC meta.log_pipeline_start
    @run_id VARCHAR(255),
    @pipeline_name VARCHAR(255),
    @layer VARCHAR(10)
AS
BEGIN
    INSERT INTO meta.pipeline_run_log (run_id, pipeline_name, layer, status, started_at)
    VALUES (@run_id, @pipeline_name, @layer, 'running', CURRENT_TIMESTAMP)
    ;
END;