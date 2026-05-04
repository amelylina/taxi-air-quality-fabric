CREATE PROC meta.log_pipeline_update
    @run_id VARCHAR(255),
    @status VARCHAR(255),
    @message VARCHAR(MAX)
AS
BEGIN
    UPDATE meta.pipeline_run_log 
    SET status = @status,
        message = @message, 
        ended_at = CURRENT_TIMESTAMP, 
        logged_at = CURRENT_TIMESTAMP
    WHERE run_id = @run_id
    ;
END;