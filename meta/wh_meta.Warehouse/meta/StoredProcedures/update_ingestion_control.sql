CREATE PROCEDURE meta.update_ingestion_control
    @source_name VARCHAR(100),
    @layer VARCHAR(10),
    @old_status VARCHAR(20),
    @new_status VARCHAR(20),
    @error_message VARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    IF @layer = 'bronze'
    BEGIN
        UPDATE meta.ingesstion_control
        SET status = @new_status, ended_at = CURRENT_TIMESTAMP, [error_message] = @error_message
        WHERE layer = 'bronze' AND status = @old_status;
    END
    ELSE IF @layer = 'silver'
    BEGIN
        UPDATE meta.ingesstion_control
        SET silver_status = @new_status, silver_ended_at = CURRENT_TIMESTAMP,  silver_error_message = @error_message
        WHERE layer = 'silver' AND silver_status = @old_status;
    END
    ELSE
    BEGIN
        RAISERROR('Invalid layer value.', 16, 1);
    END
END;