CREATE PROCEDURE meta.get_gold_watermark
    @table_name VARCHAR(100)
AS
BEGIN
    SELECT last_processed_silver_ts
    FROM meta.gold_watermark
    WHERE gold_table_name = @table_name;
END;