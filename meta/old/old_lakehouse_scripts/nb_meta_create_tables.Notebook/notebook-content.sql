-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {}
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS meta.ingestion_control (
    source_name STRING NOT NULL,
    partition_key STRING NOT NULL,
    source_url STRING,
    status STRING  NOT NULL,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    rows_ingested BIGINT,
    error_message STRING,
    silver_status STRING,
    silver_started_at TIMESTAMP,
    silver_ended_at TIMESTAMP,
    silver_rows_written BIGINT,
    silver_error_message STRING,
    created_at TIMESTAMP NOT NULL
)
USING DELTA;

ALTER TABLE meta.ingestion_control SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported'); 
ALTER TABLE meta.ingestion_control ALTER COLUMN created_at SET DEFAULT current_timestamp();

ALTER TABLE meta.ingestion_control
ADD CONSTRAINT ingestion_valid_status
CHECK (status IN ('pending', 'running', 'succeeded', 'failed'));

ALTER TABLE meta.ingestion_control
ADD CONSTRAINT ingestion_valid_silver_status
CHECK (silver_status IS NULL OR silver_status IN ('pending', 'running', 'succeeded', 'failed'));

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS meta.pipeline_run_log (
    run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    layer STRING NOT NULL,
    status STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    message STRING,
    logged_at TIMESTAMP NOT NULL
)
USING DELTA;

ALTER TABLE meta.pipeline_run_log SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported'); 
ALTER TABLE meta.pipeline_run_log ALTER COLUMN logged_at SET DEFAULT current_timestamp();

ALTER TABLE meta.pipeline_run_log
ADD CONSTRAINT pipeline_valid_status
CHECK (status IN ('running', 'succeeded', 'failed'));

ALTER TABLE meta.pipeline_run_log
ADD CONSTRAINT pipeline_valid_layer
CHECK (layer IN ('bronze', 'silver', 'gold'));

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
