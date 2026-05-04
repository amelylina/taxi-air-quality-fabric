-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "816baf1d-ce43-49b9-b16a-07c9169f7772",
-- META       "default_lakehouse_name": "lh_meta",
-- META       "default_lakehouse_workspace_id": "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "816baf1d-ce43-49b9-b16a-07c9169f7772"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS meta;

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

-- MAGIC %%pyspark
-- MAGIC from datetime import date
-- MAGIC from dateutil.relativedelta import relativedelta
-- MAGIC from pyspark.sql import Row
-- MAGIC 
-- MAGIC SOURCE_NAME    = "TLC_yellow_taxi"
-- MAGIC START_YEAR_MONTH = "2023-01"
-- MAGIC END_YEAR_MONTH   = "2024-12"
-- MAGIC URL_TEMPLATE   = "yellow_tripdata_{ym}.parquet"
-- MAGIC 
-- MAGIC def month_range(start_ym, end_ym):
-- MAGIC     cur  = date.fromisoformat(start_ym + "-01")
-- MAGIC     stop = date.fromisoformat(end_ym + "-01")
-- MAGIC     while cur <= stop:
-- MAGIC         yield cur.strftime("%Y-%m")
-- MAGIC         cur += relativedelta(months=1)
-- MAGIC 
-- MAGIC candidates = [
-- MAGIC     Row(source_name=SOURCE_NAME,
-- MAGIC         partition_key=ym,
-- MAGIC         source_url=URL_TEMPLATE.format(ym=ym))
-- MAGIC     for ym in month_range(START_YEAR_MONTH, END_YEAR_MONTH)
-- MAGIC ]
-- MAGIC 
-- MAGIC df = spark.createDataFrame(candidates)
-- MAGIC df.createOrReplaceTempView("candidates")
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC MERGE INTO meta.ingestion_control AS target
-- MAGIC USING candidates AS source
-- MAGIC   ON target.source_name = source.source_name
-- MAGIC  AND target.partition_key = source.partition_key
-- MAGIC WHEN NOT MATCHED THEN INSERT (
-- MAGIC     source_name, partition_key, source_url, status, created_at
-- MAGIC ) VALUES (
-- MAGIC     source.source_name, source.partition_key, source.source_url, 'pending', current_timestamp()
-- MAGIC )
-- MAGIC """)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM meta.ingestion_control;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE meta.pipeline_run_log(
    run_id STRING NOT NULL,
    pipeline_name STRING NOT NULL,
    layer STRING NOT NULL,
    status STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    message STRING,
    logged_at TIMESTAMP
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
