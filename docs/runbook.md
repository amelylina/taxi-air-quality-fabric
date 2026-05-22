# Runbook

## First-time deployment

The following files live in the Git repo, but must be uploaded into the workspace once:

- `resources/wh_conn/wh_conn.py` → uploaded to the `mssql_env` Spark environment as a custom library.
- `resources/reference/sensor_parameters.csv` → uploaded to `lh_bronze/Files/reference/sensor_parameters.csv` (read by `nb_gold_dm_zone_date` to seed `dm_air_measurement`).

Other reference files (`taxi_zone_lookup.csv`, taxi zone shapefile) are downloaded automatically by `nb_bronze_taxi_zones` on first run; no manual upload needed.

1. Create a Fabric workspace on capacity that supports Lakehouse + Warehouse + Pipelines + Dataflow Gen2 + Notebooks + Power BI.
2. Import or git-sync this repo into the workspace.
3. Create the Spark environments `mssql_env` and `env_geopandas` and publish.
4. Upload the `wh_conn` module into `mssql_env`'s custom libraries section and publish again.
5. Attach environments to notebooks as defined in each notebook's metadata.
6. Create an Azure Key Vault containing secret `openaq-key` with a valid OpenAQ API key.
7. In Fabric, create a Web/HTTP connection to the Key Vault secrets endpoint. Note its connection ID.
8. Update the `fetch_openaq_api_key` Web Activity's `externalReferences.connection` in `pl_seed` if the connection ID changes.
9. Create the HTTP linked service for TLC: base URL `https://d37ci.cloudfront.net/trip-data/`.
10. Update `libs/storage_paths.VariableLibrary` with workspace and item IDs for this environment.
11. Run the `wh_meta` and `wh_gold` warehouse DDL scripts.

## Running for the first time (full bootstrap)

1. Set `pl_seed` parameters (defaults: `date_from='2023-01'`, `date_to='2024-12'`).
2. Trigger `pl_seed`. This populates dims, references, OpenAQ locations, sensor-zone mapping, GDP, and seeds `meta.ingestion_control`.
3. Trigger each `per-source pl_master_*` individually.

Running all pl_seed notebook branches concurrently exceeded available Spark vcores on small SKUs (F4). Manually triggering each notebook/dataflow, used in the pipeline, consequtively would achieve same results, with a little more time.

## Routine run (after bootstrap)

Per-source masters can be triggered independently. Suggested cadences:

- `pl_master_taxi` - monthly, around the 20th (TLC publishes month M parquet around mid-(M+2))
- `pl_master_fx` - daily, business days
- `pl_master_openaq_daily` - daily or weekly
- `pl_master_openaq_hourly` - daily or weekly
- `pl_seed` - re-run only when refreshing GDP, sensor locations, or zone definitions

No production schedules are enabled for this submission.

## Backfill scenarios

### Re-process a single failed partition
```sql
UPDATE meta.ingestion_control
SET status = 'pending'
WHERE source_name = ?
  AND partition_key = ?;
```
Then run the bronze pipeline for that source. Cascade to silver via `silver_status = 'pending'` if needed.

### Reset all partitions stuck in `running` (e.g. after a pipeline was aborted)
```sql
EXEC meta.bulk_reset_running 
    @source_name = 'TLC_yellow_taxi',
    @layer = 'bronze',
    @new_status = 'pending';
```

### Replay silver from existing bronze
```sql
UPDATE meta.ingestion_control
SET silver_status = 'pending'
WHERE source_name = ?
  AND status = 'succeeded';
```
Then run the silver pipeline.

### Full silver rebuild for a source
```sql
-- Example: taxi
DROP TABLE lh_silver.dbo.taxi_trips;
UPDATE meta.ingestion_control
SET silver_status = 'pending', silver_started_at=NULL, silver_ended_at=NULL,
    silver_rows_written=NULL, silver_error_message=NULL
WHERE source_name = 'TLC_yellow_taxi';
-- Run pl_silver_taxi
```

### Reset watermark to force a full silver re-MERGE
```sql
UPDATE meta.watermark
SET last_processed_ts = '1900-01-01'
WHERE target_table = 'lh_silver.dbo.fx_daily';
```
Or remove it completely. Stored procedure for getting watermark returns default value when there is no watermark available.

## Adding a new source

1. Add the source name to `libs/bronze_source_names.VariableLibrary/variables.json`.
2. If using a Dataflow: create the dataflow, parameterise any secrets via Web Activity + Key Vault pattern.
3. If using a notebook ingest: model after `nb_bronze_openaq_sensor_days`.
4. Create `pl_bronze_<source>`, `pl_silver_<source>`, `pl_gold_<source>`.
5. Create a new `pl_master_<source>` for the source (model after `pl_master_taxi`)
6. Seed partitions via `nb_seed_partitions` (or extend `pl_seed`).
7. Add the gold fact + relevant dim updates.

## Monitoring

- `meta.pipeline_run_log` - every pipeline run with status and message.
- `meta.ingestion_control` - per-partition status, current state of all known partitions.
- Fabric Monitor - built-in pipeline-run UI for activity-level detail.

## Operational queries

```sql
-- Anything stuck running for more than an hour
SELECT * FROM meta.ingestion_control
WHERE status = 'running' AND started_at < DATEADD(hour, -1, CURRENT_TIMESTAMP);

-- Last run per pipeline
SELECT pipeline_name, MAX(started_at) AS last_run, MAX(status) AS last_status
FROM meta.pipeline_run_log
GROUP BY pipeline_name;

-- Backfill progress
SELECT source_name, status, COUNT(*) AS partitions
FROM meta.ingestion_control
GROUP BY source_name, status
ORDER BY source_name, status;
```

## Backfilling additional pollutants from S3

The `nb_bronze_openaq_s3_archive` notebook is parameter-driven and currently configured for NO2. To backfill another pollutant historically:

1. Edit `TARGET_PARAMS` in the notebook (or parameterise it via `pl_bronze_openaq_sensor_s3`).
2. Seed `meta.ingestion_control` for the desired date range under a source name like `openaq_s3` (existing) or a new pollutant-specific source name.
3. Trigger the pipeline. Output lands in `lh_bronze.dbo.openaq_measurements_daily` (daily grain).
4. The existing daily silver and gold paths pick it up automatically - no new silver/gold code needed.

This is **not** the hourly path; that's `openaq_s3_hourly` and lands in a separate table.