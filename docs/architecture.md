# Architecture

## Medallion overview

Three lakehouses + one warehouse, organised by data maturity:

| Layer | Storage | Purpose | Write pattern |
|---|---|---|---|
| Bronze | `lh_bronze` (Lakehouse) | Raw landing, schema-on-write where possible | Append, partitioned by load_date or year/month |
| Silver | `lh_silver` (Lakehouse) | Cleaned, deduplicated, conformed | MERGE (idempotent) or partition overwrite |
| Gold (stage) | `lh_silver.stg.*` (Lakehouse) | Pre-aggregated facts before warehouse | Overwrite (volatile staging) |
| Gold | `wh_gold` (Warehouse) | Star schema, BI-ready | Upsert via Copy Activity |
| Metadata | `wh_meta` (Warehouse) | Ingestion control, watermarks, pipeline logs | Stored procedure writes |

Bronze for taxi is **file-based** (raw parquet at `Files/yellow_taxi/year=/month=/`), not a Delta table - TLC parquet is already columnar and the first useful transformation (schema normalisation) belongs at silver. All other sources land as Delta tables in bronze.

## Star schema (gold)

### Facts

| Fact | Grain | Measures |
|---|---|---|
| `fct_taxi_daily` | date × pickup zone × payment type | trip_count, total_fare_usd, total_revenue_usd, total_distance_miles, avg_trip_duration_min, total_passengers |
| `fct_taxi_hourly` | date × hour × pickup zone × payment type | same as daily, hourly grain |
| `fct_air_quality` | date × zone × parameter | avg_value, sensor_count |
| `fct_air_quality_hourly` | date × hour × zone × parameter | avg_value, sensor_count |
| `fct_fx_daily` | date × from_currency × to_currency | rate |
| `fct_gdp_yearly` | year × country | gdp_usd |

### Dimensions

| Dim | Key | Notes |
|---|---|---|
| `dm_date` | `date_key` (yyyymmdd int) | Date range covered : 2020-2030 |
| `dm_zone` | `zone_id` | NYC TLC zone lookup + service zone classification |
| `dm_payment` | `id` | Manually seeded (1-6 per TLC spec) |
| `dm_vendor` | `id` | Manually seeded (1=Creative Mobile, 2=VeriFone) |
| `dm_currency` | `id` | Manually seeded (USD=1, EUR=2). Scope-limited to project requirements. |
| `dm_air_measurement` | `p_id` | Seeded from `Files/reference/sensor_parameters.csv` (12 pollutants) |

## Orchestration topology

The platform uses a **per-source master pattern** rather than a single platform-wide master pipeline. Each data source has its own master pipeline that runs bronze → silver → gold for that source only:
| Master | Sources owned | Natural cadence |
|---|---|---|
|`pl_seed`|Reference dims, shapefile, OpenAQ locations, sensor↔zone mapping, GDP|One-off / yearly|
|`pl_master_taxi`|TLC yellow taxi (daily + hourly gold)|Monthly|
|`pl_master_openaq_daily`|OpenAQ via API, daily grain|Daily–weekly|
|`pl_master_openaq_hourly`|OpenAQ via S3, hourly grain|Daily–weekly|
|`pl_master_fx`|ECB FX daily|Daily|

### Why per-source rather than wide

1. **Independent cadences.** Sources update at different rates; a single wide master would force the loosest schedule on all of them.
2. **Blast radius.** A failure in one source doesn't cascade. The other masters keep running.
3. **Independent backfill**. Each source can be replayed without re-running the others.
4. **Schedulability**. Each per-source master can have its own trigger. (No production schedules are enabled for this submission - see [docs/known-issues.md](docs/known-issues.md).)
5. **GDP belongs in seed, not in a master**. The endpoint returns full history per call; refreshing it more than yearly is wasteful and exposes the platform to upstream availability we don't need daily.

### **Cross-source dependencies**
The only cross-source dependency in the data is the OpenAQ sensor↔zone mapping (`openaq_sensor_zones`), which depends on both the TLC zone shapefile and the OpenAQ locations table. Both are slowly-changing reference data and live in `pl_seed`, not in any master. Re-running this mapping is a `pl_seed` operation, not part of the incremental flow.
Optional convenience pipeline
A thin pl_runall pipeline invokes all four per-source masters in parallel. It exists for demos and ad-hoc full refreshes, not as a production unit.

## Orchestration patterns

Two orchestration patterns are used, deliberately:

**Pattern 1 - Per-partition state machine (taxi bronze)**

The pipeline owns the loop. Each (source, month) row in `meta.ingestion_control` transitions `pending → running → succeeded/failed`. `ForEach` iterates pending partitions, the per-iteration body sets running, copies, sets succeeded (or failed on copy failure). Used where the unit of work is a single HTTP file copy and the pipeline can drive each step.

**Pattern 2 - Bulk-claim + notebook loop (openaq bronze, all silvers, all golds)**

The pipeline claims a batch of `pending` rows (or reads watermark) and hands them to a notebook. The notebook iterates internally, applies rate-limited API calls or Spark transforms, and reports per-partition results via `executemany`. Used where:

- The unit of work needs notebook-level features (threading, rate limiter, Spark transforms)
- Round-tripping per-partition state from a notebook to the warehouse for every iteration is expensive
- The notebook can handle partial failure across the batch without aborting

The pipeline reads back the notebook's `exitValue` JSON to log overall status.

## Watermark vs partition status

Both incremental mechanisms coexist:

- **Partition status** (`meta.ingestion_control.status` and `silver_status`) - used where a partition is the natural unit of replay (taxi months, OpenAQ months). Per-partition retry, per-partition observability.
- **Watermark** (`meta.watermark.last_processed_ts`) - used where MERGE on natural keys makes per-partition state unnecessary (GDP, FX, OpenAQ silver). One row per target table, one timestamp.

## Why a `stg.*` schema in `lh_silver` then Copy to `wh_gold`?

Two-step pattern, deliberate:

1. Notebook writes the pre-aggregated fact to `lh_silver.stg.fct_*` (Delta table, overwrite).
2. Data Pipeline `Copy Activity` reads the stage and upserts to `wh_gold.dbo.fct_*` (`writeBehavior: Upsert` with natural-key merge).

Reasons:

- Spark-to-Warehouse direct writes via `synapsesql` are functional but the Copy Activity is the Microsoft-optimised path (uses COPY INTO under the hood, hits a separate temp stage either way).
- Upsert semantics are declarative in the Copy Activity (`upsertSettings.keys`), no MERGE SQL to maintain.
- The stage table is a useful debugging artifact - you can inspect what was about to be loaded.

## Custom Python library: `wh_conn`

`get_con()` and `check_con()` are custom helpers built on `mssql-python` for connecting from Fabric notebooks to the Fabric Warehouse SQL endpoint with retry/refresh semantics. The library is packaged and uploaded to the `mssql_env` Spark environment, and commited to this repo in `/resources`.

## Hourly grain - dual ingestion path

The daily gold layer answers Q1 (mobility vs air-quality correlation) and most of Q3/Q4 but leaves Q2 (*which times of day*) unaddressable. Hourly facts were added without disturbing the daily layer.

**Hourly taxi** is essentially free: silver `taxi_trips` already preserved `tpep_pickup_datetime` and `hour_of_day`. A new gold notebook `nb_gold_taxi_hourly` re-aggregates the existing silver at (date, hour, zone, payment) grain. No new ingest, no schema change in silver. One-shot backfill via `last_processed_ts='1990-01-01'`.

**Hourly air quality** required new ingestion because the daily silver had already discarded sub-day resolution. Options:

- OpenAQ `/measurements` API endpoint - rate-prohibitive for 24-month backfill at hourly grain on the free tier.
- OpenAQ public S3 archive - already in use for backfill (anonymous, no API quota).

The S3 archive path was chosen. Critical observation: the existing S3 notebook was already scanning the raw records and aggregating to daily. **Switching the `groupBy` to include `hour_utc` produced hourly output for the same scan cost.** No additional cloud read, no API calls.

This is registered as a separate source in `meta.ingestion_control` (`openaq_s3_hourly`) so its state machine is independent of the daily API path. The architecture is now genuinely dual-path:

| Path | Source | Grain | Use |
|---|---|---|---|
| API daily | `/v3/sensors/{id}/days` | daily | Incremental refresh, recent data |
| S3 hourly | `s3a://openaq-data-archive/records/csv.gz/...` | hourly | Backfill, time-of-day analysis |
| S3 daily (generic) | same S3 bucket | daily | Backfill of arbitrary pollutants (configured for NO2 in current scope) |

### Bronze schema trim for hourly

After loading one test month, ~99.9% of (sensor, hour) cells had exactly one underlying reading. Therefore:

- Dropped `min_val`, `max_val`, `median_val` (always equal to `value`)
- Dropped `coverage_pct` (always null from S3)
- Renamed `expected_count` → `reading_count` (kept as a quality indicator for possible filtering)

The `groupBy` is retained in bronze even though it's a near-no-op, to keep daily and hourly bronze structurally parallel and to enforce the hourly grain explicitly.

### Bronze robustness: missing S3 partitions

OpenAQ's S3 archive only materialises `(location_id, year, month)` directories where that location actually reported in that month. Reading a constructed path list with `spark.read.csv()` failed with `PATH_NOT_FOUND` for sensors that came online mid-window.

The fix:
- `filter_existing_paths()` helper pre-checks each S3 path via the Hadoop `FileSystem.exists()` API before passing the list to `spark.read.csv()`.

## Failure handling

- Bronze pipelines: per-partition `update_partition_status` on success, `bulk_reset_running` SP on bulk failure to flip everything stuck in `running` back to `failed` with the error message captured.
- Silver/gold: same partition update mechanism for taxi; `meta.watermark` rolled forward only on success for the watermark-driven sources.