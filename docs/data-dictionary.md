# Data Dictionary

Conventions:
- All timestamps are UTC unless noted.
- `loaded_at` = when the row landed in this layer.
- `date_key` = integer in `yyyymmdd` format, joins to `dm_date.date_key`.

## Bronze (`lh_bronze`)

### `dbo.ecb_fx_daily`
Raw ECB foreign exchange daily series (USD/EUR). Landed via Dataflow Gen2 from `https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata`.

| Column | Type | Description |
|---|---|---|
| `KEY` | varchar | ECB series key |
| `FREQ` | varchar | Frequency code (D = daily) |
| `CURRENCY` | varchar | From currency (USD) |
| `CURRENCY_DENOM` | varchar | To currency (EUR) |
| `EXR_TYPE` | varchar | Exchange rate type |
| `TIME_PERIOD` | date | Observation date |
| `OBS_VALUE` | float | The FX rate |
| `OBS_STATUS` | varchar | `A` = normal value, others indicate provisional/estimated |
| `OBS_CONF`, `OBS_PRE_BREAK`,`OBS_COM`,`TIME_FORMAT`,<br>`BREAKS`,`COLLECTION`,`COMPILING_ORG`,`DISS_ORG`,<br>`DOM_SER_IDS`,`PUBL_ECB`,`PUBL_MU`,`PUBL_PUBLIC`,<br>`UNIT_INDEX_BASE`,`COMPILATION`,`COVERAGE`,`DECIMALS`, `NAT_TITLE`,<br> `SOURCE_AGENCY`,`SOURCE_PUB`,`TITLE`,`TITLE_COMPL`,`UNIT`,`UNIT_MULT` | varchar / bigint | preserved for lineage, not used downstream | 
| `source_url` | varchar | Origin URL, added at ingest |
| `loaded_at` | datetime2 | Ingest timestamp |

### `dbo.openaq_locations`
Static-ish OpenAQ locations within the NYC bounding box (`-74.26,40.49,-73.69,40.92`), one row per (location, sensor). Refreshed on `pl_seed`.

| Column | Type | Description |
|---|---|---|
| `location_id` | bigint | OpenAQ location ID |
| `name` | varchar | Location name |
| `locality` | varchar | City/locality |
| `timezone` | varchar | IANA tz |
| `code` | varchar | Country code |
| `latitude` | float | |
| `longitude` | float | |
| `sensor_id` | bigint | OpenAQ sensor ID (one location may have many sensors) |
| `p_name` | varchar | Parameter name (pm25, no2, ...) |
| `p_units` | varchar | Units |
| `p_display_name` | varchar | Pretty name |
| `first_datetime_utc` | datetime2 | First measurement available |
| `last_datetime_utc` | datetime2 | Last measurement available |
| `provider_name` | varchar | Data provider |
| `loaded_at` | datetime2 | Ingest timestamp |

### `dbo.openaq_measurements_daily`
Daily-aggregated OpenAQ measurements. Loaded from either the `/sensors/{id}/days` API endpoint OR the public S3 archive (CSV.GZ records aggregated to day at ingest). Partitioned by `load_date`.

| Column | Type | Description |
|---|---|---|
| `sensor_id` | bigint | |
| `location_id` | bigint | |
| `parameter` | varchar | e.g. `pm25` |
| `units` | varchar | |
| `date_utc` | date | Measurement day |
| `value` | float | Daily mean |
| `min_val` | float | Daily min |
| `max_val` | float | Daily max |
| `median_val` | float | Daily median (approx) |
| `coverage_pct` | float | API-reported coverage (% of expected obs present); NULL for S3-derived rows |
| `expected_count` | int | Count of source observations contributing to the daily aggregate |
| `year` | int | Partition aid |
| `month` | int | Partition aid |
| `loaded_at` | datetime2 | Ingest timestamp |
| `load_date` | date | Partition column |
| `source_system` | varchar | `openaq_api` or `s3_archive` |

### `dbo.openaq_measurements_hourly`
Hourly-aggregated OpenAQ measurements from the public S3 archive. Partitioned by `load_date`. Sourced from S3, not the API - see [docs/architecture.md](docs/architecture.md).

| Column | Type | Description |
|---|---|---|
| `sensor_id` | bigint | |
| `location_id` | bigint | |
| `parameter` | varchar | e.g. `pm25` |
| `units` | varchar | |
| `date_utc` | date | Measurement day |
| `hour_utc` | int | Hour (0-23, UTC) |
| `value` | float | Hourly value (effectively the single reading per cell for 99.9% of rows) |
| `reading_count` | int | Number of source observations contributing to this (sensor, day, hour); almost always 1 |
| `year` | int | Partition aid |
| `month` | int | Partition aid |
| `loaded_at` | datetime2 | Ingest timestamp |
| `load_date` | date | Partition column |
| `source_system` | varchar | `s3_archive` |

### `dbo.taxi_zone_shapes`
NYC TLC taxi zone polygons (CRS 4326), loaded from the official shapefile.

| Column | Type | Description |
|---|---|---|
| `zone_id` | int | TLC LocationID |
| `zone_name` | varchar | |
| `borough` | varchar | |
| `geometry_wkt` | varchar | Polygon as WKT |
| `loaded_at` | date | |

### `dbo.worldbank_gdp`
World Bank GDP (current USD) per country per year. Currently scoped to USA only.

| Column | Type | Description |
|---|---|---|
| `indicator_id` | varchar | `NY.GDP.MKTP.CD` |
| `indicator_name` | varchar | |
| `country_id` | varchar | 2-letter code |
| `country_name` | varchar | |
| `countryiso3code` | varchar | 3-letter code, used downstream |
| `year` | bigint | |
| `value` | bigint | GDP in current USD |
| `unit` | varchar | |
| `obs_status` | varchar | |
| `decimal` | bigint | |
| `loaded_at` | datetime2 | |

### Files (non-Delta bronze)

- `Files/yellow_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet` - TLC raw monthly parquet, landed by Copy Activity.
- `Files/reference/sensor_parameters.csv` - committed reference: pollutant catalogue used to seed `dm_air_measurement`(could be swapped in a pipeline to extracting distinct parameters, units, display_names from `openaq_measurements_daily`).
- `Files/reference/taxi_zone_lookup.csv` - TLC zone lookup, downloaded by notebook if missing.
- `Files/reference/taxi_zones/*.shp` - TLC shapefile, downloaded and extracted by notebook if missing.

## Silver (`lh_silver`)

### `dbo.taxi_trips`
Cleaned taxi trips. DQ filters applied: passenger_count ∈ [1,6], fare_amount ≥ 0, total_amount ≥ 0, trip_distance ∈ [0.01, 100], pickup < dropoff, trip duration ∈ [1min, 12h]. Partitioned by (year, month).

| Column | Type | Description |
|---|---|---|
| `vendorid` | int | Joins `dm_vendor.id` |
| `tpep_pickup_datetime` | datetime2 | |
| `tpep_dropoff_datetime` | datetime2 | |
| `passenger_count` | float | |
| `trip_distance` | float | Miles |
| `ratecodeid` | float | |
| `store_and_fwd_flag` | varchar | |
| `pulocationid` | int | Pickup zone, joins `dm_zone.zone_id` |
| `dolocationid` | int | Drop-off zone |
| `payment_type` | int | Joins `dm_payment.id` |
| `fare_amount` | float | |
| `extra` | float | |
| `mta_tax` | float | |
| `tip_amount` | float | |
| `tolls_amount` | float | |
| `improvement_surcharge` | float | |
| `total_amount` | float | |
| `congestion_surcharge` | float | |
| `airport_fee` | float | |
| `year` | int | Partition |
| `month` | int | Partition |
| `trip_duration_min` | float | Derived |
| `trip_date` | date | Pickup date |
| `hour_of_day` | int | Pickup hour, used for hourly fact |
| `day_of_week` | int | |
| `loaded_at` | datetime2 | loading timestamp |
| `source_system` | varchar | for future extensibility, e.g `TLC_yellow_taxi`|

### `dbo.openaq_measurements`
Cleaned daily air quality. MERGE on (sensor_id, measurement_date, parameter). Filters: value ≥ 0, coverage_pct ≥ 75%, date_utc not null.

| Column | Type | Description |
|---|---|---|
| `sensor_id` | bigint | |
| `location_id` | bigint | |
| `parameter` | varchar | |
| `units` | varchar | |
| `measurement_date` | date | |
| `value` | float | Daily mean |
| `min_val` | float | |
| `max_val` | float | |
| `median_val` | float | |
| `coverage_pct` | float | |
| `year` | int | |
| `month` | int | |
| `loaded_at` | datetime2 | |

### `dbo.openaq_measurements_hourly`
Cleaned hourly air quality. MERGE on (sensor_id, measurement_date, hour, parameter). No coverage filter (hourly cells have ~1 reading by definition).

| Column | Type | Description |
|---|---|---|
| `sensor_id` | bigint | |
| `location_id` | bigint | |
| `parameter` | varchar | |
| `units` | varchar | |
| `measurement_date` | date | |
| `hour_utc` | int | 0-23 (UTC) |
| `value` | float | |
| `reading_count` | int | |
| `year` | int | |
| `month` | int | |
| `loaded_at` | datetime2 | |

### `dbo.openaq_sensor_zones`
Spatial join of OpenAQ sensors to NYC TLC zones. Computed by geopandas spatial-within on `pl_seed`.

| Column | Type | Description |
|---|---|---|
| `sensor_id` | bigint | |
| `location_id` | bigint | |
| `p_name` | varchar | |
| `latitude` | float | |
| `longitude` | float | |
| `zone_id` | int | NULL if sensor falls outside any NYC TLC zone |
| `zone_name` | varchar | |
| `borough` | varchar | |
| `loaded_at` | datetime2 | |

### `dbo.fx_daily`
Cleaned ECB FX with date-spine forward-fill (weekends/holidays). MERGE on (rate_date, from_currency, to_currency).

| Column | Type | Description |
|---|---|---|
| `rate_date` | date | |
| `from_currency` | varchar | |
| `to_currency` | varchar | |
| `rate` | decimal(18,6) | |
| `obs_status` | varchar | |
| `frequency` | varchar | |
| `loaded_at` | datetime2 | |

### `dbo.gdp_yearly`
Cleaned World Bank GDP. MERGE on (country_code, indicator_id, year).

| Column | Type | Description |
|---|---|---|
| `country_code` | varchar | ISO3 |
| `country_name` | varchar | |
| `indicator_id` | varchar | |
| `indicator_name` | varchar | |
| `year` | bigint | |
| `gdp_usd` | decimal(20,2) | |
| `loaded_at` | datetime2 | |

### `stg.fct_*`
Pre-aggregated facts staged in silver before Copy to `wh_gold`. Schema matches the gold fact 1:1. Overwritten on each gold notebook run.

## Gold (`wh_gold`)

### Dimensions

| Table | Key | Columns | Source |
|---|---|---|---|
| `dm_date` | `date_key` (int yyyymmdd) | date, date_key, year, month, day, day_of_week, day_name, month_name, quarter, is_weekend, year_month | Seeded by `nb_gold_seed_dimensions` |
| `dm_zone` | `zone_id` (int) | zone_id, borough, zone_name, service_zone | From `Files/reference/taxi_zone_lookup.csv` |
| `dm_payment` | `id` (int) | id, payment_type | Seeded by `nb_gold_seed_dimensions` |
| `dm_vendor` | `id` (int) | id, vendor_name | Seeded by `nb_gold_seed_dimensions` |
| `dm_currency` | `id` (int) | id, currency_name | Seeded by `nb_gold_seed_dimensions` |
| `dm_air_measurement` | `p_id` (int) | p_id, p_name, p_units, p_display_name | From `Files/reference/sensor_parameters.csv` |

### Facts

| Table | Grain | FKs |
|---|---|---|
| `fct_taxi_daily` | date + zone + payment | date_key→dm_date, pickup_zone_id→dm_zone, payment_id→dm_payment |
| `fct_taxi_hourly` | date + hour + zone + payment | same + hour (0-23) |
| `fct_air_quality` | date + zone + parameter | date_key→dm_date, zone_id→dm_zone, p_id→dm_air_measurement |
| `fct_air_quality_hourly` | date + hour + zone + parameter | same + hour |
| `fct_fx_daily` | date + from + to | date_key→dm_date, id_cur_from/id_cur_to→dm_currency |
| `fct_gdp_yearly` | year + country | year (joins `dm_date.year`, many-to-many) |

Measures (full column list):

**`fct_taxi_daily` / `fct_taxi_hourly`**: trip_count, total_fare_usd, total_revenue_usd, total_distance_miles, avg_trip_duration_min, total_passengers

**`fct_air_quality` / `fct_air_quality_hourly`**: avg_value, sensor_count

**`fct_fx_daily`**: rate

**`fct_gdp_yearly`**: gdp_usd

Hourly facts share the dimensional model of their daily counterparts. The hour column (int 0-23) is intentionally not a dimension - a single-column 0-23 lookup doesn't justify a dm_hour table and would only add a join cost.

## Metadata (`wh_meta`)

### `meta.ingestion_control`
One row per (source, partition). Drives all bronze and partition-tracked silver runs.

| Column | Type | Description |
|---|---|---|
| `source_name` | varchar(255) | e.g. `TLC_yellow_taxi`, `openaq_nyc_daily`, `openaq_s3`, `openaq_s3_hourly` |
| `partition_key` | varchar(255) | `YYYY-MM` for monthly sources |
| `source_url` | varchar(1000) | Filename or full URL (NULL for API-driven where URL is built in code) |
| `status` | varchar(50) | Bronze: `pending` / `running` / `succeeded` / `failed` |
| `started_at` | datetime2 | Bronze run start |
| `ended_at` | datetime2 | Bronze run end |
| `bronze_rows_written` | bigint | |
| `error_message` | varchar(max) | |
| `silver_status` | varchar(50) | `pending` / `running` / `succeeded` / `failed` (taxi only) |
| `silver_started_at` | datetime2 | |
| `silver_ended_at` | datetime2 | |
| `silver_rows_written` | bigint | |
| `silver_error_message` | varchar(max) | |
| `created_at` | datetime2 | Seed timestamp |

### `meta.watermark`
One row per target table for watermark-driven incremental sources.

| Column | Type | Description |
|---|---|---|
| `target_table` | varchar(255) | e.g. `lh_silver.dbo.fx_daily` |
| `source_table` | varchar(255) | e.g. `lh_bronze.dbo.ecb_fx_daily` |
| `last_processed_ts` | datetime2 | Max `loaded_at` of source rows already processed |
| `last_run_at` | datetime2 | |
| `rows_merged` | bigint | |
| `pipeline_run_id` | varchar(255) | |

### `meta.pipeline_run_log`
One row per pipeline activation.

| Column | Type | Description |
|---|---|---|
| `run_id` | varchar(255) | `@pipeline().RunId` |
| `pipeline_name` | varchar(255) | |
| `layer` | varchar(10) | `bronze` / `silver` / `gold` / `master` / `seed` |
| `status` | varchar(10) | `running` / `succeeded` / `failed` |
| `started_at` | datetime2 | |
| `ended_at` | datetime2 | |
| `message` | varchar(max) | Free-form: row counts, errors, partial-failure summary |
| `logged_at` | datetime2 | |

### Stored procedures

| Proc | Purpose |
|---|---|
| `meta.get_pending` | Returns pending partitions (no state change). Used by taxi bronze. For easier individual processing in pipelines.|
| `meta.claim_pending` | Atomically flips `pending` → `running` and returns count. Used by openaq bronze and silvers. For partition processing inside a notebook. |
| `meta.update_partition_status` | Per-partition status update; can cascade `silver_status = 'pending'` on bronze success. |
| `meta.bulk_reset_running` | Recovery: flips all stuck-`running` rows for a source back to a target status. |
| `meta.get_watermark` | Returns `COALESCE(MAX(last_processed_ts), '1900-01-01')` for a target table. |
| `meta.log_watermark` | Upsert into `meta.watermark`. |
| `meta.log_pipeline_start` | Insert into `meta.pipeline_run_log` with status `running`. |
| `meta.log_pipeline_update` | Update an existing run row with final status + message. |