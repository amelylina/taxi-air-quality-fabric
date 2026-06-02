CREATE TABLE [stg].[influx_export_log] (

	[condition] varchar(max) NULL, 
	[date_key] bigint NULL, 
	[dest_ts_utc] datetime2(6) NULL, 
	[feels_like_c] float NULL, 
	[hour] bigint NULL, 
	[humidity] float NULL, 
	[is_rainy] bit NULL, 
	[precipitation_mm] float NULL, 
	[revenue_usd] float NULL, 
	[run_id] varchar(max) NULL, 
	[source_ts_utc] datetime2(6) NULL, 
	[temp_c] float NULL, 
	[trip_count] bigint NULL, 
	[weather_code] bigint NULL, 
	[wind_speed_kmh] float NULL, 
	[written_at_utc] datetime2(6) NULL
);