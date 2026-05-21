CREATE TABLE [dbo].[fct_air_quality_hourly] (

	[zone_id] int NULL, 
	[hour] int NULL, 
	[avg_value] float NULL, 
	[sensor_count] bigint NULL, 
	[date_key] int NULL, 
	[p_id] int NULL
);