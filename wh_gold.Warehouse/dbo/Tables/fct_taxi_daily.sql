CREATE TABLE [dbo].[fct_taxi_daily] (

	[pickup_zone_id] int NULL, 
	[payment_id] int NULL, 
	[trip_count] bigint NULL, 
	[total_fare_usd] float NULL, 
	[total_revenue_usd] float NULL, 
	[total_distance_miles] float NULL, 
	[avg_trip_duration_min] float NULL, 
	[total_passengers] int NULL, 
	[date_key] int NULL
);