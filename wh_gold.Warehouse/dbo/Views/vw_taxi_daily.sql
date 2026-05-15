-- Auto Generated (Do not modify) 67F6B0A549A478FBD13EAEBC004382BE11FD29FF4D063B592C56D9E4B3C0E730
CREATE VIEW dbo.vw_taxi_daily AS
SELECT
    date_key,
    pickup_zone_id,
    SUM(trip_count) AS trip_count,
    SUM(total_fare_usd) AS total_fare_usd,
    SUM(total_revenue_usd) AS total_revenue_usd,
    SUM(total_distance_miles) AS total_distance_miles,
    SUM(total_passengers) AS total_passengers,
    SUM(avg_trip_duration_min * trip_count) / NULLIF(SUM(trip_count), 0) AS avg_trip_duration_min,
    SUM(CASE WHEN payment_id = 1 THEN total_revenue_usd ELSE 0 END) AS revenue_credit_usd,
    SUM(CASE WHEN payment_id = 2 THEN total_revenue_usd ELSE 0 END) AS revenue_cash_usd
FROM dbo.fct_taxi_daily
GROUP BY date_key, pickup_zone_id;