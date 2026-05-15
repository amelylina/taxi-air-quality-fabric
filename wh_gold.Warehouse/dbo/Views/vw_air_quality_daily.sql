-- Auto Generated (Do not modify) 150B49BFCA89C031E598600341DBCD722FA505435CC36014B37706309E8A18C0
CREATE VIEW dbo.vw_air_quality_daily AS
SELECT
    f.date_key,
    f.zone_id,
    MAX(CASE WHEN d.p_name = 'pm25' THEN avg_value END) AS pm25_avg,
    MAX(CASE WHEN d.p_name = 'no2'  THEN avg_value END) AS no2_avg,
    MAX(CASE WHEN d.p_name = 'o3'   THEN avg_value END) AS o3_avg,
    MAX(CASE WHEN d.p_name = 'pm10' THEN avg_value END) AS pm10_avg,
    MAX(CASE WHEN d.p_name = 'pm25' THEN sensor_count END) AS pm25_samples,
    MAX(CASE WHEN d.p_name = 'no2'  THEN sensor_count END) AS no2_samples
FROM dbo.fct_air_quality f
LEFT JOIN dbo.dm_air_measurement d ON f.p_id=d.p_id
GROUP BY f.date_key, f.zone_id;