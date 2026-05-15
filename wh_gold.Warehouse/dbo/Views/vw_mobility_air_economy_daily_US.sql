-- Auto Generated (Do not modify) 81FBE6264B72F64BFA8F27013CBF17B0FB4A75B29A9467B11FE11B9FF80C6B7D
CREATE VIEW dbo.vw_mobility_air_economy_daily_US AS
WITH usd_to_eur AS (
    SELECT
        fx.date_key,
        fx.rate
    FROM dbo.fct_fx_daily fx
    LEFT JOIN dbo.dm_currency c_from
        ON fx.id_cur_from = c_from.id
    LEFT JOIN dbo.dm_currency c_to
        ON fx.id_cur_to = c_to.id
    WHERE c_from.currency_name = 'USD'
      AND c_to.currency_name = 'EUR'
)
SELECT
    d.date,
    z.zone_name,
    z.borough,
    t.trip_count,
    t.total_revenue_usd,
    t.total_revenue_usd / ue.rate AS total_revenue_eur,
    a.pm25_avg,
    a.no2_avg,
    g.gdp_usd
FROM dbo.vw_taxi_daily t
LEFT JOIN dbo.dm_zone z ON t.pickup_zone_id = z.zone_id
LEFT JOIN dbo.vw_air_quality_daily a ON a.date_key = t.date_key AND a.zone_id = t.pickup_zone_id
LEFT JOIN usd_to_eur ue ON ue.date_key = t.date_key
LEFT JOIN dbo.dm_date d ON t.date_key = d.date_key
LEFT JOIN dbo.fct_gdp_yearly g ON g.year = d.year AND g.country_code = 'USA';