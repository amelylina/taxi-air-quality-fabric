# Semantic Model

The semantic model is the analytical interface to `wh_gold`. It defines relationships, measures, and formatting so that Power BI (and any other BI tool) can produce interactive reports without report authors writing SQL.

## Why a semantic model (rather than just views)

A warehouse view returns a fixed result set. A semantic model defines:

- **Relationships** between facts and dimensions, applied lazily at query time
- **Measures** (DAX) that recompute under filters and slicers, supporting interactive drill-down
- **Formatting, hierarchies, perspectives, RLS** - none of which live in the warehouse

Pre-joining everything into a single flat view defeats this. The model consumes the **star schema directly** and lets relationships do the join work.

## Tables included

Facts: `fct_taxi_daily`, `fct_taxi_hourly`, `fct_air_quality`, `fct_air_quality_hourly`, `fct_fx_daily`, `fct_gdp_yearly`

Dimensions: `dm_date`, `dm_zone`, `dm_payment`, `dm_vendor`, `dm_currency`, `dm_air_measurement`

## Relationships

| From | To | Cardinality | Cross-filter | Status |
|---|---|---|---|---|
| `fct_taxi_daily[date_key]` | `dm_date[date_key]` | Many-to-One | Single | Active |
| `fct_taxi_daily[pickup_zone_id]` | `dm_zone[zone_id]` | Many-to-One | Single | Active |
| `fct_taxi_daily[payment_id]` | `dm_payment[id]` | Many-to-One | Single | Active |
| `fct_taxi_hourly[date_key]` | `dm_date[date_key]` | Many-to-One | Single | Active |
| `fct_taxi_hourly[pickup_zone_id]` | `dm_zone[zone_id]` | Many-to-One | Single | Active |
| `fct_taxi_hourly[payment_id]` | `dm_payment[id]` | Many-to-One | Single | Active |
| `fct_air_quality[date_key]` | `dm_date[date_key]` | Many-to-One | Single | Active |
| `fct_air_quality[zone_id]` | `dm_zone[zone_id]` | Many-to-One | Single | Active |
| `fct_air_quality[p_id]` | `dm_air_measurement[p_id]` | Many-to-One | Single | Active |
| `fct_air_quality_hourly[date_key]` | `dm_date[date_key]` | Many-to-One | Single | Active |
| `fct_air_quality_hourly[zone_id]` | `dm_zone[zone_id]` | Many-to-One | Single | Active |
| `fct_air_quality_hourly[p_id]` | `dm_air_measurement[p_id]` | Many-to-One | Single | Active |
| `fct_fx_daily[date_key]` | `dm_date[date_key]` | Many-to-One | Single | Active |
| `fct_gdp_yearly[year]` | `dm_date[year]` | Many-to-Many | Single | Active |
| `fct_fx_daily[id_cur_from]` | `dm_currency[id]` | Many-to-One | Single | Inactive; USD→EUR rate is derived via DAX LOOKUPVALUE to avoid role-playing dim ambiguity (id_cur_to is not related as a second active path). |

## Measures

```dax
Trips = SUM(fct_taxi_daily[trip_count])

Trips Hourly = SUM(fct_taxi_hourly[trip_count])

Total Revenue USD = SUM(fct_taxi_daily[total_revenue_usd])

Total Fare USD = SUM(fct_taxi_daily[total_fare_usd])

Total Distance (miles) = SUM(fct_taxi_daily[total_distance_miles])

Total Passengers = SUM(fct_taxi_daily[total_passengers])

Avg Pollution = AVERAGE(fct_air_quality[avg_value])

Avg Pollution Hourly = AVERAGE(fct_air_quality_hourly[avg_value])

Avg Revenue per Trip USD = DIVIDE([Total Revenue USD], [Trips])

Avg Trip Duration (min) = 
DIVIDE(
    SUMX(fct_taxi_daily, fct_taxi_daily[avg_trip_duration_min] * fct_taxi_daily[trip_count]),
    [Trips]
)

USD to EUR Rate = 
VAR USD_id = LOOKUPVALUE(dm_currency[id], dm_currency[currency_name], "USD")
VAR EUR_id = LOOKUPVALUE(dm_currency[id], dm_currency[currency_name], "EUR")
RETURN
    CALCULATE(
        AVERAGE(fct_fx_daily[rate]),
        fct_fx_daily[id_cur_from] = USD_id,
        fct_fx_daily[id_cur_to] = EUR_id
    )

Total Revenue EUR = DIVIDE([Total Revenue USD], [USD to EUR Rate])
```

## Report pages

| Page | Question Answered | Visuals |
|---|---|---|
| Overview | General values | KPI cards (Total Trips, Total Revenue USD, Zones with sensors), Matrix "Sensor Coverage by Zone and Parameter" |
| Traffic vs Air Quality | What's the volume of trips and air pollution by zone and time? | Line chart Trips by date, scatter chart of days by trip count and avg pollution |
| Diurnal patterns | How do pollutants and taxi trips change depending on day of week and time of day? | Trips heatmap by hour/zone, Pollution heatmap by hour/zone, Line chart of Trips and Avg pollution by hour |
| Revenue in USD vs EUR | How did revenue change depending on daily FX exchane rates? | KPI cards (Avg Rev per Trip EUR, Avg Rev per Trip USD), Line graph of monthly revenue in USD vs EUR, line graph of USD to EUR exchange rate |
| Long-term Mobility/Economy/Environment | How do the three correlate | 3 Line graphs for : Trips by year, Avg Pollution by year and US GDP by year |