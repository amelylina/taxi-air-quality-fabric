# Fabric → InfluxDB → Grafana Time-Series Export

A backfill export that enriches gold-layer taxi data with historical weather, writes it to InfluxDB as time-series, and visualizes it in a Grafana dashboard.

## What it does

This demonstrates a time-series analytics path out of the project: it joins the hourly taxi aggregates with NYC weather for the same hours, pushes the combined series into InfluxDB, and surfaces it in Grafana. The dashboard shows relationships like dry hours vs. rainy hours, wind speed vs. trip count, revenue patterns, and similar weather-vs-demand comparisons.

## How it works

The Fabric notebook (in `integrations/`) runs the export, and Grafana reads from InfluxDB to render the dashboard:

1. **Read watermark** - looks up where the last export left off from a meta warehouse (`wh_meta`, via `meta.get_watermark`), so runs are incremental and resumable.
2. **Fetch weather** - pulls hourly NYC weather (temperature, feels-like, precipitation, wind, humidity, weather code) from the [Open-Meteo archive API](https://open-meteo.com/) for the current chunk, and categorizes each hour (clear, rain, snow, fog, etc.).
3. **Pull taxi data** - queries hourly trip counts and revenue from gold (`wh_gold.dbo.fct_taxi_hourly`) for the same window.
4. **Join & enrich** - joins weather and taxi on `(date_key, hour)` into a single enriched record set.
5. **Stage & write** - writes the batch to a staging table (`wh_gold.stg.influx_export_log`) for traceability, then writes the points to InfluxDB (`weather` bucket, `nyc_hourly` measurement).
6. **Log watermark** - records progress (`meta.log_watermark`) so the next run continues from where this one ended.

Then Grafana, pointed at the InfluxDB bucket, renders the dashboard.

## Incremental design

The notebook processes the full 2023–2024 source range in chunks (currently 7 days per run), driven by the watermark. Each run advances the watermark and exits cleanly; once all source data is exported, it short-circuits and reports nothing left to do. This makes it safe to run repeatedly and resume after interruptions.

## Scheduling

The notebook is **built to be scheduled** but currently runs manually, because the Fabric Trial capacity is too low to run it on a timer reliably. The watermark mechanism means manual runs work the same as scheduled ones - each picks up where the last stopped - so enabling a schedule later is just a matter of capacity, with no code change.

## The date-remapping caveat

The project's data spans 2023–2024, but the free InfluxDB tier only retains the **last 30 days**. To keep the data visible in Grafana, the notebook remaps the historical timestamps onto recent dates: each chunk keeps its internal shape and spacing but is shifted forward so it lands inside InfluxDB's retention window.

This is a deliberate workaround for the demo, not a modeling decision. The `source_ts_utc` (true historical time) and `dest_ts_utc` (shifted time) are both kept in the staging table, so the original timeline is never lost - only the InfluxDB copy is shifted. The point of the integration is to show the end-to-end pipeline and dashboards work; the remapping is what makes a 2023–2024 dataset usable on a 30-day-retention free tier.

## Secrets & connections

- InfluxDB URL, token, and org are read from Azure Key Vault at runtime; the bucket is `weather`.
- Warehouse access (meta and gold) uses the server URL from the Fabric variable library (`storage_lib`), via the `wh_conn` helper.

## The Grafana dashboard

The dashboard visualizes the enriched series - comparisons such as dry vs. rainy hours, wind speed vs. trip count, and revenue/demand against weather conditions.

> **[Published dashboard link](https://smallfondue1445.grafana.net/public-dashboards/431b543e6c414b34918cdd9d94338f32)**

I have exported dashboard as JSON, and it is available in `resources/` foler