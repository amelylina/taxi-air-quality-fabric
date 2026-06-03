# Fabric → OneDrive → Power Automate Export

A manual data export that pushes a gold-layer summary to OneDrive, where a Power Automate flow picks it up, emails a preview, and sends a mobile notification.

## What it does

This gives the project a lightweight way to get a snapshot of gold-layer data out of Fabric and into your inbox. You run the notebook, and shortly after you get an email with the export summary and the first few rows, plus a push notification on your phone.

It's a notify-and-preview tool: run it when you want a quick, shareable look at the latest taxi daily aggregates.

## How it works

There are two pieces:

1. **The notebook** - a Fabric PySpark notebook (in `integrations/`), triggered manually. It queries the gold warehouse (`wh_gold.dbo.fct_taxi_daily`), wraps the rows in a JSON payload with some metadata, and writes that file to OneDrive via the Microsoft Graph API.

2. **The Power Automate flow** - watches a OneDrive folder, and when a new export file lands it parses the contents and fans out an email + a mobile notification.

So the flow is: you run the notebook → it drops a JSON file in OneDrive (`/FabricExport/`) → Power Automate detects the new file → it parses, previews, and notifies. The notebook produces; Power Automate delivers.

## The notebook

- Reads from `wh_gold.dbo.fct_taxi_daily` (currently the top 50 December 2024 rows, ordered by date and pickup zone).
- Builds a JSON payload containing a UTC timestamp, the source table, a human-readable period description, the row count, and the rows themselves.
- Uploads it to OneDrive as `FabricExport/taxi_report_<YYYYMMDD_HHMM>.json` using a Graph API `PUT`, authenticated with a token pulled from Azure Key Vault at runtime.
- The warehouse connection details (server URL) come from a Fabric variable library; the helper lives in `wh_conn`.

## The Power Automate flow

Rough sketch of the steps:

```
When a file is created (OneDrive, /FabricExport)
  → Get file content
  → Parse JSON
  → Compose (take first 3 rows)
  → Create HTML table
  → Send an email (Gmail)
  → Send a mobile notification (Power Automate app)
```

The flow reads the metadata fields from the payload (`source`, `period_description`, `row_count`) for the email body and renders the first 3 rows as an HTML table preview.

### Example email

```
New data export received from Fabric.
Source: wh_gold.dbo.fct_taxi_daily
Period: December 2024, top 50 (date, zone) rows
Row count: 50
First 3 rows:
date_key    pickup_zone_id   trip_count   total_revenue_usd
20241201    1                1            1.01
20241201    1                3            376.98
20241201    3                1            34
```

## Setup notes

- The notebook needs a OneDrive upload token in Key Vault (`onedrive-upload-refresh-token`) and warehouse access via the variable library. Due to this being tested on Free Trial Fabric Capacity, I was able to connect only personal OneDrive, but doing this with OneDrive for Business would be much easier.
- The Power Automate flow is owned in the Power Automate portal (not in this repo) and is keyed to the `/FabricExport` OneDrive folder - the folder path must match what the notebook writes to.
