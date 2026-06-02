# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "c61bc94c-683c-8492-4822-7239fb5fe524",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "bc3150e9-84db-a7dc-45b0-8091bba045ea",
# META       "known_warehouses": [
# META         {
# META           "id": "bc3150e9-84db-a7dc-45b0-8091bba045ea",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
import json
from datetime import datetime, timedelta, timezone
import requests
from pyspark.sql import functions as F
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from wh_conn import get_con, check_con

INFLUX_URL    = mssparkutils.credentials.getSecret(
    "https://fabric-project.vault.azure.net/", 
    "influx-url"
)
INFLUX_TOKEN  = mssparkutils.credentials.getSecret(
    "https://fabric-project.vault.azure.net/", 
    "influx-token"
    )
INFLUX_ORG    = mssparkutils.credentials.getSecret(
    "https://fabric-project.vault.azure.net/", 
    "influx-org"
    )
INFLUX_BUCKET = "weather"

SERVER_NAME = notebookutils.variableLibrary.getLibrary("storage_lib").server_url
META_WAREHOUSE = "wh_meta"
GOLD_WAREHOUSE = "wh_gold"

WATERMARK_TARGET = "influx_nyc_hourly"
WATERMARK_SOURCE = "wh_gold.dbo.fct_taxi_hourly"
STAGING_TABLE = "wh_gold.stg.influx_export_log"

NYC_LAT, NYC_LON = 40.7128, -74.0060

SOURCE_START = datetime(2023, 1, 1, tzinfo=timezone.utc)
SOURCE_END = datetime(2025, 1, 1, tzinfo=timezone.utc)  # exclusive

CHUNK_DAYS = 7

run_id = f"influx_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meta_conn = get_con(SERVER_NAME, META_WAREHOUSE)
try:
    with meta_conn.cursor() as cur:
        cur.execute("EXEC meta.get_watermark @target_table=?", (WATERMARK_TARGET,))
        wm_row = cur.fetchone()

    if wm_row and wm_row[0]:
        last_source_ts = wm_row[0]
        if last_source_ts.tzinfo is None:
            last_source_ts = last_source_ts.replace(tzinfo=timezone.utc)
        if last_source_ts < SOURCE_START:
            last_source_ts = SOURCE_START
    else:
        last_source_ts = SOURCE_START
except Exception as e:
    meta_conn.close()
    raise e

source_chunk_start = last_source_ts
source_chunk_end = min(source_chunk_start + timedelta(days=CHUNK_DAYS), SOURCE_END)

if source_chunk_start >= SOURCE_END:
    meta_conn.close()
    mssparkutils.notebook.exit(json.dumps({
        "status": "succeeded",
        "message": "All available source data has been exported.",
        "row_count": 0,
    }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

chunk_duration = source_chunk_end - source_chunk_start
chunk_index = (source_chunk_start - SOURCE_START) // chunk_duration

now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
dest_chunk_end = now_hour - (chunk_index * chunk_duration)
dest_chunk_start = dest_chunk_end - chunk_duration

def remap(ts: datetime) -> datetime:
    delta = ts - source_chunk_start
    return dest_chunk_start + delta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

weather_url = (
    "https://archive-api.open-meteo.com/v1/archive"
    f"?latitude={NYC_LAT}&longitude={NYC_LON}"
    f"&start_date={source_chunk_start.date().isoformat()}"
    f"&end_date={(source_chunk_end - timedelta(hours=1)).date().isoformat()}"
    "&hourly=temperature_2m,apparent_temperature,precipitation,"
    "wind_speed_10m,relative_humidity_2m,weather_code"
    "&timezone=UTC"
)
resp = requests.get(weather_url, timeout=60)
resp.raise_for_status()
wj = resp.json()

def categorize(code):
    if code is None: return "unknown"
    if code == 0: return "clear"
    if code in (1, 2, 3): return "partly_cloudy"
    if 40 <= code <= 49: return "fog"
    if 50 <= code <= 67: return "rain"
    if 70 <= code <= 77: return "snow"
    if 80 <= code <= 82: return "showers"
    if 95 <= code <= 99: return "thunderstorm"
    return "other"

weather_rows = []
times = wj["hourly"]["time"]
for i, t in enumerate(times):
    src_ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
    if src_ts < source_chunk_start or src_ts >= source_chunk_end:
        continue
    temp = wj["hourly"]["temperature_2m"][i]
    if temp is None:
        continue
    code = wj["hourly"]["weather_code"][i]
    precip = wj["hourly"]["precipitation"][i] or 0.0
    weather_rows.append({
        "src_ts_utc": src_ts,
        "date_key": int(src_ts.strftime("%Y%m%d")),
        "hour": src_ts.hour,
        "temp_c": float(temp),
        "feels_like_c": float(wj["hourly"]["apparent_temperature"][i] or temp),
        "precipitation_mm": float(precip),
        "wind_speed_kmh": float(wj["hourly"]["wind_speed_10m"][i] or 0.0),
        "humidity": float(wj["hourly"]["relative_humidity_2m"][i] or 0.0),
        "weather_code": int(code) if code is not None else None,
        "condition": categorize(code),
        "is_rainy": precip > 0.1,
    })

if not weather_rows:
    meta_conn.close()
    mssparkutils.notebook.exit(json.dumps({
        "status": "succeeded",
        "message": "No weather data for chunk",
        "row_count": 0,
    }))

weather_df = spark.createDataFrame(weather_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dk_start = int(source_chunk_start.strftime("%Y%m%d"))
dk_end   = int(source_chunk_end.strftime("%Y%m%d"))

gold_conn = get_con(SERVER_NAME, GOLD_WAREHOUSE)

try:
    with gold_conn.cursor() as cur:
        cur.execute("""
            SELECT date_key, hour,
                SUM(trip_count) AS trip_count,
                SUM(total_revenue_usd) AS total_revenue_usd
            FROM wh_gold.dbo.fct_taxi_hourly
            WHERE date_key >= ? AND date_key < ?
            GROUP BY date_key, hour
        """, (dk_start,dk_end))
        columns = [c[0] for c in cur.description]
        rows = [tuple(r) for r in cur.fetchall()]
        taxi_df = spark.createDataFrame(rows, schema=columns)
finally:
    gold_conn.close()

joined = weather_df.join(taxi_df,["date_key","hour"],"left").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

records=[]
for _, r in joined.iterrows():
    src_ts = r["src_ts_utc"]
    if hasattr(src_ts, "to_pydatetime"):
        src_ts = src_ts.to_pydatetime()
    if src_ts.tzinfo is None:
        src_ts = src_ts.replace(tzinfo=timezone.utc)
    dest_ts = remap(src_ts)
    records.append({
        "run_id": run_id,
        "source_ts_utc": src_ts,
        "dest_ts_utc": dest_ts,
        "date_key": int(r["date_key"]),
        "hour": int(r["hour"]),
        "temp_c": float(r["temp_c"]),
        "feels_like_c": float(r["feels_like_c"]),
        "precipitation_mm": float(r["precipitation_mm"]),
        "wind_speed_kmh": float(r["wind_speed_kmh"]),
        "humidity": float(r["humidity"]),
        "is_rainy": bool(r["is_rainy"]),
        "condition": r["condition"],
        "weather_code": int(r["weather_code"]) if r["weather_code"] is not None else None,
        "trip_count": int(r["trip_count"]) if r["trip_count"] is not None else 0,
        "revenue_usd": float(r["total_revenue_usd"]) if r["total_revenue_usd"] is not None else 0.0,
        "written_at_utc": datetime.now(timezone.utc),
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stg_df = spark.createDataFrame(records)
stg_df.write.mode("overwrite").synapsesql(STAGING_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

points = []
for r in records:
    p = (
        Point("nyc_hourly")
        .tag("location", "nyc")
        .tag("condition", r["condition"])
        .field("temp_c", r["temp_c"])
        .field("feels_like_c", r["feels_like_c"])
        .field("precipitation_mm", r["precipitation_mm"])
        .field("wind_speed_kmh", r["wind_speed_kmh"])
        .field("humidity", r["humidity"])
        .field("is_rainy", 1 if r["is_rainy"] else 0)
        .field("trip_count", r["trip_count"])
        .field("revenue_usd", r["revenue_usd"])
        .time(r["dest_ts_utc"])
    )
    points.append(p)
write_api.write(bucket=INFLUX_BUCKET, record=points)
client.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not check_con(meta_conn):
    meta_conn = get_con(SERVER_NAME, META_WAREHOUSE)

try:
    with meta_conn.cursor() as cur:
        cur.execute(
            "EXEC meta.log_watermark @target_table=?, @source_table=?, @ts=?, "
            "@rows_merged=?, @run_id=?",
            (WATERMARK_TARGET, WATERMARK_SOURCE, source_chunk_end,
             len(points), run_id)
        )
    meta_conn.commit()
finally:
    meta_conn.close()

mssparkutils.notebook.exit(json.dumps({
    "status": "succeeded",
    "run_id": run_id,
    "source_chunk_start": source_chunk_start.isoformat(),
    "source_chunk_end": source_chunk_end.isoformat(),
    "dest_chunk_start": dest_chunk_start.isoformat(),
    "dest_chunk_end": dest_chunk_end.isoformat(),
    "row_count": len(points),
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
