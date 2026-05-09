# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c4c9cf64-1667-4152-bd26-12854382cfbe",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6",
# META       "known_lakehouses": [
# META         {
# META           "id": "c4c9cf64-1667-4152-bd26-12854382cfbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

DATE_FROM = "2023-01-01"
DATE_TO   = "2023-12-31"
MAX_WORKERS = 10
CHUNK_MONTHS = 4

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

API_KEY = mssparkutils.credentials.getSecret(
    "https://fabric-project.vault.azure.net/",
    "openaq-key"
)
HEADERS = {"X-API-Key": API_KEY, "Accept": "application/json"}
BASE = "https://api.openaq.org/v3"
PRIORITY_PARAMS = {"pm25", "no2", "o3", "co", "pm10"}
LOC_TABLE = "lh_bronze.dbo.openaq_locations"
TARGET_TABLE = "lh_bronze.dbo.openaq_measurements_daily"
PAGE_LIMIT = 1000

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def month_chunks(date_from: str, date_to: str, months: int = 1):
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    cur = start
    while cur <= end:
        nxt = min(cur + relativedelta(months=months) - relativedelta(days=1), end)
        yield cur.isoformat(), nxt.isoformat()
        cur = nxt + relativedelta(days=1)


def request_with_retry(url, params, max_attempts=5):
    for attempt in range(max_attempts):
        r = requests.get(url, headers=HEADERS, params=params, timeout=90)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (408, 429, 500, 502, 503, 504):
            retry_after = r.headers.get("Retry-After", "")
            try:
                wait = int(retry_after) + random.uniform(0, 1)
            except (ValueError, TypeError):
                wait = 2 ** attempt + random.uniform(0, 1)

            time.sleep(wait)
            continue
        r.raise_for_status()
    raise RuntimeError(f"Exhausted retries for {url} (last status {r.status_code})")


def get_paginated(url, params):
    out, page = [], 1
    while True:
        data = request_with_retry(url, {**params, "limit": PAGE_LIMIT, "page": page})
        results = data.get("results", [])
        out.extend(results)
        if len(results) < PAGE_LIMIT:
            break
        page += 1
    return out


def fetch_sensor_chunk(job, chunk_from, chunk_to):
    rows = get_paginated(
        f"{BASE}/sensors/{job['sensor_id']}/days",
        {"date_from": chunk_from, "date_to": chunk_to}
    )
    return [{
        "sensor_id": job["sensor_id"],
        "location_id": job["location_id"],
        "parameter": job["p_name"],
        "units": job["p_units"],
        "date_utc": r["period"]["datetimeFrom"]["utc"][:10],
        "value": r["value"],
        "min_val": (r.get("summary") or {}).get("min"),
        "max_val": (r.get("summary") or {}).get("max"),
        "median_val": (r.get("summary") or {}).get("median"),
        "coverage_pct": (r.get("coverage") or {}).get("percentComplete"),
        "expected_count": (r.get("coverage") or {}).get("expectedCount"),
    } for r in rows]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensor_jobs = (
    spark.read.table(LOC_TABLE)
    .filter(F.lower(F.col("p_name")).isin(PRIORITY_PARAMS))
    .filter(F.col("last_datetime_utc") >= DATE_FROM)
    .select("sensor_id", "location_id", "p_name", "p_units")
    .collect()
)
print(len(sensor_jobs))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for chunk_from, chunk_to in month_chunks(DATE_FROM, DATE_TO, CHUNK_MONTHS):
    rows = []
    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_sensor_chunk, j, chunk_from, chunk_to): j for j in sensor_jobs}
        for f in as_completed(futures):
            job = futures[f]
            try:
                rows.extend(f.result())
            except Exception as e:
                failed.append((job["sensor_id"], str(e)))

    if failed:
        print(f"  {len(failed)} sensors failed in this chunk; first 3: {failed[:3]}")

    if not rows:
        print("  no rows returned, skipping write")
        continue

    df = (
        spark.createDataFrame(rows)
        .withColumn("date",  F.to_date("date_utc"))
        .withColumn("year",  F.year("date"))
        .withColumn("month", F.month("date"))
    )

    (df.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("parameter", "year", "month")
        .saveAsTable(TARGET_TABLE))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
