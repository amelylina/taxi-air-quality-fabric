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
DATE_TO   = "2023-06-30"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import requests
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

MAX_WORKERS = 5
CHUNK_MONTHS = 1
RATE_LIMIT_PER_MINUTE = 50
PAGE_LIMIT = 1000

BASE = "https://api.openaq.org/v3"
PRIORITY_PARAMS = {"pm25", "no2", "o3", "co", "pm10"}

LOC_TABLE = "lh_bronze.dbo.openaq_locations"
TARGET_TABLE = "lh_bronze.dbo.openaq_measurements_daily"
LOG_TABLE  = "lh_bronze.dbo.openaq_load_log"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_headers():
    api_key = mssparkutils.credentials.getSecret(
        "https://fabric-project.vault.azure.net/",
        "openaq-key",
    )
    return {"X-API-Key": api_key, "Accept": "application/json"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class RateLimiter:
    def __init__(self, max_calls: int, period_sec: float = 60.0):
        self.max_calls = max_calls
        self.period = period_sec
        self.calls = deque()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                while self.calls and now - self.calls[0] >= self.period:
                    self.calls.popleft()
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                sleep_for = self.period - (now - self.calls[0])
            time.sleep(max(sleep_for, 0.05))
 
 
_RATE_LIMITER = RateLimiter(RATE_LIMIT_PER_MINUTE)

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


def request_with_retry(url, params, headers, max_attempts=8):
    last_status = None
    last_exc = None
    for attempt in range(max_attempts):
        _RATE_LIMITER.acquire()
        try:
            r = requests.get(url, headers=headers, params=params, timeout=90)
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            wait = min(2 ** attempt, 60)
            time.sleep(random.uniform(0, wait))
            continue
 
        last_status = r.status_code
        if r.status_code == 200:
            return r.json()
 
        if r.status_code in (408, 429, 500, 502, 503, 504):
            retry_after = r.headers.get("Retry-After", "")
            try:
                base_wait = int(retry_after)
            except (ValueError, TypeError):
                base_wait = min(2 ** attempt, 60)
            time.sleep(random.uniform(0, base_wait if base_wait > 0 else 1))
            continue

        r.raise_for_status()
 
    raise RuntimeError(
        f"Exhausted retries for {url} "
        f"(last status {last_status}, last exc {last_exc})"
    )

def get_paginated(url, params, headers):
    out, page = [], 1
    while True:
        data = request_with_retry(
            url, {**params, "limit": PAGE_LIMIT, "page": page}, headers
        )
        results = data.get("results", [])
        out.extend(results)
        if len(results) < PAGE_LIMIT:
            break
        page += 1
    return out


def fetch_sensor_chunk(job, chunk_from, chunk_to, headers):
    rows = get_paginated(
        f"{BASE}/sensors/{job['sensor_id']}/days",
        {"date_from": chunk_from, "date_to": chunk_to},
        headers,
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

ROW_SCHEMA = StructType([
StructField("sensor_id", LongType(), True),
StructField("location_id", LongType(), True),
StructField("parameter", StringType(), True),
StructField("units", StringType(), True),
StructField("date_utc", StringType(), True),
StructField("value", DoubleType(), True),
StructField("min_val", DoubleType(), True),
StructField("max_val", DoubleType(), True),
StructField("median_val", DoubleType(), True),
StructField("coverage_pct", DoubleType(), True),
StructField("expected_count", IntegerType(), True),
])

def load_sensor_jobs(spark):
    return (
        spark.read.table(LOC_TABLE)
        .filter(F.lower(F.col("p_name")).isin(PRIORITY_PARAMS))
        .filter(F.col("last_datetime_utc") >= F.lit(DATE_FROM).cast("timestamp"))
        .select("sensor_id", "location_id", "p_name", "p_units")
        .collect()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_chunk(spark, rows, chunk_from, chunk_to):
    if not rows:
        return 0

    df = (
        spark.createDataFrame(rows, schema=ROW_SCHEMA)
        .withColumn("date_utc", F.to_date("date_utc"))
        .withColumn("year", F.year("date_utc"))
        .withColumn("month", F.month("date_utc"))
        .withColumn("loaded_at",  F.current_timestamp())
    )

    (
    df.write
        .format("delta")
        .mode("append")
        .partitionBy("loaded_at")
        .saveAsTable(TARGET_TABLE)
    )

    return df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_chunk_result(spark, chunk_from, chunk_to, n_ok, n_failed, failed_sample):
    log_df = spark.createDataFrame(
        [(chunk_from, chunk_to, n_ok, n_failed, str(failed_sample[:5]))],
        "chunk_from string, chunk_to string, sensors_ok int, sensors_failed int, failed_sample string",
    ).withColumn("logged_at", F.current_timestamp())
    log_df.write.format("delta").mode("append").saveAsTable(LOG_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

headers = get_headers()
sensor_jobs = load_sensor_jobs(spark)
print(f"sensors to load: {len(sensor_jobs)}")

for chunk_from, chunk_to in month_chunks(DATE_FROM, DATE_TO, CHUNK_MONTHS):
    print(f"chunk {chunk_from} -> {chunk_to}")
    rows = []
    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(fetch_sensor_chunk, j, chunk_from, chunk_to, headers): j
            for j in sensor_jobs
        }
        for f in as_completed(futures):
            job = futures[f]
            try:
                rows.extend(f.result())
            except Exception as e:
                failed.append((job["sensor_id"], type(e).__name__, str(e)[:200]))

    n_ok = len(sensor_jobs) - len(failed)
    if failed:
        print(f"  {len(failed)} sensors failed in this chunk")

    n_written = write_chunk(spark, rows, chunk_from, chunk_to)
    print(f"  merged {n_written} rows into {TARGET_TABLE}")
    log_chunk_result(spark, chunk_from, chunk_to, n_ok, len(failed), failed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
