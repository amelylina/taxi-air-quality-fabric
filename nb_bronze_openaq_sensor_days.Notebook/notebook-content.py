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
# META     },
# META     "environment": {
# META       "environmentId": "465e8bc1-939a-9227-4dc4-5b5c6bda6737",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import json
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from wh_conn import get_con, check_con

SERVER_NAME = notebookutils.variableLibrary.getLibrary('storage_lib').server_url
WAREHOUSE_NAME = 'wh_meta'
SOURCE_NAME = notebookutils.variableLibrary.getLibrary('bronze_source_names').openaq
API_KEY = mssparkutils.credentials.getSecret(
    "https://fabric-project.vault.azure.net/",
    "openaq-key",
)
HEADERS = {"X-API-Key": API_KEY, "Accept": "application/json"}

MAX_WORKERS = 5
CHUNK_MONTHS = 1
RATE_LIMIT_PER_MINUTE = 50
PAGE_LIMIT = 1000

BASE = "https://api.openaq.org/v3"
PRIORITY_PARAMS = {"pm25", "no2", "o3", "co", "pm10"}

LOC_TABLE = "lh_bronze.dbo.openaq_locations"
TARGET_TABLE = "lh_bronze.dbo.openaq_measurements_daily"

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

def get_month_range(pk):
    start = date.fromisoformat(pk + "-01")
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start.isoformat(), end.isoformat()

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

def write_chunk(spark, rows, chunk_from, chunk_to):
    if not rows:
        return 0

    df = (
        spark.createDataFrame(rows, schema=ROW_SCHEMA)
        .withColumn("date_utc", F.to_date("date_utc"))
        .withColumn("year", F.year("date_utc"))
        .withColumn("month", F.month("date_utc"))
        .withColumn("loaded_at",  F.current_timestamp())
        .withColumn("load_date", F.to_date("loaded_at"))
    )

    (
    df.write
        .format("delta")
        .mode("append")
        .partitionBy("load_date")
        .saveAsTable(TARGET_TABLE)
    )

    return df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conn = get_con(SERVER_NAME, WAREHOUSE_NAME)
try:
    with conn.cursor() as cur:
        cur.execute ("""
            SELECT partition_key
            FROM meta.ingestion_control
            WHERE source_name = ?
            AND status = 'running'
            ORDER BY partition_key
        """, (SOURCE_NAME,))
        partition_keys = [row[0] for row in cur.fetchall()]

    if not partition_keys:
        mssparkutils.notebook.exit({"status": "succeeded", "partitions_processed": 0})
except Exception:
    conn.close()
    raise

all_sensors = (
    spark.read.table(LOC_TABLE)
    .filter(F.lower(F.col("p_name")).isin(PRIORITY_PARAMS))
    .select("sensor_id", "location_id", "p_name", "p_units",
            "first_datetime_utc", "last_datetime_utc")
    .collect()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results: list[dict] = []
for partition in partition_keys:
    chunk_from, chunk_to = get_month_range(partition)

    sensor_jobs = [
        s for s in all_sensors
        if s["last_datetime_utc"]  is not None and s["last_datetime_utc"]  >= datetime.fromisoformat(chunk_from)
        and s["first_datetime_utc"] is not None and s["first_datetime_utc"] <= datetime.fromisoformat(chunk_to)
    ]

    rows = []
    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(fetch_sensor_chunk, j, chunk_from, chunk_to, HEADERS): j
            for j in sensor_jobs
        }
        for f in as_completed(futures):
            job = futures[f]
            try:
                rows.extend(f.result())
            except Exception as e:
                failed.append((job["sensor_id"], type(e).__name__, str(e)[:200]))

    try:
        n_written = write_chunk(spark, rows, chunk_from, chunk_to)

        if sensor_jobs and not rows and failed:
            results.append({
                'source_name' : SOURCE_NAME,
                'partition_key' : partition,
                'layer' : 'bronze',
                'status': 'failed', 
                'rows_written': 0,
                'error_message': json.dumps({
                    "reason": "all sensors failed",
                    "failed_count": len(failed),
                    "failed_sample": failed[:5],
                })
            })
            continue

        err_summary = None
        if failed:
            err_summary = json.dumps({
                "failed_sensor_count": len(failed),
                "failed_sample": failed[:5],
            })

        results.append({
            'source_name' : SOURCE_NAME, 
            'partition_key': partition,
            'layer' : 'bronze',
            'status': "succeeded", 
            'rows_written' : n_written, 
            'error_message' : err_summary,
            'cascade_silver' : True
        })
    except Exception as e:
        results.append({
            'source_name' : SOURCE_NAME, 
            'partition_key': partition, 
            'layer' : 'bronze',
            'status': "failed", 
            'rows_written' : 0, 
            'error_message' : f"write_chunk error: {e}",
            'cascade_silver' : False
        })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not check_con(conn):
    conn = get_con(SERVER_NAME, WAREHOUSE_NAME)

try:
    with conn.cursor() as cur:
        cur.executemany(
            "EXEC meta.update_partition_status "
            "@source_name=%(source_name)s, @partition_key=%(partition_key)s, @layer=%(layer)s, @new_status=%(status)s, "
            "@rows_written=%(rows_written)s, @error_message=%(error_message)s, @cascade_silver_pending=%(cascade_silver)s",
            results
        )
    conn.commit()
finally:
    conn.close()

succeeded = [r for r in results if r.get('status') == "succeeded"]
failed_p = [r for r in results if r.get('status') == "failed"]
mssparkutils.notebook.exit(json.dumps({
    "status": "succeeded" if not failed_p else "partial_failure",
    "succeeded_count": len(succeeded),
    "failed_count": len(failed_p),
    "total_rows": sum(r.get('rows_written') or 0 for r in succeeded),
    "log_msg": f"OpenAQ bronze: {len(succeeded)} ok, {len(failed_p)} failed",
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
