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
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType
)
from wh_conn import get_con, check_con

SERVER_NAME = notebookutils.variableLibrary.getLibrary('storage_lib').server_url
SOURCE_NAME = notebookutils.variableLibrary.getLibrary('bronze_source_names').openaq_s3
WAREHOUSE_NAME = 'wh_meta'

SOURCE_SYSTEM = 's3_archive'
S3_BUCKET = 'openaq-data-archive'
S3_PREFIX = 'records/csv.gz'

LOC_TABLE = "lh_bronze.dbo.openaq_locations"
TARGET_TABLE = "lh_bronze.dbo.openaq_measurements_daily"

#i can later add list of needed params to source_url column in ingestion control meta table
TARGET_PARAMS = {"no2"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set(
    "fs.s3a.bucket.openaq-data-archive.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
)
hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_month_range(pk):
    start = date.fromisoformat(pk + "-01")
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start, end

def build_month_path(location_id: int, year: int, month: int) -> str:
    return (
f"s3a://{S3_BUCKET}/{S3_PREFIX}/"
f"locationid={location_id}/year={year}/month={month:02d}/"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RAW_CSV_SCHEMA = StructType([
    StructField("location_id",LongType(),True),
    StructField("sensors_id",LongType(),True),
    StructField("location",StringType(),True),
    StructField("datetime",StringType(),True),
    StructField("lat",DoubleType(),True),
    StructField("lon",DoubleType(),True),
    StructField("parameter",StringType(),True),
    StructField("units",StringType(),True),
    StructField("value",DoubleType(),True),
])

ROW_SCHEMA = StructType([
    StructField("sensor_id",LongType(),True),
    StructField("location_id",LongType(),True),
    StructField("parameter",StringType(),True),
    StructField("units",StringType(),True),
    StructField("date_utc", StringType(),True),
    StructField("value",DoubleType(),True),
    StructField("min_val",DoubleType(),True),
    StructField("max_val",DoubleType(),True),
    StructField("median_val",DoubleType(),True),
    StructField("coverage_pct",DoubleType(),True),
    StructField("expected_count", IntegerType(),True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_and_aggregate(paths: list[str], target_sensor_ids: set[int]):
    if not paths:
        return None
    raw = (
        spark.read
        .schema(RAW_CSV_SCHEMA)
        .option("header", "true")
        .option("pathGlobFilter", "*.csv.gz")
        .option("mode", "PERMISSIVE")
        .csv(paths)
    )
    filtered = (
        raw
        .filter(F.col("sensors_id").isin(list(target_sensor_ids)))
        .withColumn("ts_utc", F.to_timestamp("datetime"))
        .filter(F.col("ts_utc").isNotNull())
        .withColumn("date_utc", F.to_date("ts_utc"))
    )
    daily = (
        filtered.groupBy("sensors_id", "location_id", "parameter", "units", "date_utc")
        .agg(
            F.avg("value").alias("value"),
            F.min("value").alias("min_val"),
            F.max("value").alias("max_val"),
            F.expr("percentile_approx(value, 0.5)").cast("double").alias("median_val"),
            F.count("value").cast("int").alias("expected_count"),
        )
        .withColumnRenamed("sensors_id", "sensor_id")
        .withColumn("coverage_pct", F.lit(None).cast("double"))
        .select(
            "sensor_id", "location_id", "parameter", "units",
            F.col("date_utc").cast("string").alias("date_utc"),
            "value", "min_val", "max_val", "median_val",
            "coverage_pct", "expected_count",
        )
    )
    return daily


def write_partition(df, chunk_from: date, chunk_to: date) -> int:
    if df is None:
        return 0

    out = (
        df
        .withColumn("date_utc", F.to_date("date_utc"))
        .filter((F.col("date_utc") >= F.lit(chunk_from)) & (F.col("date_utc") <= F.lit(chunk_to)))
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("year", F.year("date_utc"))
        .withColumn("month", F.month("date_utc"))
        .withColumn("loaded_at", F.current_timestamp())
        .withColumn("load_date", F.to_date("loaded_at"))
    )

    out = out.cache()
    n = out.count()
    if n == 0:
        out.unpersist()
        return 0

    (
        out.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("load_date")
        .saveAsTable(TARGET_TABLE)
    )
    out.unpersist()
    return n

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conn = get_con(SERVER_NAME, WAREHOUSE_NAME)
try:
    with conn.cursor() as cur:
        cur.execute("""
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

print(partition_keys)

all_sensors = (
    spark.read.table(LOC_TABLE)
    .filter(F.lower(F.col("p_name")).isin(TARGET_PARAMS))
    .select("sensor_id", "location_id", "p_name", "p_units",
  "first_datetime_utc", "last_datetime_utc")
    .collect()
)

sensor_by_id = {s["sensor_id"]: s for s in all_sensors}
locations_to_sensors: dict[int, set[int]] = {}
for s in all_sensors:
    locations_to_sensors.setdefault(s["location_id"], set()).add(s["sensor_id"])

print(locations_to_sensors)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime as dt

results: list[dict] = []

for partition in partition_keys:
    chunk_from, chunk_to = get_month_range(partition)
    chunk_fromdt = dt.combine(chunk_from, dt.min.time())
    chunk_todt   = dt.combine(chunk_to,   dt.max.time())

    active_locations = set()
    for s in all_sensors:
        last_ok  = s["last_datetime_utc"]  is None or s["last_datetime_utc"]  >= chunk_fromdt
        first_ok = s["first_datetime_utc"] is None or s["first_datetime_utc"] <= chunk_todt
        if last_ok and first_ok:
            active_locations.add(s["location_id"])

    paths = [
        build_month_path(loc_id, chunk_from.year, chunk_from.month)
        for loc_id in active_locations
    ]

    target_sensor_ids = set()
    for loc_id in active_locations:
        target_sensor_ids |= locations_to_sensors.get(loc_id, set())

    try:
        if not paths or not target_sensor_ids:
            results.append({
            'source_name' : SOURCE_NAME,
            'partition_key': partition,
            'layer' : 'bronze',
            'status' : 'succeeded',
            'rows_written' : 0,
            'error_message': json.dumps({"note": "no active locations/sensors for this month"})
            })
            continue

        daily_df = read_and_aggregate(paths, target_sensor_ids)
        n_written = write_partition(daily_df, chunk_from, chunk_to)

        results.append({
        'source_name' : SOURCE_NAME,
        'partition_key': partition,
        'layer' : 'bronze',
        'status' : 'succeeded',
        'rows_written' : n_written,
        'error_message': None,
        })

    except Exception as e:
        results.append({
        'source_name'  : SOURCE_NAME,
        'partition_key': partition,
        'layer': 'bronze',
        'status' : 'failed',
        'rows_written' : 0,
        'error_message': f"{type(e).__name__}: {str(e)[:500]}"
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
        "@source_name=%(source_name)s, @partition_key=%(partition_key)s, @layer=%(layer)s,"
        "@new_status=%(status)s, @rows_written=%(rows_written)s, @error_message=%(error_message)s",
        results
        )
    conn.commit()
finally:
    conn.close()

succeeded = [r for r in results if r.get('status') == "succeeded"]
failed_p  = [r for r in results if r.get('status') == "failed"]

mssparkutils.notebook.exit(json.dumps({
    "status": "succeeded" if not failed_p else "partial_failure",
    "succeeded_count": len(succeeded),
    "failed_count": len(failed_p),
    "total_rows": sum(r.get('rows_written') or 0 for r in succeeded),
    "log_msg": f"OpenAQ S3 archive bronze: {len(succeeded)} ok, {len(failed_p)} failed",
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
