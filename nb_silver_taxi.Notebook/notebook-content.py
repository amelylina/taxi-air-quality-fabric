# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "41c4f02d-1dba-4787-a05c-4fb6306e3f15",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6",
# META       "known_lakehouses": [
# META         {
# META           "id": "41c4f02d-1dba-4787-a05c-4fb6306e3f15"
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
from pyspark.sql import functions as F, DataFrame
from datetime import datetime
from functools import reduce
import pandas as pd
from wh_conn import get_con, check_con

BRONZE_PATH = notebookutils.variableLibrary.getLibrary('storage_lib').bronze_path
SERVER_NAME = notebookutils.variableLibrary.getLibrary('storage_lib').server_url
SOURCE_NAME = notebookutils.variableLibrary.getLibrary('bronze_source_names').yellow_taxi

BRONZE_BASE = BRONZE_PATH + "/Files/yellow_taxi"
SILVER_TABLE = "taxi_trips"
META_TABLE = "meta.ingestion_control"
META_WAREHOUSE = "wh_meta"

TARGET_TYPES = {
    "vendorid": "integer",
    "passenger_count": "double",
    "trip_distance": "double",
    "ratecodeid": "double",
    "store_and_fwd_flag": "string",
    "pulocationid": "integer",
    "dolocationid": "integer",
    "payment_type": "integer",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}

conn = get_con(SERVER_NAME, META_WAREHOUSE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    pdf = pd.read_sql("""
        SELECT partition_key
        FROM meta.ingestion_control
        WHERE source_name = ?
        AND status = 'succeeded'
        AND silver_status = 'running'
        ORDER BY partition_key
    """, conn, params=[SOURCE_NAME])

    pending_df = spark.createDataFrame(pdf) if not pdf.empty else None
    partition_keys = pdf["partition_key"].tolist()

    if not partition_keys:
        mssparkutils.notebook.exit({"status": "succeeded", "partitions_processed": 0})
except Exception:
    conn.close()
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_partitions(df):
    return (df
        .withColumn("file_path", F.input_file_name())
        .withColumn("year",  F.regexp_extract("file_path", r"year=(\d{4})", 1).cast("int"))
        .withColumn("month", F.regexp_extract("file_path", r"month=(\d{1,2})", 1).cast("int"))
        .drop("file_path"))

def normalize(df: DataFrame) -> DataFrame:
    df = df.toDF(*[c.lower() for c in df.columns])
    for col_name, target_type in TARGET_TYPES.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df

def path_for_partition(pk: str) -> str:
    y, m = pk.split("-")
    return f"{BRONZE_BASE}/year={y}/month={m}/yellow_tripdata_{pk}.parquet"

normalized_dfs = []
failed_partitions = []
for pk in partition_keys:
    try:
        path = path_for_partition(pk)
        raw  = spark.read.parquet(path)
        normalized_dfs.append(add_partitions(normalize(raw)))
    except Exception as e:
        failed_partitions.append((pk, str(e)))

succeeded_partitions = [pk for pk in partition_keys
                        if pk not in {p for p, _ in failed_partitions}]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if normalized_dfs:
    df_bronze = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        normalized_dfs
    )

    df_clean = (
        df_bronze
        .filter(F.col("pulocationid").isNotNull())
        .filter(F.col("dolocationid").isNotNull())
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("passenger_count").between(1, 6))
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("total_amount") >= 0)
        .filter(F.col("trip_distance").between(0.01, 100))
        .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )
    df_clean = df_clean.withColumn(
        "trip_duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    )

    df_clean = df_clean.filter(F.col("trip_duration_min").between(1, 12 * 60))

    df_silver = (
        df_clean
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_system", F.lit(SOURCE_NAME))
    )

    silver_row_counts = (df_silver.groupBy("year", "month").count().collect())
    rows_by_partition = {f"{r.year:04d}-{r.month:02d}": r['count']
                         for r in silver_row_counts}

    new_data = []
    for pk, cnt in rows_by_partition.items():
        new_data.append({"partition_key":pk,"silver_status":"succeeded","silver_rows_written":cnt,"silver_error_message":None})

    for pk, err in failed_partitions:
        new_data.append({"partition_key":pk,"silver_status":"failed","silver_rows_written":None,"silver_error_message":err})

else:
    rows_by_partition = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exit_payload = {
    "status": "succeeded" if not failed_partitions else "partial_failure",
    "succeeded_count": len(rows_by_partition),
    "failed_count": len(failed_partitions),
    "total_rows": sum(rows_by_partition.values()),
    "log_msg": f"Loaded: {len(rows_by_partition)} Failed: {len(failed_partitions)}",
    "failed_partitions": [pk for pk, _ in failed_partitions],
}

try:
    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year", "month")
        .saveAsTable(SILVER_TABLE))

    if not check_con(conn): 
        conn = get_con(SERVER_NAME, META_WAREHOUSE)

    with conn.cursor() as cur:
        cur.executemany(
            """
            UPDATE meta.ingestion_control 
            SET silver_status=%(silver_status)s, silver_rows_written=%(silver_rows_written)s, silver_error_message=%(silver_error_message)s
            WHERE partition_key=%(partition_key)s
            """,
            new_data
        )
    conn.commit()
    mssparkutils.notebook.exit(json.dumps(exit_payload))
except Exception as e:
    conn.rollback()
    raise
finally:
    conn.close()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
