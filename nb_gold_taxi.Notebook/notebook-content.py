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
# META     "environment": {}
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS stg;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

last_processed_ts="1990-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_watermark = spark.sql("SELECT MAX(ingested_at) FROM lh_silver.dbo.taxi_trips").collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
import json
from pyspark.sql import functions as F, DataFrame
from datetime import datetime

SILVER_TABLE = "lh_silver.dbo.taxi_trips"
STG_TABLE = "lh_silver.stg.fct_taxi_daily"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver = spark.read.table(SILVER_TABLE)

gold = (silver.filter((F.col("ingested_at")>last_processed_ts) & (F.col("ingested_At")<=new_watermark))
    .groupBy("trip_date", "pulocationid").agg(
        F.count("*").alias("trip_count"),
        F.sum("fare_amount").alias("total_fare_usd"),
        F.sum("total_amount").alias("total_revenue_usd"),
        F.sum("trip_distance").alias("total_distance_miles"),
        F.avg("trip_duration_min").alias("avg_trip_duration_min"),
        F.sum("passenger_count").alias("total_passengers"),
    ).withColumnRenamed("pulocationid", "pickup_zone_id")
)
gold = (gold.withColumn("total_passengers", F.col("total_passengers").cast("int"))
    .withColumn("date_key", F.date_format("trip_date", "yyyyMMdd").cast("int"))
    .drop('trip_date')
)

(gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(STG_TABLE))

mssparkutils.notebook.exit(new_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
