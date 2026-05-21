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
# META     }
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

last_processed_ts = "1990-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_watermark = spark.sql(
    "SELECT MAX(loaded_at) FROM lh_silver.dbo.openaq_measurements_hourly"
).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from pyspark.sql import functions as F

STG_TABLE = "lh_silver.stg.fct_air_quality_hourly"
PARAM_TABLE = "wh_gold.dbo.dm_air_measurement"

silver_meas = spark.read.table("lh_silver.dbo.openaq_measurements_hourly")
silver_zones = spark.read.table("lh_silver.dbo.openaq_sensor_zones")
measurements = spark.read.synapsesql(PARAM_TABLE)

new_data = (silver_meas
    .filter(F.col("loaded_at") > last_processed_ts)
    .filter(F.col("loaded_at") <= new_watermark)
    .join(silver_zones.select("sensor_id", "zone_id"), "sensor_id", "left")
    .filter(F.col("zone_id").isNotNull())
    .filter(F.lower(F.col("parameter")).isin("pm25","no2","o3","co","pm10"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold = (
    new_data
    .groupBy("zone_id", "measurement_date", "hour_utc", "parameter")
    .agg(
        F.avg("value").alias("avg_value"),
        F.count("*").alias("sensor_count"),
    )
    .withColumn("date_key", F.date_format("measurement_date", "yyyyMMdd").cast("int"))
    .withColumnRenamed("hour_utc", "hour")
    .withColumnRenamed("parameter", "p_name")
    .drop("measurement_date")
    .join(measurements.select("p_id", "p_name"), "p_name", "left")
    .drop("p_name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(gold.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(STG_TABLE))

mssparkutils.notebook.exit(new_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
